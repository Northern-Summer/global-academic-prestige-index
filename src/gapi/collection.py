"""Single-writer finite collection loop with per-source failure isolation.

One process, one connection stream at a time, bounded work per source and no
scheduled retries. The existing persisted queue/feed owns all continuation state.
"""
import time
import math
import json
import sqlite3
from collections import deque
from contextlib import nullcontext

from .global_sources import ingest_orcid_api, ingest_wikidata, enqueue_discovery
from .dump_discovery import discover_dump
from .openalex_discovery import discover_openalex
from .crossref_discovery import discover_crossref
from .task import BudgetStop
from .source_http import SourcePause, StageDeadline, WIKIDATA_BATCH_LIMIT

DISCOVERY_WINDOW_SECONDS = 60
CANDIDATE_PROGRESS_INTERVAL_SECONDS = 900


def collect(store,http,feed=None,*,max_seconds=900,batch_members=50,progress=None,discovery=True,candidate_selection=None,orcid_record=False,full_report=True,reuse_annual=False):
    if type(full_report) is not bool:
        raise ValueError('full_report must be boolean')
    if type(orcid_record) is not bool:
        raise ValueError('orcid_record must be boolean')
    if type(reuse_annual) is not bool or (reuse_annual and candidate_selection is None):
        raise ValueError('annual reuse requires an explicit frozen candidate selection')
    if not math.isfinite(max_seconds) or max_seconds<=0 or not 1<=batch_members<=200:
        raise ValueError('invalid collection time/batch budget')
    if not discovery and feed is not None:
        raise ValueError('queued-only acquisition cannot enable dump discovery')
    selection_options={}
    if candidate_selection is not None:
        if discovery or feed is not None:
            raise ValueError('frozen candidate collection requires queued-only acquisition')
        from .candidate_selection import processing
        selection=processing(store,candidate_selection)
        selection_options={'max_candidate_rowid':selection['max_candidate_rowid']}
    deadline=time.monotonic()+max_seconds
    next_candidate_progress=time.monotonic()+CANDIDATE_PROGRESS_INTERVAL_SECONDS
    if candidate_selection is not None and progress:
        progress({'stage':'frozen-candidate-processing','phase':'start','processing':selection})
    suspended=set()
    dump_attempted=False
    discovery_suspended=set()
    discovery_retry_after={}
    wikidata_scope='academic'
    stages=deque(maxlen=10)
    discovery_pages=0
    storage_contended=False
    # Equal batch counts give slow transports most of the wall-clock budget.
    # Charge queue selection and acquisition to their source, then prefer the
    # least-served ready source. One bounded batch may overrun the fair share;
    # paused, empty and failed sources never prevent another source progressing.
    service_seconds={'ingest-orcid-api':0.0,'ingest-wikidata':0.0}
    stage_stalls={'ingest-orcid-api':0,'ingest-wikidata':0}
    source_retry_after={}
    def window(seconds):
        return http.within(seconds) if hasattr(http,'within') else nullcontext()
    while time.monotonic()<deadline:
        store.task.check();store.task.check_memory()
        progressed=False
        for command,feed_name in (('ingest-orcid-api','openalex-discovery'),
                                  ('ingest-orcid-api','crossref-discovery'),
                                  ('ingest-wikidata','wikidata-discovery')):
            if not discovery:continue
            if feed_name in discovery_retry_after and time.monotonic()>=discovery_retry_after[feed_name]:
                discovery_suspended.discard(feed_name)
                del discovery_retry_after[feed_name]
            if command in suspended or feed_name in discovery_suspended or time.monotonic()>=deadline or not store.source_ready(feed_name):
                continue
            if store.pending(command,batch_members):
                continue
            try:
                before=store.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0]
                with window(min(DISCOVERY_WINDOW_SECONDS,deadline-time.monotonic())):
                    if feed_name=='openalex-discovery':
                        result=discover_openalex(store,http,pages=1,progress=progress)
                    elif feed_name=='crossref-discovery':
                        result=discover_crossref(store,http,pages=1,progress=progress)
                    else:
                        options={} if wikidata_scope=='academic' else {'scope':wikidata_scope}
                        result=enqueue_discovery(store,http,command,limit=200,**options)
                        if result.get('status')=='live-query-exhausted-not-global-complete' and wikidata_scope=='academic':
                            # Separate persisted selection/cursor, same deduped
                            # candidate queue. Exhausting an occupation query
                            # must not strand the existing explicit-degree feed.
                            result=enqueue_discovery(store,http,command,limit=200,
                                                     scope='degree-employer')
                            wikidata_scope='degree-employer'
                        result={'scope':wikidata_scope,**result}
                advanced=store.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0]-before
                discovery_pages+=advanced
                progressed|=advanced>0
                if not advanced:discovery_suspended.add(feed_name)
                if progress:progress({'stage':feed_name,**result})
            except BudgetStop:
                raise
            except StageDeadline:
                discovery_suspended.add(feed_name)
                # No sleep or idle loop: only independent productive work can
                # keep this invocation alive until a full window has elapsed.
                discovery_retry_after[feed_name]=time.monotonic()+DISCOVERY_WINDOW_SECONDS
                if feed_name=='wikidata-discovery':
                    # Independent persisted selections share the same source
                    # backoff and dedup queue. A slow query must not require
                    # exhausting its whole universe before trying the other.
                    wikidata_scope=('degree-employer' if wikidata_scope=='academic' else 'academic')
                if progress:progress({'stage':feed_name,'status':'resumable-stage-time-boundary'})
            except SourcePause as exc:
                # A server deadline is already persisted and checked above.
                # Re-admit after independent work outlasts it; suspending this
                # feed for the invocation would ignore an expired Retry-After.
                store.pause_source(feed_name,exc.retry_at,str(exc))
                store.failure(feed_name,'collection-loop',str(exc))
                if progress:progress({'stage':feed_name,'status':'server-backoff-pending',
                                      'retry_at':exc.retry_at,'failed':str(exc)[:256]})
            except (OSError,ValueError,RuntimeError) as exc:
                discovery_suspended.add(feed_name)
                store.failure(feed_name,'collection-loop',str(exc))
                if progress:progress({'stage':feed_name,'failed':str(exc)[:256]})
        adapters=(('ingest-orcid-api',ingest_orcid_api),('ingest-wikidata',ingest_wikidata))
        for command,adapter in sorted(adapters,key=lambda entry:service_seconds[entry[0]]):
            if command in suspended or time.monotonic()>=deadline:continue
            retry_at=source_retry_after.get(command)
            if retry_at is not None:
                if time.monotonic()<retry_at:continue
                del source_retry_after[command]
            source='orcid-public-api' if command=='ingest-orcid-api' else 'wikidata'
            if not store.source_ready(source):
                continue
            batch_started=time.monotonic()
            route_options={'reuse_annual':True} if reuse_annual and command=='ingest-orcid-api' else {}
            ids=store.pending(command,batch_members,**route_options,
                priority_source='wikidata' if command=='ingest-orcid-api' else None,**selection_options)
            if not ids:continue
            if time.monotonic()>=deadline:break
            # The bounded mode uses existing source-leading indexes rather
            # than recounting unrelated annual archives for every API batch.
            count_where='' if full_report else ' WHERE source=?'
            count_parameters=() if full_report else (source,)
            events_before=store.db.execute('SELECT count(*) FROM events'+count_where,count_parameters).fetchone()[0]
            before=store.db.execute('SELECT count(*) FROM members'+count_where,count_parameters).fetchone()[0]
            retried_before=store.db.execute('SELECT COALESCE(sum(attempts),0) FROM discovery_retries').fetchone()[0]
            stage_exhausted=False
            try:
                seconds=min(60,deadline-time.monotonic())
                with window(seconds):
                    options={'batch_size':WIKIDATA_BATCH_LIMIT} if command=='ingest-wikidata' else {}
                    if command=='ingest-orcid-api' and orcid_record:
                        options['whole_record']=True
                    adapter(store,http,ids,max_seconds=seconds,progress=None,report=False,**options)
            except BudgetStop:
                raise
            except StageDeadline:
                stage_exhausted=True
            except sqlite3.OperationalError as exc:
                # Member/request contexts roll back their unfinished writes.
                # Busy/locked storage is neither bad source data nor a reason
                # to retry the same network request in a tight loop.
                if getattr(exc,'sqlite_errorcode',0)&255 not in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED):
                    raise
                storage_contended=True
            except (OSError,ValueError,RuntimeError) as exc:
                store.failure(command,'collection-loop',str(exc))
                suspended.add(command)
            after=store.db.execute('SELECT count(*) FROM members'+count_where,count_parameters).fetchone()[0]
            completed=after-before
            retried=store.db.execute('SELECT COALESCE(sum(attempts),0) FROM discovery_retries').fetchone()[0]-retried_before
            state=store.db.execute('SELECT status,detail FROM runs WHERE source=?',(source,)).fetchone()
            try:detail=json.loads(state[1]) if state and state[1] else {}
            except (ValueError,TypeError):detail={}
            stage_exhausted|=isinstance(detail,dict) and detail.get('stage_time_exhausted') is True
            # A persisted maxlag/429 pause expires independently. Keep collecting
            # other sources and re-admit this one after its deadline; do not turn
            # a one-minute service pause into an invocation-long suspension.
            if not storage_contended and not stage_exhausted and store.source_ready(source) and ((state and state[0]=='access_failed') or (not completed and not retried)):
                suspended.add(command)
            progressed|=completed>0 or retried>0
            bounded_backoff=False
            if stage_exhausted and completed==0 and retried==0:
                stage_stalls[command]+=1
                if stage_stalls[command]>=2:
                    # A repeatedly exhausted, non-productive source should not
                    # consume the other source's fair-share window. Keep its
                    # queue intact and re-admit it after a bounded cooldown.
                    source_retry_after[command]=time.monotonic()+300
                    stage_stalls[command]=0
                    bounded_backoff=True
                    if progress:progress({'stage':command,'status':'bounded-no-progress-backoff',
                                          'retry_seconds':300})
            elif completed>0 or retried>0:
                stage_stalls[command]=0
            elapsed=time.monotonic()-batch_started
            service_seconds[command]+=elapsed
            events_after=store.db.execute('SELECT count(*) FROM events'+count_where,count_parameters).fetchone()[0]
            item={'stage':command,'new_members':completed,'events':events_after,
                'new_events':events_after-events_before,'elapsed_seconds':round(elapsed,3),
                'members_per_second':round(completed/elapsed,4) if elapsed>0 else None,
                'storage_bytes':store.task.used(),'deferred_this_batch':retried,
                'storage_contended':storage_contended,
                'priority_deferred':getattr(store,'last_priority_deferred',False),
                'stage_time_exhausted':stage_exhausted,'suspended_this_run':command in suspended,
                'bounded_no_progress_backoff':bounded_backoff}
            stages.append(item)
            if not full_report:
                item['events_scope']='adapter-source-only'
                item['source']=source
            if progress:progress(item)
            if storage_contended:break
            if (candidate_selection is not None and progress
                    and next_candidate_progress<=time.monotonic()<deadline):
                # The sole writer has settled this batch. Reuse the full owner
                # calculation instead of copying a live database or inferring
                # candidate completion from member/event deltas.
                progress({'stage':'frozen-candidate-processing','phase':'interval',
                          'processing':processing(store,candidate_selection)})
                next_candidate_progress=time.monotonic()+CANDIDATE_PROGRESS_INTERVAL_SECONDS
            # Reconsider service allocation after productive work, rather than
            # forcing exactly one slow and one fast batch into every round.
            if completed>0 or retried>0:break
        if storage_contended:break
        if progressed:continue
        if feed is not None and not dump_attempted and time.monotonic()<deadline:
            dump_attempted=True
            previous=store.db.execute('SELECT compressed_bytes FROM discovery_feeds WHERE id=?',(feed['id'],)).fetchone()
            # Increase the observed prefix instead of replaying a fixed first slice forever.
            ceiling=min(feed['size_bytes'],max(32*1024**2,(previous[0] if previous else 0)*2))
            try:
                seconds=min(300,deadline-time.monotonic())
                with window(seconds):
                    result=discover_dump(store,http,feed,max_seconds=seconds,max_bytes=ceiling,progress=progress)
                if progress:progress({'stage':'dump-discovery',**result})
            except BudgetStop:
                raise
            except (OSError,ValueError,RuntimeError) as exc:
                store.failure('wikidata-dump','discovery',str(exc))
                if progress:progress({'stage':'dump-discovery','failed':str(exc)[:256]})
            continue
        break
    try:
        settlement_report = store.report() if full_report else store.bounded_report()
    except MemoryError:
        # Preserve the source settlement without allowing optional aggregate
        # metrics to turn committed collection work into a failed run.
        settlement_report = store.bounded_report()
        settlement_report['report_unavailable'] = 'MemoryError'
    result={'status':'resumable-storage-contention' if storage_contended else
            'resumable-time-boundary' if time.monotonic()>=deadline else 'resumable-source-boundary',
        'suspended_sources':sorted(suspended),'suspended_discovery':sorted(discovery_suspended),
        'discovery_pages_committed':discovery_pages,'stages':list(stages),'report':settlement_report,
        'scheduling_policy':'least-source-service-time/1',
        'source_service_seconds':{key:round(value,3) for key,value in service_seconds.items()},
        'global_complete':False,'archive_discovery_enabled':feed is not None,'discovery_enabled':discovery}
    if candidate_selection is not None:
        result['frozen_candidate_processing']=processing(store,candidate_selection)
    if orcid_record:result['orcid_api_route']='record'
    if reuse_annual:result['candidate_disposition_policy']='verified-annual-reuse-and-retained-error-documents/1'
    return result
