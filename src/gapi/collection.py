"""Single-writer finite collection loop with per-source failure isolation.

One process, one connection stream at a time, bounded work per source and no
scheduled retries. The existing persisted queue/feed owns all continuation state.
"""
import time
import math
from collections import deque
from contextlib import nullcontext

from .global_sources import ingest_orcid_api, ingest_wikidata, enqueue_discovery
from .dump_discovery import discover_dump
from .openalex_discovery import discover_openalex
from .crossref_discovery import discover_crossref
from .task import BudgetStop
from .source_http import SourcePause, StageDeadline


def collect(store,http,feed=None,*,max_seconds=900,batch_members=50,progress=None,discovery=True):
    if not math.isfinite(max_seconds) or max_seconds<=0 or not 1<=batch_members<=200:
        raise ValueError('invalid collection time/batch budget')
    if not discovery and feed is not None:
        raise ValueError('queued-only acquisition cannot enable dump discovery')
    deadline=time.monotonic()+max_seconds
    suspended=set()
    dump_attempted=False
    discovery_suspended=set()
    stages=deque(maxlen=10)
    discovery_pages=0
    def window(seconds):
        return http.within(seconds) if hasattr(http,'within') else nullcontext()
    while time.monotonic()<deadline:
        store.task.check();store.task.check_memory()
        progressed=False
        for command,feed_name in (('ingest-orcid-api','openalex-discovery'),
                                  ('ingest-orcid-api','crossref-discovery'),
                                  ('ingest-wikidata','wikidata-discovery')):
            if not discovery:continue
            if command in suspended or feed_name in discovery_suspended or time.monotonic()>=deadline or not store.source_ready(feed_name):
                continue
            if store.pending(command,batch_members):
                continue
            try:
                before=store.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0]
                with window(min(60,deadline-time.monotonic())):
                    if feed_name=='openalex-discovery':
                        result=discover_openalex(store,http,pages=1,progress=progress)
                    elif feed_name=='crossref-discovery':
                        result=discover_crossref(store,http,pages=1,progress=progress)
                    else:
                        result=enqueue_discovery(store,http,command,limit=200)
                advanced=store.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0]-before
                discovery_pages+=advanced
                progressed|=advanced>0
                if not advanced:discovery_suspended.add(feed_name)
                if progress:progress({'stage':feed_name,**result})
            except BudgetStop:
                raise
            except StageDeadline:
                discovery_suspended.add(feed_name)
                if progress:progress({'stage':feed_name,'status':'resumable-stage-time-boundary'})
            except (OSError,ValueError,RuntimeError) as exc:
                if isinstance(exc,SourcePause):store.pause_source(feed_name,exc.retry_at,str(exc))
                discovery_suspended.add(feed_name)
                store.failure(feed_name,'collection-loop',str(exc))
                if progress:progress({'stage':feed_name,'failed':str(exc)[:256]})
        for command,adapter in (('ingest-orcid-api',ingest_orcid_api),('ingest-wikidata',ingest_wikidata)):
            if command in suspended or time.monotonic()>=deadline:continue
            source='orcid-public-api' if command=='ingest-orcid-api' else 'wikidata'
            if not store.source_ready(source):
                suspended.add(command)
                continue
            ids=store.pending(command,batch_members)
            if not ids:continue
            before=store.db.execute('SELECT count(*) FROM members').fetchone()[0]
            retried_before=store.db.execute('SELECT COALESCE(sum(attempts),0) FROM discovery_retries').fetchone()[0]
            try:
                seconds=min(60,deadline-time.monotonic())
                with window(seconds):
                    options={'batch_size':10} if command=='ingest-wikidata' else {}
                    adapter(store,http,ids,max_seconds=seconds,progress=None,**options)
            except BudgetStop:
                raise
            except (OSError,ValueError,RuntimeError) as exc:
                store.failure(command,'collection-loop',str(exc))
                suspended.add(command)
            after=store.db.execute('SELECT count(*) FROM members').fetchone()[0]
            completed=after-before
            retried=store.db.execute('SELECT COALESCE(sum(attempts),0) FROM discovery_retries').fetchone()[0]-retried_before
            state=store.db.execute('SELECT status FROM runs WHERE source=?',(source,)).fetchone()
            if (state and state[0]=='access_failed') or (not completed and not retried):
                suspended.add(command)
            progressed|=completed>0 or retried>0
            item={'stage':command,'new_members':completed,'events':store.db.execute('SELECT count(*) FROM events').fetchone()[0],
                'storage_bytes':store.task.used(),'deferred_this_batch':retried,'suspended_this_run':command in suspended}
            stages.append(item)
            if progress:progress(item)
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
    return {'status':'resumable-time-boundary' if time.monotonic()>=deadline else 'resumable-source-boundary',
        'suspended_sources':sorted(suspended),'suspended_discovery':sorted(discovery_suspended),
        'discovery_pages_committed':discovery_pages,'stages':list(stages),'report':store.report(),
        'global_complete':False,'archive_discovery_enabled':feed is not None,'discovery_enabled':discovery}
