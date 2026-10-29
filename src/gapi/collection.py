"""Single-writer finite collection loop with per-source failure isolation.

One process, one connection stream at a time, bounded work per source and no
scheduled retries. The existing persisted queue/feed owns all continuation state.
"""
import time

from .global_sources import ingest_orcid_api, ingest_wikidata
from .dump_discovery import discover_dump
from .openalex_discovery import discover_openalex
from .task import BudgetStop


def collect(store,http,feed,*,max_seconds=900,batch_members=50,progress=None):
    if max_seconds<=0 or not 1<=batch_members<=200:
        raise ValueError('invalid collection time/batch budget')
    deadline=time.monotonic()+max_seconds
    suspended=set()
    dump_attempted=False
    openalex_attempted=False
    stages=[]
    while time.monotonic()<deadline:
        store.task.check();store.task.check_memory()
        if not store.pending('ingest-orcid-api',1) and not openalex_attempted:
            openalex_attempted=True
            try:
                result=discover_openalex(store,http,pages=1,progress=progress)
                if progress:progress({'stage':'openalex-discovery',**result})
            except BudgetStop:
                raise
            except (OSError,ValueError,RuntimeError) as exc:
                store.failure('openalex-discovery','collection-loop',str(exc))
                if progress:progress({'stage':'openalex-discovery','failed':str(exc)[:256]})
        progressed=False
        for command,adapter in (('ingest-orcid-api',ingest_orcid_api),('ingest-wikidata',ingest_wikidata)):
            if command in suspended or time.monotonic()>=deadline:continue
            ids=store.pending(command,batch_members)
            if not ids:continue
            before=store.db.execute('SELECT count(*) FROM members').fetchone()[0]
            try:
                adapter(store,http,ids,max_seconds=min(60,deadline-time.monotonic()),progress=None)
            except BudgetStop:
                raise
            except (OSError,ValueError,RuntimeError) as exc:
                store.failure(command,'collection-loop',str(exc))
                suspended.add(command)
            after=store.db.execute('SELECT count(*) FROM members').fetchone()[0]
            completed=after-before
            if not completed:suspended.add(command)
            progressed|=completed>0
            item={'stage':command,'new_members':completed,'events':store.db.execute('SELECT count(*) FROM events').fetchone()[0],
                'storage_bytes':store.task.used(),'suspended_this_run':command in suspended}
            stages.append(item)
            if progress:progress(item)
        if progressed:continue
        if not dump_attempted and time.monotonic()<deadline:
            dump_attempted=True
            previous=store.db.execute('SELECT compressed_bytes FROM discovery_feeds WHERE id=?',(feed['id'],)).fetchone()
            # Increase the observed prefix instead of replaying a fixed first slice forever.
            ceiling=min(feed['size_bytes'],max(32*1024**2,(previous[0] if previous else 0)*2))
            try:
                result=discover_dump(store,http,feed,max_seconds=min(300,deadline-time.monotonic()),
                    max_bytes=ceiling,progress=progress)
                if progress:progress({'stage':'dump-discovery',**result})
            except BudgetStop:
                raise
            except (OSError,ValueError,RuntimeError) as exc:
                store.failure('wikidata-dump','discovery',str(exc))
                if progress:progress({'stage':'dump-discovery','failed':str(exc)[:256]})
            continue
        break
    return {'status':'resumable-time-boundary' if time.monotonic()>=deadline else 'resumable-source-boundary',
        'suspended_sources':sorted(suspended),'stages':stages[-10:],'report':store.report(),
        'global_complete':False}
