"""OpenAlex supplies ORCID candidate identifiers, never career facts or corroboration.

Uses only unauthenticated public access: no paid account, key or billing interface.
The service's reported request cost is retained as metadata, not a payment action.
"""
import re
from urllib.parse import urlencode

from .career_rules import valid_orcid
from .task import digest
from .source_http import SourcePause

FEED='openalex-orcid-candidates/v1'
POLICY={'endpoint':'https://api.openalex.org/authors','filter':'has_orcid:true',
    'select':'id,orcid','sort':'id:asc','purpose':'candidate-discovery-only-no-affiliation-inference'}


def discover_openalex(store,http,*,pages=1,per_page=200,progress=None):
    if not 1<=pages<=50 or not 1<=per_page<=200:
        raise ValueError('OpenAlex page/batch limit exceeded')
    if not store.source_ready('openalex-discovery'):
        return {'feed':FEED,'status':'server-backoff-pending','pages':0}
    previous=store.db.execute('SELECT cursor_after,row_count FROM discovery_pages WHERE source=? ORDER BY rowid DESC LIMIT 1',(FEED,)).fetchone()
    cursor=previous[0] if previous else '*'
    if previous and not cursor:
        return {'feed':FEED,'status':'live-selection-exhausted-not-global-complete','pages':0}
    committed=queued=0
    for _ in range(pages):
        store.task.check();store.task.check_memory()
        params={k:POLICY[k] for k in ('filter','select','sort')}
        params.update(per_page=per_page,cursor=cursor)
        url=POLICY['endpoint']+'?'+urlencode(params)
        try:data,response_hash=http.json(url)
        except SourcePause as exc:
            store.pause_source('openalex-discovery',exc.retry_at,str(exc))
            raise
        if not isinstance(data,dict):raise ValueError('OpenAlex discovery response must be an object')
        rows=data.get('results')
        if not isinstance(rows,list) or len(rows)>per_page or 'error' in data:
            raise ValueError('invalid OpenAlex discovery response')
        meta=data.get('meta')
        if not isinstance(meta,dict):raise ValueError('invalid OpenAlex discovery metadata')
        following=meta.get('next_cursor') or ''
        if not isinstance(following,str) or len(following)>2048 or following==cursor:
            raise ValueError('non-advancing OpenAlex cursor')
        if following and not rows:
            raise ValueError('empty OpenAlex page cannot advance a cursor')
        ids=set()
        for row in rows:
            if not isinstance(row,dict):raise ValueError('invalid OpenAlex discovery row')
            author=row.get('id','')
            orcid=row.get('orcid') or ''
            if not isinstance(author,str) or not re.fullmatch(r'https://openalex.org/A[0-9]+',author):
                raise ValueError('invalid OpenAlex author identifier')
            if isinstance(orcid,str) and orcid.startswith('https://orcid.org/'):
                value=orcid.removeprefix('https://orcid.org/')
                if valid_orcid(value):ids.add(value)
        store.queue_page(FEED,'ingest-orcid-api',cursor,following,digest(params),response_hash,ids,len(rows))
        committed+=1;queued+=len(ids);cursor=following
        if progress:progress({'feed':FEED,'page_committed':committed,'candidate_ids':len(ids),
            'reported_author_count':data.get('meta',{}).get('count'),'requested_fields':['id','orcid']})
        if not cursor:break
    return {'feed':FEED,'pages':committed,'candidate_ids':queued,'status':'live-selection-incomplete',
        'canonical_events_written':0,'paid_api_key_used':False}
