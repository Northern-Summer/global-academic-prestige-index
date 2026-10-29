"""Free public Crossref metadata supplies candidate ORCIDs, never career facts.

Publisher-deposited author identifiers are discovery leads. Neither a paper's
affiliations nor an authenticated-ORCID flag proves a degree or an employment.
No author names, paper metadata, API keys or raw responses are persisted.
"""
import re
from urllib.parse import urlencode

from .career_rules import valid_orcid
from .task import digest
from .source_http import SourcePause

# v1's ascending-index pages remain in the database as predecessor evidence.
# Some old indexed hits have no ORCID in their current author payloads. The new
# selection starts a separate cursor; it never rewrites that observed history.
FEED='crossref-orcid-candidates/v2'
POLICY={'endpoint':'https://api.crossref.org/works','filter':'has-orcid:true',
        'select':'DOI,author','sort':'indexed','order':'desc',
        'purpose':'candidate-discovery-only-no-affiliation-inference'}


def discover_crossref(store,http,*,pages=1,per_page=50,progress=None):
    if not 1<=pages<=50 or not 1<=per_page<=100:
        raise ValueError('Crossref page/batch limit exceeded')
    if not store.source_ready('crossref-discovery'):
        return {'feed':FEED,'status':'server-backoff-pending','pages':0}
    previous=store.db.execute('SELECT cursor_after FROM discovery_pages WHERE source=? ORDER BY rowid DESC LIMIT 1',(FEED,)).fetchone()
    cursor=previous[0] if previous else '*'
    if previous and not cursor:
        return {'feed':FEED,'status':'live-selection-exhausted-not-global-complete','pages':0}
    committed=queued=0
    for _ in range(pages):
        store.task.check();store.task.check_memory()
        params={key:POLICY[key] for key in ('filter','select','sort','order')}
        params.update(rows=per_page,cursor=cursor)
        try:data,response_hash=http.json(POLICY['endpoint']+'?'+urlencode(params))
        except SourcePause as exc:
            store.pause_source('crossref-discovery',exc.retry_at,str(exc))
            raise
        if not isinstance(data,dict):raise ValueError('Crossref discovery response must be an object')
        message=data.get('message')
        if data.get('status')!='ok' or not isinstance(message,dict):
            raise ValueError('invalid Crossref discovery envelope')
        rows=message.get('items')
        if not isinstance(rows,list) or len(rows)>per_page:
            raise ValueError('invalid Crossref discovery rows')
        following=message.get('next-cursor') or ''
        if not isinstance(following,str) or len(following)>16384:
            raise ValueError('invalid Crossref discovery cursor')
        # Crossref may return a next-cursor even after the last partial page.
        if len(rows)<per_page:following=''
        elif not following or following==cursor:
            raise ValueError('non-advancing Crossref cursor')
        ids=set()
        for row in rows:
            if not isinstance(row,dict) or not isinstance(row.get('DOI'),str) or not re.fullmatch(r'10\.[0-9]{4,9}/[^\s]+',row['DOI'],re.ASCII):
                raise ValueError('invalid Crossref work identifier')
            authors=row.get('author') or []
            if not isinstance(authors,list):raise ValueError('invalid Crossref authors')
            for author in authors:
                value=author.get('ORCID') if isinstance(author,dict) else None
                if not isinstance(value,str):continue
                if value.startswith(('https://orcid.org/','http://orcid.org/')):
                    value=value.rsplit('/',1)[-1]
                    if valid_orcid(value):ids.add(value)
        store.queue_page(FEED,'ingest-orcid-api',cursor,following,digest(params),response_hash,ids,len(rows))
        committed+=1;queued+=len(ids);cursor=following
        if progress:progress({'feed':FEED,'page_committed':committed,'candidate_ids':len(ids),
            'reported_work_count':message.get('total-results'),'requested_fields':['DOI','author']})
        if not cursor:break
    return {'feed':FEED,'pages':committed,'candidate_ids':queued,'status':'live-selection-incomplete' if cursor else 'live-selection-exhausted-not-global-complete',
            'canonical_events_written':0,'paid_api_key_used':False}
