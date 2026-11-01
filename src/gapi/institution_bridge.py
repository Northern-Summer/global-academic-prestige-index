"""Bounded Ringgold -> Wikidata -> ROR bridge with reciprocal ROR agreement.

Queries only discover candidates. A revision-pinned entity must explicitly carry
one unqualified Ringgold and ROR value, and the frozen ROR directory must point
back to that same entity. Names play no part. No personal or raw response storage.
"""
import json
import hashlib
import gzip
import io
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from .career_rules import institution_id
from .global_analysis import resolve_institutions
from .global_sources import wd_value, wikidata_batch_url
from .source_http import SourcePause
from .task import canonical, digest

PREDECESSOR = 'wikidata-ringgold-ror/20261030'
VERSION = 'wikidata-ringgold-ror/20261030-v2'
ROR_SHA256 = '959747b1ee6075d5598cc658d5599051714d4e054e93a706e422c43071f07db6'
SPEC = {'version': VERSION, 'license': 'CC0-1.0', 'properties': ['P3500','P6782'],
    'property_revisions': {'P3500':2559905787, 'P6782':2537555919},
    'ror_sha256': ROR_SHA256, 'policy': 'single-current-values-and-current-scan-conflict-free-reciprocal/2',
    'source': 'https://www.wikidata.org/wiki/Special:EntityData/'}
DDL = '''CREATE TABLE IF NOT EXISTS institution_bridge(
    alias TEXT NOT NULL, version TEXT NOT NULL, spec_sha256 TEXT NOT NULL,
    status TEXT NOT NULL, evidence TEXT NOT NULL, PRIMARY KEY(alias,version));'''


def entity_target(entity, qid, alias):
    if (not isinstance(entity,dict) or entity.get('id') != qid
            or type(entity.get('lastrevid')) is not int or entity['lastrevid'] <= 0):
        raise ValueError('invalid institution entity identity/revision')
    claims=entity.get('claims',{})
    if not isinstance(claims,dict): raise ValueError('invalid institution claims')
    values={}; statements=[]
    for prop in ('P3500','P6782'):
        rows=claims.get(prop,[])
        if not isinstance(rows,list): raise ValueError('invalid institution statement list')
        current=[]
        for row in rows:
            if not isinstance(row,dict): raise ValueError('invalid institution statement')
            if row.get('rank')=='deprecated': continue
            # A historical/qualified value cannot silently become a current ID.
            if row.get('qualifiers'): return None
            value=wd_value(row.get('mainsnak',{}))
            if not isinstance(value,str): return None
            current.append(value)
            statement=row.get('id')
            if not isinstance(statement,str) or not statement.startswith(qid+'$'):return None
            statements.append(statement)
        if len(set(current)) != 1:return None
        values[prop]=current[0]
    if 'ringgold:'+values['P3500'] != alias:return None
    target=institution_id(values['P6782'],'ror')
    return (target, sorted(statements)) if target else None


def discover(http, aliases):
    if not aliases or len(aliases)>20 or any(not re.fullmatch('ringgold:[0-9]{4,6}',a) for a in aliases):
        raise ValueError('invalid bounded institution identifier selection')
    literals=' '.join(json.dumps(a.split(':')[1]) for a in aliases)
    query='SELECT DISTINCT ?entity ?id WHERE { VALUES ?id { '+literals+' } ?entity wdt:P3500 ?id } ORDER BY ?id ?entity LIMIT 1001'
    url='https://query.wikidata.org/sparql?format=json&query='+quote(query)
    data,sha=http.json(url)
    envelope=data.get('results') if isinstance(data,dict) else None
    rows=envelope.get('bindings') if isinstance(envelope,dict) else None
    if not isinstance(rows,list) or len(rows)>=1001:raise ValueError('invalid or overfull institution candidate response')
    result={alias:set() for alias in aliases}
    for row in rows:
        id_value=row.get('id') if isinstance(row,dict) else None
        entity_value=row.get('entity') if isinstance(row,dict) else None
        identifier=id_value.get('value') if isinstance(id_value,dict) else None
        uri=entity_value.get('value') if isinstance(entity_value,dict) else None
        if not isinstance(uri,str) or not re.fullmatch('http://www.wikidata.org/entity/Q[1-9][0-9]*',uri):
            raise ValueError('invalid institution candidate entity')
        if not isinstance(identifier,str) or 'ringgold:'+identifier not in result:
            raise ValueError('institution response outside requested selection')
        result['ringgold:'+identifier].add(uri.rsplit('/',1)[-1])
    return result, {'query_sha256':digest(query),'response_sha256':sha,'query_url':url,
                    'retrieved_at':datetime.now(timezone.utc).isoformat()}


def fetch_entities(http,qids,*,batch=False):
    """Reuse the existing bounded claims/info route, retaining EntityData reads.

    Only a response-size limit permits splitting. Refusals and source pauses
    propagate without trying a different route or consuming candidate decisions.
    """
    if (type(batch) is not bool or not isinstance(qids,(list,tuple)) or len(qids)>20
            or any(not isinstance(q,str) or not re.fullmatch('Q[1-9][0-9]*',q) for q in qids)
            or len(qids)!=len(set(qids))):
        raise ValueError('invalid bounded institution entity selection')
    if batch and len(qids)>1:
        url=wikidata_batch_url(qids)
        try:data,sha=http.json(url)
        except ValueError as exc:
            if str(exc)!='structured response exceeds bound':raise
            middle=len(qids)//2
            return {**fetch_entities(http,qids[:middle],batch=True),
                    **fetch_entities(http,qids[middle:],batch=True)}
        if isinstance(data,dict) and isinstance(data.get('error'),dict) and data['error'].get('code') in ('maxlag','ratelimited','readonly'):
            raise SourcePause(503,time.time()+60,'Wikibase service requested a source pause')
        entities=data.get('entities') if isinstance(data,dict) else None
        if not isinstance(entities,dict) or set(entities)!=set(qids):
            raise ValueError('institution API returned a different entity selection')
        retrieved=datetime.now(timezone.utc).isoformat()
        return {qid:(entities[qid],sha,url,retrieved) for qid in qids}
    found={}
    for qid in qids:
        url=SPEC['source']+qid+'.json'
        try:data,sha=http.json(url)
        except ValueError as exc:
            if str(exc)!='structured response exceeds bound':raise
            found[qid]=(None,None,url,datetime.now(timezone.utc).isoformat());continue
        entities=data.get('entities') if isinstance(data,dict) else None
        if not isinstance(entities,dict) or set(entities)!={qid}:
            raise ValueError('institution API returned a different entity selection')
        retrieved=datetime.now(timezone.utc).isoformat()
        found[qid]=(entities[qid],sha,url,retrieved)
    return found


def revalidate_predecessor(store):
    """Recheck committed snapshots against the safe ROR owner, without refetch."""
    old=list(store.db.execute('''SELECT alias,status,evidence FROM institution_bridge b
        WHERE version=? AND NOT EXISTS(SELECT 1 FROM institution_bridge n
            WHERE n.alias=b.alias AND n.version=?) ORDER BY alias''',(PREDECESSOR,VERSION)))
    if len(old)>200000:raise ValueError('predecessor identity ledger exceeds bound')
    if not old:return 0
    qids=['wd:'+d['qid'] for _,_,e in old if (d:=json.loads(e)).get('qid')]
    safe=resolve_institutions(store,extra_identifiers=qids)['verified_reciprocal']
    store.task.check(4*1024**2)
    with store.db:
        for alias,status,evidence in old:
            detail=json.loads(evidence)
            detail['predecessor']={'version':PREDECESSOR,'evidence_sha256':digest(json.loads(evidence))}
            if status=='confirmed_identifier_bridge':
                target=detail.get('target')
                active=store.db.execute('SELECT institution FROM active_institution_aliases WHERE alias=?',(alias,)).fetchone()
                if (safe.get('wd:'+detail.get('qid',''))!=target or not active or active[0]!=target):
                    status='predecessor_reciprocal_conflict'
                    store.db.execute('INSERT OR IGNORE INTO institution_alias_issues VALUES(?,?,?,?)',
                        (alias,VERSION,status,canonical(detail).decode()))
            store.db.execute('INSERT INTO institution_bridge VALUES(?,?,?,?,?)',
                (alias,VERSION,digest(SPEC),status,canonical(detail).decode()))
    return len(old)


def aggregate_candidates(manifest_path):
    """Read only hash-verified public institution IDs, never personal projections.

    These are discovery candidates, not identity evidence. Promotion still needs
    the revision-pinned reciprocal identifier checks below.
    """
    path=Path(manifest_path)
    def bounded_read(file,limit):
        if file.is_symlink() or getattr(file,'is_junction',lambda:False)():
            raise ValueError('institution candidate links forbidden')
        with file.open('rb') as handle:raw=handle.read(limit+1)
        if len(raw)>limit:raise ValueError('institution candidate asset exceeds bound')
        return raw
    raw=bounded_read(path,1024**2);manifest=json.loads(raw)
    if not isinstance(manifest,dict) or manifest.get('schema_version')!='gapi-derived-browser/v1':
        raise ValueError('invalid institution candidate manifest')
    entry=manifest.get('nodes');columns=manifest.get('node_columns');count=manifest.get('node_count')
    if (not isinstance(entry,dict) or not isinstance(columns,list)
            or columns.count('institution_id')!=1 or type(count) is not int or not 0<=count<=200000):
        raise ValueError('invalid institution candidate node contract')
    sha=entry.get('sha256')
    if not isinstance(sha,str) or not re.fullmatch('[0-9a-f]{64}',sha) or entry.get('file')!=sha+'.json':
        raise ValueError('invalid content-addressed institution candidate path')
    asset=path.parent/entry['file']
    if asset.exists():nodes_raw=bounded_read(asset,32*1024**2)
    else:
        # Public repository assets use gzip storage while the manifest hashes
        # the unchanged decoded JSON. Never materialize the decoded file.
        packed=bounded_read(asset.with_suffix('.json.gz'),32*1024**2)
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:nodes_raw=stream.read(32*1024**2+1)
        except (OSError,EOFError) as exc:raise ValueError('invalid compressed institution candidate asset') from exc
        if len(nodes_raw)>32*1024**2:raise ValueError('institution candidate asset exceeds bound')
    if len(nodes_raw)!=entry.get('bytes') or hashlib.sha256(nodes_raw).hexdigest()!=sha:
        raise ValueError('institution candidate asset hash/size mismatch')
    nodes=json.loads(nodes_raw);index=columns.index('institution_id');identifiers=set()
    if not isinstance(nodes,list) or len(nodes)!=count:
        raise ValueError('institution candidate count mismatch')
    seen=set()
    for row in nodes:
        if not isinstance(row,list) or len(row)!=len(columns) or not isinstance(row[index],str):
            raise ValueError('invalid institution candidate row')
        identifier=row[index]
        if identifier in seen:raise ValueError('duplicate institution candidate')
        seen.add(identifier)
        if identifier.startswith('ringgold:'):identifiers.add(identifier)
    return identifiers,{'manifest_sha256':hashlib.sha256(raw).hexdigest(),
        'nodes_sha256':sha,'source':manifest.get('source'),'node_count':count,
        'ringgold_candidates':len(identifiers),'identity_evidence':False}


def resolve_online(store,http,*,max_aliases=100,max_seconds=300,progress=None,candidate_manifest=None):
    if not 1<=max_aliases<=1000 or not 0<max_seconds<=3600:raise ValueError('invalid institution bridge budget')
    identifiers,aggregate=(aggregate_candidates(candidate_manifest) if candidate_manifest is not None else (set(),None))
    for (identifier,) in store.db.execute("SELECT DISTINCT institution FROM events WHERE institution LIKE 'ringgold:%'"):
        identifiers.add(identifier)
        if len(identifiers)>200000:raise ValueError('institution candidate selection exceeds bound')
    identifiers=sorted(identifiers)
    store.task.check_memory()
    store.task.check(2*1024**2)
    store.db.executescript(DDL)
    mismatch=store.db.execute('SELECT 1 FROM institution_bridge WHERE version=? AND spec_sha256!=? LIMIT 1',
        (VERSION,digest(SPEC))).fetchone()
    if mismatch:raise ValueError('institution bridge contract changed without version change')
    revalidated=revalidate_predecessor(store)
    # Reuse the existing upstream pause owners; a new adapter must not bypass a
    # WDQS or EntityData backoff by inventing a different scheduler key.
    source='wikidata-discovery'
    if not store.source_ready(source) or not store.source_ready('wikidata'):
        return {'status':'server-backoff-pending','global_complete':False}
    # Already decided aliases, including no-match/ambiguous cases, do not refetch
    # within this frozen selection version. New versions must be explicit.
    decided={r[0] for r in store.db.execute('SELECT alias FROM institution_bridge WHERE version=?',(VERSION,))}
    aliases=[identifier for identifier in identifiers if identifier not in decided][:max_aliases]
    started=time.monotonic(); processed=promoted=failed=0
    for offset in range(0,len(aliases),20):
        if time.monotonic()-started>=max_seconds:break
        selected=aliases[offset:offset+20]
        valid=[a for a in selected if re.fullmatch('ringgold:[0-9]{4,6}',a)]
        try:
            with http.within(max_seconds-(time.monotonic()-started)):
                candidates,evidence=discover(http,valid) if valid else ({},{})
                qids=sorted({next(iter(v)) for v in candidates.values() if len(v)==1})
                safe=resolve_institutions(store,extra_identifiers=['wd:'+q for q in qids])['verified_reciprocal']
                entities=fetch_entities(http,qids,batch=True)
                records=[]
                for alias in selected:
                    detail=dict(evidence); status='unmatched'; target=None
                    matches=candidates.get(alias,set())
                    if alias not in valid:status='invalid_identifier'
                    elif len(matches)>1:status='ambiguous'
                    elif matches:
                        qid=next(iter(matches));entity,sha,url,retrieved=entities[qid]
                        if entity is None:
                            records.append((alias,'oversized_identity_entity',dict(detail,qid=qid,transport_url=url,transport_retrieved_at=retrieved),None))
                            continue
                        resolved=entity_target(entity,qid,alias)
                        detail.update(qid=qid,revision=entity['lastrevid'],entity_sha256=sha,
                            normalized_entity_sha256=digest(entity),transport_url=url)
                        detail['transport_retrieved_at']=retrieved
                        reciprocal=safe.get('wd:'+qid)
                        if resolved and reciprocal and resolved[0]==reciprocal:
                            target=resolved[0];detail.update(target=target,statements=resolved[1],ror_sha256=ROR_SHA256)
                            old=store.db.execute('SELECT institution FROM institution_aliases WHERE alias=?',(alias,)).fetchone()
                            issue=store.db.execute('SELECT 1 FROM institution_alias_issues WHERE alias=? LIMIT 1',(alias,)).fetchone()
                            status='confirmed_identifier_bridge' if not issue and (not old or old[0]==target) else 'conflicting_existing_alias'
                        else:status='reciprocal_or_current_identifier_unconfirmed'
                    records.append((alias,status,detail,target))
                store.task.check(2*1024**2);store.task.check_memory()
                with store.db:
                    for alias,status,detail,target in records:
                        store.db.execute('INSERT INTO institution_bridge VALUES(?,?,?,?,?)',
                            (alias,VERSION,digest(SPEC),status,canonical(detail).decode()))
                        if status=='confirmed_identifier_bridge':
                            store.db.execute('INSERT OR IGNORE INTO institution_aliases VALUES(?,?,?)',
                                (alias,target,digest({'spec':SPEC,'evidence':detail})))
                        elif status=='conflicting_existing_alias':
                            store.db.execute('INSERT OR IGNORE INTO institution_alias_issues VALUES(?,?,?,?)',
                                (alias,VERSION,status,canonical(detail).decode()))
                processed+=len(records);promoted+=sum(r[1]=='confirmed_identifier_bridge' for r in records)
                decided.update(r[0] for r in records)
                if progress:progress({'processed_aliases':processed,'confirmed_identifier_bridges':promoted})
        except (OSError,ValueError,RuntimeError) as exc:
            from .task import BudgetStop
            if isinstance(exc,BudgetStop):raise
            if isinstance(exc,SourcePause):
                store.pause_source(source,exc.retry_at,str(exc))
                store.pause_source('wikidata',exc.retry_at,str(exc))
            store.failure(source,digest(selected),str(exc));failed+=1
            break
    observed=len(identifiers)
    remaining=sum(identifier not in decided for identifier in identifiers)
    selection_hash=hashlib.sha256()
    for identifier in identifiers:
        selection_hash.update(canonical(identifier))
    return {'version':VERSION,'spec_sha256':digest(SPEC),'processed_aliases':processed,'revalidated_predecessor_decisions':revalidated,
        'selection':{'kind':'observed-and-aggregate-ringgold-identifiers' if aggregate else 'observed-ringgold-identifiers',
            'aggregate_input':aggregate,'total':observed,'processed':observed-remaining,
            'pending':remaining,'sha256':selection_hash.hexdigest(),'enumeration_complete':remaining==0,
            'not_a_global_personnel_denominator':True},
        'confirmed_identifier_bridges':promoted,'failed_batches':failed,'global_complete':False,
        'status':'resumable-source-or-time-boundary' if failed or processed<len(aliases) else 'selected-identifiers-processed',
        'identity_only':'institution identifiers; no person or career fact corroboration',
        'totals':dict(store.db.execute('SELECT status,count(*) FROM institution_bridge WHERE version=? GROUP BY status',(VERSION,)))}
