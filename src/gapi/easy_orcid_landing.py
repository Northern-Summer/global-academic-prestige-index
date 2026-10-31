"""Compact private scoring projection of the frozen Easy ORCID derivative.

The full gzip is replayed and verified on every resume; there is no byte seek.
Only rows containing both training/education and employment are retained. Other
rows are enumerated in the census, not silently sampled or presented as careers.
No data in this isolated landing is promoted into the canonical event store.
"""
from __future__ import annotations

import json
import gzip
import hashlib
from contextlib import nullcontext
import sqlite3
import tempfile
import time
import zlib

from .easy_orcid_census import census
from .orcid_stream import private_person
from .task import canonical, digest

POLICY = 'easy-orcid-scoring-projection/1'


def person_index(store, count, *, table='easy_careers'):
    """SQLite's bounded external merge sort, confined to task-owned scratch.

    MEMORY temp storage materialized the entire sorter and exhausted SQLite's
    heap on 2.3 million real rows. Keep its heap cap and bound disk spill instead.
    """
    if table not in ('easy_careers','easy_full_delta'):raise ValueError('unsupported projection table')
    index_name=table+'_person'
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",(index_name,)).fetchone():
        return
    store.task.check(count*128+4*1024**2)
    with tempfile.TemporaryDirectory(prefix='gapi-index-',dir=store.task.root) as scratch:
        literal=scratch.replace("'","''")
        store.db.execute(f"PRAGMA temp_store_directory='{literal}'")
        actual=store.db.execute('PRAGMA temp_store_directory').fetchone()
        if not actual or actual[0]!=scratch:
            raise RuntimeError('SQLite controlled temporary directory unavailable')
        store.db.execute('PRAGMA temp_store=FILE')
        stopped=None;last=0.0
        def check():
            nonlocal stopped,last
            if time.monotonic()-last<.1:return 0
            last=time.monotonic()
            try:
                store.task.check();store.task.check_memory()
                return 0
            except RuntimeError as exc:
                stopped=exc
                return 1
        store.db.set_progress_handler(check,10000)
        try:
            with store.db:store.db.execute(f'CREATE INDEX {index_name} ON {table}(person,row_number)')
        except sqlite3.Error:
            if stopped:raise stopped
            raise
        finally:
            store.db.set_progress_handler(None,0)
            store.db.execute('PRAGMA temp_store=MEMORY')
            store.db.execute("PRAGMA temp_store_directory=''")


def initialize(store, spec):
    store.task.check(1024**2)
    store.db.executescript('''
        CREATE TABLE IF NOT EXISTS easy_career_state(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            identity TEXT NOT NULL, spec TEXT NOT NULL, status TEXT NOT NULL,
            scanned INTEGER NOT NULL, report TEXT);
        CREATE TABLE IF NOT EXISTS easy_careers(
            row_number INTEGER PRIMARY KEY, person BLOB NOT NULL,
            row_sha256 BLOB NOT NULL, payload BLOB NOT NULL);
    ''')
    identity = digest({'spec': spec, 'policy': POLICY})
    old = store.db.execute('SELECT identity FROM easy_career_state WHERE singleton=1').fetchone()
    if old and old[0] != identity:
        raise ValueError('Easy ORCID landing identity changed; mixed resume forbidden')
    with store.db:
        store.db.execute('INSERT OR IGNORE INTO easy_career_state VALUES(1,?,?,?,0,NULL)',
                         (identity, canonical(spec).decode(), 'provisional'))
    return identity


def ingest(store, http, spec, *, max_records=None, max_seconds=1800, progress=None):
    """Commit <=1000 projections at a time, with stable row keys and evidence.

    Unchanged rows are ignored; a changed previously committed projection fails
    closed. Whole-file validation, including the replayed prefix, gates readers.
    A budget stop rolls back only the current batch and retains prior progress.
    """
    initialize(store, spec)
    with store.db:
        store.db.execute("UPDATE easy_career_state SET status='provisional' WHERE singleton=1")
    pending = []
    pending_bytes = 0
    scanned = 0
    selected = 0
    limit = min(1000, store.task.config['batch_events'])
    def commit():
        nonlocal pending_bytes
        if not pending:
            return
        # Covers SQLite page/index growth and a rollback journal, conservatively.
        store.task.check(sum(len(p[3]) + 512 for p in pending) * 4 + 1024**2)
        with store.db:
            for row in pending:
                old = store.db.execute('SELECT person,row_sha256,payload AS projected_payload FROM easy_careers WHERE row_number=?',
                                       (row[0],)).fetchone()
                if old and tuple(old) != row[1:]:
                    raise ValueError('committed Easy ORCID row changed on replay')
                store.db.execute('INSERT OR IGNORE INTO easy_careers VALUES(?,?,?,?)', row)
            store.db.execute('UPDATE easy_career_state SET scanned=max(scanned,?) WHERE singleton=1', (scanned,))
        pending.clear()
        pending_bytes = 0
    def consume(number, orcid, rows, checksum):
        nonlocal scanned, selected, pending_bytes
        scanned = number
        # Preserve unknown stages, identifiers and years for later rules. This
        # is a disclosed two-sided network projection, not an evidence upgrade.
        if not (any(r[0] in ('education', 'training') for r in rows)
                and any(r[0] == 'employment' for r in rows)):
            return
        selected += 1
        payload = zlib.compress(canonical(rows), 6)
        if pending and pending_bytes + len(payload) > 1024**2:
            commit()
        pending.append((number, bytes.fromhex(private_person(orcid)), checksum, payload))
        pending_bytes += len(payload)
        if len(pending) >= limit:
            commit()
    def report_progress(report):
        if progress:
            report.update(storage='private-normalized-projection',
                          personal_records_saved=True, selected_source_rows=selected,
                          committed_source_rows=store.db.execute('SELECT count(*) FROM easy_careers').fetchone()[0],
                          canonical_events_added=0, storage_bytes=store.task.used())
            progress(report)
    result = census(store.task, http, spec, max_records=max_records,
                    max_seconds=max_seconds, progress=report_progress, consume=consume)
    # census returns errors as structured partial results. Never commit the
    # uncommitted batch after a failure (especially a capacity admission stop).
    if result['error'] is None:
        try:
            commit()
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
            result['source_complete'] = False
            result['status'] = 'partial-unverified-census'
            result['error'] = type(exc).__name__ + ': ' + str(exc)[:256]
    count = store.db.execute('SELECT count(*) FROM easy_careers').fetchone()[0]
    result.update(policy=POLICY, retained_source_rows=count,
                  selected_source_rows=selected,
                  excluded_valid_rows=result['counts'].get('valid_rows',0)-selected,
                  personal_records_saved=count > 0, storage='private-normalized-projection',
                  excluded_projection='rows without both education/training and employment',
                  declaration_origin='unknown-in-derivative', canonical_events_added=0)
    with store.db:
        store.db.execute('UPDATE easy_career_state SET status=?,report=? WHERE singleton=1',
                         ('verified' if result['source_complete'] else 'provisional', canonical(result).decode()))
    return result


def verified_rows(store):
    """Private downstream input; the caller must deduplicate people/relations.

    Row hashes bind exact source lines; the archive checksum and spec bind their
    source version. Derived institution identifiers are not independent evidence.
    """
    state = store.db.execute('SELECT status,spec FROM easy_career_state WHERE singleton=1').fetchone()
    if not state or state[0] != 'verified':
        raise ValueError('complete source checksum verification required')
    spec = json.loads(state[1])
    for number, person, checksum, payload in store.db.execute(
            'SELECT row_number,person,row_sha256,payload AS projected_payload FROM easy_careers ORDER BY row_number'):
        rows = json.loads(zlib.decompress(payload))
        yield {'source': spec['id'], 'row_number': number, 'person': person.hex(),
               'row_sha256': checksum.hex(), 'rows': rows,
               'independent_corroboration': False, 'declaration_origin': 'unknown-in-derivative'}


def rank_projection(store, *, stream_edges=False, include_full=False):
    """Separate observed source score, using canonical relation and PR owners.

    Never mixes this derivative into independently corroborated qualifications.
    A private person index permits bounded consecutive-person deduplication.
    """
    from collections import Counter
    from array import array
    import math
    from .global_analysis import observed_relations, resolve_institutions
    from .ranking import DEFAULT_CONFIG, pagerank_sparse
    from .task import BudgetStop
    state=store.db.execute('SELECT status,report,identity FROM easy_career_state WHERE singleton=1').fetchone()
    if not state or state[0]!='verified':
        raise ValueError('complete source checksum verification required')
    count=store.db.execute('SELECT count(*) FROM easy_careers').fetchone()[0]
    person_index(store,count)
    source='easy-orcid-2023.6-hq';source_report=json.loads(state[1]);tables=['easy_careers']
    if include_full:
        full=store.db.execute('SELECT status,report FROM easy_full_state WHERE singleton=1').fetchone()
        if not full or full[0]!='verified':raise ValueError('verified full derivative required')
        source_report=json.loads(full[1])
        if source_report.get('hq_identity')!=state[2]:
            raise ValueError('full derivative HQ identity differs from current HQ')
        if not source_report.get('all_hq_projections_present'):
            raise ValueError('full derivative must prove every reused HQ projection present')
        source='easy-orcid-2023.6-full';tables.append('easy_full_delta')
        person_index(store,store.db.execute('SELECT count(*) FROM easy_full_delta').fetchone()[0],table='easy_full_delta')
    # Runtime diagnostics stay in the private ingest report/log. Different
    # download duration or reconnect count cannot change released score bytes.
    source_report={k:v for k,v in source_report.items()
                   if k not in ('elapsed_seconds','transport_reconnects')}
    def payloads():
        for table in tables:
            yield from store.db.execute(f'SELECT payload AS projected_payload FROM {table}')
    # Resolve only exact external identifiers, using the existing frozen ROR owner.
    wanted=set()
    for (payload,) in payloads():
        for row in json.loads(zlib.decompress(payload)):
            # Unknown stages cannot enter this view's relation owner. Keep them
            # in private input, but do not inflate its in-memory crosswalk with
            # identifiers that cannot contribute to any scoring edge.
            if row[2] and row[1] in {*DEFAULT_CONFIG['stage_weights'],'academic'}:
                wanted.add(row[2])
        if len(wanted)>200000:raise BudgetStop('institution projection exceeds bounded crosswalk')
    resolve_institutions(store,extra_identifiers=wanted)
    aliases=dict(store.db.execute('SELECT alias,institution FROM active_institution_aliases'))
    graph={};counts=Counter();institutions={}
    stage_names=tuple(DEFAULT_CONFIG['stage_weights'])
    stage_index={stage:i for i,stage in enumerate(stage_names)}
    funnel=Counter()
    people=0;relations=0;person=None;events=[]
    def add_person(items):
        nonlocal relations
        trainings=[e for e in items if e['kind'] in ('education','training') and e['stage'] in DEFAULT_CONFIG['stage_weights']]
        jobs=[e for e in items if e['kind']=='employment' and e['stage']=='academic']
        funnel['people_with_identified_training_and_academic_job']+=bool(trainings and jobs)
        funnel['people_with_required_dates_on_both_sides']+=bool(
            any(e['year_end'] for e in trainings) and any(e['year_start'] for e in jobs))
        before=relations
        for (employer,training,stage),_,_ in observed_relations(items):
            relations+=1;key=(employer,training)
            weight=DEFAULT_CONFIG['stage_weights'][stage]
            if key not in graph:graph[key]=array('d',[0.0])*len(stage_names)
            graph[key][stage_index[stage]]+=weight;counts[training]+=1
            if relations%1000==0:store.task.check_memory()
        funnel['people_contributing_relations']+=relations>before
    from heapq import merge
    def ordered_rows(table):
        for number,pid,payload in store.db.execute(f'SELECT row_number,person,payload AS projected_payload FROM {table} INDEXED BY {table}_person ORDER BY person,row_number'):
            yield pid,table,number,payload
    for pid,table,number,payload in merge(*(ordered_rows(table) for table in tables),key=lambda row:row[:3]):
        if person!=pid:
            if person is not None:add_person(events)
            person=pid;events=[];people+=1
            if people%1000==0:store.task.check_memory()
        for kind,stage,inst,start,end,role in json.loads(zlib.decompress(payload)):
            if not inst:continue
            if len(events)>=4096:raise BudgetStop('person event fanout exceeds bounded relation analysis')
            inst=aliases.get(inst,inst)
            inst=institutions.setdefault(inst,inst)
            events.append({'kind':kind,'stage':stage,'institution':inst,
                           'year_start':start,'year_end':end,'role':role,
                           'source':source,'locator':table+':'+str(number),'status':'observed'})
    if events:add_person(events)
    nodes=sorted({n for edge in graph for n in edge})
    keys=sorted(graph)
    def edge_rows():
        return ((a,b,sum(graph[(a,b)])) for a,b in keys)
    scores,convergence=pagerank_sparse(nodes,edge_rows(),.85,1e-12,200,check=store.task.check_memory)
    scenarios={}
    for alpha in (.65,.95):
        values,conv=pagerank_sparse(nodes,edge_rows(),alpha,1e-12,1000,check=store.task.check_memory)
        scenarios['alpha_'+str(alpha)]={'convergence':conv,'order':sorted(nodes,key=lambda n:(-values[n],n))}
    directory={r[0]:(r[1],r[2],json.loads(r[3] or '{}').get('region'))
               for r in store.db.execute('SELECT id,name,country,payload FROM entities')}
    ordered=sorted(nodes,key=lambda n:(-scores[n],n))
    edge_output=({'source':a,'target':b,'weight':sum(graph[(a,b)]),
                 'stages':{stage:graph[(a,b)][i] for i,stage in enumerate(stage_names) if graph[(a,b)][i]}}
                for a,b in keys)
    return {'schema_version':'gapi-derived-observations/v1','source':source,
        'source_census':source_report,'global_complete':False,'formal_ranking':False,
        'scope':('separate frozen 2023 full derivative' if include_full else 'separate frozen 2023 HQ derivative')+'; not current global personnel census',
        'direction':'employer -> training','distinct_retained_people':people,
        'resolved_institutions':sum(n in directory for n in nodes),
        'unresolved_institution_identifiers':sum(n not in directory for n in nodes),
        'person_level_funnel':dict(funnel),
        'deduplicated_relations':relations,'config':DEFAULT_CONFIG,'convergence':convergence,
        'probability_mass':math.fsum(scores.values()),'sensitivity':scenarios,
        'nodes':[{'institution_id':n,'institution_name':directory.get(n,(n,None))[0],
                  'country':directory.get(n,(None,None,None))[1],
                  'region':directory.get(n,(None,None,None))[2],'observed_order':i+1,
                  'network_score':scores[n],'award_score':None,'composite_score':None,
                  'relation_count':counts[n],'formal_rank':None,
                  'institution_identity_status':'resolved_exact_ror' if n in directory else 'unresolved_external_identifier',
                  'unranked_reasons':['independent_corroboration_missing']+
                      ([] if n in directory else ['institution_resolution_pending'])} for i,n in enumerate(ordered)],
        'edge_count':len(graph),'edges':edge_output if stream_edges else list(edge_output)}


def write_projection(store, target, *, include_full=False):
    """Atomic JSON or deterministic gzip, without population-sized buffers."""
    if target.is_symlink() or (target.exists() and not target.is_file()):
        raise ValueError('score export target must be a regular file')
    result=rank_projection(store,stream_edges=True,include_full=include_full)
    edges=result.pop('edges')
    temporary=target.with_suffix('.tmp');created=False;written=0;checked=0
    encoder=json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
    def write(handle, value):
        nonlocal written,checked
        for text in encoder.iterencode(value):
            raw=text.encode();handle.write(raw);written+=len(raw)
            if written-checked>=1024**2:
                store.task.check(1024**2);store.task.check_memory();checked=written
    store.task.check(4*1024**2)
    try:
        with temporary.open('xb') as raw_handle:
            created=True
            stream=(gzip.GzipFile(filename='',mode='wb',fileobj=raw_handle,mtime=0,compresslevel=6)
                    if target.suffix=='.gz' else nullcontext(raw_handle))
            with stream as handle:
                handle.write(b'{"edges":[')
                for i,edge in enumerate(edges):
                    if i:handle.write(b',')
                    if i%1000==0:
                        store.task.check(1024**2);store.task.check_memory()
                    write(handle,edge)
                handle.write(b']')
                for key in sorted(result):
                    handle.write(b',');write(handle,key);handle.write(b':');write(handle,result[key])
                handle.write(b'}')
        def checksum(path):
            result=hashlib.sha256()
            with path.open('rb') as stream:
                while chunk:=stream.read(1024**2):
                    store.task.check_memory();result.update(chunk)
            return result.hexdigest()
        output_hash=checksum(temporary)
        # Byte-identical reruns retain the predecessor's mtime and avoid a
        # second filesystem write. Comparison remains bounded and exact.
        if target.exists() and target.stat().st_size==temporary.stat().st_size and checksum(target)==output_hash:
            temporary.unlink()
        else:
            temporary.replace(target)
    finally:
        if created and temporary.exists():temporary.unlink()
    with target.open('rb') as handle:
        checksum=hashlib.file_digest(handle,'sha256').hexdigest()
    return {'output':str(target),'bytes':target.stat().st_size,'sha256':checksum,
            'formal_ranking':False,'global_complete':False,
            'nodes':len(result['nodes']),'edges':result['edge_count'],
            **{key:result[key] for key in ('source','distinct_retained_people',
                'resolved_institutions','unresolved_institution_identifiers',
                'person_level_funnel','deduplicated_relations','convergence','probability_mass')}}
