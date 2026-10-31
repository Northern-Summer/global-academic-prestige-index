"""Full-source delta: reuse verified HQ projections without copying them.

Only an exact pseudonymous identity AND normalized payload match permits reuse.
Different projections remain separate source observations. Full-source integrity
gates the delta; existing HQ identity and status are never rewritten.
"""
import hashlib
import json
import sqlite3
import zlib

from .easy_orcid_census import census
from .easy_orcid_landing import POLICY, person_index
from .orcid_stream import private_person
from .task import canonical, digest

POLICY_FULL='easy-orcid-full-delta/1'


def ingest_full(store,http,spec,*,max_records=None,max_seconds=1800,progress=None):
    if spec['id']!='easy-orcid-2023.6-full':raise ValueError('full derivative source required')
    hq=store.db.execute('SELECT status,spec,identity FROM easy_career_state WHERE singleton=1').fetchone()
    if not hq or hq[0]!='verified':raise ValueError('verified HQ source required for exact reuse')
    if json.loads(hq[1])['derived_from']!=spec['derived_from']:
        raise ValueError('HQ and full derivative lineage differ')
    person_index(store,store.db.execute('SELECT count(*) FROM easy_careers').fetchone()[0])
    hq_count,hq_max=store.db.execute('SELECT count(*),coalesce(max(row_number),0) FROM easy_careers').fetchone()
    if hq_max>100000000:raise ValueError('HQ coverage bitmap exceeds bounded source universe')
    covered=bytearray(hq_max//8+1);covered_count=0
    identity=digest({'source':spec,'policy':POLICY_FULL,'projection_policy':POLICY,'hq_identity':hq[2]})
    store.task.check(1024**2)
    store.db.executescript('''
        CREATE TABLE IF NOT EXISTS easy_full_state(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity TEXT NOT NULL,
            status TEXT NOT NULL, report TEXT);
        CREATE TABLE IF NOT EXISTS easy_full_delta(
            row_number INTEGER PRIMARY KEY,person BLOB NOT NULL,
            row_sha256 BLOB NOT NULL,payload BLOB NOT NULL);
    ''')
    old=store.db.execute('SELECT identity FROM easy_full_state WHERE singleton=1').fetchone()
    if old and old[0]!=identity:raise ValueError('full delta identity changed; mixed resume forbidden')
    with store.db:
        store.db.execute('INSERT OR IGNORE INTO easy_full_state VALUES(1,?,?,NULL)',(identity,'provisional'))
        store.db.execute("UPDATE easy_full_state SET status='provisional' WHERE singleton=1")
    pending=[];pending_bytes=0;selected=0;reused=0;reuse_hash=hashlib.sha256()
    def commit():
        nonlocal pending_bytes
        if not pending:return
        store.task.check(sum(len(row[3])+512 for row in pending)*4+1024**2)
        with store.db:
            for row in pending:
                before=store.db.execute('SELECT person,row_sha256,payload AS projected_payload FROM easy_full_delta WHERE row_number=?',(row[0],)).fetchone()
                if before and tuple(before)!=row[1:]:raise ValueError('full delta row changed during replay')
                store.db.execute('INSERT OR IGNORE INTO easy_full_delta VALUES(?,?,?,?)',row)
        pending.clear();pending_bytes=0
    def consume(number,orcid,rows,checksum):
        nonlocal selected,reused,pending_bytes,covered_count
        if not(any(r[0] in ('education','training') for r in rows) and any(r[0]=='employment' for r in rows)):return
        selected+=1;person=bytes.fromhex(private_person(orcid));payload=zlib.compress(canonical(rows),6)
        match=None
        for hq_number,hq_payload in store.db.execute('SELECT row_number,payload AS projected_payload FROM easy_careers WHERE person=? ORDER BY row_number',(person,)):
            if hq_payload==payload:
                # Coverage means normalized projection content, not source row
                # multiplicity. Every equivalent HQ row is represented here.
                if match is None:match=hq_number
                byte,bit=divmod(hq_number,8)
                if not covered[byte]&(1<<bit):covered[byte]|=1<<bit;covered_count+=1
        if match is not None:
            reused+=1
            # Audit digest of the ordered exact-match proof; recomputable from
            # frozen source + private HQ. No additional per-person evidence copy.
            reuse_hash.update(canonical([number,checksum.hex(),match]))
            return
        if pending_bytes+len(payload)>1024**2:commit()
        pending.append((number,person,checksum,payload));pending_bytes+=len(payload)
        if len(pending)>=min(1000,store.task.config['batch_events']):commit()
    def report(r):
        if progress:
            r.update(selected_source_rows=selected,reused_hq_source_rows=reused,
                     committed_delta_rows=store.db.execute('SELECT count(*) FROM easy_full_delta').fetchone()[0],
                     storage_bytes=store.task.used(),personal_records_saved=True)
            progress(r)
    result=census(store.task,http,spec,max_records=max_records,max_seconds=max_seconds,progress=report,consume=consume)
    if result['error'] is None:
        try:commit()
        except (OSError,ValueError,RuntimeError,sqlite3.Error) as exc:
            result.update(source_complete=False,status='partial-unverified-census',error=type(exc).__name__+': '+str(exc)[:256])
    result.update(selected_source_rows=selected,reused_hq_source_rows=reused,
                  committed_delta_rows=store.db.execute('SELECT count(*) FROM easy_full_delta').fetchone()[0],
                  exact_reuse_sha256=reuse_hash.hexdigest(),policy=POLICY_FULL,
                  hq_identity=hq[2],canonical_events_added=0,personal_records_saved=True)
    result.update(hq_projection_rows=hq_count,distinct_hq_rows_reused=covered_count,
                  all_hq_projections_present=covered_count==hq_count)
    with store.db:
        store.db.execute('UPDATE easy_full_state SET status=?,report=? WHERE singleton=1',
                         ('verified' if result['source_complete'] else 'provisional',canonical(result).decode()))
    return result
