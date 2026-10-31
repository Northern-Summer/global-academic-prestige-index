"""Differential checks preserve legacy locator semantics and public hashes."""
import hashlib
import random
import unittest

import test_streaming as fixtures
from test_streaming import archive
from gapi.career_store import CANDIDATE_SOURCES
from gapi.task import canonical


def legacy_progress(store):
    rows=[]
    for command,source in CANDIDATE_SOURCES.items():
        checksum=hashlib.sha256()
        for row in store.db.execute('SELECT identifier,query_sha256,response_sha256 FROM discovery WHERE source=? ORDER BY identifier',(command,)):
            checksum.update(canonical(row))
        total,processed,observed,failed=store.db.execute('''SELECT count(*),COALESCE(sum(committed),0),
            COALESCE(sum(observed),0),COALESCE(sum(attempted AND NOT committed),0) FROM (
            SELECT EXISTS(SELECT 1 FROM members m WHERE m.source=? AND
                (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%')) AS committed,
                EXISTS(SELECT 1 FROM members m WHERE m.source=? AND m.status='observed' AND
                (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%')) AS observed,
                EXISTS(SELECT 1 FROM discovery_retries r WHERE r.source=d.source AND r.identifier=d.identifier) AS attempted
            FROM discovery d WHERE d.source=?)''',(source,source,command)).fetchone()
        if total:
            rows.append(dict(source=source,candidates=total,processed=processed,observed_members=observed,
                rejected_members=processed-observed,pending=total-processed,pending_with_errors=failed,
                selection_sha256=checksum.hexdigest(),enumeration_complete=processed==total))
    return dict(scope='current-discovered-candidates-only',complete_global_census=False,
                world_personnel_denominator=None,sources=rows)


class ProgressProjectionTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_locator_projection_matches_original_sql_on_mixed_and_legacy_records(self):
        store=self.store(archive([]));rng=random.Random(31)
        identifiers=['Q1','q1','Q2','Q10','private-id','PRIVATE-ID','0000-0002-1825-0097',
                     'name_','name%','x@y','école','ÉCOLE','', '含中文', 'trailing-', 'Q1\x00tail']
        identifiers += ['Q'+str(i) for i in range(20,220)]
        try:
            with store.db:
                store.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)',[
                    ('wikidata','Q20\x00@1','a'*64,'observed',None),
                    ('wikidata','Q21@\x001','b'*64,'observed',None)])
                for command,source in CANDIDATE_SOURCES.items():
                    for i,identifier in enumerate(identifiers):
                        store.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',(command,identifier,str(i),'b'))
                        if i%3:
                            store.db.execute('INSERT INTO discovery_retries VALUES(?,?,?,?,?)',
                                             (command,identifier,1,100,'test'))
                        for suffix in ('','@1','@2#education','@3@nested'):
                            if rng.random()<0.5:
                                locator=identifier+suffix
                                if rng.random()<0.3:locator=locator.swapcase()
                                store.db.execute('INSERT OR IGNORE INTO members VALUES(?,?,?,?,?)',
                                    (source,locator,'a'*64,rng.choice(['observed','rejected']),None))
            self.assertEqual(store.candidate_progress(),legacy_progress(store))
            self.assertEqual(store.candidate_progress(),store.candidate_progress())
            self.assertEqual(store.candidate_progress(allowed_sources={'wikidata'})['sources'],
                             [r for r in legacy_progress(store)['sources'] if r['source']=='wikidata'])
        finally:store.close()

    def test_exact_rejected_and_observed_revision_combine_without_duplicate_candidates(self):
        store=self.store(archive([]))
        try:
            with store.db:
                store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
                store.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)',[
                    ('wikidata','Q1','a'*64,'rejected',None),
                    ('wikidata','q1@1','b'*64,'observed',None),
                    ('wikidata','Q1@2','c'*64,'observed',None)])
            row=store.candidate_progress()['sources'][0]
            self.assertEqual((row['candidates'],row['processed'],row['observed_members'],row['rejected_members']),
                             (1,1,1,0))
            self.assertEqual(store.candidate_progress(),legacy_progress(store))
        finally:store.close()
