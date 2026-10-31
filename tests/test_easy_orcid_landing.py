from contextlib import nullcontext
import gzip
import hashlib
import io
import json
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gapi.easy_orcid_landing import ingest, verified_rows, rank_projection, initialize, person_index, write_projection
from gapi.task import BudgetStop


class LandingTests(unittest.TestCase):
    def test_cli_defaults_to_compressed_export_and_keeps_explicit_json(self):
        import argparse
        from gapi.stream_cli import register
        parser=argparse.ArgumentParser();register(parser.add_subparsers(dest='command'))
        self.assertEqual(parser.parse_args(['rank-easy-orcid']).out,Path('outputs/easy-orcid-observed.json.gz'))
        self.assertEqual(parser.parse_args(['rank-easy-orcid','--out','outputs/previous.json']).out,Path('outputs/previous.json'))

    def store(self, batch=1):
        scratch=tempfile.TemporaryDirectory();self.addCleanup(scratch.cleanup)
        return SimpleNamespace(db=sqlite3.connect(':memory:'),task=SimpleNamespace(
            root=Path(scratch.name),config={'batch_events':batch},check=lambda *a:None,check_memory=lambda:None))

    def record(self):
        return {'orcid':'0000-0002-1825-0097','name':'SECRET NAME',
                'educations':[{'role':'PhD','end':{'year':2000},'xrefs':{'ror':'05dxps055'}}],
                'employments':[{'role':'Professor','start':{'year':2001},'xrefs':{'ror':'05dxps055'}}]}

    def source(self, rows):
        raw=gzip.compress(b''.join(json.dumps(r).encode()+b'\n' for r in rows),mtime=0)
        spec={'id':'test','url':'https://test.invalid/data','scope':'fixture',
              'size_bytes':len(raw),'md5':hashlib.md5(raw).hexdigest()}
        class Response(io.BytesIO):
            headers={}
            def __enter__(self):return self
            def __exit__(self,*a):self.close()
        return spec,SimpleNamespace(open=lambda *a,**k:Response(raw),within=lambda *a:nullcontext())

    def test_verified_projection_is_private_and_replay_is_idempotent(self):
        s=self.store();spec,http=self.source([self.record(),{'orcid':self.record()['orcid']}])
        r=ingest(s,http,spec,max_seconds=5)
        self.assertTrue(r['source_complete']);self.assertEqual(r['retained_source_rows'],1)
        self.assertEqual(r['canonical_events_added'],0)
        rows=list(verified_rows(s));self.assertEqual(len(rows),1)
        self.assertNotIn('SECRET',json.dumps(rows));self.assertNotIn(self.record()['orcid'],json.dumps(rows))
        self.assertFalse(rows[0]['independent_corroboration'])
        before=s.db.execute('SELECT * FROM easy_careers').fetchall()
        ingest(s,http,spec,max_seconds=5)
        self.assertEqual(before,s.db.execute('SELECT * FROM easy_careers').fetchall())

    def test_prefix_is_quarantined_then_resume_verifies_entire_stream(self):
        s=self.store();spec,http=self.source([self.record(),self.record()])
        r=ingest(s,http,spec,max_records=1,max_seconds=5)
        self.assertFalse(r['source_complete'])
        with self.assertRaisesRegex(ValueError,'checksum'):list(verified_rows(s))
        r=ingest(s,http,spec,max_seconds=5)
        self.assertTrue(r['source_complete']);self.assertEqual(r['retained_source_rows'],2)

    def test_wrong_checksum_never_promotes(self):
        s=self.store();spec,http=self.source([self.record()]);spec['md5']='0'*32
        self.assertFalse(ingest(s,http,spec,max_seconds=5)['source_complete'])
        with self.assertRaises(ValueError):list(verified_rows(s))

    def test_budget_failure_keeps_only_committed_batches(self):
        s=self.store();spec,http=self.source([self.record(),self.record()]);calls=0
        def check(*a):
            nonlocal calls
            calls+=1
            if calls>=3:raise BudgetStop('fixture capacity')
        s.task.check=check
        r=ingest(s,http,spec,max_seconds=5)
        self.assertFalse(r['source_complete']);self.assertIn('BudgetStop',r['error'])
        self.assertEqual(s.db.execute('SELECT count(*) FROM easy_careers').fetchone()[0],1)

    def test_changed_source_or_committed_row_fails_closed(self):
        s=self.store();spec,http=self.source([self.record()]);ingest(s,http,spec,max_seconds=5)
        with s.db:s.db.execute("UPDATE easy_careers SET row_sha256=x'00'")
        r=ingest(s,http,spec,max_seconds=5)
        self.assertFalse(r['source_complete']);self.assertIn('changed',r['error'])
        with self.assertRaises(ValueError):list(verified_rows(s))
        with self.assertRaisesRegex(ValueError,'identity'):
            ingest(s,http,dict(spec,version='changed'),max_seconds=5)

    def test_final_batch_capacity_stop_cannot_publish_verified_state(self):
        s=self.store(batch=1000);spec,http=self.source([self.record()]);calls=0
        def check(*a):
            nonlocal calls
            calls+=1
            if calls==2:raise BudgetStop('final batch full')
        s.task.check=check
        r=ingest(s,http,spec,max_seconds=5)
        self.assertFalse(r['source_complete']);self.assertIn('final batch',r['error'])
        self.assertEqual(r['retained_source_rows'],0)
        with self.assertRaises(ValueError):list(verified_rows(s))

    def test_scoring_deduplicates_people_and_preserves_direction_and_missing_awards(self):
        s=self.store();s.task.used=lambda:0
        s.db.executescript('CREATE TABLE active_institution_aliases(alias,institution); CREATE TABLE entities(id,name,country,payload);')
        record=self.record();record['employments'][0]['xrefs']['ror']='03yrm5c26'
        spec,http=self.source([record,record])
        ingest(s,http,spec,max_seconds=5)
        from gapi.event_storage import configure_reader
        configure_reader(s.db)
        # Production CareerStore decodes canonical event payload columns. The
        # derivative owns a separate encoding, so every read must use its alias.
        ingest(s,http,spec,max_seconds=5)
        self.assertEqual(len(list(verified_rows(s))),2)
        with patch('gapi.global_analysis.resolve_institutions'):
            first=rank_projection(s);second=rank_projection(s)
        self.assertEqual(first,second)
        self.assertEqual(first['distinct_retained_people'],1)
        self.assertEqual(first['deduplicated_relations'],1)
        self.assertEqual(first['edges'],[{'source':'ror:03yrm5c26','target':'ror:05dxps055',
                                        'weight':1.0,'stages':{'doctorate':1.0}}])
        self.assertAlmostEqual(first['probability_mass'],1)
        self.assertGreater(first['nodes'][0]['network_score'],first['nodes'][1]['network_score'])
        for row in first['nodes']:
            self.assertIsNone(row['formal_rank']);self.assertIsNone(row['award_score'])
        text=json.dumps(first)
        self.assertNotIn('SECRET',text);self.assertNotIn(record['orcid'],text)
        report=json.loads(s.db.execute('SELECT report FROM easy_career_state').fetchone()[0])
        report.update(elapsed_seconds=9999,transport_reconnects=7)
        with s.db:s.db.execute('UPDATE easy_career_state SET report=?',(json.dumps(report),))
        with patch('gapi.global_analysis.resolve_institutions'):
            self.assertEqual(rank_projection(s),first)
        target=s.task.root/'observed.json'
        with patch('gapi.global_analysis.resolve_institutions'):
            receipt=write_projection(s,target);before=target.read_bytes();write_projection(s,target)
            self.assertEqual(receipt['sha256'],hashlib.sha256(before).hexdigest())
            self.assertEqual(receipt['distinct_retained_people'],first['distinct_retained_people'])
            self.assertEqual(receipt['convergence'],first['convergence'])
            self.assertEqual(target.read_bytes(),before)
            self.assertEqual(json.loads(before),first)
            packed=s.task.root/'observed.json.gz'
            packed_receipt=write_projection(s,packed);packed_before=packed.read_bytes()
            self.assertEqual(gzip.decompress(packed_before),before)
            self.assertEqual(packed_receipt['sha256'],hashlib.sha256(packed_before).hexdigest())
            write_projection(s,packed)
            self.assertEqual(packed.read_bytes(),packed_before)
            with patch.object(s.task,'check',side_effect=[None,BudgetStop('storage full')]):
                with self.assertRaises(BudgetStop):write_projection(s,packed)
            self.assertEqual(packed.read_bytes(),packed_before)
            self.assertFalse(packed.with_suffix('.tmp').exists())
            temporary=target.with_suffix('.tmp');temporary.write_bytes(b'unique recovery state')
            with self.assertRaises(FileExistsError):write_projection(s,target)
            self.assertEqual(temporary.read_bytes(),b'unique recovery state')
            self.assertEqual(target.read_bytes(),before)

    def test_scoring_cannot_read_a_partial_source(self):
        s=self.store();spec,http=self.source([self.record(),self.record()])
        ingest(s,http,spec,max_records=1,max_seconds=5)
        with self.assertRaisesRegex(ValueError,'checksum'):rank_projection(s)

    def test_sort_budget_cancellation_rolls_back_index_and_cleans_scratch(self):
        s=self.store();spec,_=self.source([self.record()]);initialize(s,spec)
        s.db.executemany('INSERT INTO easy_careers VALUES(?,?,?,?)',
            ((i,i.to_bytes(32,'big'),b'0'*32,b'x') for i in range(10000)))
        s.db.commit();calls=0
        def check(*a):
            nonlocal calls
            calls+=1
            if calls>1:raise BudgetStop('sort capacity')
        s.task.check=check
        with self.assertRaisesRegex(BudgetStop,'sort capacity'):person_index(s,10000)
        self.assertIsNone(s.db.execute("SELECT 1 FROM sqlite_master WHERE name='easy_careers_person'").fetchone())
        self.assertEqual(list(s.task.root.iterdir()),[])
        self.assertEqual(s.db.execute('PRAGMA temp_store').fetchone()[0],2)
