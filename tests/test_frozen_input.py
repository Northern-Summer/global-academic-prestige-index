import sqlite3
import unittest

from gapi.frozen_input import freeze_input,frozen_reader,compress_snapshot,verified_packed_snapshot
from gapi.task import BudgetStop,canonical
from gapi.global_analysis import rank_global
import test_streaming as fixtures
from test_streaming import archive
import test_global_analysis as analysis


class FrozenInputTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_ranking_projection_replays_scores_preserves_raw_payload_and_is_idempotent(self):
        import json
        from gapi.event_storage import pack
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            with s.db:
                event_id,text=s.db.execute('SELECT id,payload FROM events ORDER BY id LIMIT 1').fetchone()
                payload=json.loads(text);payload['fixture_note']='preserve bytes '*100
                packed=pack(canonical(payload).decode())
                self.assertIsInstance(packed,bytes)
                s.db.execute('UPDATE events SET payload=? WHERE id=?',(packed,event_id))
                s.db.execute('CREATE TABLE easy_careers(row_number INTEGER PRIMARY KEY,payload BLOB)')
                s.db.execute('INSERT INTO easy_careers VALUES(1,?)',(b'x'*1024**2,))
                s.db.execute('CREATE TABLE easy_full_delta(row_number INTEGER PRIMARY KEY,payload BLOB)')
                s.db.execute('INSERT INTO easy_full_delta VALUES(1,?)',(b'y'*1024**2,))
                s.db.execute('CREATE TABLE dump_landing(id TEXT PRIMARY KEY,payload BLOB)')
                s.db.execute('INSERT INTO dump_landing VALUES(?,?)',('private-staging',b'z'*1024**2))
                s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q123','q','r'))
                s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-orcid-api','candidate-only','q','r'))
            expected={public:canonical(rank_global(s,public=public)) for public in (True,False)}
            progress=s.candidate_progress()
            snapshot=freeze_input(s,scope='global-ranking')
            self.assertEqual(snapshot['projection']['excluded_tables'],{'easy_careers':1,'easy_full_delta':1,'discovery':2,'dump_landing':1})
            self.assertLess(snapshot['bytes'],1024**2)
            raw=s.task.root/'frozen-inputs'/snapshot['filename']
            self.assertFalse(raw.exists())
            self.assertTrue(verified_packed_snapshot(s,raw.with_suffix('.sqlite.gz')))
            self.assertEqual(freeze_input(s,scope='global-ranking'),snapshot)
            with s.db:s.db.execute("UPDATE events SET status='unresolved'")
            live=s.db
            with frozen_reader(s,snapshot['filename']):
                self.assertEqual(s.candidate_progress(),progress)
                self.assertEqual(s.candidate_progress(allowed_sources={'wikidata'})['sources'],
                                 [r for r in progress['sources'] if r['source']=='wikidata'])
                for public in (True,False):self.assertEqual(canonical(rank_global(s,public=public)),expected[public])
                stored=s.db.execute('SELECT payload AS stored_payload FROM events WHERE id=?',(event_id,)).fetchone()[0]
                self.assertEqual(stored,packed)
                for table in ('easy_careers','easy_full_delta','dump_landing','discovery'):
                    with self.assertRaises(sqlite3.OperationalError):s.db.execute('SELECT * FROM '+table)
            self.assertIs(s.db,live)
            self.assertEqual(live.execute('SELECT length(payload) FROM easy_careers').fetchone()[0],1024**2)
            self.assertFalse(live.in_transaction)
        finally:s.close()

    def test_ranking_projection_preserves_optional_award_tables(self):
        from test_nobel import HTTP,page,laureate,NobelTests
        from gapi.nobel import ingest_nobel
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            ingest_nobel(s,HTTP([page(0,[laureate()],1)]),NobelTests.spec(self))
            expected=canonical(rank_global(s,public=True))
            snapshot=freeze_input(s,scope='global-ranking')
            with frozen_reader(s,snapshot['filename']):
                self.assertEqual(canonical(rank_global(s,public=True)),expected)
                self.assertEqual(s.db.execute('SELECT count(*) FROM official_awards').fetchone()[0],1)
        finally:s.close()

    def test_ranking_projection_preserves_official_adjudication_and_normalization(self):
        from test_corroboration import CorroborationTests
        from gapi.corroboration import decisions
        from gapi.career_normalization import normalize_careers
        s=self.store(archive([]))
        try:
            CorroborationTests.seed(self,s)
            normalize_careers(s)
            verified=decisions(s)
            self.assertTrue(verified)
            expected=canonical(rank_global(s))
            snapshot=freeze_input(s,scope='global-ranking')
            with frozen_reader(s,snapshot['filename']):
                self.assertEqual(decisions(s),verified)
                self.assertEqual(canonical(rank_global(s)),expected)
        finally:s.close()

    def test_ranking_projection_fits_when_full_acquisition_backup_cannot(self):
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            with s.db:
                s.db.execute('CREATE TABLE easy_careers(row_number INTEGER PRIMARY KEY,payload BLOB)')
                s.db.execute('INSERT INTO easy_careers VALUES(1,zeroblob(8388608))')
            s.task.config['budget_bytes']=s.task.used()+s.task.config['reserve_bytes']+3*1024**2
            with self.assertRaises(BudgetStop):freeze_input(s)
            snapshot=freeze_input(s,scope='global-ranking')
            with frozen_reader(s,snapshot['filename']):
                self.assertEqual(s.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
            s.task.check()
        finally:s.close()

    def test_ranking_projection_failure_removes_owned_staging_and_preserves_source(self):
        from unittest.mock import patch
        s=self.store(archive([]))
        try:
            before=s.db.execute('SELECT count(*) FROM members').fetchone()[0]
            original=s.task.check
            def check(*args):
                if (s.task.root/'frozen-inputs'/'normalized-input.partial').exists():
                    raise BudgetStop('budget changed during copy')
                return original(*args)
            with patch.object(s.task,'check',side_effect=check),self.assertRaises(BudgetStop):
                freeze_input(s,scope='global-ranking')
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],before)
            self.assertEqual(list((s.task.root/'frozen-inputs').iterdir()),[])
        finally:s.close()

    def test_ranking_projection_rejects_triggers_and_unknown_scope(self):
        s=self.store(archive([]))
        try:
            with self.assertRaisesRegex(ValueError,'unsupported frozen input scope'):
                freeze_input(s,scope='partial')
            with s.db:s.db.execute('CREATE TRIGGER unexpected AFTER INSERT ON events BEGIN SELECT 1; END')
            with self.assertRaisesRegex(ValueError,'triggers'):
                freeze_input(s,scope='global-ranking')
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(list((s.task.root/'frozen-inputs').iterdir()),[])
        finally:s.close()

    def test_freeze_preserves_preexisting_staging_recovery_evidence(self):
        s=self.store(archive([]))
        try:
            folder=s.task.root/'frozen-inputs';folder.mkdir()
            staging=folder/'normalized-input.partial'
            staging.write_bytes(b'unique interrupted predecessor')
            for scope in ('full','global-ranking'):
                with self.assertRaisesRegex(ValueError,'unfinished private freeze'):
                    freeze_input(s,scope=scope)
                self.assertEqual(staging.read_bytes(),b'unique interrupted predecessor')
        finally:s.close()

    def test_cli_uses_writer_exclusion_and_retains_manifest_identity(self):
        import io,json
        from argparse import Namespace
        from contextlib import redirect_stdout
        from gapi.stream_cli import execute_task
        s=self.store(archive([]));task=s.task
        snapshot=freeze_input(s)
        args=Namespace(command='compact-frozen-inputs',filenames=[snapshot['filename']])
        try:
            with self.assertRaisesRegex(RuntimeError,'another task writer'):
                execute_task(task,args)
        finally:s.close()
        output=io.StringIO()
        with redirect_stdout(output):execute_task(task,args)
        result=json.loads(output.getvalue())
        self.assertEqual(result['snapshots'][0]['filename'],snapshot['filename'])

    def test_valid_gzip_with_wrong_content_is_rejected_without_retiring_raw(self):
        import gzip
        s=self.store(archive([]))
        try:
            snapshot=freeze_input(s)
            raw=s.task.root/'frozen-inputs'/snapshot['filename']
            packed=raw.with_suffix('.sqlite.gz')
            packed.write_bytes(gzip.compress(b'wrong input',mtime=0))
            self.assertFalse(verified_packed_snapshot(s,packed))
            with self.assertRaisesRegex(ValueError,'hash mismatch'):
                compress_snapshot(s,snapshot['filename'])
            self.assertTrue(raw.exists())
            packed.unlink()
            compress_snapshot(s,snapshot['filename'])
            self.assertTrue(verified_packed_snapshot(s,packed))
            outside=s.task.root/packed.name
            outside.write_bytes(packed.read_bytes())
            self.assertFalse(verified_packed_snapshot(s,outside))
            content=packed.read_bytes()
            packed.write_bytes(content[:-5])
            with self.assertRaises((EOFError,OSError)):
                with frozen_reader(s,snapshot['filename']):self.fail('truncated input accepted')
            self.assertEqual(list(raw.parent.glob('frozen-read-*')),[])
        finally:s.close()

    def test_compressed_snapshot_preserves_rank_and_identity_and_is_idempotent(self):
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            expected=canonical(rank_global(s,public=True))
            snapshot=freeze_input(s)
            result=compress_snapshot(s,snapshot['filename'])
            self.assertGreater(result['saved_bytes'],0)
            self.assertTrue(compress_snapshot(s,snapshot['filename'])['already_compressed'])
            repeated=freeze_input(s)
            self.assertEqual(repeated,snapshot)
            self.assertFalse((s.task.root/'frozen-inputs'/snapshot['filename']).exists())
            with s.db:s.db.execute("UPDATE events SET status='unresolved'")
            live=s.db
            with frozen_reader(s,snapshot['filename']):
                self.assertEqual(canonical(rank_global(s,public=True)),expected)
                with self.assertRaises(sqlite3.OperationalError):s.db.execute('DELETE FROM events')
            self.assertIs(s.db,live)
            self.assertEqual(list((s.task.root/'frozen-inputs').glob('frozen-read-*')),[])
        finally:s.close()

    def test_packed_corruption_budget_failure_and_reader_exception_cleanup(self):
        from unittest.mock import patch
        s=self.store(archive([]))
        try:
            snapshot=freeze_input(s)
            folder=s.task.root/'frozen-inputs'
            raw=folder/snapshot['filename']
            with patch.object(s.task,'check',side_effect=BudgetStop('no room')),self.assertRaises(BudgetStop):
                compress_snapshot(s,snapshot['filename'])
            self.assertTrue(raw.exists())
            self.assertEqual(list(folder.glob('*.partial')),[])
            compress_snapshot(s,snapshot['filename'])
            live=s.db
            with self.assertRaises(RuntimeError):
                with frozen_reader(s,snapshot['filename']):raise RuntimeError('caller failure')
            self.assertIs(s.db,live)
            with patch.object(s.task,'check',side_effect=[0,BudgetStop('no room')]),self.assertRaises(BudgetStop):
                with frozen_reader(s,snapshot['filename']):self.fail('budget was bypassed')
            self.assertEqual(list(folder.glob('frozen-read-*')),[])
            raw.with_suffix('.sqlite.gz').write_bytes(b'not gzip')
            with self.assertRaises(OSError):
                with frozen_reader(s,snapshot['filename']):self.fail('corruption accepted')
            self.assertEqual(list(folder.glob('frozen-read-*')),[])
        finally:s.close()

    def test_old_release_reproduces_after_live_ingest_and_reader_is_read_only(self):
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            expected=canonical(rank_global(s,public=True))
            snapshot=freeze_input(s)
            self.assertFalse(snapshot['publicly_bundled'])
            with s.db:
                s.db.execute("UPDATE events SET status='unresolved'")
            self.assertNotEqual(canonical(rank_global(s,public=True)),expected)
            live=s.db
            with frozen_reader(s,snapshot['filename']):
                self.assertEqual(canonical(rank_global(s,public=True)),expected)
                with self.assertRaises(sqlite3.OperationalError):s.db.execute('DELETE FROM events')
            self.assertIs(s.db,live)
        finally:s.close()

    def test_mutated_snapshot_and_path_traversal_are_rejected(self):
        s=self.store(archive([]))
        try:
            snapshot=freeze_input(s)
            path=s.task.root/'frozen-inputs'/snapshot['filename']
            with path.open('ab') as f:f.write(b'tampered')
            for name in (snapshot['filename'],'../careers.sqlite'):
                with self.subTest(name=name),self.assertRaises(ValueError):
                    with frozen_reader(s,name):self.fail('invalid snapshot opened')
        finally:s.close()

    def test_snapshot_growth_is_part_of_same_budget(self):
        from unittest.mock import patch
        s=self.store(archive([]))
        try:
            with patch.object(s.task,'check',side_effect=BudgetStop('no room')),self.assertRaises(BudgetStop):
                freeze_input(s)
            self.assertFalse((s.task.root/'frozen-inputs').exists())
        finally:s.close()
