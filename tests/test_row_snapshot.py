import hashlib
import lzma
import sqlite3
import unittest
from unittest.mock import patch

from gapi.frozen_input import frozen_reader, verified_packed_snapshot
from gapi.row_snapshot import freeze_rows, inspect, _packed_lines
from gapi.task import BudgetStop, canonical
from gapi.global_analysis import rank_global
import test_streaming as fixtures
import test_global_analysis as analysis


class RowSnapshotTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_line_reader_preserves_bytes_at_every_chunk_boundary(self):
        from types import SimpleNamespace
        task=SimpleNamespace(config={'budget_bytes':1024},check_memory=lambda:None)
        body='a\n\n证据\r\n'.encode()+b'\x00\nlast\n'
        expected=[b'a\n',b'\n','证据\r\n'.encode(),b'\x00\n',b'last\n']
        for cut in range(len(body)+1):
            for chunks in ([body[:cut],body[cut:]],[body[i:i+1] for i in range(len(body))]):
                with patch('gapi.frozen_input._snapshot_chunks',return_value=iter(chunks)):
                    self.assertEqual(list(_packed_lines(self.root/'unused',task)),expected)

    def test_line_reader_keeps_line_total_and_unterminated_bounds(self):
        from types import SimpleNamespace
        task=SimpleNamespace(config={'budget_bytes':8},check_memory=lambda:None)
        for chunks in ([b'1234',b'5678\n'],[b'12345678\n']):
            with patch('gapi.frozen_input._snapshot_chunks',return_value=iter(chunks)):
                self.assertEqual(list(_packed_lines(self.root/'unused',task,max_line=8)),[b'12345678\n'])
        for chunks in ([b'123456789\n'],[b'12345',b'6789'],[b'123456789']):
            with patch('gapi.frozen_input._snapshot_chunks',return_value=iter(chunks)):
                with self.assertRaisesRegex(ValueError,'line exceeds bound'):
                    list(_packed_lines(self.root/'unused',task,max_line=8))
        with patch('gapi.frozen_input._snapshot_chunks',return_value=iter([b'unfinished'])):
            with self.assertRaisesRegex(ValueError,'unterminated'):
                list(_packed_lines(self.root/'unused',task))
        with patch('gapi.frozen_input._snapshot_chunks',return_value=iter([b'x\n'*17])):
            with self.assertRaisesRegex(ValueError,'decoded bound'):
                list(_packed_lines(self.root/'unused',task))

    def test_freeze_restore_scores_and_exact_events_idempotently(self):
        s=self.store(fixtures.archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            before=list(s.db.execute('SELECT * FROM events ORDER BY id'))
            expected=canonical(rank_global(s))
            progress=s.candidate_progress()
            frozen=freeze_rows(s);path=s.task.root/'frozen-inputs'/(frozen['filename']+'.xz')
            self.assertTrue(verified_packed_snapshot(s,path))
            modified=path.stat().st_mtime_ns
            self.assertEqual(freeze_rows(s),frozen)
            self.assertEqual(path.stat().st_mtime_ns,modified)
            self.assertEqual(list(s.db.execute('SELECT * FROM events ORDER BY id')),before)
            live=s.db
            with frozen_reader(s,frozen['filename']):
                self.assertEqual(list(s.db.execute('SELECT * FROM events ORDER BY id')),before)
                self.assertEqual(canonical(rank_global(s)),expected)
                self.assertEqual(s.candidate_progress(),progress)
            self.assertIs(s.db,live)
            self.assertEqual(list(path.parent.glob('frozen-read-*')),[])
        finally:s.close()

    def test_failure_cleans_owned_staging_preserves_foreign_staging_and_live(self):
        s=self.store(fixtures.archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            with patch.object(s.task,'check',side_effect=[0,BudgetStop('full')]):
                with self.assertRaises(BudgetStop):freeze_rows(s)
            folder=s.task.root/'frozen-inputs'
            self.assertFalse(list(folder.glob('*.partial')))
            staging=folder/'normalized-rows.xz.partial';staging.write_bytes(b'recovery')
            with self.assertRaises(FileExistsError):freeze_rows(s)
            self.assertEqual(staging.read_bytes(),b'recovery')
            self.assertFalse(s.db.in_transaction)
        finally:s.close()

    def test_crc_truncation_trailing_and_changed_identity_fail_before_restore(self):
        s=self.store(fixtures.archive([]))
        try:
            frozen=freeze_rows(s);path=s.task.root/'frozen-inputs'/(frozen['filename']+'.xz')
            original=path.read_bytes()
            for bad in (original[:-1],original+b'x',original+original):
                path.write_bytes(bad)
                with self.assertRaises((ValueError,EOFError,lzma.LZMAError)):
                    with frozen_reader(s,frozen['filename']):pass
                self.assertEqual(list(path.parent.glob('frozen-read-*')),[])
            path.write_bytes(original)
            other=path.with_name('f'*64+'.rows.xz');other.write_bytes(original)
            with self.assertRaisesRegex(ValueError,'identity'):
                with frozen_reader(s,'f'*64+'.rows'):pass
            self.assertFalse(s.db.in_transaction)
        finally:s.close()

    def test_schema_sql_is_not_an_arbitrary_execution_channel(self):
        s=self.store(fixtures.archive([]))
        try:
            header={'format':'gapi-private-ranking-rows/1','schema':[
                ['table','evil','evil',"ATTACH DATABASE 'untrusted' AS evil"]],'metadata':{}}
            body=canonical(header)+b'\n'+canonical({'end':{}})+b'\n'
            checksum=hashlib.sha256(body).hexdigest();folder=s.task.root/'frozen-inputs';folder.mkdir()
            (folder/(checksum+'.rows.xz')).write_bytes(lzma.compress(body,preset=3))
            with self.assertRaisesRegex(ValueError,'schema'):
                with frozen_reader(s,checksum+'.rows'):pass
            self.assertEqual(list(folder.glob('frozen-read-*')),[])
        finally:s.close()

    def test_schema_select_and_mismatched_identity_are_rejected(self):
        s=self.store(fixtures.archive([]))
        try:
            folder=s.task.root/'frozen-inputs';folder.mkdir()
            for sql in ('CREATE TABLE evil AS SELECT randomblob(1000000000)',
                        'CREATE TABLE other (value TEXT)'):
                header={'format':'gapi-private-ranking-rows/1','schema':[
                    ['table','evil','evil',sql]],'metadata':{}}
                body=canonical(header)+b'\n'+canonical({'end':{'evil':0}})+b'\n'
                checksum=hashlib.sha256(body).hexdigest()
                (folder/(checksum+'.rows.xz')).write_bytes(lzma.compress(body,preset=3))
                with self.assertRaises((ValueError,sqlite3.DatabaseError)):
                    with frozen_reader(s,checksum+'.rows'):pass
                self.assertEqual(list(folder.glob('frozen-read-*')),[])
        finally:s.close()

    def test_restore_budget_failure_preserves_live_and_cleans_target(self):
        s=self.store(fixtures.archive([]))
        try:
            frozen=freeze_rows(s);live=s.db
            with patch.object(s.task,'check',side_effect=[0,BudgetStop('full')]):
                with self.assertRaises(BudgetStop):
                    with frozen_reader(s,frozen['filename']):pass
            self.assertIs(s.db,live)
            self.assertEqual(list((s.task.root/'frozen-inputs').glob('frozen-read-*')),[])
        finally:s.close()

    def test_fresh_cli_freezes_without_allocating_restore_database(self):
        import io
        import json
        from argparse import Namespace
        from contextlib import redirect_stdout
        from pathlib import Path
        from gapi.stream_cli import execute_task
        s=self.store(fixtures.archive([]));task=s.task
        analysis.AnalysisTests.seed(self,s);s.close()
        args=Namespace(command='export-release',publication_policy=None,
            freeze_input=True,freeze_format='rows',frozen_input=None,out=Path('outputs/global'))
        def export(store,out,**kwargs):
            self.assertFalse(store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE name='gapi_snapshot_scope'").fetchone())
            self.assertGreater(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            return kwargs['frozen_input']
        with patch('gapi.global_analysis.export_release',side_effect=export), \
                patch('gapi.frozen_input.frozen_reader',side_effect=AssertionError('no restore permitted')):
            output=io.StringIO()
            with redirect_stdout(output):execute_task(task,args)
        self.assertTrue(json.loads(output.getvalue())['filename'].endswith('.rows'))
