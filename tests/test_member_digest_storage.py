"""Member hashes stay exact across compaction, replay, snapshots and crashes."""
import json
import subprocess
import sys
import unittest
from unittest.mock import patch

from gapi.career_store import CareerStore
from gapi.frozen_input import freeze_input, frozen_reader
from gapi.global_analysis import rank_global
from gapi.queue_storage import compact_members
from gapi.row_snapshot import freeze_rows
from gapi.task import BudgetStop, Task, canonical
import test_global_analysis as analysis
import test_streaming as fixtures


class MemberDigestStorageTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def seed_legacy(self, store, count=4000):
        store.start('wikidata')
        with store.db:
            store.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)', [
                ('wikidata', 'member-'+str(n), format(n, '064x'),
                 'observed' if n % 2 else 'rejected', None if n % 2 else '保留拒绝理由')
                for n in range(count)])
            store.db.execute('UPDATE runs SET members=? WHERE source=?', (count, 'wikidata'))

    def test_new_writer_replay_hash_conflict_and_legacy_case(self):
        store = self.store(fixtures.archive([]))
        try:
            store.start('wikidata')
            for checksum in ('a'*64, 'F'*64, 'legacy-hash'):
                with store.member('wikidata', checksum):
                    store.finish_member('wikidata', checksum, checksum)
                self.assertEqual(store.seen('wikidata', checksum), (checksum, 'observed'))
                kind = store.db.execute('SELECT typeof(sha256) FROM members WHERE locator=?', (checksum,)).fetchone()[0]
                self.assertEqual(kind, 'blob' if checksum == 'a'*64 else 'text')
            with store.member('wikidata', 'a'*64):
                store.finish_member('wikidata', 'a'*64, 'a'*64)
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?', ('wikidata',)).fetchone()[0], 3)
            with self.assertRaisesRegex(ValueError, 'changed within frozen source'):
                with store.member('wikidata', 'a'*64):
                    store.finish_member('wikidata', 'a'*64, 'b'*64)
            with store.db:
                store.db.execute('UPDATE members SET sha256=? WHERE locator=?', (b'corrupt', 'a'*64))
            with self.assertRaises(ValueError):
                store.seen('wikidata', 'a'*64)
        finally:
            store.close()

    def test_existing_rows_resume_progress_pages_and_second_run_noop(self):
        store = self.store(fixtures.archive([]))
        try:
            self.seed_legacy(store)
            before = store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall()
            progress = store.report()['member_status']
            first = compact_members(store, max_rows=64)
            second = compact_members(store, after_rowid=first['after_rowid'])
            self.assertEqual(first['compacted_rows']+second['compacted_rows'], len(before))
            self.assertGreater(second['net_reusable_bytes'], 0)
            self.assertEqual(store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall(), before)
            self.assertEqual(store.report()['member_status'], progress)
            pages = store.db.execute('PRAGMA page_count').fetchone()[0]
            with patch.object(store.task, 'check', wraps=store.task.check) as gate:
                self.assertEqual(compact_members(store)['compacted_rows'], 0)
                gate.assert_not_called()
            self.assertEqual(store.db.execute('PRAGMA page_count').fetchone()[0], pages)
            self.assertEqual(compact_members(store, after_rowid=second['after_rowid'])['scanned_rows'], 0)
        finally:
            store.close()

    def test_mixed_member_formats_preserve_all_snapshot_routes_and_rank(self):
        store = self.store(fixtures.archive([]))
        try:
            analysis.AnalysisTests.seed(self, store)
            with store.db:
                store.db.execute('UPDATE members SET sha256=? WHERE source=?', ('c'*64, 'wikidata'))
            before = store.db.execute('SELECT * FROM members ORDER BY source,locator').fetchall()
            rank = canonical(rank_global(store))
            compact_members(store)
            for frozen in (freeze_input(store), freeze_input(store, scope='global-ranking'), freeze_rows(store)):
                with self.subTest(filename=frozen['filename']):
                    with frozen_reader(store, frozen['filename']):
                        self.assertEqual(store.db.execute('SELECT * FROM members ORDER BY source,locator').fetchall(), before)
                        self.assertEqual(canonical(rank_global(store)), rank)
            self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
        finally:
            store.close()

    def test_migration_failure_rolls_back_physical_digest_and_reason(self):
        store = self.store(fixtures.archive([]))
        try:
            self.seed_legacy(store, 20)
            reader = store.db.cursor(); reader.row_factory = None
            before = reader.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall()
            with patch.object(store.task, 'check_memory', side_effect=[None, BudgetStop('injected')]):
                with self.assertRaises(BudgetStop):
                    compact_members(store)
            self.assertFalse(store.db.in_transaction)
            self.assertEqual(reader.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall(), before)
            reader.close()
        finally:
            store.close()

    def test_cli_excludes_other_writer_and_repeated_execution_does_not_grow(self):
        store = self.store(fixtures.archive([]))
        self.seed_legacy(store, 20)
        command = [sys.executable, '-B', '-m', 'gapi', 'compact-member-digests', '--task', str(self.path)]
        try:
            denied = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertNotEqual(denied.returncode, 0)
        finally:
            store.close()
        for count in (20, 0):
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['compacted_rows'], count)

    def test_process_exit_before_and_after_real_compaction_commit(self):
        child = '''
import os,sys
from pathlib import Path
from gapi.career_store import CareerStore
from gapi.queue_storage import compact_members
from gapi.task import Task
store=CareerStore(Task(Path(sys.argv[1])))
if sys.argv[2]=='before':
    store.db.set_trace_callback(lambda sql: os._exit(73) if sql=='COMMIT' else None)
compact_members(store)
os._exit(73)
'''
        for point in ('before', 'after'):
            store = self.store(fixtures.archive([]))
            with store.db:
                store.db.execute('DELETE FROM members')
            self.seed_legacy(store, 20)
            before = store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall()
            store.close()
            result = subprocess.run([sys.executable, '-B', '-c', child, str(self.path), point],
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 73, result.stderr)
            store = CareerStore(Task(self.path))
            try:
                self.assertEqual(store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall(), before)
                packed = store.db.execute("SELECT count(*) FROM members WHERE typeof(sha256)='blob'").fetchone()[0]
                self.assertEqual(packed, 20 if point=='after' else 0)
                self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
            finally:
                store.close()

    def test_limits_side_effects_and_large_legacy_rows_are_preserved(self):
        store = self.store(fixtures.archive([]))
        try:
            self.seed_legacy(store, 1)
            for kwargs in ({'after_rowid':-1}, {'max_rows':True}, {'max_seconds':float('nan')}):
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    compact_members(store, **kwargs)
            with store.db:
                store.db.execute('UPDATE members SET reason=?', ('unique legacy reason '*100,))
            result = compact_members(store)
            self.assertEqual(result['skipped_large_rows'], 1)
            self.assertEqual(result['compacted_rows'], 0)
            with store.db:
                store.db.execute('CREATE TABLE dependent(locator TEXT REFERENCES members(locator))')
            with self.assertRaisesRegex(ValueError, 'foreign keys'):
                compact_members(store)
            with store.db:
                store.db.execute('DROP TABLE dependent')
                store.db.execute('CREATE TRIGGER member_delete AFTER DELETE ON members BEGIN SELECT 1; END')
            with self.assertRaisesRegex(ValueError, 'triggers'):
                compact_members(store)
        finally:
            store.close()
