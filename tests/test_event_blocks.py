"""Opt-in event blocks preserve ingestion, queries and frozen recovery."""
import json
import subprocess
import sys
import unittest
from unittest.mock import patch

from gapi.event_storage import (
    BLOCK_MAGIC, decode_event, logical_event_hash, repack_event_blocks,
)
from gapi.frozen_input import freeze_input, frozen_reader
from gapi.global_analysis import rank_global
from gapi.row_snapshot import freeze_rows
from gapi.task import BudgetStop, canonical
import test_global_analysis as analysis
import test_streaming as fixtures


class EventBlockTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_grouped_blocks_preserve_rows_and_reduce_transactions(self):
        import io
        from test_archive_commit_batches import members, SOURCE
        from gapi.orcid_stream import stream_archive
        store = self.store(fixtures.archive(members(300)))
        try:
            stream_archive(store, SOURCE, io.BytesIO(fixtures.archive(members(300))))
            expected = logical_event_hash(store.db)
            with patch.object(store, 'member', wraps=store.member) as commits:
                result = repack_event_blocks(store, commit_blocks=8)
            self.assertEqual(commits.call_count, 1)
            self.assertEqual(result['scanned_events'], 300)
            self.assertEqual(logical_event_hash(store.db), expected)
            self.assertEqual(repack_event_blocks(store)['repacked_events'], 0)
        finally:
            store.close()

    def test_failure_in_second_grouped_block_rolls_back_first_block(self):
        import io
        import gapi.event_storage as storage
        from test_archive_commit_batches import members, SOURCE
        from gapi.orcid_stream import stream_archive
        data = fixtures.archive(members(300))
        store = self.store(data)
        try:
            stream_archive(store, SOURCE, io.BytesIO(data))
            expected = logical_event_hash(store.db)
            original = storage.encode_event_block
            calls = []
            def fail(texts):
                calls.append(1)
                if len(calls) == 2:
                    raise BudgetStop('injected later block capacity failure')
                return original(texts)
            with patch.object(storage, 'encode_event_block', side_effect=fail):
                with self.assertRaises(BudgetStop):
                    repack_event_blocks(store, commit_blocks=8)
            self.assertEqual(logical_event_hash(store.db), expected)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events WHERE substr(payload,1,7)=?', (BLOCK_MAGIC,)).fetchone()[0], 0)
            self.assertFalse(store.db.in_transaction)
        finally:
            store.close()

    def test_larger_group_preserves_cursor_rolls_back_and_resumes(self):
        import io
        import gapi.event_storage as storage
        from test_archive_commit_batches import members, SOURCE
        from gapi.orcid_stream import stream_archive
        data = fixtures.archive(members(2050))
        store = self.store(data)
        try:
            stream_archive(store, SOURCE, io.BytesIO(data))
            expected = logical_event_hash(store.db)
            original = storage.encode_event_block
            calls = []
            def fail(texts):
                calls.append(1)
                if len(calls) == 17:
                    raise BudgetStop('injected final block capacity failure')
                return original(texts)
            with patch.object(storage, 'encode_event_block', side_effect=fail):
                with self.assertRaisesRegex(BudgetStop, 'final block'):
                    repack_event_blocks(store, commit_blocks=64)
            self.assertEqual(logical_event_hash(store.db), expected)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events WHERE substr(payload,1,7)=?', (BLOCK_MAGIC,)).fetchone()[0], 0)
            self.assertFalse(store.db.in_transaction)
            with patch.object(store, 'member', wraps=store.member) as commits:
                result = repack_event_blocks(store, commit_blocks=64)
            self.assertEqual(commits.call_count, 1)
            self.assertEqual(result['repacked_events'], 2050)
            self.assertEqual(logical_event_hash(store.db), expected)
            self.assertEqual(repack_event_blocks(store, commit_blocks=64)['repacked_events'], 0)
            self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
        finally:
            store.close()

    def test_event_cache_reuses_interleaved_blocks_and_obeys_byte_limit(self):
        import hashlib
        from gapi.event_storage import _decoded_stored_block, encode_event_block
        from gapi.projection_storage import CACHE_BYTES
        cached = _decoded_stored_block
        cached.cache_clear()
        self.addCleanup(cached.cache_clear)
        blocks = [encode_event_block([str(n)*100]) for n in range(12)]
        for key, body in blocks:
            cached(key, body)
        with patch('gapi.event_storage.decode_event_block', side_effect=AssertionError('unexpected decode')):
            for key, body in blocks:
                cached(key, body)
        self.assertEqual(cached.cache_info().hits, 12)
        cached.cache_clear()
        for n in range(8):
            key, body = encode_event_block([chr(65+n)*16000]*128)
            self.assertEqual(len(cached(key, body)), 128)
            self.assertLessEqual(cached.cache_info().bytes, CACHE_BYTES)
        self.assertLess(cached.cache_info().currsize, 8)
        count = cached.cache_info().currsize
        with self.assertRaises(ValueError):
            cached(hashlib.sha256(b'wrong').digest(), body)
        self.assertEqual(cached.cache_info().currsize, count)

    def seed(self, store):
        analysis.AnalysisTests.seed(self, store)
        rows = list(store.db.execute('SELECT id,payload FROM events ORDER BY id'))
        with store.db:
            for identifier, payload in rows:
                value = json.loads(payload)
                value['test_evidence'] = '多语言证据\n' * 300
                store.db.execute('UPDATE events SET payload=? WHERE id=?',
                                 (canonical(value).decode(), identifier))

    def test_queries_rank_and_all_snapshot_routes_preserve_exact_rows(self):
        store = self.store(fixtures.archive([]))
        try:
            self.seed(store)
            before = list(store.db.execute('SELECT rowid,* FROM events ORDER BY id'))
            expected_hash = logical_event_hash(store.db)
            expected_rank = canonical(rank_global(store))
            result = repack_event_blocks(store)
            self.assertEqual(result['repacked_events'], len(before))
            self.assertEqual(list(store.db.execute('SELECT rowid,* FROM events ORDER BY id')), before)
            self.assertEqual(logical_event_hash(store.db), expected_hash)
            self.assertEqual(canonical(rank_global(store)), expected_rank)
            sql_rows = store.db.execute('SELECT gapi_event_text(payload) FROM events ORDER BY id').fetchall()
            self.assertEqual([row[0] for row in sql_rows], [row[-1] for row in before])
            snapshots = [freeze_input(store), freeze_input(store, scope='global-ranking'), freeze_rows(store)]
            for snapshot in snapshots:
                with self.subTest(filename=snapshot['filename']):
                    with frozen_reader(store, snapshot['filename']):
                        self.assertEqual(logical_event_hash(store.db), expected_hash)
                        self.assertEqual(canonical(rank_global(store)), expected_rank)
            self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
        finally:
            store.close()

    def test_replay_and_committed_cursor_do_not_grow_blocks(self):
        store = self.store(fixtures.archive([]))
        try:
            self.seed(store)
            first = repack_event_blocks(store, max_events=2)
            second = repack_event_blocks(store, after_rowid=first['after_rowid'])
            self.assertEqual(first['repacked_events']+second['repacked_events'], 4)
            before = store.db.execute('SELECT digest,packed FROM event_blocks ORDER BY digest').fetchall()
            pages = store.db.execute('PRAGMA page_count').fetchone()[0]
            self.assertEqual(repack_event_blocks(store)['repacked_events'], 0)
            self.assertEqual(repack_event_blocks(store, after_rowid=second['after_rowid'])['scanned_events'], 0)
            self.assertEqual(store.db.execute('SELECT digest,packed FROM event_blocks ORDER BY digest').fetchall(), before)
            self.assertEqual(store.db.execute('PRAGMA page_count').fetchone()[0], pages)
        finally:
            store.close()

    def test_failures_rollback_block_references_and_rows(self):
        store = self.store(fixtures.archive([]))
        try:
            self.seed(store)
            before = logical_event_hash(store.db)
            with patch.object(store.task, 'check_memory', side_effect=[None, BudgetStop('injected capacity')]):
                with self.assertRaises(BudgetStop):
                    repack_event_blocks(store)
            self.assertEqual(logical_event_hash(store.db), before)
            self.assertIsNone(store.db.execute("SELECT name FROM sqlite_master WHERE name='event_blocks'").fetchone())
            self.assertEqual(repack_event_blocks(store)['repacked_events'], 4)
        finally:
            store.close()

    def test_cached_block_cannot_hide_mutation_missing_rows_or_bad_slots(self):
        store = self.store(fixtures.archive([]))
        try:
            self.seed(store)
            repack_event_blocks(store)
            ref = store.db.execute('SELECT payload AS stored_payload FROM events LIMIT 1').fetchone()[0]
            self.assertTrue(ref.startswith(BLOCK_MAGIC))
            self.assertIsInstance(decode_event(store.db, ref), str)
            with self.assertRaisesRegex(ValueError, 'slot'):
                decode_event(store.db, ref[:-2]+b'\xff\xff')
            with self.assertRaisesRegex(ValueError, 'reference'):
                decode_event(store.db, ref[:-1])
            with store.db:
                store.db.execute('UPDATE event_blocks SET packed=?', (b'corrupt',))
            with self.assertRaises(ValueError):
                decode_event(store.db, ref)
            with store.db:
                store.db.execute('DELETE FROM event_blocks')
            with self.assertRaisesRegex(ValueError, 'missing'):
                decode_event(store.db, ref)
        finally:
            store.close()

    def test_conflicting_block_rolls_back_without_changing_events(self):
        from gapi.event_storage import BLOCK_DDL, encode_event_block
        store = self.store(fixtures.archive([]))
        try:
            self.seed(store)
            before = logical_event_hash(store.db)
            rows = store.db.execute('SELECT payload FROM events ORDER BY rowid').fetchall()
            key, _ = encode_event_block(row[0] for row in rows)
            with store.db:
                store.db.execute(BLOCK_DDL)
                store.db.execute('INSERT INTO event_blocks VALUES(?,?)', (key, b'corrupt'))
            with self.assertRaises(ValueError):
                repack_event_blocks(store)
            self.assertEqual(logical_event_hash(store.db), before)
        finally:
            store.close()

    def test_real_migration_process_exit_before_and_after_commit(self):
        from gapi.career_store import CareerStore
        from gapi.task import Task
        child = '''
import os,sys
from contextlib import contextmanager
from pathlib import Path
from gapi.career_store import CareerStore
from gapi.event_storage import repack_event_blocks
from gapi.task import Task
store=CareerStore(Task(Path(sys.argv[1])))
original=store.member
@contextmanager
def crashing(*args,**kwargs):
    with original(*args,**kwargs):
        yield
        if sys.argv[2]=='before': os._exit(73)
    os._exit(73)
store.member=crashing
repack_event_blocks(store)
'''
        for point in ('before', 'after'):
            with self.subTest(point=point):
                store = self.store(fixtures.archive([]))
                self.seed(store)
                expected = logical_event_hash(store.db)
                store.close()
                result = subprocess.run([sys.executable, '-B', '-c', child, str(self.path), point],
                                        capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 73, result.stderr)
                store = CareerStore(Task(self.path))
                try:
                    self.assertEqual(logical_event_hash(store.db), expected)
                    self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
                    migrated = store.db.execute("SELECT count(*) FROM events WHERE substr(payload,1,7)=?", (BLOCK_MAGIC,)).fetchone()[0]
                    self.assertEqual(migrated, 4 if point == 'after' else 0)
                    # Restore original payloads for the next crash case, without
                    # duplicating fixture identities or leaking the writer lock.
                    with store.db:
                        store.db.execute('DELETE FROM events')
                finally:
                    store.close()

    def test_invalid_limits_and_side_effect_schema_are_rejected(self):
        store = self.store(fixtures.archive([]))
        try:
            for kwargs in ({'after_rowid':-1}, {'max_events':True}, {'max_seconds':float('inf')},
                           {'commit_blocks':0}, {'commit_blocks':65}, {'commit_blocks':True}):
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    repack_event_blocks(store, **kwargs)
            with store.db:
                store.db.execute('CREATE TABLE dependent(event TEXT REFERENCES events(id))')
            with self.assertRaisesRegex(ValueError, 'incoming foreign keys'):
                repack_event_blocks(store)
            with store.db:
                store.db.execute('DROP TABLE dependent')
                store.db.execute('CREATE TRIGGER event_delete AFTER DELETE ON events BEGIN SELECT 1; END')
            with self.assertRaisesRegex(ValueError, 'triggers'):
                repack_event_blocks(store)
        finally:
            store.close()

    def test_cli_uses_existing_writer_exclusion_and_reports_idempotent_replay(self):
        from gapi.career_store import CareerStore
        from gapi.task import Task
        store = self.store(fixtures.archive([]), source_compression=True)
        self.seed(store)
        expected = logical_event_hash(store.db)
        command = [sys.executable, '-B', '-m', 'gapi', 'repack-event-pages',
                   '--task', str(self.path), '--blocks', '--max-events', '4']
        try:
            denied = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertNotEqual(denied.returncode, 0)
            self.assertEqual(logical_event_hash(store.db), expected)
        finally:
            store.close()
        for expected_changes in (4, 0):
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['repacked_events'], expected_changes)
        store = CareerStore(Task(self.path))
        try:
            self.assertEqual(logical_event_hash(store.db), expected)
        finally:
            store.close()
