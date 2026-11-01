import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch
import zlib

from gapi.evidence_storage import MAGIC, MAX_BYTES, pack_url, unpack_url, repack_urls
from gapi.task import BudgetStop, canonical
import test_streaming as fixtures
from test_streaming import archive


class EvidenceStorageTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def seed(self, store):
        with store.db:
            for i in range(9):
                store.db.execute('insert into member_parts values(?,?,?,?,?,?,?)',
                    ('wikidata', 'locator-' + str(i), 'entity-batch/v1',
                     'https://www.wikidata.org/w/api.php?action=wbgetentities&ids=' + '|'.join('Q' + str(j) for j in range(80)) + '&format=json',
                     hashlib.sha256(str(i).encode()).hexdigest(), 123456, '2026-11-01T00:00:00+00:00'))

    def test_codec_exact_unicode_legacy_and_hostile_frames(self):
        for text in ('https://example.org/a', '', 'https://example.org/路径?' + 'field=中文&' * 300, 'x' * (MAX_BYTES + 1)):
            value = pack_url(text)
            self.assertEqual(unpack_url(value), text)
            if isinstance(value, bytes):
                self.assertLess(len(value), len(text.encode()))
        for value in (None, 0, b'unknown', MAGIC, MAGIC + (MAX_BYTES + 1).to_bytes(4, 'big') + zlib.compress(b'a'),
                      MAGIC + (1).to_bytes(4, 'big') + zlib.compress(b'a' * (MAX_BYTES + 1)),
                      MAGIC + (1).to_bytes(4, 'big') + zlib.compress(b'\xff')):
            with self.assertRaises(ValueError):
                unpack_url(value)
        valid = pack_url('https://example.org/?' + 'a=1&' * 1000)
        for value in (valid[:-1], valid + b'x', valid[:-1] + bytes([valid[-1] ^ 1])):
            with self.assertRaises(ValueError):
                unpack_url(value)

    def test_resume_identity_rowids_and_idempotence(self):
        store = self.store(archive([]))
        try:
            self.seed(store)
            query = 'select rowid,* from member_parts order by source,locator,part'
            original = store.db.execute(query).fetchall()
            first = repack_urls(store, max_rows=3)
            self.assertEqual(first['scanned_rows'], 3)
            self.assertEqual(first['repacked_rows'], 3)
            second = repack_urls(store, after=first['after'])
            self.assertEqual(second['scanned_rows'], 6)
            self.assertEqual(store.db.execute(query).fetchall(), original)
            self.assertEqual(repack_urls(store)['repacked_rows'], 0)
            self.assertEqual(repack_urls(store, after=second['after'])['scanned_rows'], 0)
            raw = store.db.cursor()
            raw.row_factory = None
            self.assertTrue(all(isinstance(row[0], bytes) for row in raw.execute('select url from member_parts')))
            self.assertEqual(store.db.execute('pragma quick_check').fetchone()[0], 'ok')
        finally:
            store.close()

    def test_cli_command_keeps_explicit_resume_key_and_limits(self):
        import argparse
        from gapi.stream_cli import register
        parser = argparse.ArgumentParser()
        register(parser.add_subparsers(dest='command'))
        args = parser.parse_args(['repack-evidence-urls', '--after', 'wikidata', 'Q1@2', 'entity-batch/v1',
                                  '--max-rows', '3', '--max-seconds', '2'])
        self.assertEqual(args.after, ['wikidata', 'Q1@2', 'entity-batch/v1'])
        self.assertEqual(args.max_rows, 3)
        self.assertEqual(args.max_seconds, 2)

    def test_shorter_urls_release_pages_without_changing_rowids(self):
        store = self.store(archive([]))
        try:
            with store.db:
                for i in range(300):
                    store.db.execute('insert into member_parts values(?,?,?,?,?,?,?)',
                        ('wikidata', f'item-{i:04}', 'entity-batch/v1',
                         'https://example.org/?' + 'evidence=unchanged&' * 170,
                         hashlib.sha256(str(i).encode()).hexdigest(), 1234, '2026-11-01'))
            original = store.db.execute('select rowid,* from member_parts order by rowid').fetchall()
            result = repack_urls(store)
            self.assertEqual(result['repacked_rows'], 300)
            self.assertGreater(result['net_reusable_bytes'], 0)
            self.assertEqual(result['physical_file_bytes_reclaimed'], 0)
            self.assertEqual(store.db.execute('select rowid,* from member_parts order by rowid').fetchall(), original)
            self.assertEqual(repack_urls(store)['net_reusable_bytes'], 0)
            self.assertEqual(store.db.execute('pragma quick_check').fetchone()[0], 'ok')
        finally:
            store.close()

    def test_reinsert_rejects_unknown_columns_and_incoming_dependencies(self):
        store = self.store(archive([]))
        try:
            self.seed(store)
            original = store.db.execute('select rowid,* from member_parts order by rowid').fetchall()
            store.db.execute('create table dependent(source text,locator text,part text, '
                             'foreign key(source,locator,part) references member_parts(source,locator,part))')
            with self.assertRaisesRegex(ValueError, 'incoming foreign keys'):
                repack_urls(store)
            self.assertEqual(store.db.execute('select rowid,* from member_parts order by rowid').fetchall(), original)
            store.db.execute('drop table dependent')
            store.db.execute('alter table member_parts add column extra text')
            with self.assertRaisesRegex(ValueError, 'columns'):
                repack_urls(store)
        finally:
            store.close()

    def test_interrupted_transaction_rollback_and_bounds(self):
        store = self.store(archive([]))
        try:
            self.seed(store)
            original = store.db.execute('select rowid,* from member_parts order by rowid').fetchall()
            with patch.object(store.task, 'check_memory', side_effect=[0, BudgetStop('injected')]):
                with self.assertRaises(BudgetStop):
                    repack_urls(store)
            self.assertFalse(store.db.in_transaction)
            self.assertEqual(store.db.execute('select rowid,* from member_parts order by rowid').fetchall(), original)
            raw = store.db.cursor()
            raw.row_factory = None
            self.assertTrue(all(isinstance(row[0], str) for row in raw.execute('select url from member_parts')))
            for args in ({'after': ('only-one',)}, {'max_rows': 0}, {'max_seconds': float('nan')}):
                with self.assertRaises(ValueError):
                    repack_urls(store, **args)
            store.db.execute('create trigger evidence_side_effect after update on member_parts begin select 1; end')
            with self.assertRaisesRegex(ValueError, 'triggers'):
                repack_urls(store)
        finally:
            store.close()

    def test_private_row_snapshot_restores_without_external_dictionary(self):
        from gapi.row_snapshot import ranking_lines, freeze_rows, archive_path, restore_rows
        from gapi.event_storage import configure_reader
        store = self.store(archive([]))
        try:
            self.seed(store)
            expected = store.db.execute('select * from member_parts order by source,locator,part').fetchall()
            logical_before = hashlib.sha256(b''.join(ranking_lines(store))).hexdigest()
            repack_urls(store)
            self.assertEqual(hashlib.sha256(b''.join(ranking_lines(store))).hexdigest(), logical_before)
            frozen = freeze_rows(store)
            target = store.task.root / 'restored.sqlite'
            target.touch(exist_ok=False)
            db = restore_rows(archive_path(store, frozen['filename']), target, store.task)
            try:
                configure_reader(db)
                self.assertEqual(db.execute('select * from member_parts order by source,locator,part').fetchall(), expected)
            finally:
                db.close()
        finally:
            store.close()

    def test_cross_source_decisions_survive_compressed_url_reads(self):
        from test_corroboration import CorroborationTests
        from gapi.corroboration import decisions
        store = self.store(archive([]))
        try:
            CorroborationTests.seed(self, store)
            with store.db:
                rows = store.db.execute("select id,payload from events where source='orcid-public-api'").fetchall()
                for identifier, text in rows:
                    record = json.loads(text)
                    record['url'] += '?' + 'evidence=unchanged&' * 100
                    store.db.execute('update events set payload=? where id=?', (json.dumps(record), identifier))
                    store.db.execute("update member_parts set url=? where source='orcid-public-api'", (record['url'],))
            before = decisions(store)
            self.assertEqual(len(before), 2)
            self.assertGreater(repack_urls(store)['repacked_rows'], 0)
            self.assertEqual(decisions(store), before)
        finally:
            store.close()
