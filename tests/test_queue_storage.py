import sqlite3
import unittest
from unittest.mock import patch

import test_streaming as fixtures
from test_streaming import archive
from gapi.career_store import CareerStore
from gapi.event_storage import configure_reader
from gapi.queue_storage import pack_digest, unpack_digest, compact_queue
from gapi.task import BudgetStop


class QueueStorageTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_codec_preserves_case_legacy_and_rejects_corruption(self):
        for value in ['a'*64,'0'*64,'F'*64,'legacy','研究', '']:
            self.assertEqual(unpack_digest(pack_digest(value)),value)
        self.assertIsInstance(pack_digest('F'*64),str)
        for value in [b'unknown',pack_digest('a'*64)[:-1],pack_digest('a'*64)+b'x']:
            with self.assertRaises(ValueError):unpack_digest(value)
        d=sqlite3.connect(':memory:');configure_reader(d)
        try:
            encoded=pack_digest('b'*64)
            self.assertEqual(d.execute('SELECT ? AS query_sha256,? AS other',(encoded,encoded)).fetchone(),('b'*64,encoded))
        finally:d.close()

    def test_live_progress_order_reopen_and_idempotence(self):
        s=self.store(archive([]))
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-wikidata','Q'+str(n),'a'*64,'b'*64) for n in range(4000)])
            before=s.candidate_progress();pending=s.pending('ingest-wikidata',200)
            first=compact_queue(s,max_rows=64)
            self.assertEqual(first['compacted_rows'],64)
            repacked=compact_queue(s,after_rowid=first['after_rowid'])
            self.assertGreater(repacked['net_reusable_bytes'],0)
            self.assertEqual(s.candidate_progress(),before)
            self.assertEqual(s.pending('ingest-wikidata',200),pending)
            with patch.object(s.task,'check',wraps=s.task.check) as gate:
                self.assertEqual(compact_queue(s)['compacted_rows'],0)
                gate.assert_not_called()
            s.queue_page('feed','ingest-wikidata','before','after','c'*64,'d'*64,['Q99999'],1)
            self.assertEqual(s.db.execute("SELECT typeof(query_sha256) FROM discovery WHERE identifier='Q99999'").fetchone()[0],'blob')
            s.close();s=CareerStore(s.task)
            self.assertEqual(s.db.execute("SELECT query_sha256,response_sha256 FROM discovery WHERE identifier='Q99999'").fetchone(),('c'*64,'d'*64))
            self.assertEqual(s.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
        finally:s.close()

    def test_compaction_rolls_back_fault_and_rejects_side_effects(self):
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('source','Q1','a'*64,'b'*64))
            raw=lambda:s.db.execute('SELECT rowid,typeof(query_sha256),query_sha256 FROM discovery').fetchall()
            before=raw()
            with patch.object(s.task,'check_memory',side_effect=[0,BudgetStop('injected')]):
                with self.assertRaises(BudgetStop):compact_queue(s)
            self.assertFalse(s.db.in_transaction);self.assertEqual(raw(),before)
            with s.db:s.db.execute('CREATE TRIGGER side_effect AFTER DELETE ON discovery BEGIN SELECT 1; END')
            with self.assertRaisesRegex(ValueError,'triggers'):compact_queue(s)
            with s.db:
                s.db.execute('DROP TRIGGER side_effect')
                s.db.execute('CREATE TABLE dependent(candidate TEXT REFERENCES discovery(identifier))')
            with self.assertRaisesRegex(ValueError,'foreign keys'):compact_queue(s)
        finally:s.close()

    def test_member_admission_uses_real_freelist_and_preserves_reserve(self):
        s=self.store(archive([]))
        try:
            # Empty freelist can now admit an enforced smaller transaction.
            # Explicit worst-case maintenance reservation still refuses it.
            used=s.task.config['budget_bytes']-s.task.config['reserve_bytes']-46*1024**2
            with patch.object(s.task,'used',return_value=used):
                with self.assertRaises(BudgetStop):
                    with s.member('test','empty',max_growth_bytes=48*1024**2):pass
            with s.db:
                s.db.execute('CREATE TABLE filler(value BLOB)')
                s.db.executemany('INSERT INTO filler VALUES(?)',[(b'x'*8192,)]*1024)
            with s.db:s.db.execute('DELETE FROM filler')
            free=s.db.execute('PRAGMA freelist_count').fetchone()[0]*4096
            self.assertGreater(free,2*1024**2)
            pages=s.db.execute('PRAGMA page_count').fetchone()[0]
            with patch.object(s.task,'used',return_value=used):
                with s.member('test','reuse'):
                    s.db.execute('INSERT INTO filler VALUES(?)',(b'new'*1024,))
            self.assertEqual(s.db.execute('PRAGMA page_count').fetchone()[0],pages)
            # Free pages cannot bypass the fixed reserve or disk-space check.
            with patch.object(s.task,'used',return_value=s.task.config['budget_bytes']-s.task.config['reserve_bytes']):
                with self.assertRaises(BudgetStop):
                    with s.member('test','reserve'):pass
        finally:s.close()
