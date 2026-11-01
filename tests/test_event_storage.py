import json
import zlib
import unittest
from unittest.mock import patch

from gapi.event_storage import pack, unpack, MAGIC, compact_events, logical_event_hash
from gapi.frozen_input import freeze_input, frozen_reader
from gapi.monitoring import snapshot_report
from gapi.task import canonical
from gapi.global_analysis import rank_global
import test_streaming as fixtures
from test_streaming import archive
import test_global_analysis as analysis


class EventStorageTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_block_codec_exact_deterministic_utf8_and_legacy_independence(self):
        from gapi.event_storage import encode_event_block,decode_event_block,MAX_BYTES
        texts=['研究履历\n\"evidence\"','{}','x'*MAX_BYTES]
        key,body=encode_event_block(iter(texts))
        import hashlib
        framed=b''.join(len(t.encode('utf-8')).to_bytes(4,'big')+t.encode('utf-8') for t in texts)
        self.assertEqual(zlib.decompress(body),framed)
        self.assertEqual(key,hashlib.sha256(framed).digest())
        self.assertEqual(encode_event_block(texts),(key,body))
        self.assertEqual(decode_event_block(key,body),tuple(texts))
        self.assertNotEqual(encode_event_block(reversed(texts))[0],key)
        # Existing readers cannot silently accept a new database representation.
        with self.assertRaises(ValueError):unpack(body)

    def test_block_codec_bounds_iterables_and_accepts_maximum_group(self):
        from gapi.event_storage import encode_event_block,decode_event_block,MAX_BYTES,MAX_BLOCK_EVENTS
        texts=['x'*MAX_BYTES]*MAX_BLOCK_EVENTS
        key,body=encode_event_block(texts)
        self.assertEqual(decode_event_block(key,body),tuple(texts))
        for bad in ([],[''],[b'text'],['x'*(MAX_BYTES+1)],['x']*(MAX_BLOCK_EVENTS+1)):
            with self.subTest(kind=str(type(bad))),self.assertRaises(ValueError):encode_event_block(bad)
        seen=[]
        def endless():
            while True:
                seen.append(1);yield 'x'
        with self.assertRaises(ValueError):encode_event_block(endless())
        self.assertEqual(len(seen),MAX_BLOCK_EVENTS+1)

    def test_block_codec_rejects_corruption_trailing_data_and_bombs(self):
        import hashlib
        from gapi.event_storage import encode_event_block,decode_event_block,MAX_BLOCK_BYTES
        key,body=encode_event_block(['evidence'])
        for digest,value in ((b'bad',body),(b'x'*32,body),(key,body[:-1]),
                             (key,body+b'trailing'),(key,body+body),
                             (key,zlib.compress(b'x'*(MAX_BLOCK_BYTES+1))),
                             (key,b'x'*(MAX_BLOCK_BYTES+65537))):
            with self.subTest(size=len(value)),self.assertRaises(ValueError):decode_event_block(digest,value)
        # Correct hashes must not legitimize malformed frames or invalid UTF-8.
        for raw in (b'',b'abc',b'\0\0\0\0',b'\0\0\0\x02x',
                    b'\0\0\0\x01\xff',(b'\0\0\0\x01x')*129):
            with self.subTest(raw=raw[:8]),self.assertRaises(ValueError):
                decode_event_block(hashlib.sha256(raw).digest(),zlib.compress(raw))

    def test_block_transaction_process_exit_recovers_old_or_committed_state(self):
        """Synthetic file-backed crash test, not a production migration claim."""
        import sqlite3
        import subprocess
        import sys
        from gapi.event_storage import encode_event_block,decode_event_block
        child='''
import os,sqlite3,sys
from gapi.event_storage import encode_event_block
db=sqlite3.connect(sys.argv[1])
db.execute('PRAGMA foreign_keys=ON')
db.execute('PRAGMA journal_mode=DELETE')
db.execute('PRAGMA synchronous=FULL')
key,body=encode_event_block(['synthetic replacement'])
db.execute('BEGIN IMMEDIATE')
db.execute('INSERT INTO blocks VALUES(?,?)',(key,body))
if sys.argv[2]=='block':os._exit(73)
db.execute('UPDATE events SET block_digest=? WHERE id=1',(key,))
db.execute('DELETE FROM blocks WHERE digest!=?',(key,))
if sys.argv[2]=='pointer':os._exit(73)
db.commit()
os._exit(73)
'''
        for point in ('block','pointer','commit'):
            with self.subTest(point=point):
                path=self.root/('codec-crash-'+point+'.sqlite')
                key,body=encode_event_block(['synthetic original'])
                with sqlite3.connect(path) as db:
                    db.execute('CREATE TABLE blocks(digest BLOB PRIMARY KEY,body BLOB NOT NULL)')
                    db.execute('CREATE TABLE events(id INTEGER PRIMARY KEY,block_digest BLOB REFERENCES blocks(digest))')
                    db.execute('INSERT INTO blocks VALUES(?,?)',(key,body))
                    db.execute('INSERT INTO events VALUES(1,?)',(key,))
                db.close()
                result=subprocess.run([sys.executable,'-B','-c',child,str(path),point],
                                      capture_output=True,text=True,timeout=15)
                self.assertEqual(result.returncode,73,result.stderr)
                with sqlite3.connect(path) as db:
                    rows=db.execute('SELECT digest,body FROM blocks JOIN events ON digest=block_digest').fetchall()
                    self.assertEqual(len(rows),1)
                    self.assertEqual(decode_event_block(*rows[0]),
                                     ('synthetic replacement' if point=='commit' else 'synthetic original',))
                    self.assertEqual(db.execute('SELECT count(*) FROM blocks').fetchone()[0],1)
                    self.assertEqual(db.execute('PRAGMA quick_check').fetchone()[0],'ok')
                    self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])
                db.close()

    def test_page_repack_preserves_rows_rank_and_committed_resume(self):
        from gapi.event_storage import repack_event_pages
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            raw=s.db.cursor();raw.row_factory=None
            before=raw.execute('SELECT rowid,* FROM events ORDER BY rowid').fetchall()
            rank=canonical(rank_global(s,public=True))
            first=repack_event_pages(s,max_events=2)
            self.assertEqual(first['scanned_events'],2)
            second=repack_event_pages(s,after_rowid=first['after_rowid'])
            self.assertEqual(second['scanned_events'],len(before)-2)
            self.assertEqual(repack_event_pages(s,after_rowid=second['after_rowid'])['scanned_events'],0)
            self.assertEqual(raw.execute('SELECT rowid,* FROM events ORDER BY rowid').fetchall(),before)
            repack_event_pages(s)
            self.assertEqual(raw.execute('SELECT rowid,* FROM events ORDER BY rowid').fetchall(),before)
            self.assertEqual(canonical(rank_global(s,public=True)),rank)
            self.assertEqual(s.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
        finally:s.close()

    def test_page_repack_rolls_back_failure_and_rejects_side_effect_schema(self):
        from gapi.event_storage import repack_event_pages
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            raw=s.db.cursor();raw.row_factory=None
            before=raw.execute('SELECT rowid,* FROM events ORDER BY rowid').fetchall()
            with patch.object(s.task,'check_memory',side_effect=[0,BudgetStop('injected')]):
                with self.assertRaises(BudgetStop):repack_event_pages(s)
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(raw.execute('SELECT rowid,* FROM events ORDER BY rowid').fetchall(),before)
            with s.db:s.db.execute('CREATE TABLE dependent(event TEXT REFERENCES events(id))')
            with self.assertRaisesRegex(ValueError,'incoming foreign keys'):repack_event_pages(s)
            with s.db:
                s.db.execute('DROP TABLE dependent')
                s.db.execute('CREATE TRIGGER event_delete AFTER DELETE ON events BEGIN SELECT 1; END')
            with self.assertRaisesRegex(ValueError,'triggers'):repack_event_pages(s)
        finally:s.close()

    def test_exact_utf8_roundtrip_and_corruption_bounds(self):
        text=json.dumps({'evidence':'研究履历'*600},ensure_ascii=False)
        packed=pack(text)
        self.assertIsInstance(packed,bytes)
        self.assertEqual(unpack(packed),text)
        self.assertEqual(pack('{}'),'{}')
        bad=[b'other',packed[:-1],packed+b'trailing',MAGIC+b'\x00\x01\x00\x00'+packed[len(MAGIC)+4:],
             MAGIC+(1).to_bytes(4,'big')+zlib.compress(b'x'*1000000)]
        for value in bad:
            with self.subTest(value=value[:15]),self.assertRaises(ValueError):unpack(value)

    def test_compaction_preserves_hash_rank_snapshot_monitor_and_idempotency(self):
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            # Retain a real-sized, redundant evidence structure in one old TEXT.
            identifier,payload=s.db.execute('SELECT id,payload FROM events LIMIT 1').fetchone()
            item=json.loads(payload);item['storage_test_evidence']='证据'*1000
            with s.db:s.db.execute('UPDATE events SET payload=? WHERE id=?',(canonical(item).decode(),identifier))
            before_hash=logical_event_hash(s.db);before_rank=canonical(rank_global(s,public=True))
            before_report=s.report()
            result=compact_events(s)
            self.assertGreater(result['compacted_events'],0)
            self.assertEqual(logical_event_hash(s.db),before_hash)
            self.assertEqual(canonical(rank_global(s,public=True)),before_rank)
            self.assertEqual(s.report()['event_status'],before_report['event_status'])
            self.assertEqual(compact_events(s)['compacted_events'],0)
            with s.db:s.db.execute("DELETE FROM events WHERE id='nonexistent'")
            self.assertEqual(snapshot_report(s.task)['events'],before_report['events'])
            snapshot=freeze_input(s)
            with frozen_reader(s,snapshot['filename']):
                self.assertEqual(logical_event_hash(s.db),before_hash)
                self.assertEqual(canonical(rank_global(s,public=True)),before_rank)
            raw=s.db.execute('SELECT payload AS stored_payload FROM events WHERE id=?',(identifier,)).fetchone()[0]
            self.assertIsInstance(raw,bytes)
        finally:s.close()

    def test_failed_compaction_batch_rolls_back_and_new_writes_decode(self):
        s=self.store(archive([]))
        try:
            analysis.AnalysisTests.seed(self,s)
            rows=list(s.db.execute('SELECT id,payload FROM events ORDER BY id'))
            with s.db:
                for identifier,payload in rows:
                    item=json.loads(payload);item['evidence_padding']='e'*3000
                    s.db.execute('UPDATE events SET payload=? WHERE id=?',(canonical(item).decode(),identifier))
            before=logical_event_hash(s.db)
            original=s.db.execute('SELECT count(*) FROM events WHERE typeof(payload)=\'text\'').fetchone()[0]
            import gapi.event_storage as storage
            real_pack=storage.pack;calls=[]
            def fail(text):
                calls.append(text)
                if len(calls)==2:raise ValueError('injected migration failure')
                return real_pack(text)
            with patch('gapi.event_storage.pack',side_effect=fail),self.assertRaises(ValueError):compact_events(s)
            self.assertEqual(logical_event_hash(s.db),before)
            self.assertEqual(s.db.execute("SELECT count(*) FROM events WHERE typeof(payload)='text'").fetchone()[0],original)
            item=json.loads(rows[0][1]);item['locator']='fresh-compressed';item['evidence_padding']='e'*3000
            with s.member('wikidata','fresh-compressed'):s.event(item)
            stored=s.db.execute("SELECT payload FROM events WHERE locator='fresh-compressed'").fetchone()[0]
            self.assertEqual(json.loads(stored),item)
        finally:s.close()
