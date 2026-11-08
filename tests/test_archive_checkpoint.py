"""Real SQLite atomic checkpoint storage; no decoder/HTTP capability claim."""
import copy
import hashlib
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

import test_streaming as fixtures
from gapi.archive_hash import ArchiveHash
from gapi.archive_recovery import load_stream_checkpoint, save_stream_checkpoint
from gapi.career_store import CareerStore, source_identity
from gapi.task import BudgetStop, Task

SOURCE = 'orcid-2026-summary'


@unittest.skipUnless(os.name == 'nt', 'optional Windows native checksum backend')
class ArchiveCheckpointTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    store = fixtures.StreamingTests.store

    def tearDown(self):
        for store in getattr(self,'opened_stores',[]):
            store.close()
        fixtures.StreamingTests.tearDown(self)

    def prepared(self):
        store = self.store(fixtures.archive([]))
        store.start(SOURCE)
        self.opened_stores = [store]
        states = {}
        for algorithm in ('md5','sha256'):
            h = ArchiveHash(algorithm); h.update(b'prefix')
            states[algorithm] = h.snapshot()
        index = b'opaque fixture index; decoder validation is a separate gate'
        locator = f'summaries/123/{fixtures.PERSON}.xml'
        checksum = hashlib.sha256(b'member').hexdigest()
        payload = {'schema':'archive-stream-checkpoint/v1',
            'source_identity':source_identity(store.task,SOURCE),'compressed_bytes':6,
            'tar_offset':512,'member_count':1,'last_member':[locator,checksum,'observed'],
            **states,'index_sha256':hashlib.sha256(index).hexdigest()}
        return store,payload,index

    def member(self, store, payload):
        locator,checksum,status = payload['last_member']
        store.finish_member(SOURCE,locator,checksum,status)

    def test_member_and_checkpoint_commit_together_and_duplicate_is_bounded(self):
        store,payload,index = self.prepared()
        self.assertIsNone(load_stream_checkpoint(store,SOURCE))
        for _ in range(2):
            with store.member(SOURCE,'checkpoint',max_growth_bytes=2*1024**2):
                self.member(store,payload)
                save_stream_checkpoint(store,SOURCE,payload,index)
            self.assertEqual(load_stream_checkpoint(store,SOURCE),(payload,index))
        self.assertEqual(store.db.execute('SELECT count(*) FROM archive_stream_checkpoints').fetchone()[0],1)
        self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?',(SOURCE,)).fetchone()[0],1)

    def test_capacity_failure_rolls_back_both_member_and_checkpoint(self):
        store,payload,index = self.prepared()
        with self.assertRaises(BudgetStop):
            with store.member(SOURCE,'checkpoint',max_growth_bytes=2*1024**2):
                self.member(store,payload)
                with patch.object(store.task,'check',side_effect=BudgetStop('injected capacity')):
                    save_stream_checkpoint(store,SOURCE,payload,index)
        self.assertIsNone(store.seen(SOURCE,payload['last_member'][0]))
        self.assertIsNone(load_stream_checkpoint(store,SOURCE))
        self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')

    def test_corruption_version_source_and_position_mismatch_are_rejected(self):
        store,payload,index = self.prepared()
        with self.assertRaisesRegex(ValueError,'transaction'):
            save_stream_checkpoint(store,SOURCE,payload,index)
        for key,value in [('source_identity','drift'),('tar_offset',513),
                          ('member_count',2),('compressed_bytes',7),('compressed_bytes',True)]:
            broken = copy.deepcopy(payload); broken[key] = value
            with self.assertRaises(ValueError):
                with store.member(SOURCE,'bad',max_growth_bytes=2*1024**2):
                    self.member(store,payload)
                    save_stream_checkpoint(store,SOURCE,broken,index)
            self.assertIsNone(store.seen(SOURCE,payload['last_member'][0]))
        with store.member(SOURCE,'good',max_growth_bytes=2*1024**2):
            self.member(store,payload); save_stream_checkpoint(store,SOURCE,payload,index)
        with store.db:
            store.db.execute("UPDATE archive_stream_checkpoints SET index_bytes=?",(b'corrupt',))
        with self.assertRaisesRegex(ValueError,'index checksum'):
            load_stream_checkpoint(store,SOURCE)

    def test_member_ledger_drift_and_checkpoint_regression_are_rejected(self):
        store,payload,index = self.prepared()
        with store.member(SOURCE,'good',max_growth_bytes=2*1024**2):
            self.member(store,payload); save_stream_checkpoint(store,SOURCE,payload,index)
        with store.db:
            store.db.execute('UPDATE runs SET members=2 WHERE source=?',(SOURCE,))
        with self.assertRaisesRegex(ValueError,'ledger'):
            load_stream_checkpoint(store,SOURCE)
        with store.db:
            store.db.execute('UPDATE runs SET members=1 WHERE source=?',(SOURCE,))
        broken = copy.deepcopy(payload); broken['tar_offset'] = 0
        with self.assertRaises(ValueError):
            with store.member(SOURCE,'bad',max_growth_bytes=2*1024**2):
                save_stream_checkpoint(store,SOURCE,broken,index)
        self.assertEqual(load_stream_checkpoint(store,SOURCE),(payload,index))

    def test_process_exit_before_and_after_commit_recovers_both_or_neither(self):
        store,payload,index = self.prepared()
        # Child receives normalized state only, owns the existing writer lock.
        store.close()
        payload_path = self.root/'checkpoint.json'
        payload_path.write_text(json.dumps(payload))
        code = '''
import json,os,sys
from pathlib import Path
from gapi.archive_recovery import save_stream_checkpoint
from gapi.career_store import CareerStore
from gapi.task import Task
s=CareerStore(Task(Path(sys.argv[1])))
p=json.loads(Path(sys.argv[2]).read_text())
if sys.argv[3]=='before':
    s.db.set_trace_callback(lambda sql:os._exit(37) if sql=='COMMIT' else None)
with s.member('orcid-2026-summary','checkpoint',max_growth_bytes=2*1024**2):
    s.finish_member('orcid-2026-summary',*p['last_member'])
    save_stream_checkpoint(s,'orcid-2026-summary',p,bytes.fromhex(sys.argv[4]))
os._exit(38)
'''
        for point,exit_code in [('before',37),('after',38)]:
            run = subprocess.run([sys.executable,'-B','-c',code,str(self.path),
                str(payload_path),point,index.hex()],text=True,capture_output=True,timeout=20)
            self.assertEqual(run.returncode,exit_code,run.stderr)
            reopened = CareerStore(Task(self.path))
            try:
                self.assertEqual(load_stream_checkpoint(reopened,SOURCE),
                                 None if point=='before' else (payload,index))
                self.assertEqual(reopened.seen(SOURCE,payload['last_member'][0]),
                                 None if point=='before' else tuple(payload['last_member'][1:]))
                self.assertEqual(reopened.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
            finally: reopened.close()

    def test_same_position_drift_and_existing_corruption_are_not_overwritten(self):
        store,payload,index = self.prepared()
        with store.member(SOURCE,'good',max_growth_bytes=2*1024**2):
            self.member(store,payload); save_stream_checkpoint(store,SOURCE,payload,index)
        other = b'different index at same position'
        changed = copy.deepcopy(payload)
        changed['index_sha256'] = hashlib.sha256(other).hexdigest()
        with self.assertRaisesRegex(ValueError,'same member position'):
            with store.member(SOURCE,'bad',max_growth_bytes=2*1024**2):
                save_stream_checkpoint(store,SOURCE,changed,other)
        self.assertEqual(load_stream_checkpoint(store,SOURCE),(payload,index))
        with store.db:
            store.db.execute("UPDATE archive_stream_checkpoints SET sha256='damaged'")
        with self.assertRaisesRegex(ValueError,'corrupted'):
            with store.member(SOURCE,'bad',max_growth_bytes=2*1024**2):
                save_stream_checkpoint(store,SOURCE,payload,index)
        self.assertEqual(store.db.execute('SELECT sha256 FROM archive_stream_checkpoints').fetchone()[0],
                         'damaged')


if __name__ == '__main__': unittest.main()
