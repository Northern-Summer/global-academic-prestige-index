"""Separate source freezes must conserve the original budget and exclusion."""
import hashlib
import io
import json
import unittest
from unittest.mock import patch

import test_streaming as fixtures
from gapi.task import Task, BudgetStop
from gapi.career_store import CareerStore, exclusive_writer
from gapi.orcid_stream import stream_archive


class SharedSourceResourcesTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def nested(self, parent, **changes):
        config = json.loads(json.dumps(parent.config))
        config.update(work_dir=str(parent.root/'annual-source'), resource_root=str(parent.root),
            resource_freeze_sha256=hashlib.sha256((parent.root/'sources.lock.json').read_bytes()).hexdigest())
        config.update(changes)
        path=self.root/'annual.json'
        path.write_text(json.dumps(config))
        return Task(path)

    def test_shared_budget_counts_siblings_and_freeze_is_idempotent(self):
        store=self.store(fixtures.archive([])); parent=store.task; store.close()
        child=self.nested(parent); child.freeze()
        self.assertEqual(parent.used(),child.used())
        old=child.used()
        (parent.root/'sibling.bin').write_bytes(b'x'*8192)
        self.assertEqual(child.used(),old+8192)
        lock=child.root/'sources.lock.json'; before=lock.stat().st_mtime_ns
        child.freeze()
        self.assertEqual(lock.stat().st_mtime_ns,before)
        with self.assertRaises(BudgetStop):child.check(parent.config['budget_bytes'])

    def test_parent_and_child_exclude_each_other_both_directions(self):
        store=self.store(fixtures.archive([])); parent=store.task; store.close()
        child=self.nested(parent); child.freeze()
        for first,second in ((parent,child),(child,parent)):
            with exclusive_writer(first):
                with self.assertRaisesRegex(RuntimeError,'another task writer'):
                    with exclusive_writer(second):self.fail('second writer admitted')
        with exclusive_writer(child):pass

    def test_repository_scratch_and_release_files_count_once(self):
        store=self.store(fixtures.archive([]));parent=store.task;store.close()
        child=self.nested(parent);child.freeze()
        repo=self.root/'repository'
        repo.mkdir()
        with patch('gapi.task.project_root',return_value=repo):
            before=child.used()
            for name in ('work','outputs','dist'):
                folder=repo/name;folder.mkdir()
                (folder/'artifact').write_bytes(b'x'*1024)
            self.assertEqual(child.used(),before+3072)
            self.assertEqual(parent.used(),child.used())

    def test_changed_owner_or_budget_and_outside_root_are_rejected(self):
        store=self.store(fixtures.archive([])); parent=store.task; store.close()
        with self.assertRaisesRegex(ValueError,'differs'):
            self.nested(parent,reserve_bytes=parent.config['reserve_bytes']+1)
        with self.assertRaisesRegex(ValueError,'must contain'):
            self.nested(parent,work_dir=str(self.root/'outside'))
        with self.assertRaisesRegex(ValueError,'not pinned'):
            self.nested(parent,resource_freeze_sha256='0'*64)
        with self.assertRaisesRegex(ValueError,'differs'):
            self.nested(parent,additional_storage_roots=[str(self.root/'unshared-delivery')])
        child=self.nested(parent);child.freeze()
        (parent.root/'sources.lock.json').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'changed'):
            child.check()
        with self.assertRaisesRegex(ValueError,'changed'):
            child.require_frozen()

    def test_archive_stays_separate_and_replay_does_not_grow_rows(self):
        data=fixtures.archive([(f'ORCID_2026_10_summaries/097/{fixtures.PERSON}.xml',fixtures.XML)])
        store=self.store(data);parent=store.task;store.close()
        child=self.nested(parent);child.freeze()
        store=CareerStore(child)
        try:
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            before=store.db.execute('SELECT count(*) FROM events').fetchone()[0]
            stream_archive(store,'orcid-2026-summary',io.BytesIO(data))
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],before)
            self.assertEqual(before,1)
        finally:store.close()
        store=CareerStore(parent)
        try:self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:store.close()
