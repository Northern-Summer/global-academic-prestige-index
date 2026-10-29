import sqlite3
import unittest

from gapi.frozen_input import freeze_input,frozen_reader
from gapi.task import BudgetStop,canonical
from gapi.global_analysis import rank_global
import test_streaming as fixtures
from test_streaming import archive
import test_global_analysis as analysis


class FrozenInputTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

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
