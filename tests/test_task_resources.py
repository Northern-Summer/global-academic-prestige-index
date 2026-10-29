import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from gapi.task import Task
from test_streaming import archive
import test_streaming as fixtures


class ResourceTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_memory_probe_reuses_native_structure_without_per_chunk_class_growth(self):
        s=self.store(archive([]))
        try:
            before=s.task.check_memory()
            for _ in range(2000):s.task.check_memory()
            after=s.task.check_memory()
            self.assertLess(after-before,8*1024**2)
        finally:s.close()

    def test_process_temp_settings_are_restored(self):
        s=self.store(archive([]))
        try:
            previous={k:os.environ.get(k) for k in ('TMP','TEMP','TMPDIR')}
            old=tempfile.tempdir
            s.task.activate_runtime()
            self.assertEqual(tempfile.gettempdir(),str(s.task.root))
            s.task.restore_runtime()
            self.assertEqual(previous,{k:os.environ.get(k) for k in previous})
            self.assertEqual(old,tempfile.tempdir)
        finally:s.close()

    def test_disk_free_space_refusal_preserves_prior_progress(self):
        s=self.store(archive([]))
        try:
            s.start('wikidata')
            with patch('gapi.task.shutil.disk_usage',return_value=__import__('collections').namedtuple('Usage','total used free')(100,99,1)):
                from gapi.task import BudgetStop
                with self.assertRaises(BudgetStop):s.task.check()
            self.assertEqual(s.report()['sources'][0]['source'],'wikidata')
        finally:s.close()
