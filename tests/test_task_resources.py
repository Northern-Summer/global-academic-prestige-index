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

    def test_disappearing_journal_does_not_turn_budget_probe_into_source_failure(self):
        s=self.store(archive([]))
        try:
            journal=s.task.root/'disappearing.sqlite-journal'
            journal.write_bytes(b'x'*32)
            baseline=s.task.used()
            original=Path.stat
            def stat(path,*args,**kwargs):
                if path==journal:raise FileNotFoundError('journal committed during scan')
                return original(path,*args,**kwargs)
            with patch.object(Path,'stat',stat):self.assertEqual(s.task.used(),baseline-32)
            original_link = Path.is_symlink
            def missing_link(path):
                if path==journal:raise FileNotFoundError('journal vanished during link probe')
                return original_link(path)
            with patch.object(Path,'is_symlink',missing_link),patch.object(Path,'stat',stat):
                self.assertEqual(s.task.used(),baseline-32)
            def forbidden_link(path):
                if path==journal:raise PermissionError('link observation denied')
                return original_link(path)
            with patch.object(Path,'is_symlink',forbidden_link),self.assertRaises(PermissionError):
                s.task.used()
            def forbidden(path,*args,**kwargs):
                if path==journal:raise PermissionError('cannot observe budget')
                return original(path,*args,**kwargs)
            with patch.object(Path,'stat',forbidden),self.assertRaises(PermissionError):s.task.used()
        finally:s.close()

    def test_http_request_reservation_settles_and_survives_reopen(self):
        from types import SimpleNamespace
        from gapi.source_http import SourcePause
        from gapi.career_store import CareerStore
        s=self.store(archive([]))
        http=SimpleNamespace(requests_count=0)
        with s.request_budget('source',8,10,http):http.requests_count+=3
        self.assertEqual(s.db.execute('SELECT sum(requests) FROM request_reservations').fetchone()[0],3)
        s.close()
        s=CareerStore(Task(self.path))
        try:
            with self.assertRaises(SourcePause):
                with s.request_budget('source',8,10,http):self.fail('quota must be reserved before transport')
            self.assertEqual(http.requests_count,3)
        finally:s.close()

    def test_request_reservation_is_conservative_for_an_uninstrumented_transport(self):
        s=self.store(archive([]))
        try:
            with self.assertRaises(OSError):
                with s.request_budget('source',8,10,object()):raise OSError('interrupted transport')
            self.assertEqual(s.db.execute('SELECT sum(requests) FROM request_reservations').fetchone()[0],8)
        finally:s.close()
