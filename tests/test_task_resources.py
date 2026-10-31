import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from gapi.task import Task
from test_streaming import archive
import test_streaming as fixtures


class ResourceTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_file_maintenance_reuses_writer_lock_without_database_allowance(self):
        from gapi.career_store import CareerStore,exclusive_writer
        from gapi.task import BudgetStop
        s=self.store(archive([]));task=s.task
        with self.assertRaisesRegex(RuntimeError,'another task writer'):
            with exclusive_writer(task):pass
        s.close()
        used=task.config['budget_bytes']-task.config['reserve_bytes']-512*1024
        with patch.object(task,'used',return_value=used):
            with self.assertRaises(BudgetStop):CareerStore(task)
            with patch('gapi.career_store.sqlite3.connect',side_effect=AssertionError('unexpected database open')):
                with exclusive_writer(task):
                    with self.assertRaisesRegex(RuntimeError,'another task writer'):
                        with exclusive_writer(task):pass
                with exclusive_writer(task):pass
        with self.assertRaisesRegex(ValueError,'maintenance failed'):
            with exclusive_writer(task):raise ValueError('maintenance failed')
        with exclusive_writer(task):pass

    def test_small_source_transaction_uses_enforced_available_capacity(self):
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('CREATE TABLE growth_probe(value BLOB)')
            cap=s.db.execute('PRAGMA max_page_count').fetchone()[0]
            used=s.task.config['budget_bytes']-s.task.config['reserve_bytes']-2*1024**2
            with patch.object(s.task,'used',return_value=used):
                with s.member('source','small'):
                    self.assertLess(s.db.execute('PRAGMA max_page_count').fetchone()[0],cap)
                    s.db.execute('INSERT INTO growth_probe VALUES(?)',(b'x'*1000,))
                with self.assertRaises(BudgetStop):
                    with s.member('source','oversized'):
                        s.db.execute('INSERT INTO growth_probe VALUES(?)',(b'x'*4*1024**2,))
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(s.db.execute('SELECT count(*) FROM growth_probe').fetchone()[0],1)
            self.assertEqual(s.db.execute('PRAGMA max_page_count').fetchone()[0],cap)
        finally:s.close()

    def test_source_admission_keeps_minimum_headroom_and_fixed_reserve(self):
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            used=s.task.config['budget_bytes']-s.task.config['reserve_bytes']-1024**2+1
            with patch.object(s.task,'used',return_value=used),self.assertRaises(BudgetStop):
                with s.member('source','no-headroom'):self.fail('must not enter')
            self.assertFalse(s.db.in_transaction)
        finally:s.close()

    def test_bounded_maintenance_growth_is_enforced_and_cap_restored(self):
        import sqlite3
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('CREATE TABLE growth_probe(value BLOB)')
            cap=s.db.execute('PRAGMA max_page_count').fetchone()[0]
            with s.member('maintenance','small',max_growth_bytes=65536):
                s.db.execute('INSERT INTO growth_probe VALUES(?)',(b'x'*1000,))
            self.assertEqual(s.db.execute('PRAGMA max_page_count').fetchone()[0],cap)
            with self.assertRaises(sqlite3.OperationalError):
                with s.member('maintenance','too-large',max_growth_bytes=4096):
                    s.db.execute('INSERT INTO growth_probe VALUES(?)',(b'x'*1024**2,))
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(s.db.execute('SELECT count(*) FROM growth_probe').fetchone()[0],1)
            self.assertEqual(s.db.execute('PRAGMA max_page_count').fetchone()[0],cap)
            for invalid in (True,0,-1,50*1024**2):
                with self.assertRaises(ValueError):
                    with s.member('maintenance','invalid',max_growth_bytes=invalid):pass
        finally:s.close()

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
        # The fixture measures a disappearing owned journal, not unrelated logs
        # growing under the real repository while another check/collector runs.
        project=self.root/'budget-project'
        (project/'outputs').mkdir(parents=True)
        (project/'outputs'/'owned.txt').write_bytes(b'owned')
        self.enterContext(patch('gapi.task.project_root',return_value=project))
        s=self.store(archive([]))
        try:
            journal=s.task.root/'disappearing.sqlite-journal'
            journal.write_bytes(b'x'*32)
            baseline=s.task.used()
            original=os.scandir
            class Entry:
                def __init__(self,entry,error):self.entry,self.error=entry,error
                def __getattr__(self,name):return getattr(self.entry,name)
                def stat(self,**kwargs):
                    if Path(self.entry.path)==journal:raise self.error('journal changed during scan')
                    return self.entry.stat(**kwargs)
            def probe(error):
                @contextmanager
                def scan(folder):
                    with original(folder) as entries:
                        yield (Entry(entry,error) for entry in entries)
                return scan
            with patch('gapi.task.os.scandir',probe(FileNotFoundError)):
                self.assertEqual(s.task.used(),baseline-32)
            with patch('gapi.task.os.scandir',probe(PermissionError)),self.assertRaises(PermissionError):
                s.task.used()
        finally:s.close()

    def test_scan_counts_nested_files_fresh_and_propagates_directory_denial(self):
        project=self.root/'budget-project'
        (project/'outputs'/'nested').mkdir(parents=True)
        (project/'dist').mkdir()
        (project/'outputs'/'nested'/'one').write_bytes(b'123')
        (project/'dist'/'two').write_bytes(b'45')
        self.enterContext(patch('gapi.task.project_root',return_value=project))
        task=Task.__new__(Task);task.root=self.root/'owned';task.root.mkdir()
        self.assertEqual(task.used(),5)
        (task.root/'new').write_bytes(b'6789')
        self.assertEqual(task.used(),9)
        original=os.scandir
        def scan(folder):
            if Path(folder)==project/'outputs'/'nested':raise PermissionError('directory denied')
            return original(folder)
        with patch('gapi.task.os.scandir',scan),self.assertRaises(PermissionError):task.used()

    def test_scan_rejects_task_reparse_points_and_counts_output_file_links(self):
        from types import SimpleNamespace
        import stat
        project=self.root/'budget-project';project.mkdir()
        task=Task.__new__(Task);task.root=self.root/'owned';task.root.mkdir()
        self.enterContext(patch('gapi.task.project_root',return_value=project))
        class Entry:
            path='unused'
            def stat(self,*,follow_symlinks):
                return SimpleNamespace(st_mode=stat.S_IFREG,st_file_attributes=0x400 if not follow_symlinks else 0,st_size=7)
        @contextmanager
        def scan(folder):yield iter([Entry()]) if Path(folder)==task.root else iter([])
        with patch('gapi.task.os.scandir',scan),self.assertRaisesRegex(ValueError,'links are forbidden'):task.used()
        @contextmanager
        def output_scan(folder):yield iter([Entry()]) if Path(folder)==project/'outputs' else iter([])
        with patch('gapi.task.os.scandir',output_scan):self.assertEqual(task.used(),7)

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
