import sqlite3
import unittest
from unittest.mock import patch

import test_streaming as fixtures
from test_streaming import archive
from gapi.career_store import CareerStore
from gapi.monitoring import snapshot_report
from gapi.task import BudgetStop


class MonitoringTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_projection_excludes_large_private_landing_and_matches_report_owner(self):
        store=self.store(archive([]))
        try:
            with store.db:
                store.db.execute('CREATE TABLE private_landing(payload BLOB)')
                store.db.execute('INSERT INTO private_landing VALUES(zeroblob(2097152))')
                store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
            expected=store.report()
            store.task.config['memory_bytes']=1024
            with patch.object(store.task,'check_memory',return_value=0):
                actual=snapshot_report(store.task)
            self.assertEqual(actual,expected)
            with store.db:store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q2','a','b')")
        finally:store.close()

    def test_expensive_report_has_no_live_read_lock_and_uses_consistent_snapshot(self):
        store=self.store(archive([]))
        before={p.name for p in store.task.root.iterdir()}
        original=CareerStore.report
        def report(view):
            self.assertIsNot(view.db,store.db)
            # Hold a real read transaction throughout the expensive report.
            view.db.execute('BEGIN')
            view.db.execute('SELECT count(*) FROM discovery').fetchone()
            store.db.execute('PRAGMA busy_timeout=1')
            with store.db:
                store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
            self.assertEqual(view.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
            return original(view)
        try:
            # Reproduce the rejected live-reading route before testing its repair.
            live=sqlite3.connect((store.task.root/'careers.sqlite').as_uri()+'?mode=ro',uri=True)
            try:
                live.execute('BEGIN')
                live.execute('SELECT count(*) FROM discovery').fetchone()
                store.db.execute('PRAGMA busy_timeout=1')
                with self.assertRaisesRegex(sqlite3.OperationalError,'locked'),store.db:
                    store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q2','a','b')")
            finally:live.close()
            with patch.object(CareerStore,'report',report):
                result=snapshot_report(store.task)
            self.assertEqual(result['events'],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM discovery').fetchone()[0],1)
            self.assertEqual({p.name for p in store.task.root.iterdir()},before)
        finally:store.close()

    def test_missing_database_not_created_and_invalid_timeout_refused(self):
        store=self.store(archive([]));task=store.task;store.close()
        (task.root/'careers.sqlite').unlink()
        with self.assertRaises(sqlite3.OperationalError):snapshot_report(task)
        self.assertFalse((task.root/'careers.sqlite').exists())
        for invalid in (0,float('nan'),float('inf')):
            with self.assertRaises(ValueError):snapshot_report(task,max_seconds=invalid)

    def test_snapshot_deadline_releases_source_and_leaves_writer_usable(self):
        store=self.store(archive([]))
        try:
            with patch('gapi.monitoring.time.monotonic',side_effect=[100,102]):
                with self.assertRaises(BudgetStop):snapshot_report(store.task,max_seconds=1)
            store.db.execute('PRAGMA busy_timeout=1')
            with store.db:
                store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
        finally:store.close()

    def test_cli_status_does_not_acquire_second_writer_lock(self):
        import contextlib
        import io
        import json
        from types import SimpleNamespace
        from gapi.stream_cli import execute_task
        store=self.store(archive([]))
        try:
            output=io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertTrue(execute_task(store.task,SimpleNamespace(command='acquisition-status',max_seconds=60)))
            self.assertEqual(json.loads(output.getvalue())['events'],0)
        finally:store.close()

    def test_expensive_snapshot_query_expires_as_budget_stop_not_sql_failure(self):
        store=self.store(archive([]));clock=[100]
        def slow(view):
            clock[0]=102
            return view.db.execute('''WITH RECURSIVE seq(n) AS
                (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<1000000)
                SELECT sum(n) FROM seq''').fetchone()
        try:
            with patch('gapi.monitoring.time.monotonic',side_effect=lambda:clock[0]),patch.object(CareerStore,'report',slow):
                with self.assertRaisesRegex(BudgetStop,'time budget'):snapshot_report(store.task,max_seconds=1)
            store.db.execute('PRAGMA busy_timeout=1')
            with store.db:
                store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
        finally:store.close()
