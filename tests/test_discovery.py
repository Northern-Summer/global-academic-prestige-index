import unittest
from urllib.parse import unquote

from gapi.global_sources import discover_wikidata, enqueue_discovery
import test_streaming as fixtures
from test_streaming import archive


class FakeHTTP:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.urls = []

    def json(self, url):
        self.urls.append(unquote(url))
        rows = next(self.pages)
        if isinstance(rows, Exception):
            raise rows
        return {'results': {'bindings': [
            {'person': {'value': 'http://www.wikidata.org/entity/'+qid},
             'orcids': {'value': orcid}} for qid, orcid in rows]}}, 'a'*64


class DiscoveryTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_advancing_atomic_pages_survive_reopen_and_keep_legacy_queue(self):
        s = self.store(archive([]))
        http = FakeHTTP([[('Q10', ''), ('Q2', '')], [('Q30', '')], []])
        try:
            with s.db:
                s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)', ('ingest-wikidata','Q1','old','old'))
            self.assertEqual(enqueue_discovery(s,http,'ingest-wikidata')['cursor'], 'Q2')
            s.close()
            from gapi.career_store import CareerStore
            from gapi.task import Task
            s = CareerStore(Task(self.path))
            self.assertEqual(enqueue_discovery(s,http,'ingest-wikidata')['cursor'], 'Q30')
            self.assertIn('FILTER(STR(?person) > "http://www.wikidata.org/entity/Q2")', http.urls[-1])
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0], 4)
            enqueue_discovery(s,http,'ingest-wikidata')
            self.assertEqual(enqueue_discovery(s,http,'ingest-wikidata')['status'], 'live-query-exhausted-not-global-complete')
            self.assertEqual(len(http.urls), 3)
        finally:
            s.close()

    def test_failed_network_does_not_advance_or_remove_queue(self):
        s = self.store(archive([]))
        try:
            http = FakeHTTP([[('Q1','')], OSError('offline')])
            enqueue_discovery(s,http,'ingest-wikidata')
            with self.assertRaises(OSError):
                enqueue_discovery(s,http,'ingest-wikidata')
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0], 1)
            self.assertEqual(s.db.execute('SELECT identifier FROM discovery').fetchone()[0], 'Q1')
        finally:
            s.close()

    def test_page_commit_failure_rolls_back_queue_and_cursor_together(self):
        import sqlite3
        s = self.store(archive([]))
        try:
            s.db.execute('''CREATE TRIGGER fail_page BEFORE INSERT ON discovery_pages
                BEGIN SELECT RAISE(ABORT, 'injected disk/transaction failure'); END''')
            with self.assertRaises(sqlite3.IntegrityError):
                enqueue_discovery(s, FakeHTTP([[('Q1','')]]), 'ingest-wikidata')
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0], 0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0], 0)
        finally:
            s.close()

    def test_rejects_non_advancing_and_unordered_pages(self):
        for rows, cursor in [([('Q1','')], 'Q1'), ([('Q2',''),('Q10','')], '')]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                discover_wikidata(FakeHTTP([rows]), after=cursor)

    def test_conflicting_orcid_not_selected_and_boundary_is_per_person(self):
        http = FakeHTTP([[('Q1', '0000-0002-1825-0097|0000-0001-1111-1111')]])
        page = discover_wikidata(http)
        self.assertIsNone(page['rows'][0]['orcid'])
        self.assertIn('GROUP BY ?person', http.urls[0])
        self.assertIn('GROUP_CONCAT(DISTINCT', http.urls[0])

    def test_injection_and_page_size_rejected_before_http(self):
        http = FakeHTTP([])
        for kwargs in ({'after':'Q1") }'}, {'limit':201}, {'limit':0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                discover_wikidata(http, **kwargs)
        self.assertEqual(http.urls, [])
