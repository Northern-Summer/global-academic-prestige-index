import sqlite3
import unittest
from urllib.parse import parse_qs,urlsplit

from gapi.crossref_discovery import discover_crossref
import test_streaming as fixtures
from test_streaming import archive,PERSON
from test_openalex_discovery import HTTP


def page(cursor,orcids=(PERSON,)):
    return {'status':'ok','message':{'next-cursor':cursor,'total-results':1000,
        'items':[{'DOI':'10.1234/work','author':[{'ORCID':'https://orcid.org/'+value,
                    'affiliation':[{'name':'Must not become employment'}]} for value in orcids]}]}}


class CrossrefDiscoveryTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_multiple_pages_resume_no_personal_payload_or_career_inference(self):
        s=self.store(archive([]))
        try:
            http=HTTP([page('next'),page('later')])
            discover_crossref(s,http,per_page=1)
            discover_crossref(s,http,per_page=1)
            params=parse_qs(urlsplit(http.urls[1]).query)
            self.assertEqual(params['cursor'],['next'])
            self.assertEqual(params['select'],['DOI,author'])
            self.assertNotIn('api_key',params)
            self.assertEqual(s.pending('ingest-orcid-api'),[PERSON])
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],2)
        finally:s.close()

    def test_bad_orcid_not_queued_and_partial_last_page_is_exhausted(self):
        s=self.store(archive([]))
        try:
            result=discover_crossref(s,HTTP([page('unused',('0000-0002-1825-0098',))]),per_page=2)
            self.assertFalse(s.pending('ingest-orcid-api'))
            self.assertIn('exhausted',result['status'])
            self.assertEqual(discover_crossref(s,HTTP([]))['pages'],0)
        finally:s.close()

    def test_transaction_failure_keeps_cursor_and_queue_together(self):
        s=self.store(archive([]))
        try:
            s.db.execute("CREATE TRIGGER fail BEFORE INSERT ON discovery_pages BEGIN SELECT RAISE(ABORT,'full'); END")
            with self.assertRaises(sqlite3.IntegrityError):discover_crossref(s,HTTP([page('next')]))
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],0)
        finally:s.close()

    def test_network_failure_does_not_advance_or_remove_existing_work(self):
        s=self.store(archive([]))
        try:
            discover_crossref(s,HTTP([page('next')]),per_page=1)
            with self.assertRaises(OSError):discover_crossref(s,HTTP([OSError('offline')]))
            self.assertEqual(s.pending('ingest-orcid-api'),[PERSON])
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],1)
        finally:s.close()

    def test_nonadvancing_full_page_and_malformed_envelope_rejected(self):
        s=self.store(archive([]))
        try:
            invalid_work=page('next')
            invalid_work['message']['items'][0].pop('DOI')
            for body in ([],page('*'),{'status':'failed','message':{}},invalid_work):
                with self.subTest(body=body),self.assertRaises(ValueError):
                    discover_crossref(s,HTTP([body]),per_page=1)
        finally:s.close()

    def test_server_pause_prevents_next_invocation_without_mutating_cursor(self):
        import time
        from gapi.source_http import SourcePause
        s=self.store(archive([]))
        try:
            with self.assertRaises(SourcePause):
                discover_crossref(s,HTTP([SourcePause(429,time.time()+120,'wait')]))
            result=discover_crossref(s,HTTP([]))
            self.assertEqual(result['status'],'server-backoff-pending')
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],0)
        finally:s.close()
