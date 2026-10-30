import io
import unittest
from unittest.mock import patch
from urllib.request import Request

from gapi.global_sources import ingest_orcid_api, queue_orcid_redirect
from gapi.source_http import PublicHTTP, SourceRedirect
from gapi.task import digest
import test_streaming as fixtures

ORIGINAL = fixtures.PERSON
TARGET = '0000-0001-5109-3700'
BASE = 'https://pub.orcid.org/v3.0/'
FEED = 'orcid-api-permanent-redirect-candidates/v1'


class RedirectTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_transport_closes_redirect_without_issuing_target_request(self):
        from types import SimpleNamespace
        task = SimpleNamespace(config={'max_retries': 2, 'request_interval': 1, 'http_timeout': 2})
        with patch('gapi.source_http.build_opener') as factory:
            PublicHTTP(task)
        handler = factory.call_args.args[0]
        body = io.BytesIO(b'ignored redirect body')
        origin, target = BASE+ORIGINAL+'/educations', BASE+TARGET+'/educations'
        with self.assertRaises(SourceRedirect) as caught:
            handler.redirect_request(Request(origin), body, 301, 'Moved', {}, target)
        self.assertTrue(body.closed)
        self.assertEqual((caught.exception.source_url, caught.exception.target_url, caught.exception.code),
                         (origin, target, 301))
        self.assertNotIn(TARGET, str(caught.exception))
        self.assertNotIn(ORIGINAL, str(caught.exception))

    def test_untrusted_transport_redirect_is_closed_and_rejected(self):
        from types import SimpleNamespace
        with patch('gapi.source_http.build_opener') as factory:
            PublicHTTP(SimpleNamespace())
        body = io.BytesIO(b'ignored')
        with self.assertRaises(ValueError):
            factory.call_args.args[0].redirect_request(
                Request(BASE+ORIGINAL+'/educations'), body, 301, '', {}, 'https://evil.test/')
        self.assertTrue(body.closed)

    def test_permanent_candidate_proof_is_idempotent_and_never_accepts_old_identity(self):
        s = self.store(fixtures.archive([]))
        origin, target = BASE+ORIGINAL+'/educations', BASE+TARGET+'/educations'
        try:
            for _ in range(2):
                self.assertTrue(queue_orcid_redirect(s, SourceRedirect(origin, target, 301), origin))
            self.assertEqual(s.pending('ingest-orcid-api'), [TARGET])
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages WHERE source=?', (FEED,)).fetchone()[0], 1)
            metadata = {'representation': 'http-permanent-redirect-metadata/v1', 'from': origin, 'to': target, 'status': 301}
            row = s.db.execute('SELECT cursor_before,cursor_after,response_sha256,retrieved_at FROM discovery_pages WHERE source=?', (FEED,)).fetchone()
            self.assertEqual(row[0], origin+'#301@'+digest(metadata))
            self.assertEqual((row[1], row[2]), (target, digest(metadata)))
            self.assertTrue(row[3])
            self.assertEqual(s.report()['events'], 0)
            self.assertIsNone(s.seen('orcid-public-api', ORIGINAL))
        finally:
            s.close()

    def test_temporary_ambiguous_and_wrong_identity_routes_never_queue(self):
        s = self.store(fixtures.archive([]))
        origin, target = BASE+ORIGINAL+'/educations', BASE+TARGET+'/educations'
        cases = [(origin, target, code, origin) for code in (302, 303, 307)]
        cases += [(origin, bad, 301, origin) for bad in (
            origin, target+'?source=other', target+'#fragment',
            BASE+TARGET+'/employments', 'https://user@pub.orcid.org/v3.0/'+TARGET+'/educations',
            target.replace('pub.orcid.org', 'www.wikidata.org'),
            target.replace(TARGET, '0000-0001-5109-3701'))]
        cases.append((origin, target, 301, BASE+TARGET+'/employments'))
        try:
            for source, dest, status, requested in cases:
                with self.subTest(dest=dest, status=status):
                    self.assertFalse(queue_orcid_redirect(s, SourceRedirect(source, dest, status), requested))
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0], 0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0], 0)
        finally:
            s.close()

    def test_partial_original_rolls_back_and_target_requires_its_own_verified_pair(self):
        class Response(io.BytesIO):
            headers = {'Content-Type': 'application/vnd.orcid+xml'}

        class HTTP:
            def __init__(self): self.urls = []
            def open(self, url, accept):
                self.urls.append(url)
                person, section = url.rsplit('/', 2)[-2:]
                if person == ORIGINAL and section == 'employments':
                    raise SourceRedirect(url, BASE+TARGET+'/employments', 308)
                body = (fixtures.XML.decode().replace('<record ', '<educations ').replace('</record>', '</educations>')
                        .replace('path="/'+ORIGINAL+'"', 'path="/'+person+'/educations"')
                        if section == 'educations' else '<employments path="/'+person+'/employments"/>')
                return Response(body.encode())

        s, http = self.store(fixtures.archive([])), HTTP()
        try:
            with s.db:
                s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)', ('ingest-orcid-api', ORIGINAL, 'a', 'b'))
            ingest_orcid_api(s, http, [ORIGINAL])
            self.assertIsNone(s.seen('orcid-public-api', ORIGINAL))
            self.assertEqual(s.report()['events'], 0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0], 0)
            self.assertEqual(s.pending('ingest-orcid-api'), [TARGET])
            self.assertTrue(all(TARGET not in url for url in http.urls))
            ingest_orcid_api(s, http, [TARGET])
            self.assertIsNotNone(s.seen('orcid-public-api', TARGET))
            self.assertIsNone(s.seen('orcid-public-api', ORIGINAL))
            self.assertEqual(s.report()['events'], 1)
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0], 2)
            progress = s.candidate_progress()['sources'][0]
            self.assertEqual((progress['candidates'], progress['processed'], progress['pending']), (2, 1, 1))
            self.assertFalse(progress['enumeration_complete'])
            self.assertFalse(any(p.suffix in ('.xml', '.tar', '.gz') for p in s.task.root.rglob('*')))
        finally:
            s.close()
