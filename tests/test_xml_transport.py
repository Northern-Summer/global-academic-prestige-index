import gzip
import hashlib
import io
from types import SimpleNamespace
import unittest

from gapi.source_http import PublicHTTP, SourceResponse, XMLResponse, StageDeadline
import test_streaming as fixtures


URL = 'https://pub.orcid.org/v3.0/0000-0002-1825-0097/educations'


class Response(io.BytesIO):
    status = 200

    def __init__(self, body, encoding='gzip', fragment=65536):
        super().__init__(body)
        self.headers = {'Content-Type': 'application/vnd.orcid+xml', 'Content-Encoding': encoding}
        self.fragment = fragment

    def read(self, size):
        if not 0 <= size <= 65536:
            raise AssertionError('unbounded source read')
        return super().read(min(size, self.fragment))

    def geturl(self):return URL


def consume(reader, size=65536):
    chunks = []
    while chunk := reader.read(size):
        if len(chunk) > size:
            raise AssertionError('output exceeds requested chunk')
        chunks.append(chunk)
    return b''.join(chunks)


class XMLTransportTests(unittest.TestCase):
    def test_negotiation_hash_wire_accounting_and_identity_compatibility(self):
        body = b'<educations>' + b'<degree>PhD</degree>' * 10000 + b'</educations>'
        for encoding in ('identity', 'gzip'):
            with self.subTest(encoding=encoding):
                wire = gzip.compress(body, mtime=0) if encoding == 'gzip' else body
                response = Response(wire, encoding)
                requests = []
                def opener(request, **kwargs):
                    requests.append(request)
                    return response
                task = SimpleNamespace(config={'max_retries': 0, 'request_interval': 0,
                    'http_timeout': 1, 'max_member_bytes': len(body)})
                http = PublicHTTP(task, opener=opener)
                with http.open(URL, 'application/vnd.orcid+xml') as reader:
                    actual = consume(reader, 7)
                self.assertEqual(actual, body)
                self.assertEqual(hashlib.sha256(actual).hexdigest(), hashlib.sha256(body).hexdigest())
                self.assertEqual(http.bytes_received, len(wire))
                self.assertEqual(requests[0].get_header('Accept-encoding'), 'gzip')
                self.assertTrue(response.closed)

    def test_fragmented_wire_crc_truncation_trailing_members_and_expansion(self):
        body = b'x' * 100000
        wire = gzip.compress(body, mtime=0)
        response = Response(wire, fragment=1)
        with XMLResponse(SourceResponse(response), len(body), lambda size: None) as reader:
            self.assertEqual(consume(reader, 29), body)
        corrupted = bytearray(wire);corrupted[-8] ^= 1
        cases = [(wire[:-1], len(body), 'truncated'),
            (wire + b'x', len(body), 'trailing'), (wire + wire, len(body), 'trailing'),
            (bytes(corrupted), len(body), 'invalid'), (wire, len(body)-1, 'exceeds'),
            (b'x'*100, 99, 'wire')]
        for invalid, limit, reason in cases:
            with self.subTest(reason=reason):
                response = Response(invalid, 'identity' if reason == 'wire' else 'gzip')
                with self.assertRaisesRegex(ValueError, reason):
                    with XMLResponse(SourceResponse(response), limit, lambda size: None) as reader:
                        consume(reader)
                self.assertTrue(response.closed)

    def test_unsupported_encoding_deadline_and_unbounded_reads(self):
        response = Response(b'', 'br')
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            XMLResponse(SourceResponse(response), 100, lambda size: None)
        self.assertTrue(response.closed)
        response = Response(gzip.compress(b'xml'))
        calls = []
        def check():
            calls.append(1)
            if len(calls) > 2:raise StageDeadline('bounded test deadline')
        with XMLResponse(SourceResponse(response, check), 100, lambda size: None) as reader:
            self.assertEqual(reader.read(0), b'')
            with self.assertRaisesRegex(ValueError, 'unbounded'):reader.read(-1)
            self.assertEqual(reader.read(1), b'x')
            with self.assertRaises(StageDeadline):reader.read(1)
        self.assertTrue(response.closed)


class XMLIngestTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_compressed_affiliation_evidence_and_bad_second_crc_rollback(self):
        from gapi.global_sources import ingest_orcid_api
        person = fixtures.PERSON
        body = fixtures.XML.replace(b'<record path="'+b'/'+person.encode()+b'"',
            b'<educations path="/'+person.encode()+b'/educations"').replace(b'</record>', b'</educations>')
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt):
                s = self.store(fixtures.archive([]), work_dir=str(self.root / ('bad' if corrupt else 'good')))
                def opener(request, **kwargs):
                    education = request.full_url.endswith('/educations')
                    raw = body if education else b'<employments path="/'+person.encode()+b'/employments"/>'
                    packed = bytearray(gzip.compress(raw, mtime=0))
                    if corrupt and not education:packed[-8] ^= 1
                    response = Response(bytes(packed))
                    response.geturl = lambda: request.full_url
                    return response
                try:
                    http = PublicHTTP(s.task, opener=opener, sleep=lambda _: None)
                    result = ingest_orcid_api(s, http, [person])
                    self.assertEqual(result['events'], 0 if corrupt else 1)
                    self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0], 0 if corrupt else 2)
                    if not corrupt:
                        digest = s.db.execute("SELECT sha256 FROM member_parts WHERE part='educations'").fetchone()[0]
                        self.assertEqual(digest, hashlib.sha256(body).hexdigest())
                    else:
                        self.assertIsNone(s.seen('orcid-public-api', person))
                finally:s.close()


if __name__ == '__main__':unittest.main()
