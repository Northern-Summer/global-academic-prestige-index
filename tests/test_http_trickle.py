"""Exercise the actual stdlib HTTP buffer without a network, sleeps or raw files."""
import io
import gzip
import hashlib
from http.client import HTTPResponse
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gapi.source_http import PublicHTTP, SourceResponse, StageDeadline
from gapi.persistent_http import ManagedResponse


class Trickle(io.RawIOBase):
    def __init__(self, clock, body=b'abcdefghij', chunked=False, encoding='identity'):
        self.clock = clock
        self.header = (b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n'
                       if chunked else b'HTTP/1.1 200 OK\r\nContent-Length: '
                       + str(len(body)).encode() + b'\r\n\r\n')
        self.header = self.header[:-2] + b'Content-Encoding: ' + encoding.encode() + b'\r\n\r\n'
        self.body = (b'1\r\n' + b'\r\n1\r\n'.join(bytes([b]) for b in body)
                     + b'\r\n0\r\n\r\n') if chunked else body
        self.calls = 0

    def readable(self):return True

    def readinto(self, buffer):
        if self.header:
            data, self.header = self.header[:len(buffer)], self.header[len(buffer):]
        elif self.body:
            self.calls += 1
            self.clock[0] += 1
            data, self.body = self.body[:1], self.body[1:]
        else:
            return 0
        buffer[:len(data)] = data
        return len(data)


def response(clock, *, chunked=False, body=b'abcdefghij', encoding='identity'):
    raw = Trickle(clock, body=body, chunked=chunked, encoding=encoding)
    sock = SimpleNamespace(makefile=lambda mode: io.BufferedReader(raw))
    result = HTTPResponse(sock)
    result.begin()
    result.url = 'https://pub.orcid.org/v3.0/0000-0002-1825-0097/record'
    return result, raw


class HTTPTrickleTests(unittest.TestCase):
    def test_stdlib_filling_read_hides_multiple_socket_reads(self):
        clock = [100]
        stream, raw = response(clock)
        with stream:
            self.assertEqual(stream.read(65536), b'abcdefghij')
        self.assertEqual(raw.calls, 10)
        self.assertEqual(clock[0], 110)

    def test_stage_rechecks_between_real_buffered_body_reads(self):
        for managed in (False, True):
            with self.subTest(managed=managed):
                clock = [100]
                stream, raw = response(clock)
                closed = []
                if managed:
                    stream = ManagedResponse(stream, SimpleNamespace(close=lambda: closed.append(True)))
                task = SimpleNamespace(config={'http_timeout': 30})
                http = PublicHTTP(task)
                with patch('gapi.source_http.time.monotonic', side_effect=lambda: clock[0]):
                    with http.within(2), SourceResponse(stream, http.remaining) as reader:
                        self.assertEqual(reader.read(65536), b'a')
                        self.assertEqual(reader.read(65536), b'b')
                        with self.assertRaises(StageDeadline):reader.read(65536)
                self.assertTrue(raw.closed)
                self.assertEqual(raw.calls, 2)
                if managed:self.assertEqual(closed, [True])

    def test_complete_identity_and_chunked_bodies_are_unchanged(self):
        for chunked in (False, True):
            with self.subTest(chunked=chunked):
                stream, raw = response([100], chunked=chunked)
                with SourceResponse(stream) as reader:
                    parts = []
                    while block := reader.read(65536):parts.append(block)
                self.assertEqual(b''.join(parts), b'abcdefghij')
                self.assertTrue(raw.closed)

    def test_real_short_reads_preserve_gzip_hash_and_reject_invalid_trailers(self):
        body = b'{"value":"' + b'x' * 1000 + b'"}'
        wire = gzip.compress(body, mtime=0)
        corrupt = bytearray(wire)
        corrupt[-8] ^= 1
        task = SimpleNamespace(config={'http_timeout': 30, 'request_interval': 1, 'max_retries': 0})
        for packed, error in ((wire, None), (wire[:-1], 'truncated'), (bytes(corrupt), 'invalid')):
            with self.subTest(error=error):
                stream, raw = response([100], body=packed, encoding='gzip')
                http = PublicHTTP(task, opener=lambda *args, **kwargs: stream)
                if error:
                    with self.assertRaisesRegex(ValueError, error):http.json(stream.url)
                else:
                    data, checksum = http.json(stream.url)
                    self.assertEqual(data, {'value': 'x' * 1000})
                    self.assertEqual(checksum, hashlib.sha256(body).hexdigest())
                    self.assertEqual(http.bytes_received, len(wire))
                self.assertTrue(raw.closed)

    def test_partial_chunked_body_discards_managed_connection(self):
        stream, raw = response([100], chunked=True)
        raw.body = raw.body[:-5]
        closed = []
        managed = ManagedResponse(stream, SimpleNamespace(close=lambda: closed.append(True)))
        with self.assertRaisesRegex(OSError, 'IncompleteRead'):
            with SourceResponse(managed) as reader:
                while reader.read(65536):pass
        self.assertTrue(closed)
        self.assertTrue(raw.closed)


if __name__ == '__main__':unittest.main()
