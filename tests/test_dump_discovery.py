import bz2
import gzip
import hashlib
import io
import json
import unittest

from gapi.dump_discovery import bounded_lines, discover_dump, load_feed, MAX_LINE
from gapi.task import BudgetStop
import test_streaming as fixtures
from test_streaming import archive


def dump_bytes(count=3, compression='bz2'):
    rows = [{'id':'Q'+str(i+1),'claims':{'P69':[{}],'P108':[{}],
        'P496':[{'mainsnak':{'datavalue':{'value':'0000-0002-1825-0097'}}}]}} for i in range(count)]
    plain = b'[\n'+b',\n'.join(json.dumps(r).encode() for r in rows)+b'\n]\n'
    return bz2.compress(plain) if compression=='bz2' else gzip.compress(plain,mtime=0)


def spec_for(body, compression='bz2'):
    spec = json.loads(__import__('pathlib').Path('configs/discovery-dump.json').read_text())
    spec.update(size_bytes=len(body),sha1=hashlib.sha1(body).hexdigest(),compression=compression)
    if compression=='gzip':
        spec['url'] = spec['url'].removesuffix('bz2')+'gz'
    return spec


class HTTP:
    def __init__(self, body): self.body=body
    def open(self, url):
        class Response(io.BytesIO):
            headers = {'Content-Length':str(len(self.body))}
            def read(self,n=-1):
                if n < 0 or n > 65536:
                    raise AssertionError('unbounded compressed read')
                return super().read(n)
        return Response(self.body)


class DumpTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_realistic_bulk_discovery_no_raw_or_canonical_events(self):
        for compression in ('bz2','gzip'):
            with self.subTest(compression=compression):
                s=self.store(archive([]))
                try:
                    body=dump_bytes(compression=compression)
                    spec=spec_for(body,compression)
                    with s.db:
                        s.db.execute('DELETE FROM discovery_feeds')
                    result=discover_dump(s,HTTP(body),spec)
                    self.assertEqual(result['status'],'verified')
                    self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],4)
                    self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
                    self.assertTrue(discover_dump(s,HTTP(body),spec)['already_complete'])
                    self.assertFalse(any(p.suffix in ('.xml','.bz2','.gz') for p in s.task.root.rglob('*')))
                finally:s.close()

    def test_oversized_line_is_drained_and_next_line_retained(self):
        rows=list(bounded_lines(io.BytesIO(b'x'*(MAX_LINE+1)+b'\n{"id":"Q1"}\n')))
        self.assertIsNone(rows[0][0])
        self.assertEqual(rows[1][0],b'{"id":"Q1"}')

    def test_hash_mismatch_and_truncation_never_verify_or_create_events(self):
        s=self.store(archive([]))
        try:
            body=dump_bytes()
            spec=spec_for(body);spec['sha1']='0'*40
            with self.assertRaisesRegex(ValueError,'checksum'):
                discover_dump(s,HTTP(body),spec)
            self.assertEqual(s.db.execute('SELECT status FROM discovery_feeds').fetchone()[0],'failed')
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            with s.db:s.db.execute('DELETE FROM discovery_feeds')
            with self.assertRaises((EOFError,OSError,ValueError)):
                discover_dump(s,HTTP(body[:-8]),spec_for(body[:-8]))
            self.assertNotEqual(s.db.execute('SELECT status FROM discovery_feeds').fetchone()[0],'verified')
        finally:s.close()

    def test_interruption_replay_deduplicates_and_checks_prefix(self):
        from unittest.mock import patch
        import gapi.dump_discovery as module
        body=dump_bytes(600)
        s=self.store(archive([]))
        original=module.bounded_lines
        def stop_after(stream):
            for i,item in enumerate(original(stream)):
                if i==300:raise BudgetStop('injected process boundary')
                yield item
        try:
            with patch.object(module,'bounded_lines',stop_after):
                result=discover_dump(s,HTTP(body),spec_for(body))
            self.assertEqual(result['status'],'provisional')
            self.assertGreater(result['lines'],0)
            self.assertEqual(discover_dump(s,HTTP(body),spec_for(body))['status'],'verified')
            self.assertEqual(s.db.execute("SELECT count(*) FROM discovery WHERE source='ingest-wikidata'").fetchone()[0],600)
        finally:s.close()

    def test_changed_feed_identity_refuses_resume(self):
        s=self.store(archive([]))
        try:
            body=dump_bytes()
            discover_dump(s,HTTP(body),spec_for(body))
            changed=spec_for(body);changed['sha1']='0'*40
            with self.assertRaisesRegex(ValueError,'changed'):
                discover_dump(s,HTTP(body),changed)
        finally:s.close()

    def test_changed_replay_prefix_is_rejected_repeatedly_without_rebasing(self):
        from unittest.mock import patch
        import gapi.dump_discovery as module
        body=dump_bytes(600)
        s=self.store(archive([]))
        original=module.bounded_lines
        def stop_after(stream):
            for i,item in enumerate(original(stream)):
                if i==300:raise BudgetStop('stop')
                yield item
        def corrupt_first(stream):
            for i,item in enumerate(original(stream)):
                yield (item[0],'0'*64) if i==0 else item
        try:
            with patch.object(module,'bounded_lines',stop_after):
                discover_dump(s,HTTP(body),spec_for(body))
            before=s.db.execute('SELECT prefix_sha256 FROM discovery_feeds').fetchone()[0]
            for _ in range(2):
                with patch.object(module,'bounded_lines',corrupt_first), self.assertRaisesRegex(ValueError,'prefix changed'):
                    discover_dump(s,HTTP(body),spec_for(body))
                self.assertEqual(s.db.execute('SELECT prefix_sha256 FROM discovery_feeds').fetchone()[0],before)
        finally:s.close()

    def test_compressed_budget_stops_without_promotion(self):
        s=self.store(archive([]))
        try:
            body=dump_bytes(1000)
            result=discover_dump(s,HTTP(body),spec_for(body),max_bytes=64)
            self.assertEqual(result['status'],'provisional')
            self.assertLessEqual(result['compressed_bytes'],64)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:s.close()

    def test_exact_official_feed_validation(self):
        p=self.root/'feed.json'
        p.write_text(json.dumps(spec_for(dump_bytes())))
        self.assertEqual(load_feed(p)['license'],'CC0-1.0')
        data=json.loads(p.read_text());data['url']='https://dumps.wikimedia.org/latest.json.bz2'
        p.write_text(json.dumps(data))
        with self.assertRaises(ValueError):load_feed(p)
