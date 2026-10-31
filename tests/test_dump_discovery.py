import bz2
import gzip
import hashlib
import io
import json
import unittest

from gapi.dump_discovery import bounded_lines, discover_dump, load_feed, MAX_LINE, FeedReader
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

    def test_same_process_reconnect_preserves_decompression_hash_and_dedupe(self):
        from contextlib import nullcontext
        for compression in ('bz2','gzip'):
            for transport_error in (False,True):
                with self.subTest(compression=compression,error=transport_error):
                    body=dump_bytes(600,compression);cut=len(body)//2
                    class Interrupted(io.BytesIO):
                        headers={'Content-Length':str(len(body)),'ETag':'"fixed"'}
                        def read(self,n):
                            if transport_error and self.tell()==cut:raise OSError('connection lost')
                            return super().read(n)
                    first=Interrupted(body[:cut]);resumed=[]
                    class ReconnectHTTP:
                        def open(self,url):return first
                        def within(self,seconds):return nullcontext()
                        def open_range(self,url,offset,total,headers):
                            self_offset=offset
                            self_total=total
                            if self_offset!=cut or self_total!=len(body):raise AssertionError('wrong range')
                            stream=io.BytesIO(body[offset:]);resumed.append(stream);return stream
                    s=self.store(archive([]))
                    try:
                        with s.db:s.db.execute('DELETE FROM discovery_feeds')
                        result=discover_dump(s,ReconnectHTTP(),spec_for(body,compression))
                        self.assertEqual(result['status'],'verified');self.assertEqual(result['reconnects'],1)
                        self.assertEqual(s.db.execute('SELECT sha256 FROM discovery_feeds').fetchone()[0],hashlib.sha256(body).hexdigest())
                        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],601)
                        self.assertTrue(first.closed);self.assertTrue(resumed[0].closed)
                    finally:s.close()

    def test_reconnect_is_finite_and_does_not_defeat_budgets(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        task=SimpleNamespace(check_memory=lambda:None)
        resume=Mock(side_effect=lambda offset:io.BytesIO(b''))
        reader=FeedReader(io.BytesIO(b'x'),task,resume=resume,expected_bytes=10,max_reconnects=2)
        self.assertEqual(reader.read(1),b'x')
        with self.assertRaises(EOFError):reader.read(1)
        self.assertEqual(resume.call_count,2);reader.close()
        for kwargs in ({'max_bytes':1},{'deadline':0}):
            resume=Mock();reader=FeedReader(io.BytesIO(b'x'),task,resume=resume,expected_bytes=10,**kwargs)
            if 'max_bytes' in kwargs:self.assertEqual(reader.read(1),b'x')
            with self.assertRaises(BudgetStop):reader.read(1)
            resume.assert_not_called();reader.close()
        resume=Mock();reader=FeedReader(io.BytesIO(b'x'),task,resume=resume,expected_bytes=1)
        self.assertEqual(reader.read(1),b'x');self.assertEqual(reader.read(1),b'')
        resume.assert_not_called();reader.close()
        resume=Mock();reader=FeedReader(io.BytesIO(b'xx'),task,resume=resume,expected_bytes=1,max_bytes=1)
        self.assertEqual(reader.read(8),b'x');self.assertEqual(reader.read(1),b'')
        resume.assert_not_called();reader.close()

    def test_successful_last_read_after_deadline_cannot_verify(self):
        from unittest.mock import patch
        import gapi.dump_discovery as module
        clock=[100]
        body=dump_bytes()
        class Late(io.BytesIO):
            headers={'Content-Length':str(len(body))}
            def read(self,n):
                data=super().read(n);clock[0]=102;return data
        response=Late(body)
        class LateHTTP:
            def open(self,url):return response
        s=self.store(archive([]))
        try:
            with patch.object(module.time,'monotonic',side_effect=lambda:clock[0]):
                result=discover_dump(s,LateHTTP(),spec_for(body),max_seconds=1)
            self.assertEqual(result['status'],'provisional')
            self.assertEqual(result['compressed_bytes'],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
            self.assertTrue(response.closed)
        finally:s.close()

    def test_long_stream_distinguishes_successful_reconnects_from_stalled_retries(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        task=SimpleNamespace(check_memory=lambda:None)
        resume=Mock(side_effect=lambda offset:io.BytesIO(b''))
        reader=FeedReader(io.BytesIO(b'x'),task,resume=resume,expected_bytes=100,
                          max_reconnects=20,max_stalled_reconnects=2)
        self.assertEqual(reader.read(1),b'x')
        with self.assertRaises(EOFError):reader.read(1)
        self.assertEqual(resume.call_count,2);reader.close()
        resume=Mock(side_effect=lambda offset:io.BytesIO(b'x'))
        reader=FeedReader(io.BytesIO(b'x'),task,resume=resume,expected_bytes=100,
                          max_reconnects=10,max_stalled_reconnects=2)
        try:
            for _ in range(11):self.assertEqual(reader.read(1),b'x')
            with self.assertRaises(EOFError):reader.read(1)
            self.assertEqual(resume.call_count,10)
        finally:reader.close()

    def test_initial_open_is_inside_one_stage_deadline(self):
        from unittest.mock import patch
        import gapi.dump_discovery as module
        from gapi.source_http import StageDeadline
        body=dump_bytes();clock=[100]
        class LateOpen(HTTP):
            def open(self,url):
                clock[0]=102
                return super().open(url)
        s=self.store(archive([]))
        try:
            with patch.object(module.time,'monotonic',side_effect=lambda:clock[0]):
                result=discover_dump(s,LateOpen(body),spec_for(body),max_seconds=1)
            self.assertEqual(result['status'],'provisional')
            self.assertEqual(result['compressed_bytes'],0)
            class Expired:
                def open(self,url):raise StageDeadline('source stage ended while opening')
            result=discover_dump(s,Expired(),spec_for(body),max_seconds=1)
            self.assertEqual(result['status'],'provisional')
            self.assertEqual(result['compressed_bytes'],0)
        finally:s.close()

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

    def test_rejected_records_keep_bounded_hash_and_source_line_without_raw_material(self):
        oversized=b'{"ignored":"'+b'x'*MAX_LINE+b'"}'
        invalid=b'{not valid json}'
        body=bz2.compress(b'[\n'+oversized+b'\n'+invalid+b'\n]\n')
        s=self.store(archive([]))
        try:
            result=discover_dump(s,HTTP(body),spec_for(body))
            self.assertEqual(result['status'],'verified')
            self.assertEqual(result['rejected'],2)
            self.assertIn('2 record lines rejected',result['detail'])
            rows=s.db.execute("SELECT locator,reason FROM failures WHERE source='wikidata-dump' ORDER BY locator").fetchall()
            self.assertEqual(len(rows),2)
            self.assertTrue(rows[0][0].endswith(':line:2'))
            self.assertTrue(rows[1][0].endswith(':line:3'))
            self.assertIn(hashlib.sha256(oversized).hexdigest(),rows[0][1])
            self.assertIn(hashlib.sha256(invalid).hexdigest(),rows[1][1])
            self.assertLess(max(len(row[1]) for row in rows),256)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:s.close()

    def test_complete_prefix_can_resume_and_backfill_oversize_diagnostics_without_double_count(self):
        from unittest.mock import patch
        import gapi.dump_discovery as module
        oversized=b'{"ignored":"'+b'x'*MAX_LINE+b'"}'
        body=bz2.compress(b'[\n'+oversized+b'\n]\n')
        original=module.bounded_lines
        def interrupted_after_last_line(stream):
            yield from original(stream)
            raise BudgetStop('injected stop before final source validation')
        s=self.store(archive([]))
        try:
            with patch.object(module,'bounded_lines',interrupted_after_last_line):
                self.assertEqual(discover_dump(s,HTTP(body),spec_for(body))['status'],'provisional')
            with s.db:s.db.execute("DELETE FROM failures WHERE source='wikidata-dump'")
            result=discover_dump(s,HTTP(body),spec_for(body))
            self.assertEqual(result['status'],'verified')
            self.assertEqual(result['rejected'],1)
            self.assertEqual(s.db.execute("SELECT count(*) FROM failures WHERE source='wikidata-dump'").fetchone()[0],1)
        finally:s.close()

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

    def test_short_replay_keeps_longest_observed_prefix_for_next_ceiling(self):
        from unittest.mock import patch
        import gapi.dump_discovery as module
        body = dump_bytes(600)
        s = self.store(archive([]))
        original = module.bounded_lines
        def stop_after(stream):
            for i, item in enumerate(original(stream)):
                if i == 300:
                    raise BudgetStop('injected boundary after committed prefix')
                yield item
        try:
            with patch.object(module, 'bounded_lines', stop_after):
                discover_dump(s, HTTP(body), spec_for(body))
            before = s.db.execute('SELECT compressed_bytes,sha256,lines,prefix_sha256 FROM discovery_feeds').fetchone()
            result = discover_dump(s, HTTP(body), spec_for(body), max_bytes=16)
            self.assertEqual(result['compressed_bytes'], 16)
            self.assertGreater(before[0], 16)
            self.assertEqual(before, s.db.execute(
                'SELECT compressed_bytes,sha256,lines,prefix_sha256 FROM discovery_feeds').fetchone())
            self.assertEqual(discover_dump(s, HTTP(body), spec_for(body))['status'], 'verified')
        finally:
            s.close()

    def test_failed_open_preserves_prefix_and_does_not_claim_this_attempt_bytes(self):
        from unittest.mock import patch
        import gapi.dump_discovery as module
        body = dump_bytes(600)
        s = self.store(archive([]))
        original = module.bounded_lines
        def stop_after(stream):
            for i, item in enumerate(original(stream)):
                if i == 300:
                    raise BudgetStop('injected boundary after committed prefix')
                yield item
        class Unavailable:
            def open(self, url):
                raise OSError('injected source unavailable')
        try:
            with patch.object(module, 'bounded_lines', stop_after):
                discover_dump(s, HTTP(body), spec_for(body))
            before = s.db.execute('SELECT compressed_bytes,sha256,lines,prefix_sha256 FROM discovery_feeds').fetchone()
            with self.assertRaisesRegex(OSError, 'unavailable'):
                discover_dump(s, Unavailable(), spec_for(body))
            self.assertEqual(before, s.db.execute(
                'SELECT compressed_bytes,sha256,lines,prefix_sha256 FROM discovery_feeds').fetchone())
            self.assertEqual(s.db.execute('SELECT status FROM discovery_feeds').fetchone()[0], 'failed')
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)
        finally:
            s.close()
