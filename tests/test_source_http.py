import io
import json
import gzip
import hashlib
import unittest
from types import SimpleNamespace
from urllib.error import HTTPError

from gapi.source_http import PublicHTTP, safe_url, SourcePause


class Response(io.BytesIO):
    status = 200
    def __init__(self, body=b"{}", url="https://pub.orcid.org/v3.0/0000-0002-1825-0097/record", content_type="application/json"):
        super().__init__(body); self.url=url; self.headers={"Content-Type":content_type}
    def geturl(self): return self.url


class HTTPTests(unittest.TestCase):
    def test_gzip_json_preserves_decoded_source_hash_and_counts_wire_bytes(self):
        body=json.dumps({'entities':{'Q42':{'claims':['education']*100}}}).encode()
        wire=gzip.compress(body,mtime=0)
        requests=[]
        def opener(request,**kwargs):
            requests.append(request)
            response=Response(wire)
            response.headers['Content-Encoding']='gzip'
            return response
        http=PublicHTTP(self.task(),opener=opener,sleep=lambda _:None)
        value,checksum=http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/record')
        self.assertEqual(value,json.loads(body))
        self.assertEqual(checksum,hashlib.sha256(body).hexdigest())
        self.assertEqual(http.bytes_received,len(wire))
        self.assertEqual(requests[0].get_header('Accept-encoding'),'gzip')

    def test_gzip_json_rejects_expansion_truncation_corruption_and_trailing_data(self):
        body=b'{"value":"'+b'a'*300+b'"}'
        valid=gzip.compress(body,mtime=0)
        corrupt=bytearray(valid);corrupt[-8]^=1
        cases=[(valid,100,'exceeds bound'),(valid[:-1],1000,'truncated'),
               (bytes(corrupt),1000,'invalid'),(valid+b'x',1000,'trailing'),
               (valid+valid,1000,'trailing')]
        for wire,limit,message in cases:
            with self.subTest(message=message,wire_size=len(wire)):
                response=Response(wire);response.headers['Content-Encoding']='gzip'
                http=PublicHTTP(self.task(),opener=lambda *a,**k:response,sleep=lambda _:None)
                with self.assertRaisesRegex(ValueError,message):
                    http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/record',limit=limit)
                self.assertTrue(response.closed)

    def test_gzip_json_handles_fragmented_reads_and_exact_limit(self):
        body=b'{"valid":true}'
        class Fragmented(Response):
            def read(self,n=-1):return super().read(min(n,1))
        response=Fragmented(gzip.compress(body,mtime=0))
        response.headers['Content-Encoding']='gzip'
        http=PublicHTTP(self.task(),opener=lambda *a,**k:response,sleep=lambda _:None)
        self.assertEqual(http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/record',limit=100)[0],{'valid':True})
        # Equal decoded size is accepted (wire is shorter for repetitive data).
        body=json.dumps({'value':'x'*1000}).encode()
        response=Response(gzip.compress(body,mtime=0));response.headers['Content-Encoding']='gzip'
        http=PublicHTTP(self.task(),opener=lambda *a,**k:response,sleep=lambda _:None)
        self.assertEqual(http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/record',limit=len(body))[0],json.loads(body))

    def test_json_rejects_unknown_encoding_and_invalid_bounds(self):
        response=Response();response.headers['Content-Encoding']='br'
        http=PublicHTTP(self.task(),opener=lambda *a,**k:response,sleep=lambda _:None)
        with self.assertRaisesRegex(ValueError,'unsupported'):
            http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/record')
        self.assertTrue(response.closed)
        for limit in (0,-1,True,1.5,2*1024**2+1):
            with self.assertRaisesRegex(ValueError,'bound'):
                http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/record',limit=limit)

    def test_dump_range_strict_identity_and_position(self):
        url='https://dumps.wikimedia.org/wikidatawiki/entities/20261026/wikidata-20261026-all.json.bz2'
        initial={'ETag':'"fixed"'}
        cases=[({},True),({'Content-Range':'bytes 3-9/10'},False),
               ({'Content-Range':'bytes 4-9/11'},False),({'Content-Length':'5'},False),
               ({'ETag':'"changed"'},False),({'Content-Encoding':'gzip'},False)]
        for changed,valid in cases:
            with self.subTest(changed=changed):
                response=Response(b'123456',url=url);response.status=206
                response.headers={'Content-Range':'bytes 4-9/10','Content-Length':'6',**initial,**changed}
                calls=[]
                def opener(request,**kwargs):calls.append(request);return response
                http=PublicHTTP(self.task(),opener=opener,sleep=lambda _:None)
                from unittest.mock import patch
                with patch.object(http,'allowed') as allowed:
                    if valid:
                        with http.open_range(url,4,10,initial) as stream:self.assertEqual(stream.read(6),b'123456')
                        self.assertEqual(calls[0].get_header('Range'),'bytes=4-')
                        self.assertEqual(calls[0].get_header('If-range'),'"fixed"')
                        allowed.assert_called_once_with(url)
                    else:
                        with self.assertRaises(ValueError):http.open_range(url,4,10,initial)
                self.assertTrue(response.closed)

    def test_dump_range_ignored_or_refused_is_not_concatenated(self):
        from unittest.mock import patch
        url='https://dumps.wikimedia.org/wikidatawiki/entities/20261026/wikidata-20261026-all.json.bz2'
        response=Response(url=url)
        http=PublicHTTP(self.task(),opener=lambda *a,**k:response,sleep=lambda _:None)
        with patch.object(http,'allowed'),self.assertRaisesRegex(RuntimeError,'HTTP 200'):
            http.open_range(url,4,10,{'ETag':'"fixed"'})
        self.assertTrue(response.closed)
        calls=[]
        def denied(request,**kwargs):
            calls.append(1);raise HTTPError(url,403,'Forbidden',{},None)
        http=PublicHTTP(self.task(),opener=denied,sleep=lambda _:None)
        with patch.object(http,'allowed'),self.assertRaises(HTTPError):
            http.open_range(url,4,10,{'ETag':'"fixed"'})
        self.assertEqual(calls,[1])
        for headers in ({},{'ETag':'W/"weak"'},{'Last-Modified':'bad'}):
            with self.assertRaises(ValueError):http.open_range(url,4,10,headers)
        with self.assertRaises(ValueError):http.open_range('https://www.wikidata.org/a',4,10,{'ETag':'"fixed"'})

    def test_registered_career_range_requires_pinned_checksum_and_exact_position(self):
        from gapi.easy_orcid_census import ARCHIVES
        from unittest.mock import patch
        url,total,md5=ARCHIVES['easy-orcid-2023.6-full'];offset=total-6
        for changes,valid in (({},True),({'Content-Range':f'bytes {offset-1}-{total-1}/{total}'},False)):
            response=Response(b'123456',url=url);response.status=206
            response.headers={'Content-Range':f'bytes {offset}-{total-1}/{total}','Content-Length':'6',**changes}
            http=PublicHTTP(self.task(),opener=lambda *a,**k:response,sleep=lambda _:None)
            with patch.object(http,'allowed'):
                if valid:
                    with http.open_range(url,offset,total,{},expected_md5=md5) as r:self.assertEqual(r.read(6),b'123456')
                else:
                    with self.assertRaises(ValueError):http.open_range(url,offset,total,{},expected_md5=md5)
            self.assertTrue(response.closed)
        for wrong in (None,'0'*32):
            with self.assertRaises(ValueError):http.open_range(url,offset,total,{},expected_md5=wrong)

    def task(self):
        return SimpleNamespace(config={"max_retries":2,"request_interval":1,"http_timeout":2})

    def test_no_retry_on_access_denial(self):
        calls=[]
        def opener(request,**kwargs):
            calls.append(request.full_url)
            raise HTTPError(request.full_url,403,"Forbidden",{},None)
        with self.assertRaises(HTTPError): PublicHTTP(self.task(),opener=opener,sleep=lambda _:None).json("https://pub.orcid.org/v3.0/0000-0002-1825-0097/record")
        self.assertEqual(len(calls),1)

    def test_rate_limit_honors_retry_after(self):
        calls=[]; sleeps=[]
        def opener(request,**kwargs):
            calls.append(1)
            if len(calls)==1: raise HTTPError(request.full_url,429,"Slow down",{"Retry-After":"5"},None)
            return Response()
        self.assertEqual(PublicHTTP(self.task(),opener=opener,sleep=sleeps.append).json("https://pub.orcid.org/v3.0/0000-0002-1825-0097/record")[0],{})
        self.assertIn(5,sleeps)

    def test_robots_disallow_and_unusable_202_block_crawling(self):
        def opener(request,**kwargs):
            return Response(b"User-agent: *\nDisallow: /\n",url=request.full_url,content_type="text/plain")
        with self.assertRaises(PermissionError): PublicHTTP(self.task(),opener=opener).open("https://orcid.figshare.com/ndownloader/files/70342863")
        response=Response(); response.status=202
        with self.assertRaisesRegex(RuntimeError,"HTTP 202"):
            PublicHTTP(self.task(),opener=lambda *a,**k:response).json("https://pub.orcid.org/v3.0/0000-0002-1825-0097/record")

    def test_new_official_adapter_still_obeys_robots(self):
        requests=[]
        def opener(request,**kwargs):
            requests.append(request.full_url)
            return Response(b'User-agent: *\nDisallow: /display/\n',url=request.full_url,content_type='text/plain')
        with self.assertRaises(PermissionError):
            PublicHTTP(self.task(),opener=opener).open('https://experts.colorado.edu/display/fisid_1234')
        self.assertEqual(requests,['https://experts.colorado.edu/robots.txt'])

    def test_untrusted_and_credentialed_urls_are_rejected(self):
        for url in ("http://www.wikidata.org/", "https://127.0.0.1/", "https://pub.orcid.org.evil.test/", "https://user:secret@pub.orcid.org/", "https://pub.orcid.org:444/"):
            with self.assertRaises(ValueError): safe_url(url)

    def test_oversized_response_rejected(self):
        with self.assertRaisesRegex(ValueError,"exceeds bound"):
            PublicHTTP(self.task(),opener=lambda *a,**k:Response(b"x"*100)).json("https://pub.orcid.org/v3.0/0000-0002-1825-0097/record",limit=10)

    def test_truncated_chunked_body_is_normalized_and_closed_without_retry(self):
        from http.client import IncompleteRead
        class Truncated(Response):
            def read(self,n=-1):raise IncompleteRead(b'partial')
        response=Truncated()
        calls=[]
        def opener(*args,**kwargs):calls.append(1);return response
        with self.assertRaisesRegex(OSError,'IncompleteRead'):
            PublicHTTP(self.task(),opener=opener).json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/record')
        self.assertTrue(response.closed)
        self.assertEqual(len(calls),1)

    def test_stage_deadline_bounds_retries_and_restores_outer_runtime(self):
        from unittest.mock import patch
        calls=[]
        def opener(request,**kwargs):
            calls.append(kwargs['timeout'])
            raise HTTPError(request.full_url,429,'Slow',{'Retry-After':'5'},None)
        http=PublicHTTP(self.task(),opener=opener,sleep=lambda _:self.fail('must not wait beyond deadline'))
        with patch('gapi.source_http.time.monotonic',return_value=100):
            with http.within(1),self.assertRaisesRegex(SourcePause,'stage'):
                http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/educations')
        self.assertEqual(calls,[1])
        self.assertIsNone(http.deadline)

    def test_body_read_obeys_expired_stage_and_closes_response(self):
        from unittest.mock import patch
        response=Response()
        http=PublicHTTP(self.task(),opener=lambda *a,**k:response)
        with patch('gapi.source_http.time.monotonic',return_value=100):
            with http.within(2):
                with http.open('https://pub.orcid.org/v3.0/0000-0002-1825-0097/employments') as stream:
                    with patch('gapi.source_http.time.monotonic',return_value=103),self.assertRaises(TimeoutError):
                        stream.read(1)
        self.assertTrue(response.closed)

    def test_socket_timeout_at_stage_boundary_is_not_an_early_source_failure(self):
        from unittest.mock import patch
        from gapi.source_http import StageDeadline
        clock=[100]
        class Expiring(Response):
            def read(self,size):
                clock[0]=103
                raise TimeoutError('socket exceeded admitted window')
        response=Expiring()
        http=PublicHTTP(self.task(),opener=lambda *a,**k:response)
        with patch('gapi.source_http.time.monotonic',side_effect=lambda:clock[0]):
            with http.within(2),self.assertRaises(StageDeadline):
                http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/employments')
        self.assertTrue(response.closed)
        # A timeout while there is still stage time is a real source failure.
        clock[0]=100
        response=Expiring()
        http=PublicHTTP(self.task(),opener=lambda *a,**k:response)
        with patch('gapi.source_http.time.monotonic',side_effect=lambda:clock[0]):
            with http.within(10),self.assertRaises(TimeoutError) as result:
                http.json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/employments')
        self.assertNotIsInstance(result.exception,StageDeadline)

    def test_long_and_date_retry_after_pause_entire_source(self):
        from unittest.mock import patch
        for header in ('120','Thu, 01 Jan 1970 00:03:40 GMT','malformed-date'):
            calls=[]
            def opener(request,**kwargs):
                calls.append(1)
                raise HTTPError(request.full_url,429,'Slow',{'Retry-After':header},None)
            with patch('gapi.source_http.time.time',return_value=100):
                with self.subTest(header=header),self.assertRaises(SourcePause) as result:
                    PublicHTTP(self.task(),opener=opener,sleep=lambda _:None).json('https://pub.orcid.org/v3.0/0000-0002-1825-0097/educations')
            self.assertGreaterEqual(result.exception.retry_at,220)
            self.assertEqual(calls,[1])
