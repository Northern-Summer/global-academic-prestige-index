import io
import json
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
