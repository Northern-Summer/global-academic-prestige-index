import io
import json
import unittest
from types import SimpleNamespace
from urllib.error import HTTPError

from gapi.source_http import PublicHTTP, safe_url


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
