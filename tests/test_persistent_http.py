import io
import unittest
from types import SimpleNamespace
from urllib.error import URLError
from urllib.request import Request
from email.message import Message

from gapi.persistent_http import SingleHTTPSHandler
from gapi.source_http import PublicHTTP,SourceRedirect


class Response(io.BytesIO):
    def __init__(self,body=b'{}',status=200,headers=None):
        super().__init__(body)
        self.fp=self;self.will_close=False;self.status=self.code=status
        self.reason='test';self.headers=Message()
        for key,value in {'Content-Type':'application/json',**(headers or {})}.items():self.headers[key]=value

    def read(self,n=-1):
        data=super().read(n)
        if self.tell()==len(self.getvalue()):self.fp=None
        return data

    def geturl(self):return self.url
    def info(self):return self.headers


class Connection:
    def __init__(self,host,**kwargs):
        self.host=host;self.kwargs=kwargs;self.sock=SimpleNamespace(settimeout=lambda x:None)
        self.responses=[];self.requests=[];self.closed=False;self.tunnel=None
    def set_tunnel(self,host,headers):self.tunnel=(host,headers)
    def request(self,*args):self.requests.append(args)
    def getresponse(self):
        result=self.responses.pop(0) if self.responses else Response()
        if isinstance(result,Exception):raise result
        return result
    def close(self):self.closed=True;self.sock=None


class PersistentTests(unittest.TestCase):
    def setup_handler(self):
        created=[]
        def factory(*args,**kwargs):
            c=Connection(*args,**kwargs);created.append(c);return c
        return SingleHTTPSHandler(connection_factory=factory),created

    def request(self,url='https://pub.orcid.org/v3.0/0000-0002-1825-0097/educations'):
        r=Request(url);r.timeout=3;return r

    def test_complete_body_reuses_single_connection_and_partial_body_discards(self):
        h,created=self.setup_handler()
        r=h.https_open(self.request());self.assertEqual(r.read(65536),b'{}');r.close()
        r=h.https_open(self.request());r.read(1);r.close()
        self.assertTrue(created[0].closed)
        with h.https_open(self.request()) as r:r.read(65536)
        self.assertEqual(len(created),2)
        self.assertEqual((h.opened_connections,h.reused_connections),(2,1))
        self.assertEqual(created[0].requests[0][3]['Connection'],'keep-alive')
        h.close();self.assertTrue(created[-1].closed)

    def test_one_active_response_host_switch_and_no_hidden_retry(self):
        h,created=self.setup_handler();r=h.https_open(self.request())
        with self.assertRaises(RuntimeError):h.https_open(self.request())
        r.read(65536);r.close()
        with h.https_open(self.request('https://www.wikidata.org/wiki/Special:EntityData/Q1.json')) as r:r.read(65536)
        self.assertTrue(created[0].closed)
        created[-1].responses.append(ConnectionResetError('stale connection'))
        with self.assertRaises(URLError):h.https_open(self.request('https://www.wikidata.org/wiki/Special:EntityData/Q1.json'))
        self.assertEqual(len(created),2)
        self.assertIsNone(h.connection)

    def test_proxy_tunnel_credential_is_not_forwarded_to_origin(self):
        h,created=self.setup_handler()
        r=self.request();r.set_proxy('proxy.example:8080','http');r.add_header('Proxy-Authorization','Basic test-fixture')
        with h.https_open(r) as response:response.read(65536)
        self.assertEqual(created[0].tunnel,('pub.orcid.org',{'Proxy-Authorization':'Basic test-fixture'}))
        self.assertNotIn('Proxy-Authorization',created[0].requests[0][3]);h.close()

    def test_existing_orcid_redirect_gate_is_retained(self):
        task=SimpleNamespace(config={'http_timeout':3,'request_interval':1,'max_retries':0})
        http=PublicHTTP(task,reuse_connections=True,sleep=lambda _:None)
        h,created=self.setup_handler()
        def factory(*args,**kwargs):
            c=Connection(*args,**kwargs)
            c.responses.append(Response(b'',301,{'Location':'https://pub.orcid.org/v3.0/0000-0002-1825-0097/educations'}))
            created.append(c);return c
        http.transport.factory=factory
        with self.assertRaises(SourceRedirect):http.open(self.request().full_url)
        self.assertEqual(http.requests_count,1)
        http.close();self.assertTrue(created[-1].closed)

    def test_non_get_is_rejected_before_network(self):
        h,created=self.setup_handler();req=self.request();req.data=b'payload'
        with self.assertRaises(ValueError):h.https_open(req)
        self.assertEqual(created,[])

    def test_robots_rules_after_first_chunk_and_oversize_are_not_ignored(self):
        task=SimpleNamespace(config={'http_timeout':3,'request_interval':1,'max_retries':0})
        http=PublicHTTP(task,reuse_connections=True,sleep=lambda _:None)
        body=b'# padding\n'*7000+b'User-agent: *\nDisallow: /private\n'
        def factory(*args,**kwargs):
            c=Connection(*args,**kwargs);c.responses.append(Response(body));return c
        http.transport.factory=factory
        try:
            with self.assertRaises(PermissionError):http.allowed('https://experts.colorado.edu/private')
            http.close();http.robots.clear()
            body=b'# padding\n'*30000
            with self.assertRaisesRegex(ValueError,'robots file exceeds limit'):
                http.allowed('https://experts.colorado.edu/private')
        finally:http.close()

    def test_server_close_and_http_error_retire_connection(self):
        task=SimpleNamespace(config={'http_timeout':3,'request_interval':1,'max_retries':0})
        http=PublicHTTP(task,reuse_connections=True,sleep=lambda _:None);created=[]
        def factory(*args,**kwargs):
            c=Connection(*args,**kwargs)
            c.responses.append(Response(b'not found',404) if not created else Response())
            created.append(c);return c
        http.transport.factory=factory
        from urllib.error import HTTPError
        with self.assertRaises(HTTPError):http.json(self.request().full_url)
        self.assertTrue(created[0].closed)
        self.assertEqual(http.json(self.request().full_url)[0],{})
        created[-1].responses.append(Response())
        created[-1].responses[-1].will_close=True
        self.assertEqual(http.json(self.request().full_url)[0],{})
        self.assertIsNone(http.transport.connection)
        http.close()

    def test_read_failure_discards_socket_and_never_replays_a_partial_body(self):
        h,created=self.setup_handler()
        r=h.https_open(self.request());r.read(65536);r.close()
        class Broken(Response):
            def read(self,n=-1):raise TimeoutError('partial body')
        created[0].responses.append(Broken())
        r=h.https_open(self.request())
        with self.assertRaises(TimeoutError):r.read(100)
        self.assertTrue(created[0].closed)
        self.assertEqual(len(created[0].requests),2)
        r.close();h.close()

    def test_retired_response_cannot_close_a_new_active_response(self):
        h,created=self.setup_handler();old=h.https_open(self.request())
        old.read(65536);old.close()
        current=h.https_open(self.request())
        with self.assertRaises(ValueError):old.read(1)
        old.close()
        self.assertFalse(created[0].closed)
        self.assertEqual(current.read(65536),b'{}')
        current.close();h.close()
