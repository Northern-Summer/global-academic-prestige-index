"""Same-process gzip recovery through the existing bounded range reader."""
import hashlib
import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import test_streaming as fixtures
from gapi.source_http import PublicHTTP
from gapi.orcid_stream import stream_archive


URL='https://ndownloader.figshare.com/files/70342863'
SOURCE='orcid-2026-summary'


class Body(io.BytesIO):
    status=200
    def __init__(self, data, total, *, offset=0, cutoff=None):
        super().__init__(data)
        self.headers={'Content-Length':str(total-offset),'ETag':'"fixed"'}
        self.cutoff=cutoff
        if offset:
            self.status=206
            self.headers['Content-Range']=f'bytes {offset}-{total-1}/{total}'
    def geturl(self):
        return 'https://s3-eu-west-1.amazonaws.com/official-object'
    def read(self,n=-1):
        if not 0<=n<=65536:
            raise AssertionError('unbounded read')
        if self.cutoff is not None:
            if self.tell()>=self.cutoff:
                raise ConnectionResetError('injected transport reset')
            n=min(n,self.cutoff-self.tell())
        return super().read(n)


class ArchiveReconnectTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def prepared(self):
        data=fixtures.archive([(f'summaries/097/{fixtures.PERSON}.xml',fixtures.XML)])
        self.config['sources'][SOURCE]['url']=URL
        return data,self.store(data,checksum='md5:'+hashlib.md5(data).hexdigest())

    def transfer(self, store, data, *, change=None, fail=True, max_reconnects=256,
                 lowercase_initial=False):
        calls=[];responses=[];cut=len(data)//2
        def opener(request,**kwargs):
            value=request.get_header('Range');calls.append(value)
            if not value:
                body=Body(data,len(data),cutoff=cut if fail else None)
                if lowercase_initial:
                    from email.message import Message
                    headers=Message()
                    for key,value in body.headers.items():headers[key.lower()]=value
                    body.headers=headers
            else:
                offset=int(value.removeprefix('bytes=').removesuffix('-'))
                self.assertEqual(offset,cut)
                self.assertEqual(request.get_header('If-range'),'"fixed"')
                body=Body(data[offset:],len(data),offset=offset)
                if change:change(body)
            responses.append(body)
            return body
        http=PublicHTTP(store.task,opener=opener,sleep=lambda _:None)
        with patch.object(http,'allowed'):
            with http.open_archive(SOURCE,max_reconnects=max_reconnects) as response:
                result=stream_archive(store,SOURCE,response,commit_members=128)
                count=response.reconnects
        self.assertTrue(all(r.closed for r in responses))
        return result,calls,count

    def test_reset_preserves_gzip_hashes_and_commits_same_member_without_replay(self):
        data,store=self.prepared()
        try:
            result,calls,reconnects=self.transfer(store,data)
            self.assertEqual(calls,[None,f'bytes={len(data)//2}-'])
            self.assertEqual(reconnects,1)
            run=next(r for r in result['sources'] if r['source']==SOURCE)
            self.assertEqual(run['status'],'verified')
            self.assertEqual(run['sha256'],hashlib.sha256(data).hexdigest())
            self.assertEqual(run['members'],1)
            self.assertEqual(result['events'],1)
            before=list(store.db.execute('SELECT * FROM events'))
            result,_,_=self.transfer(store,data,fail=False)
            self.assertEqual(list(store.db.execute('SELECT * FROM events')),before)
            self.assertEqual(next(r for r in result['sources'] if r['source']==SOURCE)['members'],1)
        finally:store.close()

    def test_unavailable_robots_does_not_deny_frozen_public_archive(self):
        data,store=self.prepared();calls=[]
        def opener(request,**kwargs):
            calls.append(request.full_url)
            if request.full_url.endswith('/robots.txt'):
                raise HTTPError(request.full_url,403,'robots unavailable',{},None)
            self.assertEqual(request.full_url,URL)
            return Body(data,len(data))
        http=PublicHTTP(store.task,opener=opener,sleep=lambda _:None)
        try:
            with http.open_archive(SOURCE) as response:
                result=stream_archive(store,SOURCE,response,commit_members=128)
            self.assertEqual(calls,[URL.rsplit('/',2)[0]+'/robots.txt',URL])
            self.assertEqual(next(r for r in result['sources'] if r['source']==SOURCE)['status'],'verified')
            self.assertNotIn('https://ndownloader.figshare.com',http.robots)
            with self.assertRaises(HTTPError):
                http.open(URL+'0')
            self.assertEqual(calls[-1],'https://ndownloader.figshare.com/robots.txt')
        finally:http.close();store.close()

    def test_frozen_archive_robots_exception_never_retries_file_refusal(self):
        data,store=self.prepared();calls=[]
        def opener(request,**kwargs):
            calls.append(request.full_url)
            raise HTTPError(request.full_url,403,'unavailable or denied',{},None)
        http=PublicHTTP(store.task,opener=opener,sleep=lambda _:None)
        try:
            with self.assertRaises(HTTPError):
                with http.open_archive(SOURCE):pass
            self.assertEqual(calls,['https://ndownloader.figshare.com/robots.txt',URL])
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:http.close();store.close()

    def test_public_archive_keeps_other_robots_failures_and_disallow(self):
        data,store=self.prepared()
        try:
            for code in (401,429,503):
                calls=[]
                def opener(request,**kwargs):
                    calls.append(request.full_url)
                    raise HTTPError(request.full_url,code,'still blocked',{'Retry-After':'120'},None)
                http=PublicHTTP(store.task,opener=opener,sleep=lambda _:None)
                try:
                    with self.subTest(code=code),self.assertRaises((HTTPError,RuntimeError)):
                        with http.open_archive(SOURCE):pass
                    self.assertEqual(calls,['https://ndownloader.figshare.com/robots.txt'])
                finally:http.close()
            def disallow(request,**kwargs):
                self.assertTrue(request.full_url.endswith('/robots.txt'))
                response=Body(b'User-agent: *\nDisallow: /files/\n',30)
                response.headers['Content-Type']='text/plain'
                return response
            http=PublicHTTP(store.task,opener=disallow,sleep=lambda _:None)
            try:
                with self.assertRaises(PermissionError):
                    with http.open_archive(SOURCE):pass
            finally:http.close()
        finally:store.close()

    def test_unavailable_robots_exception_requires_unchanged_pinned_contract(self):
        data,store=self.prepared()
        try:
            for field,value in (('checksum',None),('size_bytes',None),('request_interval',2)):
                target=store.task.config if field=='request_interval' else store.task.config['sources'][SOURCE]
                previous=target[field];target[field]=value;calls=[]
                def opener(request,**kwargs):
                    calls.append(request.full_url)
                    raise HTTPError(request.full_url,403,'robots unavailable',{},None)
                http=PublicHTTP(store.task,opener=opener,sleep=lambda _:None)
                try:
                    with self.subTest(field=field),self.assertRaises((HTTPError,ValueError)):
                        http.allowed(URL)
                    self.assertEqual(calls,['https://ndownloader.figshare.com/robots.txt'])
                finally:
                    target[field]=previous;http.close()
        finally:store.close()

    def test_range_drift_ignored_range_and_encoding_change_cannot_verify(self):
        for name,change in (
            ('etag',lambda r:r.headers.update(ETag='"changed"')),
            ('range',lambda r:r.headers.update({'Content-Range':'bytes 0-1/2'})),
            ('ignored',lambda r:setattr(r,'status',200)),
            ('encoding',lambda r:r.headers.update({'Content-Encoding':'gzip'})),
        ):
            with self.subTest(name=name):
                data,store=self.prepared()
                try:
                    with self.assertRaises((ValueError,RuntimeError)):
                        self.transfer(store,data,change=change)
                    self.assertNotEqual(store.db.execute('SELECT status FROM runs WHERE source=?',(SOURCE,)).fetchone()[0],'verified')
                finally:store.close()

    def test_stalled_connections_are_bounded_and_denial_is_not_retried(self):
        data,store=self.prepared();calls=[]
        def stalled(request,**kwargs):
            calls.append(request.get_header('Range'))
            if len(calls)>1:
                if denied:raise HTTPError(URL,403,'denied',{},None)
                return Body(b'',len(data),offset=len(data)//2)
            return Body(data,len(data),cutoff=len(data)//2)
        try:
            for denied in (False,True):
                calls.clear();http=PublicHTTP(store.task,opener=stalled,sleep=lambda _:None)
                with patch.object(http,'allowed'),self.assertRaises((OSError,EOFError)):
                    with http.open_archive(SOURCE,max_reconnects=2) as r:
                        while r.read(65536):pass
                self.assertEqual(len(calls),2 if denied else 3)
        finally:store.close()

    def test_changed_freeze_and_initial_length_are_refused_before_joining_bytes(self):
        data,store=self.prepared()
        try:
            http=PublicHTTP(store.task,opener=lambda *a,**k:Body(data,len(data)+1),sleep=lambda _:None)
            with patch.object(http,'allowed'),self.assertRaisesRegex(ValueError,'initial representation'):
                with http.open_archive(SOURCE):pass
            store.task.config['request_interval']=2
            with self.assertRaises(ValueError):
                http.open_range(URL,1,len(data),{'ETag':'"fixed"'},expected_md5=hashlib.md5(data).hexdigest())
        finally:store.close()

    def test_lowercase_http_validator_is_not_lost_before_range_identity_check(self):
        data,store=self.prepared()
        try:
            with self.assertRaisesRegex(ValueError,'validator changed'):
                self.transfer(store,data,lowercase_initial=True,
                    change=lambda r:r.headers.update(ETag='"changed"'))
        finally:store.close()

    def test_trailing_compressed_bytes_are_not_hidden_by_expected_length(self):
        data,store=self.prepared()
        try:
            http=PublicHTTP(store.task,opener=lambda *a,**k:Body(data+b'extra',len(data)),sleep=lambda _:None)
            with patch.object(http,'allowed'),self.assertRaisesRegex(ValueError,'exceeds frozen length'):
                with http.open_archive(SOURCE) as response:
                    stream_archive(store,SOURCE,response,commit_members=128)
            self.assertNotEqual(store.db.execute('SELECT status FROM runs WHERE source=?',(SOURCE,)).fetchone()[0],'verified')
        finally:store.close()

    @unittest.skipUnless(__import__('os').name=='nt','optional native checksum backend')
    def test_seek_adapter_uses_public_http_validated_range_without_rehashing_prefix(self):
        import random
        from gapi.source_http import ArchiveSeekReader
        data=random.Random(11).randbytes(4*65536+13)
        self.config['sources'][SOURCE]['url']=URL
        store=self.store(data,checksum='md5:'+hashlib.md5(data).hexdigest())
        spec=store.task.config['sources'][SOURCE]
        calls=[]
        def opener(request,**kwargs):
            value=request.get_header('Range')
            offset=int(value[6:-1]) if value else 0
            calls.append(offset)
            if offset:self.assertEqual(request.get_header('If-range'),'"fixed"')
            return Body(data[offset:],len(data),offset=offset)
        http=PublicHTTP(store.task,opener=opener,sleep=lambda _:None)
        try:
            with patch.object(http,'allowed'):
                reader=ArchiveSeekReader(http,spec)
                try:
                    for _ in range(3):reader.read(65536)
                    state=reader.snapshot()
                finally:reader.close()
                calls.clear()
                reader=ArchiveSeekReader(http,spec,state=state)
                try:
                    reader.seek(2*65536)
                    tail=bytearray()
                    while chunk:=reader.read(65536):tail.extend(chunk)
                    self.assertEqual(bytes(tail),data[2*65536:])
                    self.assertEqual(calls,[2*65536])
                    self.assertEqual(reader.md5.hexdigest(),hashlib.md5(data).hexdigest())
                    self.assertEqual(reader.sha256.hexdigest(),hashlib.sha256(data).hexdigest())
                finally:reader.close()
        finally:store.close()


if __name__=='__main__':unittest.main()
