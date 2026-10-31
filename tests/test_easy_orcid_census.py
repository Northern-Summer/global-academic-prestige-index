from contextlib import nullcontext
import gzip
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from gapi.easy_orcid_census import census, affiliation_projection, load_spec


class CensusTests(unittest.TestCase):
    def run_census(self, rows, *, change=None, max_records=None):
        raw=gzip.compress(b''.join(json.dumps(r).encode()+b'\n' for r in rows),mtime=0)
        spec={'id':'test','url':'https://test.invalid/data','scope':'fixture',
              'size_bytes':len(raw),'md5':hashlib.md5(raw).hexdigest()}
        if change:spec.update(change)
        class Response(io.BytesIO):
            headers={}
            def __enter__(self):return self
            def __exit__(self,*a):self.close()
        http=SimpleNamespace(open=lambda *a,**k:Response(raw),within=lambda *a:nullcontext())
        return census(SimpleNamespace(check_memory=lambda:None),http,spec,max_records=max_records,max_seconds=5)

    def record(self):
        return {'orcid':'0000-0002-1825-0097','name':'Private name',
                'educations':[{'name':'Private school','role':'PhD','end':{'year':2000},'xrefs':{'ror':'05dxps055'}}],
                'employments':[{'role':'Professor','start':{'year':2001},'xrefs':{'ror':'05dxps055'}}]}

    def test_complete_source_census_hashes_and_counts_without_personal_output(self):
        result=self.run_census([self.record(),self.record()])
        self.assertTrue(result['source_complete'])
        self.assertFalse(result['global_complete'])
        self.assertEqual(result['counts']['source_rows'],2)
        self.assertEqual(result['counts']['affiliations'],4)
        self.assertEqual(result['stage_counts'],{'doctorate':2,'academic':2})
        text=json.dumps(result)
        for private in ('Private name','Private school','0000-0002-1825-0097'):
            self.assertNotIn(private,text)

    def test_bad_checksum_or_length_cannot_complete(self):
        for change in ({'md5':'0'*32},{'size_bytes':100000},{'size_bytes':1}):
            with self.subTest(change=change):
                result=self.run_census([self.record()],change=change)
                self.assertFalse(result['source_complete'])
                self.assertIsNotNone(result['error'])

    def test_prefix_is_not_promoted_to_complete(self):
        r=self.run_census([self.record(),self.record()],max_records=1)
        self.assertFalse(r['source_complete']);self.assertEqual(r['counts']['source_rows'],1)

    def test_malformed_shape_and_bad_identifier_counted_separately(self):
        r=self.run_census([{'orcid':'bad'},dict(self.record(),educations='not a list')])
        self.assertTrue(r['source_complete'])
        self.assertEqual(r['counts']['invalid_rows'],2)

    def test_shared_rules_exclude_unfinished_and_separate_postdoc(self):
        r=affiliation_projection({'educations':[{'role':'PhD candidate'}],
                                  'employments':[{'role':'Postdoctoral fellow'}]})
        self.assertEqual(r[0][:2],['education',None])
        self.assertEqual(r[1][:2],['training','postdoc'])

    def test_source_contract_is_pinned(self):
        spec=load_spec(Path('configs/easy-orcid-2023-hq.json'))
        self.assertEqual(spec['size_bytes'],662112167)
        full=load_spec(Path('configs/easy-orcid-2023-full.json'))
        self.assertEqual(full['size_bytes'],920836363)
        self.assertEqual(full['md5'],'8829b83ba498b901fc7a1d7320040520')

    def test_in_process_reconnect_preserves_gzip_state_and_whole_hash(self):
        from gapi.easy_orcid_census import HashStream
        from gapi.easy_orcid import _lines
        import time
        body=gzip.compress(json.dumps(self.record()).encode()+b'\n',mtime=0)
        cut=len(body)//2;resumed=[]
        class Response(io.BytesIO):headers={}
        first=Response(body[:cut])
        def reconnect(url,offset,total,headers,**kwargs):
            self.assertEqual(offset,cut);self.assertEqual(total,len(body))
            r=Response(body[offset:]);resumed.append(r);return r
        task=SimpleNamespace(check_memory=lambda:None)
        stream=HashStream(first,len(body),task=task,http=SimpleNamespace(open_range=reconnect),url='fixture',md5='fixture')
        try:
            rows=list(_lines(stream,limit=2*1024**2,deadline=time.monotonic()+5,task=task,gzip_stream=True))
            self.assertEqual(json.loads(rows[0]),self.record())
            self.assertEqual(stream.md5.hexdigest(),hashlib.md5(body).hexdigest())
            self.assertEqual(stream.response.reconnects,1)
        finally:stream.close()
        self.assertTrue(first.closed);self.assertTrue(resumed[0].closed)

    def test_reconnected_census_verifies_whole_file_and_records_exhaustion(self):
        body=gzip.compress(json.dumps(self.record()).encode()+b'\n',mtime=0)
        cut=len(body)//2
        class Response(io.BytesIO):headers={}
        for mode in ('valid','wrong-checksum','changed-suffix','stalled'):
            with self.subTest(mode=mode):
                first=Response(body[:cut]);resumed=[]
                def reconnect(url,offset,total,headers,**kwargs):
                    suffix=bytearray(body[offset:])
                    if mode=='changed-suffix':suffix[-8]^=1
                    r=Response(b'' if mode=='stalled' else suffix);resumed.append(r);return r
                http=SimpleNamespace(open=lambda *a,**k:first,within=lambda *a:nullcontext(),open_range=reconnect)
                spec={'id':'fixture','url':'fixture','scope':'fixture','size_bytes':len(body),
                      'md5':'0'*32 if mode=='wrong-checksum' else hashlib.md5(body).hexdigest()}
                result=census(SimpleNamespace(check_memory=lambda:None),http,spec,max_seconds=5)
                self.assertEqual(result['source_complete'],mode=='valid')
                self.assertEqual(result['transport_reconnects'],3 if mode=='stalled' else 1)
                if mode!='valid':self.assertIsNotNone(result['error'])
                self.assertTrue(first.closed);self.assertTrue(all(r.closed for r in resumed))

    def test_initial_transport_length_cannot_hide_appended_or_replaced_source(self):
        from gapi.easy_orcid_census import HashStream
        for length in ('11','9','bad','-10'):
            with self.subTest(length=length):
                response=SimpleNamespace(headers={'Content-Length':length})
                with self.assertRaisesRegex(ValueError,'frozen size'):HashStream(response,10)


if __name__=='__main__':unittest.main()
