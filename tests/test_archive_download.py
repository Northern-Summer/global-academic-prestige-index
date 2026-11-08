import hashlib
import io
import json
from pathlib import Path
import tempfile
from contextlib import nullcontext
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gapi.archive_download import download
from gapi.source_http import StageDeadline


class Body(io.BytesIO):
    def __init__(self,data,total,cut=None):
        super().__init__(data)
        self.headers={'Content-Length':str(total),'ETag':'"fixed"'}
        self.cut=cut
    def read(self,n):
        if self.cut is not None and self.tell() >= self.cut: raise StageDeadline('test interruption')
        return super().read(min(n,self.cut-self.tell()) if self.cut is not None else n)


class HTTP:
    def __init__(self,data):self.data=data;self.calls=[];self.cut=None
    def _frozen_orcid_range(self,*args):return True
    def within(self,*args):return nullcontext()
    def remaining(self):return 30
    def open(self,*args,**kwargs):
        self.calls.append(0);return Body(self.data,len(self.data),self.cut)
    def open_range(self,url,offset,total,headers,**kwargs):
        assert headers['ETag']=='"fixed"'
        self.calls.append(offset);return Body(self.data[offset:],total-offset)


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'source.tar.gz'
        self.data=b'\x1f\x8b'+bytes(range(256))*600
        self.spec={'url':'https://ndownloader.figshare.com/files/70342863',
            'size_bytes':len(self.data),'checksum':'md5:'+hashlib.md5(self.data).hexdigest()}
        self.task=SimpleNamespace(config={'sources':{'source':self.spec}},require_frozen=lambda:None,check_memory=lambda:None)
        self.http=HTTP(self.data)
    def run_download(self):return download(self.task,self.http,'source',self.path)
    def test_full_download_and_repeat_have_equal_bytes_and_no_second_http(self):
        result=self.run_download()
        self.assertEqual(result['state'],'verified-file')
        self.assertEqual(self.path.read_bytes(),self.data)
        self.assertEqual(self.run_download()['sha256'],result['sha256'])
        self.assertEqual(self.http.calls,[0])
        self.assertFalse(self.path.with_suffix('.gz.part').exists())
    def test_interruption_resumes_at_file_byte_without_prefix_download(self):
        self.http.cut=12345
        with self.assertRaises(StageDeadline):self.run_download()
        self.assertFalse(self.path.exists())
        self.http.cut=None
        resumed=self.run_download()
        self.assertEqual(self.http.calls,[0,12345])
        self.assertEqual(self.path.read_bytes(),self.data)
        self.assertNotIn('error_type',resumed)
        self.assertEqual(resumed['previous_failure']['error_type'],'StageDeadline')
    def test_checksum_failure_keeps_partial_and_never_promotes(self):
        self.spec['checksum']='md5:'+'0'*32
        with self.assertRaisesRegex(ValueError,'MD5 mismatch'):self.run_download()
        self.assertFalse(self.path.exists())
        self.assertTrue(self.path.with_suffix('.gz.part').exists())
    def test_source_drift_refused_before_http(self):
        self.http.cut=12345
        with self.assertRaises(StageDeadline):self.run_download()
        self.spec['size_bytes']+=1
        with self.assertRaisesRegex(ValueError,'drift'):self.run_download()
        self.assertEqual(self.http.calls,[0])
    def test_refused_range_retains_prefix_without_restart(self):
        self.http.cut=12345
        with self.assertRaises(StageDeadline):self.run_download()
        def refused(*args,**kwargs):raise ValueError('Range mismatch')
        self.http.open_range=refused
        with self.assertRaisesRegex(ValueError,'Range mismatch'):self.run_download()
        self.assertEqual(self.path.with_suffix('.gz.part').stat().st_size,12345)
        self.assertEqual(self.http.calls,[0])
    def test_capacity_refused_before_http(self):
        with patch('gapi.archive_download.shutil.disk_usage',return_value=SimpleNamespace(free=1)):
            with self.assertRaisesRegex(OSError,'space'):self.run_download()
        self.assertEqual(self.http.calls,[])
    def test_unowned_file_not_overwritten(self):
        self.path.with_suffix('.gz.part').write_bytes(b'private')
        with self.assertRaisesRegex(ValueError,'identity receipt'):self.run_download()
        self.assertEqual(self.http.calls,[])
    def test_wrong_initial_length_does_not_write_data(self):
        self.http.open=lambda *a,**k:Body(self.data,len(self.data)-1)
        with self.assertRaisesRegex(ValueError,'length mismatch'):self.run_download()
        self.assertFalse(self.path.with_suffix('.gz.part').exists())
    def test_rename_before_receipt_crash_reconciles_without_network(self):
        self.run_download()
        receipt=self.path.with_suffix('.gz.download.json')
        state=json.loads(receipt.read_text());state['state']='verifying-file'
        receipt.write_text(json.dumps(state))
        self.run_download()
        self.assertEqual(self.http.calls,[0])
