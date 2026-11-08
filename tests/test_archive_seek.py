"""Bounded replay verification and checksum continuation at the seek boundary."""
import copy
import hashlib
import io
import json
import os
import random
import gzip
import subprocess
import sys
from pathlib import Path
import unittest

from gapi.source_http import ArchiveSeekReader
from gapi.archive_hash import ArchiveHash
from gapi.task import digest

BLOCK = 65536


class SeekBody(io.BytesIO):
    def __init__(self,data,total,offset=0,cutoff=None):
        super().__init__(data)
        self.headers={'Content-Length':str(total-offset),'ETag':'"fixed"'}
        self.cutoff=cutoff
    def read(self,size):
        assert 0 <= size <= BLOCK
        if self.cutoff is not None:
            if self.tell()>=self.cutoff: raise ConnectionResetError('injected reset')
            size=min(size,self.cutoff-self.tell())
        return super().read(size)


class SeekHTTP:
    def __init__(self,data,*,cutoff=None):
        self.data=data; self.cutoff=cutoff; self.calls=[]
        self.task=self
    def check_memory(self): pass
    def remaining(self): return 30
    def _frozen_orcid_range(self,*args): return True
    def open(self,*args,**kwargs):
        self.calls.append(0)
        return SeekBody(self.data,len(self.data),cutoff=self.cutoff)
    def open_range(self,url,offset,total,headers,**kwargs):
        assert total==len(self.data) and headers.get('etag')=='"fixed"'
        self.calls.append(offset)
        return SeekBody(self.data[offset:],len(self.data),offset)


@unittest.skipUnless(os.name=='nt','optional Windows native checksum backend')
class ArchiveSeekTests(unittest.TestCase):
    def test_large_spacing_with_bounded_buffer_wrap_and_corrupt_crc(self):
        runtime=Path('work/archive-index-build/lib').resolve()
        if not runtime.is_dir():self.skipTest('optional corrected Windows runtime unavailable')
        code='''
import sys,io,gzip,random
sys.path.insert(0,sys.argv[1]);import indexed_gzip
plain=random.Random(17).randbytes(20*1024**2);data=gzip.compress(plain)
for spacing in (1024**3,1024**2):
    with indexed_gzip.IndexedGzipFile(fileobj=io.BytesIO(data),spacing=spacing,readbuf_size=65536) as decoder:
        decoder.build_full_index();output=io.BytesIO();decoder.export_index(fileobj=output)
    with indexed_gzip.IndexedGzipFile(fileobj=io.BytesIO(data),readbuf_size=65536) as decoder:
        decoder.import_index(fileobj=io.BytesIO(output.getvalue()))
        for offset in (0,100,9*1024**2,19*1024**2):
            decoder.seek(offset);assert decoder.read(4096)==plain[offset:offset+4096]
broken=bytearray(data);broken[-8]^=1
try:
    with indexed_gzip.IndexedGzipFile(fileobj=io.BytesIO(broken),spacing=1024**3,readbuf_size=65536) as decoder:
        decoder.build_full_index()
except indexed_gzip.ZranError:pass
else:raise AssertionError('corrupt CRC accepted')
'''
        run=subprocess.run([sys.executable,'-B','-c',code,str(runtime)],
                           capture_output=True,text=True,timeout=20)
        self.assertEqual(run.returncode,0,run.stderr)

    def test_small_compressible_index_export_import_does_not_crash(self):
        runtime=Path('work/archive-index-build/lib').resolve()
        if not runtime.is_dir():
            self.skipTest('optional corrected Windows runtime unavailable')
        code='''
import sys,io,gzip
sys.path.insert(0,sys.argv[1]);import indexed_gzip
plain=b'<record>fixture</record>\\n'*10000
data=gzip.compress(plain)
with indexed_gzip.IndexedGzipFile(fileobj=io.BytesIO(data),spacing=1024**2,readbuf_size=65536) as decoder:
    decoder.build_full_index()
    output=io.BytesIO();decoder.export_index(fileobj=output)
    assert len(list(decoder.seek_points()))>=2
with indexed_gzip.IndexedGzipFile(fileobj=io.BytesIO(data),readbuf_size=65536) as decoder:
    decoder.import_index(fileobj=io.BytesIO(output.getvalue()))
    decoder.seek(100);assert decoder.read(4096)==plain[100:4196]
'''
        run=subprocess.run([sys.executable,'-B','-c',code,str(runtime)],
                           capture_output=True,text=True,timeout=15)
        self.assertEqual(run.returncode,0,run.stderr)

    def test_native_decoder_accepts_official_length_without_network(self):
        runtime=Path('work/archive-index-build/lib').resolve()
        if not runtime.is_dir():
            self.skipTest('optional corrected Windows index runtime unavailable')
        code='''
import sys
sys.path.insert(0,sys.argv[1]);sys.path.insert(0,'tests')
import indexed_gzip
from test_archive_seek import SeekHTTP
from gapi.source_http import ArchiveSeekReader
spec={'url':'https://ndownloader.figshare.com/files/70342863',
      'size_bytes':57290954546,'checksum':'md5:'+'a'*32}
http=SeekHTTP(b'');reader=ArchiveSeekReader(http,spec)
try:
    with indexed_gzip.IndexedGzipFile(fileobj=reader,readbuf_size=65536):pass
    assert http.calls==[] and reader.highwater==0
finally:reader.close()
'''
        run=subprocess.run([sys.executable,'-B','-c',code,str(runtime)],
            capture_output=True,text=True,timeout=10)
        self.assertEqual(run.returncode,0,run.stderr)

    def source(self,size=4*BLOCK+17):
        data=random.Random(1729).randbytes(size)
        spec={'url':'https://ndownloader.figshare.com/files/70342863',
              'size_bytes':len(data),'checksum':'md5:'+hashlib.md5(data).hexdigest()}
        return data,spec

    def prefix(self,data,spec):
        http=SeekHTTP(data)
        reader=ArchiveSeekReader(http,spec)
        for _ in range(3): reader.read(BLOCK)
        state=reader.snapshot();reader.close()
        return state

    def test_restore_replays_verified_block_without_double_hashing(self):
        data,spec=self.source();state=self.prefix(data,spec)
        http=SeekHTTP(data);reader=ArchiveSeekReader(http,spec,state=json.loads(json.dumps(state)))
        try:
            reader.seek(2*BLOCK+13)
            output=bytearray()
            while chunk:=reader.read(BLOCK): output.extend(chunk)
            self.assertEqual(bytes(output),data[2*BLOCK+13:])
            self.assertEqual(http.calls,[2*BLOCK])
            self.assertEqual(reader.highwater,len(data))
            self.assertEqual(reader.md5.hexdigest(),hashlib.md5(data).hexdigest())
            self.assertEqual(reader.sha256.hexdigest(),hashlib.sha256(data).hexdigest())
        finally: reader.close()

    def test_changed_replay_bytes_never_reach_decoder_or_hash(self):
        data,spec=self.source();state=self.prefix(data,spec)
        altered=bytearray(data);altered[2*BLOCK+7]^=1
        reader=ArchiveSeekReader(SeekHTTP(bytes(altered)),spec,state=state)
        try:
            reader.seek(2*BLOCK)
            with self.assertRaisesRegex(ValueError,'replay bytes changed'):reader.read(1)
            self.assertEqual(reader.snapshot(),state)
        finally:reader.close()

    def test_mid_block_disconnect_preserves_all_bytes_once(self):
        data,spec=self.source();http=SeekHTTP(data,cutoff=111)
        reader=ArchiveSeekReader(http,spec)
        try:
            output=bytearray()
            while chunk:=reader.read(BLOCK):output.extend(chunk)
            self.assertEqual(bytes(output),data)
            self.assertEqual(http.calls,[0,111])
            self.assertEqual(reader.reconnects,1)
            self.assertEqual(reader.md5.hexdigest(),hashlib.md5(data).hexdigest())
        finally:reader.close()

    def test_sparse_index_and_unhashed_gap_stop_before_network(self):
        data,spec=self.source((ArchiveSeekReader.RETAIN_BLOCKS+2)*BLOCK)
        http=SeekHTTP(data);reader=ArchiveSeekReader(http,spec)
        try:
            reader.seek(2*BLOCK)
            with self.assertRaisesRegex(ValueError,'skip unhashed'):reader.read(1)
            self.assertEqual(http.calls,[])
            reader.seek(0)
            while reader.read(BLOCK):pass
            reader.seek(0)
            before=list(http.calls)
            with self.assertRaisesRegex(ValueError,'retained window'):reader.read(1)
            self.assertEqual(http.calls,before)
        finally:reader.close()

    def test_corrupt_state_position_and_bounds_are_refused(self):
        data,spec=self.source();state=self.prefix(data,spec)
        bad=copy.deepcopy(state);bad['highwater']+=1
        with self.assertRaisesRegex(ValueError,'checksum'):
            ArchiveSeekReader(SeekHTTP(data),spec,state=bad)
        bad['checksum']=digest({k:v for k,v in bad.items() if k!='checksum'})
        with self.assertRaisesRegex(ValueError,'highwater'):
            ArchiveSeekReader(SeekHTTP(data),spec,state=bad)
        bad=copy.deepcopy(state);bad['blocks'][0][0]=True
        bad['checksum']=digest({k:v for k,v in bad.items() if k!='checksum'})
        with self.assertRaises(ValueError):ArchiveSeekReader(SeekHTTP(data),spec,state=bad)
        reader=ArchiveSeekReader(SeekHTTP(data),spec)
        try:
            with self.assertRaises(ValueError):reader.read()
            for offset,whence in [(-1,0),(len(data)+1,0),(0,4)]:
                with self.assertRaises(ValueError):reader.seek(offset,whence)
        finally:reader.close()

    def test_real_index_import_in_new_process_uses_range_and_full_hashes(self):
        from gapi.local_archive_index import local_runtime_build
        runtime=local_runtime_build()/'lib'
        if not runtime.is_dir():
            self.skipTest('optional indexed_gzip experiment runtime unavailable')
        sys.path.insert(0,str(runtime))
        try:import indexed_gzip
        finally:sys.path.remove(str(runtime))
        import tempfile
        rng=random.Random(20261104)
        blocks=[]
        for _ in range(32):
            unit=b'<record>'+rng.randbytes(1024)+b'</record>\n'
            blocks.append((unit*(1024**2//len(unit)+1))[:1024**2])
        plain=b''.join(blocks)
        data=gzip.compress(plain,compresslevel=6,mtime=0)
        spec={'url':'https://ndownloader.figshare.com/files/70342863',
              'size_bytes':len(data),'checksum':'md5:'+hashlib.md5(data).hexdigest()}
        reader=ArchiveSeekReader(SeekHTTP(data),spec)
        try:
            with indexed_gzip.IndexedGzipFile(fileobj=reader,spacing=1024**2,
                                             readbuf_size=65536) as decoder:
                decoder.seek(27*1024**2)
                index=io.BytesIO();decoder.export_index(fileobj=index)
                state=reader.snapshot()
        finally:reader.close()
        with tempfile.TemporaryDirectory(prefix='gapi-index-seek-test-',dir='work') as directory:
            directory=Path(directory)
            (directory/'source.gz').write_bytes(data)
            (directory/'index.gzi').write_bytes(index.getvalue())
            (directory/'state.json').write_text(json.dumps({'state':state,'spec':spec}))
            code='''
import hashlib,io,json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1]);sys.path.insert(0,'tests')
import indexed_gzip
from test_archive_seek import SeekHTTP
from gapi.source_http import ArchiveSeekReader
root=Path(sys.argv[2]);saved=json.loads((root/'state.json').read_text())
http=SeekHTTP((root/'source.gz').read_bytes())
r=ArchiveSeekReader(http,saved['spec'],state=saved['state'])
with indexed_gzip.IndexedGzipFile(fileobj=r,index_file=str(root/'index.gzi'),
                                spacing=1024**2,readbuf_size=65536) as decoder:
    decoder.seek(24*1024**2)
    result=hashlib.sha256(decoder.read(1024**2)).hexdigest()
print(json.dumps({'decoded':result,'calls':http.calls,'md5':r.md5.hexdigest(),
                  'sha256':r.sha256.hexdigest(),'highwater':r.highwater}))
r.close()
'''
            run=subprocess.run([sys.executable,'-B','-c',code,str(runtime),str(directory)],
                capture_output=True,text=True,timeout=20)
            self.assertEqual(run.returncode,0,run.stderr)
            result=json.loads(run.stdout)
        self.assertEqual(result['decoded'],hashlib.sha256(blocks[24]).hexdigest())
        self.assertTrue(result['calls'])
        self.assertGreater(min(result['calls']),0)
        self.assertEqual(result['highwater'],state['highwater'])
        self.assertEqual(result['md5'],ArchiveHash.restore(state['md5']).hexdigest())
        self.assertEqual(result['sha256'],ArchiveHash.restore(state['sha256']).hexdigest())


if __name__=='__main__':unittest.main()
