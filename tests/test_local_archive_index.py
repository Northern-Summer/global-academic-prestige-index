import gzip
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import contextmanager
from gapi.local_archive_index import build_local_index, CommittedPrefix, committed_prefix
from gapi.task import digest


class LocalArchiveIndexTests(unittest.TestCase):
    def test_compressed_index_preserves_identity_and_rejects_damage(self):
        import test_streaming as fixtures
        from gapi.local_archive_index import compress_index,index_bytes
        case=fixtures.StreamingTests();case.setUp()
        try:
            store=case.store(fixtures.archive([]))
            try:
                index=store.task.root/'fixture.gzi';raw=b'index-state\0'*10000;index.write_bytes(raw)
                record={'state':'indexed','index_bytes':len(raw),'index_sha256':hashlib.sha256(raw).hexdigest()}
                pointer=index.with_name(index.name+'.json');pointer.write_text(json.dumps(record))
                before=index_bytes(index,record);receipt=compress_index(store.task,index)
                current=json.loads(pointer.read_bytes())
                self.assertEqual(index_bytes(index,current),before);self.assertTrue(index.exists())
                self.assertLess(receipt['stored_bytes'],len(raw));self.assertFalse(receipt['original_deleted'])
                again=compress_index(store.task,index);self.assertEqual(receipt,again)
                packed=index.with_name(index.name+'.gz');body=packed.read_bytes()
                packed.write_bytes(body[:-1])
                with self.assertRaises(ValueError):index_bytes(index,current)
                packed.write_bytes(body)
                for change in ({'filename':'../escape'},{'encoding':'xz'},{'decoded_bytes':1},{'sha256':'0'*64}):
                    with self.assertRaises(ValueError):index_bytes(index,{**current,'index_storage':{**current['index_storage'],**change}})
            finally:store.close()
        finally:
            case.tearDown();case.doCleanups()
    def test_prefix_reconciles_headers_and_digests_without_xml_parser(self):
        import test_streaming as fixtures
        import test_archive_commit_batches as batches
        members=batches.members(4)
        plain=gzip.decompress(fixtures.archive(members))
        rows=[(name,hashlib.sha256(body).hexdigest()) for name,body in members[:2]]
        prefix=CommittedPrefix(rows,2)
        for offset in range(0,len(plain),173):prefix.feed(plain[offset:offset+173],offset)
        result=prefix.finish()
        self.assertEqual(result['members'],2)
        self.assertEqual(result['tar_offset'],sum(512+((len(body)+511)//512)*512 for _,body in members[:2]))
        self.assertIn(members[2][0].encode(),plain[result['tar_offset']:result['tar_offset']+512])
        for bad_rows in ([(rows[1][0],rows[0][1])],[(rows[0][0],'0'*64)]):
            with self.subTest(rows=bad_rows),self.assertRaisesRegex(ValueError,'mismatch'):
                broken=CommittedPrefix(bad_rows,1);broken.feed(plain,0);broken.finish()
        with self.assertRaisesRegex(ValueError,'nonsequential'):
            CommittedPrefix(rows,2).feed(plain,1)

    def test_cli_holds_existing_exclusion_without_opening_career_database(self):
        from gapi.stream_cli import execute
        task=SimpleNamespace(root=Path('D:/fixture/task'),resource_root=Path('D:/fixture'),
            config={'sources':{'s':{'kind':'orcid-summary-tar-gzip','filename':'source.gz'}}})
        held=[]
        @contextmanager
        def exclusion(value):
            self.assertIs(value,task);held.append(True)
            try:yield
            finally:held.pop()
        def build(*args,**kwargs):
            self.assertEqual(held,[True]);return {'state':'fixture-only'}
        with patch('gapi.stream_cli.Task',return_value=task), \
             patch('gapi.career_store.exclusive_writer',side_effect=exclusion), \
             patch('gapi.local_archive_index.build_local_index',side_effect=build), \
             patch('gapi.stream_cli.CareerStore') as database,patch('builtins.print'):
            self.assertTrue(execute(SimpleNamespace(command='index-orcid-local',task=Path('fixture.json'),source='s',max_seconds=10)))
            database.assert_not_called()
        self.assertEqual(held,[])
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.archive=self.root/'source.gz';self.output=self.root/'index.gzi'
        self.data=gzip.compress(b'<record>fixture</record>\n'*10000)
        self.archive.write_bytes(self.data)
        self.spec={'size_bytes':len(self.data),'checksum':'md5:'+hashlib.md5(self.data).hexdigest()}
        self.receipt={'state':'verified-file','source_identity':digest(self.spec),'destination':str(self.archive),
            'bytes':len(self.data),'md5':hashlib.md5(self.data).hexdigest(),'sha256':hashlib.sha256(self.data).hexdigest()}
        self.receipt_path=self.archive.with_suffix('.gz.download.json');self.write_receipt()
        self.task=SimpleNamespace(root=self.root,config={'sources':{'s':self.spec}},require_frozen=lambda:None,check=lambda n:None,check_memory=lambda:0)
    def write_receipt(self):self.receipt_path.write_text(json.dumps(self.receipt))
    def test_unverified_source_refused_before_decoder(self):
        self.receipt['state']='downloading';self.write_receipt()
        with self.assertRaisesRegex(ValueError,'verification required'):
            build_local_index(self.task,'s',self.archive,self.output,None)
        self.assertFalse(self.output.exists())
    def test_capacity_failure_precedes_attempt(self):
        def full(n):raise OSError('full disk')
        self.task.check=full
        with self.assertRaisesRegex(OSError,'full disk'):
            build_local_index(self.task,'s',self.archive,self.output,None)
        self.assertFalse(self.output.with_suffix('.gzi.json').exists())
    def test_readonly_prefix_ledger_binding_and_count_drift(self):
        import sqlite3
        from gapi.career_store import source_identity
        database=self.root/'careers.sqlite'
        self.task.config['rules_version']='fixture/v1'
        connection=sqlite3.connect(database)
        connection.executescript('CREATE TABLE runs(source TEXT,identity TEXT,members INTEGER);'
            'CREATE TABLE members(source TEXT,locator TEXT,sha256 TEXT);')
        connection.execute('INSERT INTO runs VALUES(?,?,?)',('s',source_identity(self.task,'s'),1))
        connection.execute('INSERT INTO members VALUES(?,?,?)',('s','member.xml','0'*64))
        connection.commit()
        before=database.read_bytes()
        with committed_prefix(self.task,'s') as prefix:
            self.assertEqual(prefix.count,1)
            self.assertEqual(next(prefix.rows),('member.xml','0'*64))
        self.assertEqual(database.read_bytes(),before)
        connection.execute('UPDATE runs SET members=2');connection.commit();connection.close()
        with self.assertRaisesRegex(ValueError,'source/count mismatch'):
            with committed_prefix(self.task,'s'):pass
    def test_failed_attempt_cannot_automatically_rescan(self):
        def fail(**args):raise OSError('interruption')
        with self.assertRaisesRegex(OSError,'interruption'):
            build_local_index(self.task,'s',self.archive,self.output,fail)
        with self.assertRaisesRegex(ValueError,'prior index attempt'):
            build_local_index(self.task,'s',self.archive,self.output,fail)
    def test_python_and_native_buffers_are_independent_of_index_spacing(self):
        options={}
        def inspect(**kwargs):
            options.update(kwargs)
            raise OSError('fixture stops before decoding')
        with self.assertRaisesRegex(OSError,'fixture stops'):
            build_local_index(self.task,'s',self.archive,self.output,inspect)
        self.assertEqual(options['spacing'],1024**3)
        self.assertEqual(options['readbuf_size'],65536)
        self.assertEqual(options['buffer_size'],65536)
    def test_native_callback_stop_cannot_be_reported_as_success(self):
        class SwallowsReadFailure:
            def __init__(self,**options):self.raw=options['fileobj']
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def build_full_index(self):self.raw.read(10)
        def exhausted():raise MemoryError('memory limit')
        self.task.check_memory=exhausted
        with self.assertRaisesRegex(MemoryError,'memory limit'):
            build_local_index(self.task,'s',self.archive,self.output,SwallowsReadFailure)
        self.assertFalse(self.output.exists())
        record=json.loads(self.output.with_suffix('.gzi.json').read_text())
        self.assertEqual(record['state'],'failed')
        self.assertEqual(record['error_type'],'MemoryError')
    def test_deadline_in_native_callback_is_not_success(self):
        class Reads:
            def __init__(self,**options):self.raw=options['fileobj']
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def build_full_index(self):self.raw.read(10)
        with patch('gapi.local_archive_index.time.monotonic',side_effect=[0,2]):
            with self.assertRaisesRegex(TimeoutError,'deadline'):
                build_local_index(self.task,'s',self.archive,self.output,Reads,max_seconds=1)
    def test_observer_failure_prevents_index_publication(self):
        prefix=CommittedPrefix([('wrong-name.xml','0'*64)],1)
        class ReportsOutput:
            def __init__(self,**options):self.raw=options['fileobj']
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def build_full_index(self):
                self.raw.index_output(b'\0'*1024,0)
            def seek_points(self):raise AssertionError('sticky observer failure must be checked first')
        with self.assertRaisesRegex(ValueError,'ended before'):
            build_local_index(self.task,'s',self.archive,self.output,ReportsOutput,prefix=prefix)
        self.assertFalse(self.output.exists())
    @unittest.skipUnless(sys.platform=='win32','optional Windows runtime')
    def test_native_sequential_small_reads_and_seek_reset(self):
        import io,os
        from gapi.local_archive_index import local_decoder
        plain=os.urandom(2*1024**2)
        data=gzip.compress(plain[:1000000])+gzip.compress(plain[1000000:])
        with local_decoder(fileobj=io.BytesIO(data),spacing=1024**3,buffer_size=65536) as decoder:
            decoder.build_full_index();out=io.BytesIO();decoder.export_index(fileobj=out)
        class Reader(io.BytesIO):
            gapi_sequential=True
            total=0
            def read(self,size=-1):
                result=super().read(size);self.total+=len(result);return result
        raw=Reader(data)
        with local_decoder(fileobj=raw,buffer_size=65536,readbuf_size=65536,auto_build=False) as decoder:
            # Exercise the selected compressed representation with the native
            # decoder, including sequential reads and random seek reset.
            from gapi.local_archive_index import index_bytes
            index=self.root/'native.gzi';packed=gzip.compress(out.getvalue(),mtime=0)
            index.with_name(index.name+'.gz').write_bytes(packed)
            record={'index_bytes':len(out.getvalue()),'index_sha256':hashlib.sha256(out.getvalue()).hexdigest(),
                    'index_storage':{'encoding':'gzip','filename':index.name+'.gz','bytes':len(packed),
                        'sha256':hashlib.sha256(packed).hexdigest(),'decoded_bytes':len(out.getvalue())}}
            decoder.import_index(fileobj=io.BytesIO(index_bytes(index,record)))
            observed=hashlib.sha256()
            while chunk:=decoder.read(65536):observed.update(chunk)
            self.assertEqual(observed.digest(),hashlib.sha256(plain).digest())
            self.assertLessEqual(raw.total,len(data)+65536)
            for offset in (12345,1500000,77,1900000):
                decoder.seek(offset)
                self.assertEqual(decoder.read(90000),plain[offset:offset+90000])

    @unittest.skipUnless(sys.platform=='win32','optional Windows runtime')
    def test_native_index_rejects_corrupt_gzip_crc(self):
        from gapi.local_archive_index import local_decoder
        damaged=bytearray(self.data);damaged[-8]^=1
        self.data=bytes(damaged);self.archive.write_bytes(self.data)
        self.spec['checksum']='md5:'+hashlib.md5(self.data).hexdigest()
        self.receipt.update(source_identity=digest(self.spec),md5=hashlib.md5(self.data).hexdigest(),
            sha256=hashlib.sha256(self.data).hexdigest())
        self.write_receipt()
        with self.assertRaisesRegex(Exception,'[Zz]ran|CRC|[Ii]ndex'):
            build_local_index(self.task,'s',self.archive,self.output,local_decoder)
        self.assertFalse(self.output.exists())
        self.assertEqual(json.loads(self.output.with_suffix('.gzi.json').read_text())['state'],'failed')

    @unittest.skipUnless(sys.platform=='win32','optional Windows runtime')
    def test_native_one_pass_prefix_and_imported_resume(self):
        import test_streaming as fixtures
        import test_archive_commit_batches as batches
        from gapi.local_archive_index import local_decoder
        members=batches.members(5)
        self.data=fixtures.archive(members);self.archive.write_bytes(self.data)
        self.spec.update(size_bytes=len(self.data),checksum='md5:'+hashlib.md5(self.data).hexdigest())
        self.receipt.update(source_identity=digest(self.spec),bytes=len(self.data),
            md5=hashlib.md5(self.data).hexdigest(),sha256=hashlib.sha256(self.data).hexdigest())
        self.write_receipt()
        prefix=CommittedPrefix([(name,hashlib.sha256(body).hexdigest()) for name,body in members[:3]],3)
        result=build_local_index(self.task,'s',self.archive,self.output,local_decoder,prefix=prefix)
        offset=result['committed_prefix']['tar_offset']
        self.assertEqual(result['committed_prefix']['members'],3)
        self.assertEqual(result['business_records_written'],0)
        with local_decoder(filename=str(self.archive),index_file=str(self.output),
                           buffer_size=65536,auto_build=False) as decoder:
            decoder.seek(offset)
            self.assertEqual(decoder.read(512),gzip.decompress(self.data)[offset:offset+512])
        self.assertIn(members[3][0].encode(),gzip.decompress(self.data)[offset:offset+512])

    @unittest.skipUnless(sys.platform=='win32','optional Windows runtime')
    def test_real_index_export_import_and_no_business_writes(self):
        from gapi.local_archive_index import local_decoder
        result=build_local_index(self.task,'s',self.archive,self.output,local_decoder)
        self.assertEqual(result['state'],'indexed')
        self.assertEqual(result['business_records_written'],0)
        self.assertEqual(result['xml_records_parsed'],0)
        with local_decoder(filename=str(self.archive),index_file=str(self.output)) as decoder:
            decoder.seek(100);self.assertEqual(decoder.read(100),gzip.decompress(self.data)[100:200])
        with self.assertRaisesRegex(ValueError,'prior index attempt'):
            build_local_index(self.task,'s',self.archive,self.output,local_decoder)

class LocalArchiveResumeTests(unittest.TestCase):
    import test_streaming as fixtures
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def prepare(self,count=6):
        import test_archive_commit_batches as batches
        from gapi.orcid_stream import stream_archive
        from gapi.local_archive_index import local_decoder
        members=batches.members(count)
        data=self.fixtures.archive(members)
        store=self.store(data,checksum='md5:'+hashlib.md5(data).hexdigest())
        source='orcid-2026-summary'
        stream_archive(store,source,__import__('io').BytesIO(data),max_members=2,full_report=False)
        archive=store.task.root/'fixture.tar.gz';archive.write_bytes(data)
        index=store.task.root/'fixture.gzi'
        spec=store.task.config['sources'][source]
        receipt={'state':'verified-file','source_identity':digest(spec),'destination':str(archive),
            'bytes':len(data),'md5':hashlib.md5(data).hexdigest(),'sha256':hashlib.sha256(data).hexdigest()}
        archive.with_suffix('.gz.download.json').write_text(json.dumps(receipt))
        with committed_prefix(store.task,source) as prefix:
            build_local_index(store.task,source,archive,index,local_decoder,prefix=prefix)
        return store,source,archive,index

    def test_bounded_restart_processes_only_new_members(self):
        from gapi.local_archive_index import LocalArchive
        from gapi.orcid_stream import stream_archive,SummaryParser
        from gapi.career_store import CareerStore
        store,source,archive,index=self.prepare()
        task=store.task
        try:
            with LocalArchive(store,source,archive,index) as local:
                stream_archive(store,source,None,local=local,max_new_members=2,commit_members=2,full_report=False)
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?',(source,)).fetchone()[0],4)
        finally:store.close()

        parsed=[]
        original=SummaryParser.__init__
        def capture(parser,*args,**kwargs):
            parsed.append(args[2]);original(parser,*args,**kwargs)
        store=CareerStore(task)
        try:
            with patch.object(SummaryParser,'__init__',capture),LocalArchive(store,source,archive,index) as local:
                report=stream_archive(store,source,None,local=local,commit_members=2,full_report=False)
            self.assertEqual(len(parsed),2)
            self.assertEqual(report['sources'][0]['status'],'verified')
            self.assertEqual(report['sources'][0]['members'],6)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],6)
        finally:store.close()

    def test_local_large_batch_matches_small_batch_with_event_bound(self):
        from gapi.local_archive_index import LocalArchive
        from gapi.orcid_stream import stream_archive,validate_commit_members
        store,source,archive,index=self.prepare(1102)
        try:
            with LocalArchive(store,source,archive,index) as local:
                result=stream_archive(store,source,None,local=local,commit_members=1024,full_report=False)
            self.assertEqual(result['sources'][0]['members'],1102)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],1102)
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?',(source,)).fetchone()[0],1102)
            cursor=json.loads(store.db.execute('SELECT payload FROM local_archive_cursors').fetchone()[0])
            self.assertEqual(cursor['members'],1102)
            with self.assertRaises(ValueError):validate_commit_members(1024)
            self.assertEqual(validate_commit_members(1024,local=True),1024)
            with self.assertRaises(ValueError):validate_commit_members(1025,local=True)
        finally:store.close()

    def test_cursor_and_events_roll_back_together(self):
        from gapi.local_archive_index import LocalArchive
        from gapi.orcid_stream import stream_archive
        store,source,archive,index=self.prepare()
        try:
            with LocalArchive(store,source,archive,index) as local:
                original=local.checkpoint
                def fail(*args):
                    original(*args);raise OSError('fixture transaction failure')
                local.checkpoint=fail
                with self.assertRaisesRegex(OSError,'transaction failure'):
                    stream_archive(store,source,None,local=local,commit_members=2,full_report=False)
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?',(source,)).fetchone()[0],2)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],2)
            with LocalArchive(store,source,archive,index) as local:
                result=stream_archive(store,source,None,local=local,commit_members=2,full_report=False)
            self.assertEqual(result['sources'][0]['members'],6)
            self.assertEqual(result['sources'][0]['status'],'verified')
        finally:store.close()

    def test_index_drift_and_bad_cursor_stop_before_new_records(self):
        from gapi.local_archive_index import LocalArchive
        from gapi.orcid_stream import stream_archive
        store,source,archive,index=self.prepare()
        try:
            original=index.read_bytes();index.write_bytes(original+b'changed')
            with self.assertRaisesRegex(ValueError,'index checksum'):
                LocalArchive(store,source,archive,index)
            index.write_bytes(original)
            with LocalArchive(store,source,archive,index) as local:
                stream_archive(store,source,None,local=local,max_new_members=1,commit_members=1,full_report=False)
            store.db.execute("UPDATE local_archive_cursors SET checksum='bad'");store.db.commit()
            with self.assertRaisesRegex(ValueError,'cursor checksum'):
                LocalArchive(store,source,archive,index)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],3)
        finally:store.close()

    def test_process_exit_rolls_back_large_batch_and_cursor(self):
        import subprocess
        from gapi.career_store import CareerStore
        from gapi.local_archive_index import LocalArchive
        from gapi.orcid_stream import stream_archive
        store,source,archive,index=self.prepare(1102)
        task=store.task;store.close()
        code='''import os,sys
from pathlib import Path
from gapi.task import Task
from gapi.career_store import CareerStore
from gapi.local_archive_index import LocalArchive
from gapi.orcid_stream import stream_archive
store=CareerStore(Task(Path(sys.argv[1])))
with LocalArchive(store,sys.argv[2],Path(sys.argv[3]),Path(sys.argv[4])) as local:
 original=local.checkpoint
 def crash(*args):
  original(*args);os._exit(73)
 local.checkpoint=crash
 stream_archive(store,sys.argv[2],None,local=local,commit_members=1024,full_report=False)
'''
        result=subprocess.run([sys.executable,'-B','-c',code,str(self.path),source,str(archive),str(index)],capture_output=True,timeout=30)
        self.assertEqual(result.returncode,73,result.stderr.decode(errors='replace'))
        store=CareerStore(task)
        try:
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?',(source,)).fetchone()[0],2)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],2)
            with LocalArchive(store,source,archive,index) as local:
                report=stream_archive(store,source,None,local=local,commit_members=1024,full_report=False)
            self.assertEqual(report['sources'][0]['members'],1102)
            self.assertEqual(report['sources'][0]['status'],'verified')
        finally:store.close()
