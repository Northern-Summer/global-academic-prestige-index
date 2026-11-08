"""One explicitly authorized local index pass, never career ingestion.

Caller holds the task writer lock. An attempt marker prevents an interrupted
build from silently becoming another full prefix scan. Native decoder supplied
by the existing isolated runtime; no package installation or network access.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import time
import tarfile
from contextlib import contextmanager

from .archive_download import save
from .task import digest,canonical

MAX_INDEX_BYTES=64*1024**2
SPACING=1024**3


def index_bytes(index, record):
    """Read the pinned logical index, including an explicitly selected gzip
    representation. No index is rebuilt and no archive bytes are read.
    """
    import gzip
    index=Path(index);storage=record.get('index_storage')
    path=index
    if storage is not None:
        if (not isinstance(storage,dict) or storage.get('encoding')!='gzip'
                or storage.get('filename')!=index.name+'.gz'
                or type(storage.get('bytes')) is not int or not 0<storage['bytes']<=MAX_INDEX_BYTES
                or storage.get('decoded_bytes')!=record.get('index_bytes')):
            raise ValueError('invalid compressed index descriptor')
        path=index.with_name(storage['filename'])
    for p in (path,*path.absolute().parents):
        if p.is_symlink() or (p.exists() and p.is_junction()):raise ValueError('linked index storage')
    if not 0<path.stat().st_size<=MAX_INDEX_BYTES:raise ValueError('local index exceeds bound')
    raw=path.read_bytes()
    if storage:
        if len(raw)!=storage['bytes'] or hashlib.sha256(raw).hexdigest()!=storage.get('sha256'):
            raise ValueError('compressed index identity mismatch')
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:raw=stream.read(MAX_INDEX_BYTES+1)
    if (len(raw)>MAX_INDEX_BYTES or (record.get('index_bytes') is not None and len(raw)!=record['index_bytes'])
            or hashlib.sha256(raw).hexdigest()!=record.get('index_sha256')):
        raise ValueError('local index checksum or length mismatch')
    return raw


def compress_index(task,index):
    """Select a lossless stored representation under existing writer exclusion.
    Returns retirement evidence; never deletes the original index.
    """
    import gzip
    from .global_analysis import _write_aggregate
    index=Path(index).absolute()
    if not index.is_relative_to(task.root.absolute()):raise ValueError('index outside task')
    pointer=index.with_name(index.name+'.json');original=pointer.read_bytes();record=json.loads(original)
    raw=index_bytes(index,record);packed=gzip.compress(raw,compresslevel=6,mtime=0)
    if len(packed)>=len(raw):raise ValueError('index compression does not reduce storage')
    target=index.with_name(index.name+'.gz')
    _write_aggregate(task,target,packed)
    value={**record,'index_bytes':len(raw),'index_storage':{'encoding':'gzip','filename':target.name,'bytes':len(packed),
            'sha256':hashlib.sha256(packed).hexdigest(),'decoded_bytes':len(raw)}}
    if index_bytes(index,value)!=raw or pointer.read_bytes()!=original:raise ValueError('index conversion readback or pointer drift')
    _write_aggregate(task,pointer,canonical(value))
    return {'original':str(index),'original_bytes':len(raw),'original_sha256':record['index_sha256'],
            'selected':str(target),'stored_bytes':len(packed),'stored_sha256':value['index_storage']['sha256'],
            'net_reclaimable_bytes':len(raw)-len(packed),'logical_bytes_preserved':True,'original_deleted':False}


class CommittedPrefix:
    """Match the ordered durable ledger during the one index pass; no XML SAX.

    Retain at most one header/PAX record and one digest. Body bytes are hashed
    in the decoder callback and discarded. A gap or changed member stops the
    index rather than guessing a resume offset from a record count alone.
    """
    def __init__(self, rows, count):
        self.rows=iter(rows);self.count=count;self.matched=0
        self.offset=0;self.anchor=0 if count==0 else None
        self.buffer=bytearray();self.remaining=0;self.padding=0
        self.header=None;self.expected=None;self.hash=None;self.pending_pax=False

    def feed(self, data, offset):
        from .orcid_stream import read_mtime_pax
        from .queue_storage import unpack_digest
        if offset!=self.offset:raise ValueError('nonsequential index observation')
        self.offset+=len(data)
        if self.anchor is not None:return
        view=memoryview(data);position=0
        while position<len(view) and self.anchor is None:
            if self.remaining:
                size=min(self.remaining,len(view)-position)
                piece=view[position:position+size]
                if self.header.type==tarfile.XHDTYPE:self.buffer.extend(piece)
                else:self.hash.update(piece)
                self.remaining-=size;position+=size
                continue
            if self.padding:
                size=min(self.padding,len(view)-position)
                if self.header.type==tarfile.XHDTYPE:
                    self.buffer.extend(view[position:position+size])
                self.padding-=size;position+=size
                continue
            if self.header is not None:
                if self.header.type==tarfile.XHDTYPE:
                    read_mtime_pax(io.BytesIO(self.buffer),self.header.size)
                    self.buffer.clear();self.pending_pax=True
                else:
                    if self.hash.hexdigest()!=unpack_digest(self.expected[1]):
                        raise ValueError('committed prefix member digest mismatch')
                    self.matched+=1
                    if self.matched==self.count:
                        if next(self.rows,None) is not None:
                            raise ValueError('committed prefix ledger count mismatch')
                        self.anchor=offset+position
                        return
                self.header=None
            size=min(512-len(self.buffer),len(view)-position)
            self.buffer.extend(view[position:position+size]);position+=size
            if len(self.buffer)<512:continue
            block=bytes(self.buffer);self.buffer.clear()
            if block==b'\0'*512:raise ValueError('archive ended before committed prefix')
            header=tarfile.TarInfo.frombuf(block,'utf-8','strict')
            if header.type==tarfile.XHDTYPE:
                if self.pending_pax or not 1<=header.size<=128:
                    raise ValueError('unsupported committed-prefix PAX')
            else:
                self.pending_pax=False
                if header.isdir() and header.size==0:continue
                if not header.isreg() or header.size<0:
                    raise ValueError('unsupported committed-prefix tar type')
                self.expected=next(self.rows,None)
                if self.expected is None or self.expected[0]!=header.name:
                    raise ValueError('committed prefix member order mismatch')
                self.hash=hashlib.sha256()
            self.header=header;self.remaining=header.size
            self.padding=(-header.size)%512
        # A member ending exactly at a callback boundary is settled next call.

    def finish(self):
        if self.anchor is None:raise ValueError('incomplete committed prefix')
        return {'tar_offset':self.anchor,'members':self.matched}


@contextmanager
def committed_prefix(task,source):
    """Caller holds exclusive_writer; read ledger without constructing a writer."""
    import sqlite3
    from .career_store import source_identity
    database=task.root/'careers.sqlite'
    if not database.exists():
        yield CommittedPrefix((),0)
        return
    connection=sqlite3.connect(database.as_uri()+'?mode=ro',uri=True)
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('PRAGMA cache_size=-4096')
        connection.execute('BEGIN')
        run=connection.execute('SELECT identity,members FROM runs WHERE source=?',(source,)).fetchone()
        count=connection.execute('SELECT count(*) FROM members WHERE source=?',(source,)).fetchone()[0]
        if ((run is None and count) or (run is not None and
                (run[0]!=source_identity(task,source) or run[1]!=count))):
            raise ValueError('committed prefix source/count mismatch')
        # The durable insertion order is checked against actual tar order. No
        # giant ORDER BY sort or in-memory member set is admitted here.
        rows=connection.execute('SELECT locator,sha256 FROM members NOT INDEXED WHERE source=? ORDER BY rowid',(source,))
        yield CommittedPrefix(rows,count)
    finally:connection.close()


def local_runtime_build():
    """Resolve the selected hash-bound optional runtime without importing it."""
    from .storage import project_root
    build=project_root()/'work/archive-index-build'
    selection=project_root()/'work/archive-index-runtime-selection.json'
    selected=None
    if selection.exists():
        if selection.is_symlink() or selection.stat().st_size>65536:raise ValueError('invalid native runtime selection')
        selected=json.loads(selection.read_bytes());name=selected.get('build')
        if selected.get('schema_version')!='gapi-index-runtime-selection/1' or name!='archive-index-build-callback':
            raise ValueError('invalid native runtime selection')
        build=project_root()/'work'/name
    receipt=json.loads((build/'build-receipt.json').read_text())
    binary=build/'lib/indexed_gzip/indexed_gzip.pyd'
    if hashlib.sha256(binary.read_bytes()).hexdigest()!=receipt['binary_sha256']:
        raise ValueError('native index runtime differs from its build receipt')
    if selected and selected.get('binary_sha256')!=receipt['binary_sha256']:raise ValueError('selected native identity drift')
    if receipt.get('patch')!='64-bit ftell; zero-sized I/O guard; 8 MiB buffers independent of spacing':
        raise ValueError('native runtime lacks required bounded-buffer corrections')
    if receipt.get('index_output_observer')!='decoded-bytes-and-offset/v1':
        raise ValueError('native runtime cannot observe committed tar prefix in one pass')
    if receipt.get('sequential_inflater')!='opt-in-persistent-inflater/v1':
        raise ValueError('native runtime lacks bounded sequential continuation')
    return build


def local_decoder(**options):
    """Use only the hash-bound native build already maintained by this project."""
    import importlib
    import sys
    build=local_runtime_build()
    sys.path.insert(0,str(build/'lib'))
    try:module=importlib.import_module('indexed_gzip')
    finally:sys.path.pop(0)
    if not Path(module.__file__).resolve().is_relative_to((build/'lib').resolve()):
        raise ValueError('another native index runtime is already loaded')
    return module.IndexedGzipFile(**options)


def build_local_index(task,source,archive,output,decoder_factory,*,max_seconds=21600,prefix=None):
    task.require_frozen()
    if type(max_seconds) not in (int,float) or not 0<max_seconds<=21600:
        raise ValueError('invalid index deadline')
    archive,output=Path(archive).absolute(),Path(output).absolute()
    for path in (archive,output,*archive.parents,*output.parents):
        if path.is_symlink() or (path.exists() and path.is_junction()):
            raise ValueError('index path contains a link')
    if not output.is_relative_to(task.root):
        raise ValueError('index must be charged to the normal task budget')
    spec=task.config['sources'][source]
    receipt=json.loads(archive.with_suffix(archive.suffix+'.download.json').read_text())
    if (receipt.get('state')!='verified-file' or receipt.get('source_identity')!=digest(spec)
            or receipt.get('destination')!=str(archive)
            or receipt.get('bytes')!=spec['size_bytes']
            or receipt.get('md5')!=spec['checksum'].removeprefix('md5:')
            or archive.stat().st_size!=spec['size_bytes']):
        raise ValueError('official complete file verification required')
    marker=output.with_suffix(output.suffix+'.json')
    if output.exists() or marker.exists():
        raise ValueError('prior index attempt requires reconciliation; no automatic rescan')
    task.check(MAX_INDEX_BYTES+65536)
    baseline=archive.stat()
    started=time.monotonic()
    state={'state':'building','source_identity':digest(spec),'archive_sha256':receipt['sha256'],
           'archive_size':baseline.st_size,'archive_mtime_ns':baseline.st_mtime_ns,
           'decoded_bytes':None,'compressed_reads':0,'peak_sampled_memory_bytes':0,
           'xml_records_parsed':0,'business_records_written':0,'spacing':SPACING}
    save(marker,state)
    read_failure=[]
    next_report=started+60
    class GuardedFile:
        def __init__(self,stream):self.stream=stream
        def fileno(self):raise io.UnsupportedOperation('bounded Python reader')
        def seekable(self):return True
        def readable(self):return True
        def seek(self,*args):return self.stream.seek(*args)
        def tell(self):return self.stream.tell()
        def index_output(self,data,offset):
            if read_failure:return
            try:
                if prefix is not None:prefix.feed(data,offset)
            except Exception as error:
                read_failure.append(error)
        def read(self,size):
            nonlocal next_report
            if read_failure:return b''
            try:
                if not 0<=size<=65536:raise ValueError('unbounded native input read')
                if time.monotonic()-started>=max_seconds:raise TimeoutError('index deadline')
                memory=task.check_memory() or 0
                state['peak_sampled_memory_bytes']=max(state['peak_sampled_memory_bytes'],memory)
                data=self.stream.read(size);state['compressed_reads']+=len(data)
                now=time.monotonic()
                if now>=next_report:
                    state['elapsed_seconds']=now-started
                    if prefix is not None:state['committed_members_matched']=prefix.matched
                    save(marker,state)
                    next_report=now+60
                return data
            except Exception as error:
                # Do not depend on a C callback preserving a Python exception.
                # A sticky stop prevents both further reads and false success.
                read_failure.append(error)
                return b''
    class IndexBuffer(io.BytesIO):
        def write(self,data):
            if self.tell()+len(data)>MAX_INDEX_BYTES:raise ValueError('index exceeds bound')
            return super().write(data)
    try:
        with archive.open('rb') as raw:
            # IndexedGzipFile otherwise defaults its Python BufferedReader to
            # spacing*4 (4 GiB here), independently of the native input buffer.
            with decoder_factory(fileobj=GuardedFile(raw),spacing=SPACING,
                                 readbuf_size=65536,buffer_size=65536) as decoder:
                decoder.build_full_index()
                if read_failure:raise read_failure[0]
                points=list(decoder.seek_points())
                if len(points)<2:raise ValueError('empty or incomplete index')
                state['decoded_bytes']=points[-1][0]
                if prefix is not None:state['committed_prefix']=prefix.finish()
                with IndexBuffer() as buffer:
                    decoder.export_index(fileobj=buffer);index=buffer.getvalue()
        current=archive.stat()
        if (baseline.st_size,baseline.st_mtime_ns)!=(current.st_size,current.st_mtime_ns):
            raise ValueError('archive changed while indexing')
        task.check(len(index))
        with output.open('xb') as stream:
            stream.write(index);stream.flush();os.fsync(stream.fileno())
        state.update(state='indexed',index_sha256=hashlib.sha256(index).hexdigest(),
                     index_bytes=len(index),elapsed_seconds=time.monotonic()-started)
        save(marker,state)
        return state
    except BaseException as error:
        if read_failure:error=read_failure[0]
        state.update(state='failed',error_type=type(error).__name__,error=str(error)[:200])
        save(marker,state)
        raise error


class LocalArchive:
    """Verified immutable file/index plus a cursor committed with member writes."""
    def __init__(self,store,source,archive,index,*,max_seconds=21600):
        from .career_store import source_identity
        self.store,self.source=store,source
        self.archive,self.index=Path(archive).absolute(),Path(index).absolute()
        for path in (self.archive,self.index,*self.archive.parents,*self.index.parents):
            if path.is_symlink() or (path.exists() and path.is_junction()):
                raise ValueError('local continuation path contains a link')
        self.record=json.loads(self.index.with_suffix(self.index.suffix+'.json').read_text())
        receipt=json.loads(self.archive.with_suffix(self.archive.suffix+'.download.json').read_text())
        spec=store.task.config['sources'][source]
        self.baseline=self.archive.stat()
        if (self.record.get('state')!='indexed' or self.record.get('source_identity')!=digest(spec)
                or receipt.get('state')!='verified-file' or receipt.get('source_identity')!=digest(spec)
                or receipt.get('destination')!=str(self.archive)
                or receipt.get('bytes')!=spec['size_bytes']
                or receipt.get('md5')!=spec['checksum'].removeprefix('md5:')
                or self.record.get('archive_sha256')!=receipt.get('sha256')
                or self.record.get('archive_size')!=self.baseline.st_size
                or self.record.get('archive_mtime_ns')!=self.baseline.st_mtime_ns
                or self.baseline.st_size!=spec['size_bytes']):
            raise ValueError('local verified archive/index identity mismatch')
        decoded_index=index_bytes(self.index,self.record)
        self.index_data=decoded_index if self.record.get('index_storage') else None
        self.archive_sha256=receipt['sha256']
        self.binding={'source_identity':source_identity(store.task,source),
            'archive_sha256':self.archive_sha256,'index_sha256':self.record['index_sha256']}
        exists=store.db.execute("SELECT 1 FROM sqlite_master WHERE name='local_archive_cursors'").fetchone()
        row=store.db.execute('SELECT payload,checksum FROM local_archive_cursors WHERE source=?',(source,)).fetchone() if exists else None
        if row:
            if hashlib.sha256(row[0].encode()).hexdigest()!=row[1]:raise ValueError('local cursor checksum mismatch')
            cursor=json.loads(row[0])
            if any(cursor.get(k)!=v for k,v in self.binding.items()):raise ValueError('local cursor identity drift')
            last=cursor.get('last_member')
            if not isinstance(last,list) or len(last)!=3 or store.seen(source,last[0])!=tuple(last[1:]):
                raise ValueError('local cursor last member mismatch')
        else:cursor=self.record['committed_prefix']
        run=store.db.execute('SELECT identity,members FROM runs WHERE source=?',(source,)).fetchone()
        count=store.db.execute('SELECT count(*) FROM members WHERE source=?',(source,)).fetchone()[0]
        if (not run or run!=(self.binding['source_identity'],count) or count!=cursor['members']):
            raise ValueError('local cursor member ledger mismatch')
        self.position=cursor['tar_offset']
        if type(self.position) is not int or not 0<=self.position<=self.record['decoded_bytes'] or self.position%512:
            raise ValueError('invalid local tar cursor')
        if type(max_seconds) not in (int,float) or not 0<max_seconds<=21600:
            raise ValueError('invalid local continuation deadline')
        self.deadline=time.monotonic()+max_seconds
        self.decoder=None;self.raw=None
        self.read_failure=[]

    def __enter__(self):
        owner=self
        self.raw=self.archive.open('rb')
        class Reader:
            gapi_sequential=True
            size=0
            def fileno(self):raise io.UnsupportedOperation('bounded local reader')
            def seekable(self):return True
            def readable(self):return True
            def seek(self,*args):return owner.raw.seek(*args)
            def tell(self):return owner.raw.tell()
            def read(self,size):
                from .task import BudgetStop
                if owner.read_failure:return b''
                try:
                    if not 0<=size<=65536:raise ValueError('unbounded local decoder read')
                    owner.store.task.check_memory()
                    if time.monotonic()>=owner.deadline:raise BudgetStop('local continuation deadline')
                    data=owner.raw.read(size);self.size+=len(data)
                    return data
                except Exception as error:
                    owner.read_failure.append(error)
                    return b''
        self.reader=Reader()
        try:
            native=local_decoder(fileobj=self.reader,index_file=None if self.index_data is not None else str(self.index),
                buffer_size=65536,readbuf_size=65536,auto_build=False)
            if self.index_data is not None:native.import_index(fileobj=io.BytesIO(self.index_data))
            class Decoder:
                def read(self,size):
                    try:result=native.read(size)
                    except Exception:
                        if owner.read_failure:raise owner.read_failure[0]
                        raise
                    if owner.read_failure:raise owner.read_failure[0]
                    return result
                def tell(self):return native.tell()
                def seek(self,*args):return native.seek(*args)
                def close(self):native.close()
            self.decoder=Decoder()
            self.decoder.seek(self.position)
            return self
        except BaseException:
            self.__exit__(None,None,None)
            raise

    def __exit__(self,*args):
        if self.decoder is not None:self.decoder.close()
        if self.raw is not None:self.raw.close()

    def checkpoint(self,position,last):
        from .task import canonical
        if self.read_failure:raise self.read_failure[0]
        if not self.store.db.in_transaction:raise ValueError('local cursor requires member transaction')
        if type(position) is not int or position<=self.position or position%512:
            raise ValueError('local cursor must advance at a tar boundary')
        count=self.store.db.execute('SELECT members FROM runs WHERE source=?',(self.source,)).fetchone()[0]
        payload=canonical(dict(self.binding,tar_offset=position,members=count,last_member=list(last))).decode()
        self.store.db.execute('CREATE TABLE IF NOT EXISTS local_archive_cursors(source TEXT PRIMARY KEY,payload TEXT NOT NULL,checksum TEXT NOT NULL)')
        self.store.db.execute('INSERT INTO local_archive_cursors VALUES(?,?,?) ON CONFLICT(source) DO UPDATE SET payload=excluded.payload,checksum=excluded.checksum',
            (self.source,payload,hashlib.sha256(payload.encode()).hexdigest()))

    def finish(self):
        if self.read_failure:raise self.read_failure[0]
        current=self.archive.stat()
        if ((current.st_size,current.st_mtime_ns)!=(self.baseline.st_size,self.baseline.st_mtime_ns)
                or self.decoder.tell()!=self.record['decoded_bytes']):
            raise ValueError('local archive changed or decoded length mismatch')
