"""Budgeted private snapshots of normalized inputs for historical reproduction.

These contain controlled person-level events and never enter the project/public
export. A snapshot is admitted only if its extra bytes fit the same task budget.
"""
import hashlib
import os
import re
import sqlite3
import gzip
import lzma
import tempfile
from .event_storage import configure_reader
from contextlib import contextmanager,closing

SNAPSHOT_CHUNK=1024**2

def file_hash(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def snapshot_path(store, filename):
    if not isinstance(filename,str) or not re.fullmatch(r'[a-f0-9]{64}\.(sqlite|rows)',filename):
        raise ValueError('frozen input must be a content-addressed filename')
    folder=store.task.root/'frozen-inputs'
    if folder.is_symlink() or getattr(folder,'is_junction',lambda:False)():
        raise ValueError('linked snapshot directory forbidden')
    path=folder/filename
    for candidate in (path,path.with_suffix('.sqlite.gz'),path.with_suffix('.sqlite.xz'),path.with_suffix('.rows.xz')):
        if candidate.is_symlink():raise ValueError('linked snapshot forbidden')
    return path


def _snapshot_chunks(packed):
    """One owned XZ stream, bounded dictionary and output; legacy gzip stays readable."""
    if '.xz' not in packed.suffixes:
        with gzip.open(packed, 'rb') as stream:
            while chunk := stream.read(SNAPSHOT_CHUNK):
                yield chunk
        return
    decoder = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=32*1024**2)
    with packed.open('rb') as stream:
        while not decoder.eof:
            data = stream.read(SNAPSHOT_CHUNK) if decoder.needs_input else b''
            if not data and decoder.needs_input:
                raise EOFError('truncated private XZ snapshot')
            chunk = decoder.decompress(data, max_length=SNAPSHOT_CHUNK)
            if chunk:
                yield chunk
        if decoder.unused_data or stream.read(1):
            raise ValueError('trailing private XZ snapshot data')


def _packed_path(path):
    legacy = path.with_suffix('.sqlite.gz')
    return legacy if legacy.exists() else path.with_suffix('.sqlite.xz')


def _unpack(task, packed, destination=None):
    """Bounded verification; raw SHA remains the historical identity."""
    sha=hashlib.sha256()
    total=0
    for chunk in _snapshot_chunks(packed):
        total+=len(chunk)
        if total>task.config['budget_bytes']:raise ValueError('snapshot exceeds task budget')
        task.check(len(chunk));task.check_memory()
        sha.update(chunk)
        if destination is not None:destination.write(chunk)
    return sha.hexdigest()


def verified_packed_snapshot(store, path):
    """Quality classification requires location AND full original content hash."""
    if path.parent!=store.task.root/'frozen-inputs' or not re.fullmatch(r'[a-f0-9]{64}\.(sqlite\.(gz|xz)|rows\.xz)',path.name):
        return False
    try:
        snapshot_path(store,path.name.rsplit('.',1)[0])
        if path.name.endswith('.rows.xz'):
            from .row_snapshot import inspect
            return inspect(path,store.task)[0]==path.name[:64]
        return _unpack(store.task,path)==path.name[:64]
    except (OSError,EOFError,ValueError,lzma.LZMAError):
        return False


def repack_snapshot(store, filename):
    """Stream a verified derived gzip to XZ, never materializing the SQLite file.

    Caller holds the existing task writer lock. Each successor is fully decoded
    and hash-checked before its predecessor is retired. No new input identity.
    """
    path=snapshot_path(store,filename)
    if path.exists():raise ValueError('compress raw normalized snapshot before repacking')
    source=path.with_suffix('.sqlite.gz');target=path.with_suffix('.sqlite.xz')
    expected=filename[:-7]
    if not source.exists():
        if _unpack(store.task,target)!=expected:raise ValueError('packed snapshot hash mismatch')
        return {'filename':filename,'already_repacked':True,'packed_bytes':target.stat().st_size}
    before=source.stat().st_size
    temporary=target.with_suffix('.xz.partial');created=False
    try:
        if not target.exists():
            with temporary.open('xb') as output:
                created=True
                compressor=lzma.LZMACompressor(format=lzma.FORMAT_XZ,preset=3)
                raw_hash=hashlib.sha256();total=0
                for chunk in _snapshot_chunks(source):
                    total+=len(chunk)
                    if total>store.task.config['budget_bytes']:raise ValueError('snapshot exceeds task budget')
                    store.task.check(len(chunk)*2+65536);store.task.check_memory()
                    raw_hash.update(chunk)
                    encoded=compressor.compress(chunk)
                    store.task.check(len(encoded)+65536)
                    output.write(encoded)
                encoded=compressor.flush()
                store.task.check(len(encoded)+65536);store.task.check_memory()
                output.write(encoded);output.flush();os.fsync(output.fileno())
            if raw_hash.hexdigest()!=expected:raise ValueError('source snapshot hash mismatch')
            if _unpack(store.task,temporary)!=expected:raise ValueError('successor snapshot hash mismatch')
            if temporary.stat().st_size>=before:
                temporary.unlink()
                return {'filename':filename,'already_repacked':False,'retained_gzip':True,'saved_bytes':0}
            os.replace(temporary,target)
        if _unpack(store.task,target)!=expected:raise ValueError('successor snapshot hash mismatch')
        if _unpack(store.task,source)!=expected:raise ValueError('source snapshot changed during repack')
        source.unlink()
        return {'filename':filename,'already_repacked':False,'gzip_bytes':before,
                'packed_bytes':target.stat().st_size,'saved_bytes':before-target.stat().st_size}
    finally:
        if created and temporary.exists():temporary.unlink()


def compress_snapshot(store, filename):
    """Replace one verified derived snapshot, never an upstream raw archive.

    Atomic packed publication precedes raw retirement. A crash leaves either
    the original or a verified packed successor; the logical filename is stable.
    Caller must serialize this maintenance with collection and frozen readers.
    """
    path=snapshot_path(store,filename)
    packed=path.with_suffix('.sqlite.gz')
    expected=filename[:-7]
    if not path.exists():
        packed=_packed_path(path)
        if _unpack(store.task,packed)!=expected:raise ValueError('packed snapshot hash mismatch')
        return {'filename':filename,'already_compressed':True,'packed_bytes':packed.stat().st_size}
    if file_hash(path)!=expected:raise ValueError('private frozen input hash mismatch')
    original_size=path.stat().st_size
    temporary=packed.with_suffix('.gz.partial')
    if temporary.exists():raise ValueError('unfinished compression exists; inspect before retry')
    if packed.exists():
        if _unpack(store.task,packed)!=expected:raise ValueError('packed snapshot hash mismatch')
    else:
        try:
            with path.open('rb') as source,temporary.open('xb') as target:
                with gzip.GzipFile(filename='',mode='wb',fileobj=target,mtime=0) as compressor:
                    while chunk:=source.read(SNAPSHOT_CHUNK):
                        store.task.check(len(chunk)+65536);store.task.check_memory()
                        compressor.write(chunk)
                target.flush();os.fsync(target.fileno())
            if _unpack(store.task,temporary)!=expected:raise ValueError('packed snapshot hash mismatch')
            os.replace(temporary,packed)
        finally:
            if temporary.exists():temporary.unlink()
    if file_hash(path)!=expected:raise ValueError('snapshot changed during compression')
    path.unlink()
    return {'filename':filename,'already_compressed':False,'raw_bytes':original_size,
            'packed_bytes':packed.stat().st_size,'saved_bytes':original_size-packed.stat().st_size}


def freeze_input(store, *, scope='full'):
    if store.db.in_transaction:raise ValueError('cannot freeze an unfinished input transaction')
    if scope not in ('full','global-ranking'):raise ValueError('unsupported frozen input scope')
    size=store.db.execute('PRAGMA page_count').fetchone()[0]*store.db.execute('PRAGMA page_size').fetchone()[0]
    store.task.check(size*2+1024**2 if scope=='full' else 1024**2)
    folder=store.task.root/'frozen-inputs'
    folder.mkdir(exist_ok=True)
    temporary=folder/'normalized-input.partial'
    if temporary.exists():raise ValueError('unfinished private freeze exists; inspect it before retry')
    # Establish ownership before entering cleanup; an existing/dangling link
    # must fail exclusive creation and cannot become our disposable staging.
    with temporary.open('xb'):pass
    try:
        metadata=None
        if scope=='global-ranking':
            from .ranking_snapshot import build
            metadata=build(store,temporary)
            size=temporary.stat().st_size
        else:
            with closing(sqlite3.connect(temporary)) as destination:
                def progress(status,remaining,total):
                    store.task.check();store.task.check_memory()
                store.db.backup(destination,pages=128,progress=progress)
                if destination.execute('PRAGMA quick_check').fetchone()[0]!='ok':
                    raise ValueError('private frozen input integrity failed')
        sha=file_hash(temporary)
        target=folder/(sha+'.sqlite')
        if target.exists():
            if file_hash(target)!=sha:raise ValueError('existing frozen input hash mismatch')
            temporary.unlink()
        elif target.with_suffix('.sqlite.gz').exists() or target.with_suffix('.sqlite.xz').exists():
            packed=_packed_path(target)
            if packed.is_symlink() or _unpack(store.task,packed)!=sha:
                raise ValueError('existing packed input hash mismatch')
            temporary.unlink()
        else:os.replace(temporary,target)
        result={'sha256':sha,'filename':target.name,'bytes':size,
                'contains_personal_input':True,'publicly_bundled':False}
        if metadata is not None:
            compress_snapshot(store,target.name)
            result['projection']=metadata
        return result
    finally:
        # Only this task-owned newly created staging file is disposable.
        if temporary.exists():temporary.unlink()
        journal=temporary.with_name(temporary.name+'-journal')
        if journal.exists():journal.unlink()


@contextmanager
def frozen_reader(store,filename):
    path=snapshot_path(store,filename)
    store.task.check()
    if filename.endswith('.rows'):
        from .row_snapshot import restore_rows
        temporary=None;reader=None;live=store.db
        try:
            with tempfile.NamedTemporaryFile(prefix='frozen-read-',suffix='.sqlite',dir=path.parent,delete=False) as target:
                temporary=type(path)(target.name)
            reader=restore_rows(path.with_suffix('.rows.xz'),temporary,store.task)
            reader.execute('PRAGMA query_only=ON');store.db=reader
            yield
        finally:
            store.db=live
            if reader is not None:reader.close()
            if temporary is not None:
                temporary.unlink(missing_ok=True)
                temporary.with_name(temporary.name+'-journal').unlink(missing_ok=True)
        return
    temporary=None
    if not path.exists():
        try:
            with tempfile.NamedTemporaryFile(mode='wb',prefix='frozen-read-',suffix='.sqlite',
                    dir=path.parent,delete=False) as target:
                temporary=type(path)(target.name)
                if _unpack(store.task,_packed_path(path),target)!=filename[:-7]:
                    raise ValueError('packed snapshot hash mismatch')
            path=temporary
        except BaseException:
            if temporary is not None:temporary.unlink(missing_ok=True)
            raise
    elif file_hash(path)!=filename[:-7]:
        raise ValueError('private frozen input hash mismatch')
    live=store.db
    reader=None
    try:
        reader=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
        configure_reader(reader)
        reader.execute('PRAGMA query_only=ON')
        reader.execute('PRAGMA temp_store=MEMORY')
        reader.execute('PRAGMA cache_size=-8192')
        reader.execute('PRAGMA hard_heap_limit=134217728')
        store.db=reader
        yield
    finally:
        store.db=live
        if reader is not None:reader.close()
        if temporary is not None:temporary.unlink(missing_ok=True)
