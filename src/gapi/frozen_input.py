"""Budgeted private snapshots of normalized inputs for historical reproduction.

These contain controlled person-level events and never enter the project/public
export. A snapshot is admitted only if its extra bytes fit the same task budget.
"""
import hashlib
import os
import re
import sqlite3
import gzip
import tempfile
from .event_storage import configure_reader
from contextlib import contextmanager,closing

SNAPSHOT_CHUNK=1024**2

def file_hash(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def snapshot_path(store, filename):
    if not isinstance(filename,str) or not re.fullmatch(r'[a-f0-9]{64}\.sqlite',filename):
        raise ValueError('frozen input must be a content-addressed filename')
    folder=store.task.root/'frozen-inputs'
    if folder.is_symlink() or getattr(folder,'is_junction',lambda:False)():
        raise ValueError('linked snapshot directory forbidden')
    path=folder/filename
    for candidate in (path,path.with_suffix('.sqlite.gz')):
        if candidate.is_symlink():raise ValueError('linked snapshot forbidden')
    return path


def _unpack(task, packed, destination=None):
    """Bounded gzip verification; raw SHA remains the historical identity."""
    sha=hashlib.sha256()
    total=0
    with gzip.open(packed,'rb') as stream:
        while chunk:=stream.read(SNAPSHOT_CHUNK):
            total+=len(chunk)
            if total>task.config['budget_bytes']:raise ValueError('snapshot exceeds task budget')
            task.check(len(chunk));task.check_memory()
            sha.update(chunk)
            if destination is not None:destination.write(chunk)
    return sha.hexdigest()


def verified_packed_snapshot(store, path):
    """Quality classification requires location AND full original content hash."""
    if path.parent!=store.task.root/'frozen-inputs' or not re.fullmatch(r'[a-f0-9]{64}\.sqlite\.gz',path.name):
        return False
    try:
        snapshot_path(store,path.name.removesuffix('.gz'))
        return _unpack(store.task,path)==path.name[:64]
    except (OSError,EOFError,ValueError):
        return False


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
        elif target.with_suffix('.sqlite.gz').exists():
            packed=target.with_suffix('.sqlite.gz')
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
    temporary=None
    if not path.exists():
        try:
            with tempfile.NamedTemporaryFile(mode='wb',prefix='frozen-read-',suffix='.sqlite',
                    dir=path.parent,delete=False) as target:
                temporary=type(path)(target.name)
                if _unpack(store.task,path.with_suffix('.sqlite.gz'),target)!=filename[:-7]:
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
