"""Budgeted private snapshots of normalized inputs for historical reproduction.

These contain controlled person-level events and never enter the project/public
export. A snapshot is admitted only if its extra bytes fit the same task budget.
"""
import hashlib
import os
import re
import sqlite3
from contextlib import contextmanager,closing


def file_hash(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def freeze_input(store):
    if store.db.in_transaction:raise ValueError('cannot freeze an unfinished input transaction')
    size=store.db.execute('PRAGMA page_count').fetchone()[0]*store.db.execute('PRAGMA page_size').fetchone()[0]
    store.task.check(size*2+1024**2)
    folder=store.task.root/'frozen-inputs'
    folder.mkdir(exist_ok=True)
    temporary=folder/'normalized-input.partial'
    if temporary.exists():raise ValueError('unfinished private freeze exists; inspect it before retry')
    try:
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
        else:os.replace(temporary,target)
        return {'sha256':sha,'filename':target.name,'bytes':target.stat().st_size,
                'contains_personal_input':True,'publicly_bundled':False}
    finally:
        # Only this task-owned newly created staging file is disposable.
        if temporary.exists():temporary.unlink()


@contextmanager
def frozen_reader(store,filename):
    if not isinstance(filename,str) or not re.fullmatch(r'[a-f0-9]{64}\.sqlite',filename):
        raise ValueError('frozen input must be a content-addressed filename')
    store.task.check()
    path=store.task.root/'frozen-inputs'/filename
    if path.is_symlink() or file_hash(path)!=filename[:-7]:
        raise ValueError('private frozen input hash mismatch')
    live=store.db
    reader=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
    try:
        reader.execute('PRAGMA query_only=ON')
        reader.execute('PRAGMA temp_store=MEMORY')
        reader.execute('PRAGMA cache_size=-8192')
        reader.execute('PRAGMA hard_heap_limit=134217728')
        store.db=reader
        yield
    finally:
        store.db=live
        reader.close()
