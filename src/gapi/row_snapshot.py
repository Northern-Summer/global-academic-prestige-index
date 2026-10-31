"""Stream private normalized ranking rows directly to a verified XZ archive.

No raw responses, SQLite staging copy or SQL text containing personal rows.
Restoration is separately admitted against available disk; archiving does not
promise that a second database fits beside the live acquisition database.
"""
import base64
import hashlib
import json
import lzma
import os
import re
import sqlite3

from .event_storage import configure_reader, snapshot_payload
from .ranking_snapshot import EXCLUDED_TABLES, METADATA_TABLE, POLICY, quoted
from .task import canonical, digest

FORMAT = 'gapi-private-ranking-rows/1'
MAX_LINE = 1024**2


def ranking_lines(store):
    """One deterministic row representation, shared by full and delta archives."""
    if store.db.in_transaction:raise ValueError('cannot freeze unfinished transaction')
    def line(record):
        value=canonical(record)+b'\n'
        if len(value)>MAX_LINE:raise ValueError('row archive line exceeds bound')
        return value
    store.db.execute('BEGIN')
    try:
        schema=list(store.db.execute("""SELECT type,name,tbl_name,sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"""))
        if any(kind=='trigger' or sql and 'CREATE VIRTUAL' in sql.upper() for kind,_,_,sql in schema):
            raise ValueError('triggers and virtual tables forbidden in ranking inputs')
        if any(name==METADATA_TABLE for _,name,_,_ in schema):
            raise ValueError('cannot freeze an already scoped input')
        retained=[(kind,name,table,sql) for kind,name,table,sql in schema
                  if table not in EXCLUDED_TABLES and sql]
        excluded={name:store.db.execute('SELECT count(*) FROM '+quoted(name)).fetchone()[0]
                  for kind,name,_,_ in schema if kind=='table' and name in EXCLUDED_TABLES}
        metadata={'policy':POLICY,'scope':'global-ranking','excluded_tables':excluded,
            'acquisition_progress':store.candidate_progress(),'config_sha256':digest(store.task.config),
            'not_a_full_acquisition_backup':True}
        yield line({'format':FORMAT,'schema':retained,'metadata':metadata})
        counts={}
        for kind,name,_,_ in retained:
            if kind!='table':continue
            columns=list(store.db.execute('PRAGMA table_info('+quoted(name)+')'))
            primary=sorted((row[5],row[1]) for row in columns if row[5])
            order=','.join(quoted(column) for _,column in primary) if primary else 'rowid'
            payload_column=next((i for i,c in enumerate(columns) if c[1]=='payload'),None) if name=='events' else None
            reader=store.db.cursor();reader.row_factory=None;count=0
            try:
                for row in reader.execute('SELECT * FROM '+quoted(name)+' ORDER BY '+order):
                    if name=='events':
                        row=list(row);row[payload_column]=snapshot_payload(store.db,row[payload_column]);row=tuple(row)
                    yield line({'t':name,'v':[['b',base64.b64encode(v).decode('ascii')]
                        if isinstance(v,bytes) else ['v',v] for v in row]})
                    count+=1
            finally:reader.close()
            counts[name]=count
        yield line({'end':counts})
    finally:store.db.rollback()


def archive_path(store, filename):
    from .frozen_input import snapshot_path
    path=snapshot_path(store,filename)
    full=path.with_suffix('.rows.xz')
    return full if full.exists() else path.with_suffix('.rows.delta.xz')


def lines(path, task):
    if path.name.endswith('.rows.delta.xz'):
        from .row_delta import decoded_lines
        yield from decoded_lines(path,task)
    else:yield from _packed_lines(path,task)


def _packed_lines(path, task, *, max_line=MAX_LINE):
    from .frozen_input import _snapshot_chunks
    pending = b''
    total = 0
    for chunk in _snapshot_chunks(path):
        task.check_memory()
        total += len(chunk)
        if total > task.config['budget_bytes'] * 4:
            raise ValueError('normalized row archive exceeds decoded bound')
        pending += chunk
        start = 0
        while (end := pending.find(b'\n', start)) >= 0:
            if end-start > max_line:raise ValueError('row archive line exceeds bound')
            yield pending[start:end+1]
            start = end+1
        # Retain the unfinished tail once per decoded chunk, rather than
        # copying the entire remaining chunk after every emitted row.
        pending = pending[start:]
        if len(pending) > max_line:raise ValueError('row archive line exceeds bound')
    if pending:raise ValueError('unterminated normalized row archive')


def inspect(path, task):
    sha = hashlib.sha256();count = 0;total = 0
    for line in lines(path, task):
        sha.update(line);total += len(line);count += 1
    if count < 2:raise ValueError('incomplete normalized row archive')
    return sha.hexdigest(), total


def validate_compression_preset(preset):
    """Physical encoding only; both choices fit the existing 32 MiB decoder."""
    if type(preset) is not int or preset not in (3, 6):
        raise ValueError('unsupported row archive compression preset')
    return preset


def freeze_rows(store, *, base_filename=None, compression_preset=3, allow_delta_base=False):
    validate_compression_preset(compression_preset)
    if type(allow_delta_base) is not bool:raise ValueError('allow_delta_base must be boolean')
    if base_filename is not None:
        from .row_delta import freeze_delta
        return freeze_delta(store,base_filename,compression_preset=compression_preset,allow_delta_base=allow_delta_base)
    if store.db.in_transaction:raise ValueError('cannot freeze unfinished transaction')
    task = store.task;initial_used=task.check(1024**2);task.check_memory()
    folder = task.root/'frozen-inputs'
    if folder.is_symlink() or getattr(folder, 'is_junction', lambda:False)():
        raise ValueError('linked snapshot directory forbidden')
    folder.mkdir(exist_ok=True)
    temporary = folder/'normalized-rows.xz.partial'
    sha = hashlib.sha256();decoded = 0;created = False
    try:
        with temporary.open('xb') as output:
            created = True
            encoder = lzma.LZMACompressor(format=lzma.FORMAT_XZ,preset=compression_preset)
            written=0
            def write(packed):
                nonlocal written
                task.check(len(packed)+65536,minimum_used=initial_used+written)
                task.check_memory();output.write(packed);written+=len(packed)
            def emit(line):
                nonlocal decoded
                sha.update(line);decoded += len(line)
                packed = encoder.compress(line)
                if packed:
                    write(packed)
            source=ranking_lines(store)
            try:
                for value in source:emit(value)
            finally:source.close()
            tail = encoder.flush();write(tail)
            output.flush();os.fsync(output.fileno())
        checksum = sha.hexdigest()
        if inspect(temporary,task)!=(checksum,decoded):raise ValueError('row archive verification failed')
        target = folder/(checksum+'.rows.xz')
        if target.is_symlink():raise ValueError('linked row archive forbidden')
        if target.exists():
            if inspect(target,task)!=(checksum,decoded):raise ValueError('existing row archive mismatch')
            temporary.unlink()
        else:os.replace(temporary,target)
        return {'sha256':checksum,'filename':checksum+'.rows','bytes':decoded,
            'packed_bytes':target.stat().st_size,'format':FORMAT,
            'contains_personal_input':True,'publicly_bundled':False,
            'restoration_requires_separate_resource_admission':True}
    finally:
        if created:temporary.unlink(missing_ok=True)


def restore_rows(path, target, task):
    """Restore a verified private archive; no executescript or row SQL allowed."""
    expected = path.name[:64]
    if not re.fullmatch(r'[a-f0-9]{64}\.rows(?:\.delta)?\.xz',path.name) or inspect(path,task)[0]!=expected:
        raise ValueError('row archive identity mismatch')
    # Caller owns exclusive empty target creation and failure cleanup.
    if target.is_symlink() or not target.is_file() or target.stat().st_size:
        raise ValueError('restore requires an owned empty regular target')
    task.check(1024**2);task.check_memory()
    db = sqlite3.connect(target)
    sha = hashlib.sha256()
    try:
        db.execute('PRAGMA temp_store=MEMORY');db.execute('PRAGMA cache_size=-8192')
        db.execute('PRAGMA hard_heap_limit=134217728')
        available = task.config['budget_bytes']-task.config['reserve_bytes']-task.used()
        db.execute('PRAGMA max_page_count='+str(max(1,available//4096)))
        records = iter(lines(path,task));line = next(records);sha.update(line)
        header = json.loads(line)
        if header.get('format')!=FORMAT:raise ValueError('unsupported row archive')
        schema = header['schema'];tables = {};names = set()
        def create_schema(sql):
            forbidden = {sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH,
                sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION,
                sqlite3.SQLITE_CREATE_VTABLE, sqlite3.SQLITE_CREATE_TRIGGER}
            db.set_authorizer(lambda action,*_:sqlite3.SQLITE_DENY
                if action in forbidden else sqlite3.SQLITE_OK)
            try:db.execute(sql)
            finally:db.set_authorizer(None)
        for kind,name,table,sql in schema:
            if not isinstance(name,str) or not isinstance(sql,str):raise ValueError('invalid archive schema')
            if kind not in ('table','index','view') or not re.match(
                    r'\ACREATE\s+(?:UNIQUE\s+)?'+kind+r'\s',sql,re.I):
                raise ValueError('unsupported archive schema statement')
            if name in names or name==METADATA_TABLE or table in EXCLUDED_TABLES:
                raise ValueError('invalid archive schema ownership')
            names.add(name)
            if kind=='table':
                create_schema(sql)
                actual=db.execute('SELECT type,tbl_name FROM sqlite_master WHERE name=?',(name,)).fetchone()
                if actual!=('table',table):raise ValueError('archive schema identity mismatch')
                tables[name] = len(list(db.execute('PRAGMA table_info('+quoted(name)+')')))
        counts = dict.fromkeys(tables,0);ended = False
        for line in records:
            sha.update(line);record = json.loads(line)
            if ended:raise ValueError('trailing archive records')
            if 'end' in record:
                if record['end']!=counts:raise ValueError('row archive count mismatch')
                ended=True;continue
            name = record['t'];values = record['v']
            if name not in tables or len(values)!=tables[name]:raise ValueError('invalid archive row')
            row=[]
            for tag,value in values:
                if tag=='b':value=base64.b64decode(value,validate=True)
                elif tag!='v' or not isinstance(value,(str,int,float,type(None))):
                    raise ValueError('invalid archive value')
                row.append(value)
            db.execute('INSERT INTO '+quoted(name)+' VALUES('+','.join('?' for _ in row)+')',row)
            counts[name]+=1
            if counts[name]%100==0:
                task.check(1024**2);task.check_memory();db.commit()
        if not ended or sha.hexdigest()!=expected:raise ValueError('incomplete or changed row archive')
        for kind,_,_,sql in schema:
            if kind!='table':
                task.check(1024**2);task.check_memory();create_schema(sql)
        metadata=dict(header['metadata'],copied_table_rows=counts)
        db.execute('CREATE TABLE '+METADATA_TABLE+'(payload TEXT NOT NULL)')
        db.execute('INSERT INTO '+METADATA_TABLE+' VALUES(?)',(canonical(metadata).decode(),));db.commit()
        if db.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise ValueError('restored archive integrity failed')
        configure_reader(db)
        return db
    except BaseException:
        db.close();raise
