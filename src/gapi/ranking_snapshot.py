"""Private logical snapshot for the current-events global ranking only.

The Easy ORCID projection is an independently frozen and scored dataset. Its two
large person tables and provisional dump staging are not ranking inputs. Candidate
selection hashes/counts are frozen via their authoritative progress owner instead
of copying the acquisition queue. Other evidence and optional input tables remain.
This is not a backup of the full acquisition database.
"""
import json
import sqlite3
from contextlib import closing

from .task import canonical, digest

POLICY = 'global-ranking-private-snapshot/1'
EXCLUDED_TABLES = frozenset({'easy_careers', 'easy_full_delta','dump_landing','discovery','event_dictionaries','easy_projection_blocks'})
METADATA_TABLE = 'gapi_snapshot_scope'


def candidate_progress(store, *, allowed_sources=None):
    """Return frozen selection statistics, or None for ordinary live/full inputs."""
    if not store.db.execute("SELECT 1 FROM sqlite_master WHERE name=?",(METADATA_TABLE,)).fetchone():
        return None
    row=store.db.execute('SELECT payload FROM '+METADATA_TABLE).fetchone()
    metadata=json.loads(row[0])
    if metadata.get('policy')!=POLICY:
        raise ValueError('unsupported ranking snapshot policy')
    progress=metadata['acquisition_progress']
    if allowed_sources is not None:
        progress['sources']=[r for r in progress['sources'] if r['source'] in allowed_sources]
    return progress


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def build(store, target):
    """Copy bounded batches under the caller's writer exclusion and read txn.

    No payload decoding or full-population materialization. Every batch is
    admitted against the complete task budget before writing, and destination
    page growth is capped independently. Failure leaves only caller-owned staging.
    """
    if store.db.in_transaction:
        raise ValueError('cannot freeze an unfinished input transaction')
    store.task.check(1024**2)
    store.task.check_memory()
    source = store.db.cursor()
    source.row_factory = None  # Preserve GAPIz1 bytes, not the decoded payload view.
    store.db.execute('BEGIN')
    try:
        schema = list(source.execute("""SELECT type,name,tbl_name,sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"""))
        if any(kind == 'trigger' for kind, *_ in schema):
            raise ValueError('ranking snapshot does not admit database triggers')
        if any(name == METADATA_TABLE for _, name, _, _ in schema):
            raise ValueError('cannot project an already scoped snapshot')
        tables = [(name, sql) for kind, name, _, sql in schema if kind == 'table']
        excluded = {name: source.execute('SELECT count(*) FROM '+quoted(name)).fetchone()[0]
                    for name, _ in tables if name in EXCLUDED_TABLES}
        progress=store.candidate_progress()
        with closing(sqlite3.connect(target)) as destination:
            destination.execute('PRAGMA page_size=4096')
            destination.execute('PRAGMA journal_mode=DELETE')
            destination.execute('PRAGMA synchronous=FULL')
            destination.execute('PRAGMA temp_store=MEMORY')
            destination.execute('PRAGMA cache_size=-8192')
            destination.execute('PRAGMA hard_heap_limit=134217728')
            available = (store.task.config['budget_bytes'] - store.task.used()
                         - store.task.config['reserve_bytes'])
            destination.execute('PRAGMA max_page_count='+str(max(1,available//4096)))
            for name, sql in tables:
                if name not in EXCLUDED_TABLES:
                    if not sql or sql.lstrip().upper().startswith('CREATE VIRTUAL'):
                        raise ValueError('unsupported ranking input table')
                    destination.execute(sql)
            # Create indexes before inserts: no population-sized external sort.
            for kind, _, table, sql in schema:
                if kind == 'index' and sql and table not in EXCLUDED_TABLES:
                    destination.execute(sql)
            destination.commit()
            counts = {}
            for name, sql in tables:
                if name in EXCLUDED_TABLES:
                    continue
                columns = list(source.execute('PRAGMA table_info('+quoted(name)+')'))
                primary = sorted((row[5], row[1]) for row in columns if row[5])
                ordering = ','.join(quoted(column) for _, column in primary) if primary else 'rowid'
                reader = store.db.cursor()
                reader.row_factory = None
                batch, batch_bytes, count = [], 0, 0
                insert = 'INSERT INTO '+quoted(name)+' VALUES('+','.join('?' for _ in columns)+')'

                def commit():
                    nonlocal batch_bytes
                    if not batch:
                        return
                    store.task.check(batch_bytes*4 + 1024**2)
                    store.task.check_memory()
                    with destination:
                        destination.executemany(insert,batch)
                    store.task.check()
                    batch.clear()
                    batch_bytes = 0

                try:
                    for row in reader.execute('SELECT * FROM '+quoted(name)+' ORDER BY '+ordering):
                        if name=='events':
                            from .event_storage import pack, snapshot_payload
                            # Same lossless storage owner as live ingestion. Logical
                            # event bytes/hashes are preserved; raw GAPIz1 stays raw.
                            payload_column=next(i for i,c in enumerate(columns) if c[1]=='payload')
                            if isinstance(row[payload_column],str):
                                row=list(row);row[payload_column]=pack(row[payload_column]);row=tuple(row)
                            else:
                                row=list(row);row[payload_column]=snapshot_payload(store.db,row[payload_column]);row=tuple(row)
                        size = sum(len(v.encode('utf-8')) if isinstance(v,str)
                                   else len(v) if isinstance(v,bytes) else 16 for v in row)
                        if size > store.task.config['max_member_bytes']:
                            raise ValueError('ranking snapshot input row exceeds bound')
                        if batch and batch_bytes+size > 2*1024**2:
                            commit()
                        batch.append(row)
                        batch_bytes += size
                        count += 1
                        if len(batch) == 100:
                            commit()
                    commit()
                    counts[name] = count
                finally:
                    reader.close()
            for kind, _, _, sql in schema:
                if kind == 'view':
                    destination.execute(sql)
            metadata = {'policy':POLICY,'scope':'global-ranking',
                        'excluded_tables':excluded,'copied_table_rows':counts,
                        'acquisition_progress':progress,
                        'config_sha256':digest(store.task.config),
                        'not_a_full_acquisition_backup':True}
            destination.execute('CREATE TABLE '+METADATA_TABLE+'(payload TEXT NOT NULL)')
            destination.execute('INSERT INTO '+METADATA_TABLE+' VALUES(?)',
                                (canonical(metadata).decode(),))
            destination.commit()
            store.task.check()
            if destination.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ValueError('private ranking snapshot integrity failed')
            return metadata
    finally:
        source.close()
        store.db.rollback()  # End only the read transaction created above.
