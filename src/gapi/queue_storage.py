"""Lossless physical encoding of candidate evidence digests.

Candidate identity, order and provenance remain the original text contract.
Only lowercase SHA-256 strings are packed; legacy arbitrary strings stay TEXT.
Owned readers decode the two named digest columns, leaving other BLOBs alone.
"""
import math
import re
import time

MAGIC = b'GAPIh1\0'
COLUMNS = frozenset({'query_sha256', 'response_sha256'})


def pack_digest(value):
    if isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value):
        return MAGIC + bytes.fromhex(value)
    return value


def unpack_digest(value):
    if isinstance(value, str):
        return value
    if not isinstance(value, bytes) or len(value) != len(MAGIC)+32 or not value.startswith(MAGIC):
        raise ValueError('invalid candidate digest storage')
    return value[len(MAGIC):].hex()


def pack_row(row):
    source, identifier, query, response = row
    return source, identifier, pack_digest(query), pack_digest(response)


def compact_queue(store, *, after_rowid=0, max_rows=100000, max_seconds=60):
    """Resume bounded transactions while retaining rowids and decoded bytes.

    Caller holds CareerStore's sole-writer lock. At most 128 short queue rows
    are reinserted per transaction; no schema change, VACUUM or database copy.
    An 8 MiB admission covers the small rollback journal and b-tree changes.
    Unusually large legacy rows are retained rather than widening that bound.
    """
    if type(after_rowid) is not int or after_rowid < 0:
        raise ValueError('invalid candidate compaction cursor')
    if type(max_rows) is not int or not 1 <= max_rows <= 1000000:
        raise ValueError('invalid candidate compaction limit')
    if not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('invalid candidate compaction deadline')
    if store.db.in_transaction:
        raise ValueError('cannot compact an unfinished transaction')
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='discovery'").fetchone():
        raise ValueError('candidate compaction forbids triggers')
    from .ranking_snapshot import quoted
    for (name,) in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        if any(row[2]=='discovery' for row in store.db.execute('PRAGMA foreign_key_list('+quoted(name)+')')):
            raise ValueError('candidate compaction forbids incoming foreign keys')
    def capacity():
        return (store.db.execute('PRAGMA page_count').fetchone()[0],
                store.db.execute('PRAGMA freelist_count').fetchone()[0])
    before=capacity();deadline=time.monotonic()+max_seconds
    scanned=changed=skipped=0;cursor=after_rowid
    while scanned < max_rows and time.monotonic() < deadline:
        store.task.check_memory()
        reader=store.db.cursor();reader.row_factory=None
        try:
            rows=reader.execute('SELECT rowid,* FROM discovery WHERE rowid>? ORDER BY rowid LIMIT ?',
                                 (cursor,min(128,max_rows-scanned))).fetchall()
        finally:reader.close()
        if not rows:break
        converted=[]
        for row in rows:
            if sum(len(v.encode('utf-8')) if isinstance(v,str) else 8 for v in row)>1024:
                skipped+=1
                continue
            packed=(row[0],*pack_row(row[1:]))
            if packed!=row:
                logical=(row[0],row[1],row[2],unpack_digest(row[3]),unpack_digest(row[4]))
                converted.append((logical,packed))
        if converted:
            store.task.check(8*1024**2)
            store.db.execute('BEGIN IMMEDIATE')
            try:
                # Remove the bounded contiguous group first, allowing whole
                # leaves to rebalance. Row-at-a-time replacement only creates
                # slack inside leaves, as the first live attempt demonstrated.
                store.db.executemany('DELETE FROM discovery WHERE rowid=?',[(old[0],) for old,_ in converted])
                store.db.executemany('INSERT INTO discovery(rowid,source,identifier,query_sha256,response_sha256) VALUES(?,?,?,?,?)',
                                     [packed for _,packed in converted])
                for old,packed in converted:
                    actual=store.db.execute('SELECT rowid,* FROM discovery WHERE rowid=?',(old[0],)).fetchone()
                    if actual!=old:raise ValueError('candidate compaction changed logical bytes')
                store.task.check_memory();store.task.check(1024**2)
                store.db.commit()
            except BaseException:
                store.db.rollback()
                raise
            changed+=len(converted)
        scanned+=len(rows);cursor=rows[-1][0]
    after=capacity();size=store.db.execute('PRAGMA page_size').fetchone()[0]
    return {'scanned_rows':scanned,'compacted_rows':changed,'skipped_large_rows':skipped,
            'after_rowid':cursor,'pages_before':before[0],'pages_after':after[0],
            'freelist_before':before[1],'freelist_after':after[1],
            'net_reusable_bytes':((before[0]-before[1])-(after[0]-after[1]))*size,
            'physical_file_bytes_reclaimed':0}
