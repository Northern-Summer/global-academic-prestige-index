"""Reversible inline evidence-URL storage; no evidence or trust decisions.

No dictionary/dependency table is required by historical snapshot restoration.
Uncompressed TEXT remains supported. Only explicitly aliased raw readers see
the physical BLOB; ordinary `url` readers receive its exact original string.
"""
import math
import time
import zlib

MAGIC = b'GAPIu1'
HEADER = len(MAGIC) + 4
MAX_BYTES = 65536


def pack_url(value):
    if not isinstance(value, str):
        raise ValueError('evidence URL must be text')
    raw = value.encode('utf-8')
    # Existing large TEXT retains its old reader contract, without new limits.
    if not raw or len(raw) > MAX_BYTES:
        return value
    packed = MAGIC + len(raw).to_bytes(4, 'big') + zlib.compress(raw, 6)
    return packed if len(packed) + 32 < len(raw) else value


def unpack_url(value):
    if isinstance(value, str):
        return value
    if (not isinstance(value, bytes) or not value.startswith(MAGIC)
            or not HEADER < len(value) <= MAX_BYTES):
        raise ValueError('invalid evidence URL storage format')
    size = int.from_bytes(value[len(MAGIC):HEADER], 'big')
    if not 0 < size <= MAX_BYTES:
        raise ValueError('invalid evidence URL size')
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(value[HEADER:], MAX_BYTES + 1)
        if (len(raw) != size or not decoder.eof or decoder.unused_data
                or decoder.unconsumed_tail):
            raise ValueError('invalid evidence URL framing')
        return raw.decode('utf-8')
    except (zlib.error, UnicodeDecodeError) as exc:
        raise ValueError('invalid compressed evidence URL') from exc


def repack_urls(store, *, after=None, max_rows=10000, max_seconds=30):
    """Bounded primary-key traversal under CareerStore's sole-writer lock.

    Reinsert bounded batches with their original rowids so shorter values can
    release whole pages, rather than only leaving holes in existing pages.
    An interrupted batch rolls back; replay is idempotent and the returned key
    is an explicit checkpoint. Reusable pages are not file deletion.
    """
    if after is None:
        after = ('', '', '')
    if (not isinstance(after, (tuple, list)) or len(after) != 3
            or any(not isinstance(v, str) for v in after)
            or type(max_rows) is not int or not 1 <= max_rows <= 100000
            or not math.isfinite(max_seconds) or max_seconds <= 0):
        raise ValueError('invalid evidence migration bounds')
    if store.db.in_transaction:
        raise ValueError('cannot repack unfinished transaction')
    if store.db.execute("select 1 from sqlite_master where type='trigger' and tbl_name='member_parts'").fetchone():
        raise ValueError('evidence migration forbids triggers')
    columns = ('source', 'locator', 'part', 'url', 'sha256', 'bytes', 'retrieved_at')
    if tuple(row[1] for row in store.db.execute('pragma table_info(member_parts)')) != columns:
        raise ValueError('unsupported evidence table columns')
    from .ranking_snapshot import quoted
    for (table,) in store.db.execute("select name from sqlite_master where type='table'"):
        if any(row[2] == 'member_parts' for row in store.db.execute('pragma foreign_key_list(' + quoted(table) + ')')):
            raise ValueError('evidence migration forbids incoming foreign keys')
    def allocated_pages():
        return (store.db.execute('pragma page_count').fetchone()[0]
                - store.db.execute('pragma freelist_count').fetchone()[0])
    pages_before = allocated_pages()
    cursor = tuple(after)
    scanned = changed = saved = 0
    deadline = time.monotonic() + max_seconds
    while scanned < max_rows and time.monotonic() < deadline:
        rows = store.db.execute('''select rowid,source,locator,part,url as stored_url,sha256,bytes,retrieved_at
            from member_parts where (source,locator,part)>(?,?,?)
            order by source,locator,part limit ?''', (*cursor, min(128, max_rows - scanned))).fetchall()
        if not rows:
            break
        store.task.check_memory()
        converted = []
        for row in rows:
            original = unpack_url(row[4])
            candidate = pack_url(original)
            if isinstance(candidate, bytes) and isinstance(row[4], str):
                if unpack_url(candidate) != original:
                    raise ValueError('evidence URL reconstruction mismatch')
                converted.append((row, candidate))
        if converted:
            with store.member('storage-evidence-url-repack', 'member_parts', max_growth_bytes=2*1024**2):
                replacements = {row[0]: candidate for row, candidate in converted}
                store.db.executemany('delete from member_parts where rowid=?', [(row[0],) for row in rows])
                store.db.executemany('insert into member_parts(rowid,source,locator,part,url,sha256,bytes,retrieved_at) values(?,?,?,?,?,?,?,?)',
                    [(*row[:4], replacements.get(row[0], row[4]), *row[5:]) for row in rows])
                for row in rows:
                    actual = store.db.execute('''select rowid,source,locator,part,url,sha256,bytes,retrieved_at
                        from member_parts where rowid=?''', (row[0],)).fetchone()
                    expected = (*row[:4], unpack_url(row[4]), *row[5:])
                    if tuple(actual) != expected:
                        raise ValueError('evidence replacement changed row identity')
                store.task.check_memory()
            changed += len(converted)
            saved += sum(len(row[4].encode('utf-8')) - len(packed) for row, packed in converted)
        scanned += len(rows)
        cursor = tuple(rows[-1][1:4])
    return {'after': list(cursor), 'scanned_rows': scanned, 'repacked_rows': changed,
            'payload_saving_bytes': saved, 'physical_file_bytes_reclaimed': 0,
            'net_reusable_bytes': (pages_before - allocated_pages()) * store.db.execute('pragma page_size').fetchone()[0],
            'logical_evidence_unchanged': True}
