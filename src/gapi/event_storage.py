"""Lossless event-payload storage, independent of scientific interpretation.

TEXT remains readable. GAPIz1 BLOBs decode to the exact original UTF-8 text at
owned SQLite reader boundaries, so event identities and release hashes survive.
No raw source bodies enter this format.
"""
import hashlib
import math
import time
import zlib

MAGIC = b'GAPIz1\0'
MAX_BYTES = 16384


def pack(text):
    raw = text.encode('utf-8')
    if len(raw) > MAX_BYTES:
        raise ValueError('normalized event exceeds 16 KiB')
    encoded = MAGIC + len(raw).to_bytes(4, 'big') + zlib.compress(raw, 6)
    return encoded if len(encoded) + 32 < len(raw) else text


def unpack(value):
    if isinstance(value, str):
        return value
    if not isinstance(value, bytes) or not value.startswith(MAGIC):
        raise ValueError('unsupported event payload storage format')
    header = len(MAGIC) + 4
    expected = int.from_bytes(value[len(MAGIC):header], 'big')
    if len(value) < header or not 0 < expected <= MAX_BYTES or len(value) > MAX_BYTES:
        raise ValueError('invalid compressed event size')
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(value[header:], MAX_BYTES + 1)
        if (len(raw) != expected or not decoder.eof or decoder.unused_data
                or decoder.unconsumed_tail):
            raise ValueError('invalid compressed event framing')
        return raw.decode('utf-8')
    except (zlib.error, UnicodeDecodeError) as exc:
        raise ValueError('invalid compressed event payload') from exc


def payload_row(cursor, row):
    """Preserve tuple API, exposing payload columns as their original TEXT.

    Aliasing a raw column as stored_payload explicitly inspects physical bytes.
    No other BLOB column is interpreted or modified.
    """
    if any(column[0] == 'payload' and isinstance(value, bytes)
           for column, value in zip(cursor.description, row)):
        return tuple(unpack(value) if column[0] == 'payload' and isinstance(value, bytes)
                     else value for column, value in zip(cursor.description, row))
    return row


def configure_reader(db):
    db.row_factory = payload_row
    db.create_function('gapi_event_text', 1, unpack, deterministic=True)


def logical_event_hash(db):
    from .task import canonical
    result = hashlib.sha256()
    for row in db.execute('SELECT * FROM events ORDER BY id'):
        result.update(canonical(row))
    return result.hexdigest()


def compact_events(store, *, max_events=5000, max_seconds=120):
    """Bounded idempotent migration; reuse freed pages without VACUUM or copies."""
    if type(max_events) is not int or not 1 <= max_events <= 1000000:
        raise ValueError('invalid compaction event limit')
    if not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('invalid compaction deadline')
    deadline = time.monotonic() + max_seconds
    after = ''
    scanned = changed = saved = 0
    while scanned < max_events and time.monotonic() < deadline:
        store.task.check_memory()
        rows = list(store.db.execute('''SELECT id,payload FROM events
            WHERE id>? AND typeof(payload)='text' ORDER BY id LIMIT ?''',
            (after, min(100, max_events-scanned))))
        if not rows:
            break
        with store.member('storage-compaction', 'GAPIz1'):
            for identifier, text in rows:
                encoded = pack(text)
                if isinstance(encoded, bytes):
                    if unpack(encoded) != text:
                        raise ValueError('lossless event compaction mismatch')
                    store.db.execute('UPDATE events SET payload=? WHERE id=?', (encoded, identifier))
                    changed += 1
                    saved += len(text.encode('utf-8')) - len(encoded)
        scanned += len(rows)
        after = rows[-1][0]
    return {'format': 'GAPIz1', 'scanned_events': scanned, 'compacted_events': changed,
            'payload_bytes_saved': saved, 'database_bytes': (store.task.root/'careers.sqlite').stat().st_size,
            'freelist_pages': store.db.execute('PRAGMA freelist_count').fetchone()[0],
            'note': 'payload savings and reusable pages are not filesystem bytes reclaimed'}
