"""Lossless event-payload storage, independent of scientific interpretation.

TEXT remains readable. GAPIz1 and shared-dictionary GAPIz2 BLOBs decode to the
exact original UTF-8 text at owned SQLite reader boundaries, so event identities
and logical release inputs survive.
No raw source bodies enter this format.
"""
import hashlib
import json
import math
import time
import zlib
from functools import lru_cache
from .queue_storage import COLUMNS, unpack_digest
from .evidence_storage import unpack_url

MAGIC = b'GAPIz1\0'
MAX_BYTES = 16384
DICT_MAGIC = b'GAPIz2\0'
MAX_DICTIONARY_BYTES = 8192
DICTIONARY_DDL = '''CREATE TABLE IF NOT EXISTS event_dictionaries(
 digest BLOB PRIMARY KEY, compressed_bytes BLOB NOT NULL);'''
MAX_BLOCK_EVENTS = 128
MAX_BLOCK_BYTES = MAX_BLOCK_EVENTS * (MAX_BYTES + 4)
BLOCK_MAGIC = b'GAPIb1\0'
BLOCK_REFERENCE_BYTES = len(BLOCK_MAGIC) + 32 + 2
BLOCK_DDL = '''CREATE TABLE IF NOT EXISTS event_blocks(
 digest BLOB PRIMARY KEY, packed BLOB NOT NULL);'''


def encode_event_block(texts):
    """Encode a bounded group of normalized events, retaining every UTF-8 byte.

    This codec alone does not enable a live database format or migration.
    Blocks have no external dictionary dependency; their SHA-256 binds framing
    and order as well as contents. Iterables are consumed with a hard count cap.
    """
    frames=[]
    for text in texts:
        if len(frames)>=MAX_BLOCK_EVENTS:
            raise ValueError('event block exceeds count limit')
        if not isinstance(text,str):raise ValueError('event block requires text')
        if not 0<len(text)<=MAX_BYTES:raise ValueError('invalid event block item size')
        raw=text.encode('utf-8')
        if not 0<len(raw)<=MAX_BYTES:raise ValueError('invalid event block item size')
        frames.append(len(raw).to_bytes(4,'big')+raw)
    if not frames:raise ValueError('event block is empty')
    raw=b''.join(frames)
    return hashlib.sha256(raw).digest(),zlib.compress(raw,6)


def decode_event_block(key, body):
    """Reject oversized, corrupt, trailing or invalid UTF-8 block data.

    No persistent cache: callers that cache must bind it to an immutable input
    or a transaction, and must not hide subsequent database mutations.
    """
    if not isinstance(key,bytes) or len(key)!=32:
        raise ValueError('invalid event block digest')
    # zlib overhead for this bounded payload is well under 64 KiB, including
    # incompressible legitimate inputs. Reject huge compressed input up front.
    if not isinstance(body,bytes) or not 0<len(body)<=MAX_BLOCK_BYTES+65536:
        raise ValueError('invalid compressed event block size')
    try:
        decoder=zlib.decompressobj()
        raw=decoder.decompress(body,MAX_BLOCK_BYTES+1)
        if (len(raw)>MAX_BLOCK_BYTES or not decoder.eof or decoder.unused_data
                or decoder.unconsumed_tail or hashlib.sha256(raw).digest()!=key):
            raise ValueError('event block integrity mismatch')
        cursor=0;parts=[]
        while cursor<len(raw):
            if cursor+4>len(raw):raise ValueError('truncated event block frame')
            length=int.from_bytes(raw[cursor:cursor+4],'big');cursor+=4
            if not 0<length<=MAX_BYTES or cursor+length>len(raw):
                raise ValueError('invalid event block frame size')
            parts.append(raw[cursor:cursor+length].decode('utf-8'));cursor+=length
            if len(parts)>MAX_BLOCK_EVENTS:raise ValueError('event block exceeds count limit')
        if not parts:raise ValueError('event block is empty')
        return tuple(parts)
    except (zlib.error,UnicodeDecodeError) as exc:
        raise ValueError('invalid compressed event block') from exc


def pack(text, *, dictionary=None):
    raw = text.encode('utf-8')
    if len(raw) > MAX_BYTES:
        raise ValueError('normalized event exceeds 16 KiB')
    if dictionary is None:
        encoded = MAGIC + len(raw).to_bytes(4, 'big') + zlib.compress(raw, 6)
    else:
        if not isinstance(dictionary,bytes) or not 0<len(dictionary)<=MAX_DICTIONARY_BYTES:
            raise ValueError('invalid event dictionary size')
        encoder=zlib.compressobj(6,zdict=dictionary)
        encoded=(DICT_MAGIC+len(raw).to_bytes(4,'big')+hashlib.sha256(dictionary).digest()
                 +encoder.compress(raw)+encoder.flush())
    return encoded if len(encoded) + 32 < len(raw) else text


def unpack(value, *, dictionary_lookup=None):
    if isinstance(value, str):
        return value
    if not isinstance(value, bytes) or not value.startswith((MAGIC,DICT_MAGIC)):
        raise ValueError('unsupported event payload storage format')
    header = len(MAGIC) + 4
    expected = int.from_bytes(value[len(MAGIC):header], 'big')
    if len(value) < header or not 0 < expected <= MAX_BYTES or len(value) > MAX_BYTES:
        raise ValueError('invalid compressed event size')
    try:
        if value.startswith(DICT_MAGIC):
            if dictionary_lookup is None or len(value)<header+32:
                raise ValueError('event dictionary lookup required')
            key=value[header:header+32];header+=32
            dictionary=dictionary_lookup(key)
            if (not isinstance(dictionary,bytes) or not 0<len(dictionary)<=MAX_DICTIONARY_BYTES
                    or hashlib.sha256(dictionary).digest()!=key):
                raise ValueError('event dictionary integrity mismatch')
            decoder=zlib.decompressobj(zdict=dictionary)
        else:decoder = zlib.decompressobj()
        raw = decoder.decompress(value[header:], MAX_BYTES + 1)
        if (len(raw) != expected or not decoder.eof or decoder.unused_data
                or decoder.unconsumed_tail):
            raise ValueError('invalid compressed event framing')
        return raw.decode('utf-8')
    except (zlib.error, UnicodeDecodeError) as exc:
        raise ValueError('invalid compressed event payload') from exc


def payload_row(cursor, row, *, decode=unpack):
    """Preserve tuple API, exposing owned encoded columns as original TEXT.

    Aliasing a raw column as stored_payload explicitly inspects physical bytes.
    Named evidence digest columns and evidence `url` columns
    use their own lossless codecs. Other BLOB columns are not interpreted.
    """
    if any(column[0] in COLUMNS and isinstance(value,bytes)
           for column,value in zip(cursor.description,row)):
        row=tuple(unpack_digest(value) if column[0] in COLUMNS and isinstance(value,bytes)
                  else value for column,value in zip(cursor.description,row))
    if any(column[0] == 'url' and isinstance(value, bytes)
           for column, value in zip(cursor.description, row)):
        row=tuple(unpack_url(value) if column[0] == 'url' and isinstance(value, bytes)
                  else value for column, value in zip(cursor.description, row))
    if any(column[0] == 'payload' and isinstance(value, bytes)
           for column, value in zip(cursor.description, row)):
        return tuple(decode(value) if column[0] == 'payload' and isinstance(value, bytes)
                     else value for column, value in zip(cursor.description, row))
    return row


def configure_reader(db):
    def decode(value):return decode_event(db,value)
    def row_factory(cursor,row):return payload_row(cursor,row,decode=decode)
    db.row_factory = row_factory
    db.create_function('gapi_event_text', 1, decode, deterministic=True)


def load_dictionary(db, key):
    # Read actual bytes before consulting the bounded decompression cache.
    # Snapshot replacement, deletion and database mutation remain observable.
    reader=db.cursor();reader.row_factory=None
    try:row=reader.execute('SELECT compressed_bytes FROM event_dictionaries WHERE digest=?',(key,)).fetchone()
    finally:reader.close()
    if not row:raise ValueError('event dictionary missing')
    body=row[0]
    if not isinstance(body,bytes) or not 0<len(body)<=MAX_DICTIONARY_BYTES+64:
        raise ValueError('invalid compressed event dictionary size')
    return _decoded_stored_dictionary(body)


@lru_cache(maxsize=8)
def _decoded_stored_dictionary(body):
    # Cache immutable compressed bytes, never a database handle or digest alone.
    # At most eight bounded inputs and eight <=8 KiB outputs are retained.
    # unpack() still verifies the decoded dictionary against the requested key.
    try:
        decoder=zlib.decompressobj()
        raw=decoder.decompress(body,MAX_DICTIONARY_BYTES+1)
        if (not raw or len(raw)>MAX_DICTIONARY_BYTES or not decoder.eof
                or decoder.unused_data or decoder.unconsumed_tail):
            raise ValueError('invalid event dictionary framing')
        return raw
    except zlib.error as exc:raise ValueError('invalid compressed event dictionary') from exc


def decode_event(db, value):
    if isinstance(value, bytes) and value.startswith(BLOCK_MAGIC):
        if len(value) != BLOCK_REFERENCE_BYTES:
            raise ValueError('invalid event block reference')
        key = value[len(BLOCK_MAGIC):len(BLOCK_MAGIC)+32]
        slot = int.from_bytes(value[-2:], 'big')
        reader = db.cursor(); reader.row_factory = None
        try:
            row = reader.execute('SELECT packed FROM event_blocks WHERE digest=?', (key,)).fetchone()
        finally:
            reader.close()
        if row is None:
            raise ValueError('event block missing')
        # Always read actual bytes before consulting the one-block cache.
        # A changed or missing dependency must not reuse validated old content.
        values = _decoded_stored_block(key, row[0])
        if slot >= len(values):
            raise ValueError('event block slot out of range')
        return values[slot]
    return unpack(value,dictionary_lookup=lambda key:load_dictionary(db,key))


@lru_cache(maxsize=1)
def _decoded_stored_block(key, body):
    # Retain one block only. Its framed UTF-8 input is bounded by MAX_BLOCK_BYTES;
    # Python strings and tuple overhead add to that byte bound.
    return decode_event_block(key, body)


def snapshot_payload(db, value):
    """Keep ranking snapshots self-contained in the existing GAPIz1 format."""
    if isinstance(value,bytes) and value.startswith((DICT_MAGIC, BLOCK_MAGIC)):
        return pack(decode_event(db,value))
    return value


def response_dictionary(record):
    url=record.get('response_url')
    if record.get('transport')!='wikibase-claims-info-batch/v1' or not isinstance(url,str):return None
    raw=url.encode('utf-8')
    return raw if 0<len(raw)<=MAX_DICTIONARY_BYTES else None


def source_dictionary(store, text, record):
    """A bounded codec seed, never an evidence or scientific decision.

    Seeds are private normalized event text, not raw source material. Physical
    encodings may vary by ingest order; snapshot_payload owns canonical replay.
    """
    seeds=getattr(store,'_event_codec_seeds',None)
    if seeds is None:seeds={};store._event_codec_seeds=seeds
    source=record['source']
    if source not in seeds:
        if len(seeds)>=16:return None
        seeds[source]=text.encode('utf-8')[:MAX_DICTIONARY_BYTES]
    return seeds[source]


def pack_event(db, text, record, *, source_seed=None):
    """Share exact batch URL bytes; do not remove any event fact or provenance."""
    baseline=pack(text);dictionary=None
    for seed in (response_dictionary(record),source_seed):
        if seed is None:continue
        candidate=pack(text,dictionary=seed)
        baseline_size=len(baseline.encode('utf-8')) if isinstance(baseline,str) else len(baseline)
        if isinstance(candidate,bytes) and len(candidate)+64<baseline_size:
            baseline=candidate;dictionary=seed
    if dictionary is None:return baseline
    candidate=baseline
    if not db.in_transaction:raise ValueError('dictionary and event require one transaction')
    key=hashlib.sha256(dictionary).digest()
    db.execute('INSERT OR IGNORE INTO event_dictionaries VALUES(?,?)',(key,zlib.compress(dictionary,6)))
    # An existing conflicting dictionary must never be silently accepted.
    if load_dictionary(db,key)!=dictionary:raise ValueError('event dictionary conflict')
    if decode_event(db,candidate)!=text:raise ValueError('event dictionary roundtrip mismatch')
    return candidate


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


def _validate_event_repack(store, after_rowid, max_events, max_seconds):
    if type(after_rowid) is not int or after_rowid<0:
        raise ValueError('invalid event page cursor')
    if type(max_events) is not int or not 1<=max_events<=100000:
        raise ValueError('invalid event page limit')
    if not math.isfinite(max_seconds) or max_seconds<=0:
        raise ValueError('invalid event page deadline')
    if store.db.in_transaction:raise ValueError('cannot repack unfinished transaction')
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='events'").fetchone():
        raise ValueError('event page repack forbids triggers')
    from .ranking_snapshot import quoted
    for (name,) in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        if any(row[2]=='events' for row in store.db.execute('PRAGMA foreign_key_list('+quoted(name)+')')):
            raise ValueError('event page repack forbids incoming foreign keys')
    columns=[row[1] for row in store.db.execute('PRAGMA table_info(events)')]
    return columns


def repack_event_pages(store, *, after_rowid=0, max_events=20000, max_seconds=60, share_responses=False, share_sources=False):
    """Rebalance bounded contiguous rows, retaining rowids and decoded facts.

    The caller owns CareerStore's writer lock. Every <=128-row batch is one
    rollback-protected transaction; the returned cursor advances only after
    commit. Default mode retains physical bytes. Optional response sharing
    changes only the payload codec, retaining every original decoded byte.
    """
    if type(share_responses) is not bool:raise ValueError('invalid response sharing policy')
    if type(share_sources) is not bool:raise ValueError('invalid source sharing policy')
    columns = _validate_event_repack(store, after_rowid, max_events, max_seconds)
    from .ranking_snapshot import quoted
    insert='INSERT INTO events(rowid,'+','.join(map(quoted,columns))+') VALUES('+','.join('?' for _ in range(len(columns)+1))+')'
    def pages():
        return (store.db.execute('PRAGMA page_count').fetchone()[0],
                store.db.execute('PRAGMA freelist_count').fetchone()[0])
    payload_index=columns.index('payload')+1
    before=pages();deadline=time.monotonic()+max_seconds;scanned=changed=saved=0;cursor=after_rowid
    while scanned<max_events and time.monotonic()<deadline:
        store.task.check_memory()
        reader=store.db.cursor();reader.row_factory=None
        try:rows=reader.execute('SELECT rowid,* FROM events WHERE rowid>? ORDER BY rowid LIMIT ?',
                              (cursor,min(128,max_events-scanned))).fetchall()
        finally:reader.close()
        if not rows:break
        # At most 128 normalized <=16 KiB events; 8 MiB allows payload,
        # scalar columns, dictionaries and index churn. SQLite enforces the
        # physical-growth ceiling even if this estimate is insufficient.
        with store.member('storage-page-repack','events',max_growth_bytes=8*1024**2):
            expected=[];converted=0;batch_saved=0
            for row in rows:
                value=row[payload_index];replacement=value
                if share_sources or (share_responses and not (isinstance(value,bytes) and value.startswith(DICT_MAGIC))):
                    text=decode_event(store.db,value)
                    record=json.loads(text)
                    candidate=pack_event(store.db,text,record,source_seed=source_dictionary(store,text,record) if share_sources else None)
                    old_size=len(value.encode('utf-8')) if isinstance(value,str) else len(value)
                    if (isinstance(candidate,bytes) and candidate.startswith(DICT_MAGIC)
                            and (not share_sources or len(candidate)+64<old_size)):
                        replacement=candidate;converted+=1
                        batch_saved+=(len(value.encode('utf-8')) if isinstance(value,str) else len(value))-len(candidate)
                        if decode_event(store.db,candidate)!=text:raise ValueError('shared response changed event bytes')
                expected.append((*row[:payload_index],replacement,*row[payload_index+1:]))
            if not (share_responses or share_sources) or converted:
                store.db.executemany('DELETE FROM events WHERE rowid=?',[(row[0],) for row in rows])
                store.db.executemany(insert,expected)
            store.task.check_memory()
            reader=store.db.cursor();reader.row_factory=None
            try:actual=reader.execute('SELECT rowid,* FROM events WHERE rowid>=? AND rowid<=? ORDER BY rowid',
                                     (rows[0][0],rows[-1][0])).fetchall()
            finally:reader.close()
            if actual!=expected:raise ValueError('event page repack changed stored rows')
        changed+=converted;saved+=batch_saved
        scanned+=len(rows);cursor=rows[-1][0]
    after=pages();size=store.db.execute('PRAGMA page_size').fetchone()[0]
    return {'scanned_events':scanned,'after_rowid':cursor,
            'shared_response_events':changed,'event_payload_bytes_saved':saved,
            'allocated_pages_before':before[0],'allocated_pages_after':after[0],
            'freelist_pages_before':before[1],'freelist_pages_after':after[1],
            'net_reusable_bytes':((before[0]-before[1])-(after[0]-after[1]))*size,
            'physical_file_bytes_reclaimed':0,'stored_rows_unchanged':changed==0,
            'logical_event_bytes_unchanged':True,
            'note':'internal reusable capacity only; resume with committed after_rowid'}


def repack_event_blocks(store, *, after_rowid=0, max_events=20000, max_seconds=60):
    """Opt-in, bounded replacement under CareerStore's existing writer lock.

    Commit the block and every reference atomically. Existing references are
    immutable and skipped, making retries idempotent. No source fact, scalar,
    event identity, rowid or index is removed. Snapshots remain self-contained.
    The returned cursor covers committed batches only; no live collector calls
    this maintenance function automatically.
    """
    columns = _validate_event_repack(store, after_rowid, max_events, max_seconds)
    from .ranking_snapshot import quoted
    payload_index = columns.index('payload')+1
    insert = ('INSERT INTO events(rowid,'+','.join(map(quoted, columns))+') VALUES('
              +','.join('?' for _ in range(len(columns)+1))+')')
    cursor = after_rowid; scanned = changed = blocks = saved = 0
    deadline = time.monotonic()+max_seconds
    while scanned < max_events and time.monotonic() < deadline:
        store.task.check_memory()
        reader = store.db.cursor(); reader.row_factory = None
        try:
            rows = reader.execute('SELECT rowid,* FROM events WHERE rowid>? ORDER BY rowid LIMIT ?',
                (cursor, min(MAX_BLOCK_EVENTS, max_events-scanned))).fetchall()
        finally:
            reader.close()
        if not rows:
            break
        pending = [row for row in rows if not (
            isinstance(row[payload_index], bytes) and row[payload_index].startswith(BLOCK_MAGIC))]
        if pending:
            texts = [decode_event(store.db, row[payload_index]) for row in pending]
            key, body = encode_event_block(texts)
            old_bytes = sum(len(value.encode('utf-8')) if isinstance(value, str) else len(value)
                            for value in (row[payload_index] for row in pending))
            benefit = old_bytes-len(body)-BLOCK_REFERENCE_BYTES*len(pending)-128
            if benefit > 0:
                with store.member('storage-page-repack', 'event-blocks', max_growth_bytes=8*1024**2):
                    store.db.execute(BLOCK_DDL)
                    store.db.execute('INSERT OR IGNORE INTO event_blocks VALUES(?,?)', (key, body))
                    replacements = []
                    for slot, (row, text) in enumerate(zip(pending, texts)):
                        ref = BLOCK_MAGIC+key+slot.to_bytes(2, 'big')
                        if decode_event(store.db, ref) != text:
                            raise ValueError('stored event block conflicts with predecessor')
                        replacements.append((*row[:payload_index], ref, *row[payload_index+1:]))
                    store.db.executemany('DELETE FROM events WHERE rowid=?', [(row[0],) for row in pending])
                    store.db.executemany(insert, replacements)
                    reader = store.db.cursor(); reader.row_factory = None
                    try:
                        actual = reader.execute('SELECT rowid,* FROM events WHERE rowid>=? AND rowid<=? ORDER BY rowid',
                            (rows[0][0], rows[-1][0])).fetchall()
                    finally:
                        reader.close()
                    expected = {row[0]: row for row in replacements}
                    if actual != [expected.get(row[0], row) for row in rows]:
                        raise ValueError('event block replacement changed stored rows')
                    store.task.check_memory()
                changed += len(pending); blocks += 1; saved += benefit
        scanned += len(rows); cursor = rows[-1][0]
    return {'scanned_events': scanned, 'after_rowid': cursor, 'repacked_events': changed,
            'new_blocks': blocks, 'estimated_payload_bytes_saved': saved,
            'physical_file_bytes_reclaimed': 0, 'logical_event_bytes_unchanged': True,
            'note': 'internal reusable pages only; no global capacity claim'}
