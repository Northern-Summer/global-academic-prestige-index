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
from .queue_storage import COLUMNS, unpack_digest

MAGIC = b'GAPIz1\0'
MAX_BYTES = 16384
DICT_MAGIC = b'GAPIz2\0'
MAX_DICTIONARY_BYTES = 8192
DICTIONARY_DDL = '''CREATE TABLE IF NOT EXISTS event_dictionaries(
 digest BLOB PRIMARY KEY, compressed_bytes BLOB NOT NULL);'''


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
    Candidate query_sha256/response_sha256 columns use their own lossless codec.
    Other BLOB columns are not interpreted or modified.
    """
    if any(column[0] in COLUMNS and isinstance(value,bytes)
           for column,value in zip(cursor.description,row)):
        row=tuple(unpack_digest(value) if column[0] in COLUMNS and isinstance(value,bytes)
                  else value for column,value in zip(cursor.description,row))
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
    # No persistent cache: every read checks its content-addressed dependency,
    # including on restored snapshots and after a malicious database mutation.
    reader=db.cursor();reader.row_factory=None
    try:row=reader.execute('SELECT compressed_bytes FROM event_dictionaries WHERE digest=?',(key,)).fetchone()
    finally:reader.close()
    if not row:raise ValueError('event dictionary missing')
    try:
        decoder=zlib.decompressobj()
        raw=decoder.decompress(row[0],MAX_DICTIONARY_BYTES+1)
        if (not raw or len(raw)>MAX_DICTIONARY_BYTES or not decoder.eof
                or decoder.unused_data or decoder.unconsumed_tail):
            raise ValueError('invalid event dictionary framing')
        return raw
    except zlib.error as exc:raise ValueError('invalid compressed event dictionary') from exc


def decode_event(db, value):
    return unpack(value,dictionary_lookup=lambda key:load_dictionary(db,key))


def snapshot_payload(db, value):
    """Keep ranking snapshots self-contained in the existing GAPIz1 format."""
    if isinstance(value,bytes) and value.startswith(DICT_MAGIC):
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


def repack_event_pages(store, *, after_rowid=0, max_events=20000, max_seconds=60, share_responses=False, share_sources=False):
    """Rebalance bounded contiguous rows, retaining rowids and decoded facts.

    The caller owns CareerStore's writer lock. Every <=128-row batch is one
    rollback-protected transaction; the returned cursor advances only after
    commit. Default mode retains physical bytes. Optional response sharing
    changes only the payload codec, retaining every original decoded byte.
    """
    if type(after_rowid) is not int or after_rowid<0:
        raise ValueError('invalid event page cursor')
    if type(max_events) is not int or not 1<=max_events<=100000:
        raise ValueError('invalid event page limit')
    if not math.isfinite(max_seconds) or max_seconds<=0:
        raise ValueError('invalid event page deadline')
    if type(share_responses) is not bool:raise ValueError('invalid response sharing policy')
    if type(share_sources) is not bool:raise ValueError('invalid source sharing policy')
    if store.db.in_transaction:raise ValueError('cannot repack unfinished transaction')
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='events'").fetchone():
        raise ValueError('event page repack forbids triggers')
    from .ranking_snapshot import quoted
    for (name,) in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        if any(row[2]=='events' for row in store.db.execute('PRAGMA foreign_key_list('+quoted(name)+')')):
            raise ValueError('event page repack forbids incoming foreign keys')
    columns=[row[1] for row in store.db.execute('PRAGMA table_info(events)')]
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
        with store.member('storage-page-repack','events'):
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
