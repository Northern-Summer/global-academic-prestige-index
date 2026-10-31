"""Lossless bounded blocks for private derivative payloads.

The row number, person key and source-line hash stay in their existing table.
Only the physical payload changes. Readers recover the exact legacy compressed
BLOB, so replay, HQ reuse and scientific interpretation retain one contract.
Blocks and references are committed together under CareerStore's writer lock.
"""
import hashlib
import math
import struct
import time
import zlib
from collections import OrderedDict, namedtuple
from functools import wraps
import sys
from threading import RLock

LEGACY_MAGIC = b'GAPIp1\0'
MAGIC = b'GAPIp2\0'
DICTIONARY_MAGIC = b'GAPIp3\0'
DICTIONARY_BYTES = 8192
DICTIONARY_HEADER = len(DICTIONARY_MAGIC)+32+4
TABLE = 'easy_projection_blocks'
DDL = f'CREATE TABLE IF NOT EXISTS {TABLE}(digest BLOB PRIMARY KEY, packed BLOB NOT NULL)'
MAX_ROWS = 128
MAX_ROW = 1024**2
MAX_BLOCK = 2*1024**2
REFERENCE_SIZE = len(MAGIC)+32+2
CACHE_BYTES = 8*1024**2
CACHE_ENTRIES = 128


def _bounded_block_cache(decode):
    """Retain useful random-read locality without caching unbounded blocks.

    Account the encoded key and decoded values, including Python containers.
    Keys include actual database bytes, preserving mutation detection.
    """
    entries=OrderedDict();used=hits=misses=0;lock=RLock()
    Info=namedtuple('CacheInfo','hits misses maxsize currsize bytes maxbytes')
    @wraps(decode)
    def cached(key,packed,legacy=False):
        nonlocal used,hits,misses
        identity=(key,packed,legacy)
        with lock:
            if identity in entries:
                hits+=1;entries.move_to_end(identity)
                return entries[identity][0]
            misses+=1
            values=decode(key,packed,legacy)
            size=(sys.getsizeof(identity)+sum(sys.getsizeof(v) for v in identity)
                  +sys.getsizeof(values)+sum(sys.getsizeof(v) for v in values)+256)
            if size<=CACHE_BYTES:
                while entries and (used+size>CACHE_BYTES or len(entries)>=CACHE_ENTRIES):
                    _,(_,old_size)=entries.popitem(last=False);used-=old_size
                entries[identity]=(values,size);used+=size
            return values
    def clear():
        nonlocal used,hits,misses
        with lock:entries.clear();used=hits=misses=0
    def info():
        with lock:return Info(hits,misses,CACHE_ENTRIES,len(entries),used,CACHE_BYTES)
    cached.cache_clear=clear;cached.cache_info=info
    return cached


def inflate(value, limit):
    if not isinstance(value,bytes) or len(value)>limit+65536:
        raise ValueError('projection compressed size exceeds bound')
    decoder=zlib.decompressobj()
    try:
        raw=decoder.decompress(value,limit+1)
    except zlib.error as exc:
        raise ValueError('invalid projection compression') from exc
    if len(raw)>limit or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError('invalid projection compressed framing')
    return raw


def framed(values):
    return b''.join(struct.pack('>I',len(value))+value for value in values)


def make_block(values):
    if not 1<=len(values)<=MAX_ROWS:
        raise ValueError('projection block row count exceeds bound')
    plain=[];total=0
    for value in values:
        raw=inflate(value,MAX_ROW)
        # Recompression is verified, never assumed. A different historic codec
        # must retain its predecessor rather than silently change byte identity.
        if zlib.compress(raw,6)!=value:
            raise ValueError('legacy projection codec is not exactly reversible')
        total+=4+len(raw)
        if total>MAX_BLOCK:raise ValueError('projection decoded block exceeds bound')
        plain.append(raw)
    block=framed(plain)
    key=hashlib.sha256(block).digest()
    packed=zlib.compress(block,6)
    restored=tuple(zlib.compress(v,6) for v in decode_plain_block(key,packed))
    if tuple(values)!=restored:raise ValueError('projection block roundtrip mismatch')
    return key,packed,[MAGIC+key+i.to_bytes(2,'big') for i in range(len(values))]


@_bounded_block_cache
def decode_plain_block(key,packed,legacy=False):
    # Cache includes the actual bytes just read from SQLite, not only the key.
    # Changing/deleting a block can never reuse a previously validated value.
    raw=inflate(packed,MAX_BLOCK);position=0;values=[]
    while position<len(raw):
        if position+4>len(raw):raise ValueError('truncated projection block row')
        size=struct.unpack_from('>I',raw,position)[0];position+=4
        if size>MAX_ROW or position+size>len(raw):
            raise ValueError('invalid projection block row bound')
        values.append(raw[position:position+size]);position+=size
        if len(values)>MAX_ROWS:raise ValueError('projection block row count exceeds bound')
    identity=framed([zlib.compress(v,6) for v in values]) if legacy else raw
    if not values or hashlib.sha256(identity).digest()!=key:
        raise ValueError('projection block identity mismatch')
    return tuple(values)


def decode_block(key,packed):
    """Exact legacy BLOBs for verification tools, not the scoring hot path."""
    return tuple(zlib.compress(v,6) for v in decode_plain_block(key,packed))


def referenced_plain(db,value):
    if not isinstance(value,bytes):raise ValueError('projection payload must be bytes')
    if not value.startswith((MAGIC,LEGACY_MAGIC)):raise ValueError('projection block reference required')
    if len(value)!=REFERENCE_SIZE:raise ValueError('invalid projection reference framing')
    key=value[len(MAGIC):len(MAGIC)+32];slot=int.from_bytes(value[-2:],'big')
    cursor=db.cursor();cursor.row_factory=None
    try:row=cursor.execute(f'SELECT packed FROM {TABLE} WHERE digest=?',(key,)).fetchone()
    finally:cursor.close()
    if row is None:raise ValueError('projection block missing')
    values=decode_plain_block(key,row[0],value.startswith(LEGACY_MAGIC))
    if slot>=len(values):raise ValueError('projection block slot out of range')
    return values[slot]


def original_payload(db,value):
    if not isinstance(value,bytes):raise ValueError('projection payload must be bytes')
    if value.startswith(DICTIONARY_MAGIC):return zlib.compress(dictionary_plain(db,value),6)
    if not value.startswith((MAGIC,LEGACY_MAGIC)):return value
    return zlib.compress(referenced_plain(db,value),6)


def plain_payload(db,value):
    # Preserve the pre-existing legacy reader contract; the stricter bound is
    # for newly admitted blocks, not a reclassification of retained records.
    if value.startswith((MAGIC,LEGACY_MAGIC)):return referenced_plain(db,value)
    if value.startswith(DICTIONARY_MAGIC):return dictionary_plain(db,value)
    return zlib.decompress(value)


@_bounded_block_cache
def decode_dictionary(key,packed,legacy=False):
    raw=inflate(packed,DICTIONARY_BYTES)
    if not raw or hashlib.sha256(raw).digest()!=key:
        raise ValueError('projection dictionary identity mismatch')
    return (raw,)


def dictionary_plain(db,value):
    header=DICTIONARY_HEADER
    if not isinstance(value,bytes) or not value.startswith(DICTIONARY_MAGIC) or not header<len(value)<=MAX_ROW+header+65536:
        raise ValueError('invalid projection dictionary framing')
    key=value[len(DICTIONARY_MAGIC):len(DICTIONARY_MAGIC)+32]
    size=int.from_bytes(value[header-4:header],'big')
    if not 0<size<=MAX_ROW:raise ValueError('projection dictionary row bound')
    reader=db.cursor();reader.row_factory=None
    try:row=reader.execute(f'SELECT packed FROM {TABLE} WHERE digest=?',(key,)).fetchone()
    finally:reader.close()
    if row is None:raise ValueError('projection dictionary missing')
    dictionary=decode_dictionary(key,row[0])[0]
    decoder=zlib.decompressobj(zdict=dictionary)
    try:raw=decoder.decompress(value[header:],MAX_ROW+1)
    except zlib.error as exc:raise ValueError('invalid dictionary projection compression') from exc
    if len(raw)!=size or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError('invalid projection dictionary row framing')
    return raw


def dictionary_payload(original,dictionary):
    """One independently decoded row; retain legacy bytes when not beneficial."""
    if not isinstance(dictionary,bytes) or not 0<len(dictionary)<=DICTIONARY_BYTES:
        raise ValueError('invalid projection dictionary size')
    raw=inflate(original,MAX_ROW)
    if zlib.compress(raw,6)!=original:
        raise ValueError('legacy projection codec is not exactly reversible')
    encoder=zlib.compressobj(6,zdict=dictionary)
    packed=(DICTIONARY_MAGIC+hashlib.sha256(dictionary).digest()+len(raw).to_bytes(4,'big')
            +encoder.compress(raw)+encoder.flush())
    return packed if len(packed)+16<len(original) else original


def _validate_repack(store,table,after_row,max_rows,max_seconds):
    if table not in ('easy_careers','easy_full_delta'):raise ValueError('unsupported projection table')
    if type(after_row) is not int or after_row<0:raise ValueError('invalid projection cursor')
    if type(max_rows) is not int or not 1<=max_rows<=100000:
        raise ValueError('invalid projection row limit')
    if not math.isfinite(max_seconds) or max_seconds<=0:raise ValueError('invalid projection deadline')
    if store.db.in_transaction:raise ValueError('cannot repack unfinished transaction')
    from .ranking_snapshot import quoted
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(table,)).fetchone():
        raise ValueError('projection repack forbids triggers')
    for (name,) in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        if any(r[2]==table for r in store.db.execute('PRAGMA foreign_key_list('+quoted(name)+')')):
            raise ValueError('projection repack forbids incoming foreign keys')


def projection_dictionary(db,table='easy_careers',*,max_bytes=4096):
    """Stable, bounded seed from original row content, independent of encoding."""
    if table not in ('easy_careers','easy_full_delta'):raise ValueError('unsupported projection table')
    if type(max_bytes) is not int or not 0<max_bytes<=DICTIONARY_BYTES:
        raise ValueError('invalid projection dictionary size')
    dictionary=b''
    reader=db.cursor();reader.row_factory=None
    try:
        for (value,) in reader.execute(f'SELECT payload FROM {table} ORDER BY row_number LIMIT 128'):
            dictionary=(dictionary+plain_payload(db,value))[-max_bytes:]
    finally:reader.close()
    if not dictionary:raise ValueError('no projection rows for dictionary')
    return dictionary


def repack_dictionary(store,dictionary=None,*,table='easy_careers',after_row=0,max_rows=10000,max_seconds=60):
    """Bounded, resumable row replacement using the existing block-object owner.

    Does not migrate old block references, drop indexes or copy the database.
    Exact legacy BLOBs are verified inside each <=128-row commit. Freed pages
    are reusable by ingestion; the physical database file does not shrink.
    """
    _validate_repack(store,table,after_row,max_rows,max_seconds)
    if dictionary is None:dictionary=projection_dictionary(store.db,table)
    if not isinstance(dictionary,bytes) or not 0<len(dictionary)<=DICTIONARY_BYTES:
        raise ValueError('invalid projection dictionary size')
    key=hashlib.sha256(dictionary).digest();packed_dictionary=zlib.compress(dictionary,6)
    before=store.db.execute('PRAGMA freelist_count').fetchone()[0]
    pages_before=store.db.execute('PRAGMA page_count').fetchone()[0]
    cursor=after_row;scanned=changed=saved=0;deadline=time.monotonic()+max_seconds
    while scanned<max_rows and time.monotonic()<deadline:
        store.task.check_memory()
        rows=store.db.execute(f'SELECT row_number,person,row_sha256,payload AS projected_payload FROM {table} WHERE row_number>? ORDER BY row_number LIMIT ?',
            (cursor,min(MAX_ROWS,max_rows-scanned))).fetchall()
        if not rows:break
        converted=[]
        for row in rows:
            if row[3].startswith((MAGIC,LEGACY_MAGIC,DICTIONARY_MAGIC)):continue
            # Oversized legacy rows keep their pre-existing reader contract.
            if len(row[3])>8192:continue
            replacement=dictionary_payload(row[3],dictionary)
            if replacement!=row[3]:converted.append((row,(*row[:3],replacement)))
        if converted:
            store.task.check(4*1024**2)
            with store.member('storage-dictionary-repack',table):
                store.db.execute(DDL)
                store.db.execute(f'INSERT OR IGNORE INTO {TABLE} VALUES(?,?)',(key,packed_dictionary))
                store.db.executemany(f'DELETE FROM {table} WHERE row_number=?',[(old[0],) for old,_ in converted])
                store.db.executemany(f'INSERT INTO {table} VALUES(?,?,?,?)',[new for _,new in converted])
                for old,_ in converted:
                    actual=store.db.execute(f'SELECT row_number,person,row_sha256,payload AS projected_payload FROM {table} WHERE row_number=?',(old[0],)).fetchone()
                    if (*actual[:3],original_payload(store.db,actual[3]))!=tuple(old):
                        raise ValueError('dictionary replacement changed row identity')
                store.task.check_memory()
            changed+=len(converted);saved+=sum(len(old[3])-len(new[3]) for old,new in converted)
        scanned+=len(rows);cursor=rows[-1][0]
    free=store.db.execute('PRAGMA freelist_count').fetchone()[0]
    pages=store.db.execute('PRAGMA page_count').fetchone()[0]
    return {'table':table,'scanned_rows':scanned,'repacked_rows':changed,'after_row':cursor,
        'dictionary_sha256':key.hex(),'payload_saving_bytes':saved,
        'net_reusable_bytes':((pages_before-before)-(pages-free))*store.db.execute('PRAGMA page_size').fetchone()[0],
        'physical_file_bytes_reclaimed':0,'logical_row_bytes_unchanged':True}


def repack(store,*,table='easy_careers',after_row=0,max_rows=10000,max_seconds=60):
    """Resume in primary-key order; each <=128-row replacement is atomic.

    Freed SQLite pages are reusable capacity, not reclaimed filesystem bytes.
    The caller owns CareerStore's sole writer and must not hold a transaction.
    """
    _validate_repack(store,table,after_row,max_rows,max_seconds)
    deadline=time.monotonic()+max_seconds;cursor=after_row;scanned=changed=saved=blocks=0
    free_before=store.db.execute('PRAGMA freelist_count').fetchone()[0]
    while scanned<max_rows and time.monotonic()<deadline:
        store.task.check_memory()
        rows=store.db.execute(f'SELECT row_number,person,row_sha256,payload AS projected_payload FROM {table} WHERE row_number>? ORDER BY row_number LIMIT ?',
                              (cursor,min(MAX_ROWS,max_rows-scanned))).fetchall()
        if not rows:break
        candidates=[];size=0
        for row in rows:
            if row[3].startswith((MAGIC,DICTIONARY_MAGIC)):continue
            original=original_payload(store.db,row[3])
            raw=inflate(original,MAX_ROW)
            if size+4+len(raw)>MAX_BLOCK:break
            candidates.append((*row[:3],original));size+=4+len(raw)
        if candidates:
            key,packed,refs=make_block([r[3] for r in candidates])
            benefit=sum(len(r[3]) for r in candidates)-sum(map(len,refs))-len(packed)-128
            if benefit>0:
                store.task.check(max(1024**2,size*4))
                with store.member('storage-page-repack',table):
                    store.db.execute(DDL)
                    store.db.execute(f'INSERT OR IGNORE INTO {TABLE} VALUES(?,?)',(key,packed))
                    for row,ref in zip(candidates,refs):
                        if original_payload(store.db,ref)!=row[3]:
                            raise ValueError('stored projection block conflicts with predecessor')
                    # Delete/reinsert permits SQLite to reuse sparse leaf pages.
                    # IDs, source hashes and person indexes retain their values.
                    store.db.executemany(f'DELETE FROM {table} WHERE row_number=?',[(r[0],) for r in candidates])
                    store.db.executemany(f'INSERT INTO {table} VALUES(?,?,?,?)',
                        [(r[0],r[1],r[2],ref) for r,ref in zip(candidates,refs)])
                    for row in candidates:
                        actual=store.db.execute(f'SELECT row_number,person,row_sha256,payload AS projected_payload FROM {table} WHERE row_number=?',(row[0],)).fetchone()
                        if (*actual[:3],original_payload(store.db,actual[3]))!=row:
                            raise ValueError('projection page replacement changed row identity')
                changed+=len(candidates);saved+=benefit;blocks+=1
        # If a decoded block fills early, remaining rows must be revisited.
        last=candidates[-1][0] if candidates and len(candidates)<sum(not r[3].startswith((MAGIC,DICTIONARY_MAGIC)) for r in rows) else rows[-1][0]
        scanned+=sum(r[0]<=last for r in rows);cursor=last
    free_after=store.db.execute('PRAGMA freelist_count').fetchone()[0]
    return {'table':table,'scanned_rows':scanned,'repacked_rows':changed,'after_row':cursor,
        'new_blocks':blocks,'estimated_payload_saving_bytes':saved,
        'reusable_page_bytes_delta':(free_after-free_before)*store.db.execute('PRAGMA page_size').fetchone()[0],
        'physical_file_bytes_reclaimed':0,'logical_row_bytes_unchanged':True}


def prune_unreferenced(store):
    """Delete only block objects proved unreachable from both owning tables.

    Caller retains the sole-writer lock for the complete scan and deletion.
    Source rows, historical frozen files and referenced blocks are untouched.
    """
    if store.db.in_transaction:raise ValueError('cannot prune unfinished transaction')
    if not store.db.execute("SELECT 1 FROM sqlite_master WHERE name=?",(TABLE,)).fetchone():
        return {'unreferenced_blocks_removed':0}
    referenced=set()
    for table in ('easy_careers','easy_full_delta'):
        if not store.db.execute("SELECT 1 FROM sqlite_master WHERE name=?",(table,)).fetchone():continue
        for key,size,magic in store.db.execute(f'''SELECT substr(payload,?,32),length(payload),substr(payload,1,7) FROM {table}
                WHERE substr(payload,1,?) IN (?,?,?)''',(len(MAGIC)+1,len(MAGIC),MAGIC,LEGACY_MAGIC,DICTIONARY_MAGIC)):
            valid=(DICTIONARY_HEADER<size<=MAX_ROW+65536+DICTIONARY_HEADER) if magic==DICTIONARY_MAGIC else size==REFERENCE_SIZE
            if not valid:raise ValueError('invalid projection reference framing')
            referenced.add(key)
            if len(referenced)>100000:raise ValueError('projection live-block set exceeds bound')
        store.task.check_memory()
    removed=0;cursor=b''
    while True:
        keys=[row[0] for row in store.db.execute(f'SELECT digest FROM {TABLE} WHERE digest>? ORDER BY digest LIMIT 128',(cursor,))]
        if not keys:break
        unused=[(key,) for key in keys if key not in referenced]
        if unused:
            with store.member('storage-page-repack','unreferenced-projection-blocks'):
                store.db.executemany(f'DELETE FROM {TABLE} WHERE digest=?',unused)
            removed+=len(unused)
        cursor=keys[-1]
    return {'unreferenced_blocks_removed':removed,'referenced_blocks_preserved':len(referenced),
            'source_records_deleted':0,'physical_file_bytes_reclaimed':0}
