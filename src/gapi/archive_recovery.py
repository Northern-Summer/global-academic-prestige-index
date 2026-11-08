"""Queue bounded follow-up for rejected annual members, without promoting facts.

The member ledger retains each rejection and its source digest. The existing
ORCID API discovery queue owns subsequent fetches and backoff; this module
creates no second retry ledger, independent evidence or ranking qualification.
"""
import hashlib
import json
import io
import math
import re
import time

from .career_rules import valid_orcid
from .career_store import orcid_archive_sources, source_identity
from .orcid_stream import summary_member_pattern
from .queue_storage import pack_row
from .task import digest

MAX_CHECKPOINT_INDEX = 8*1024**2
_CHECKPOINT_TABLE = 'archive_stream_checkpoints'


def _validate_checkpoint(store, source, payload, index):
    """Validate the storage binding; this does not validate a gzip seek index."""
    from .archive_hash import ArchiveHash
    if source not in orcid_archive_sources(store.task):
        raise ValueError('checkpoint requires a frozen annual archive')
    store.task.require_frozen()
    keys = {'schema', 'source_identity', 'compressed_bytes', 'tar_offset',
            'member_count', 'last_member', 'md5', 'sha256', 'index_sha256'}
    if not isinstance(payload, dict) or set(payload) not in (keys,keys|{'seek_state'}):
        raise ValueError('archive checkpoint fields mismatch')
    if payload['schema'] != 'archive-stream-checkpoint/v1':
        raise ValueError('archive checkpoint schema mismatch')
    if not isinstance(index, bytes) or not 0 < len(index) <= MAX_CHECKPOINT_INDEX:
        raise ValueError('archive checkpoint index size invalid')
    if hashlib.sha256(index).hexdigest() != payload['index_sha256']:
        raise ValueError('archive checkpoint index checksum mismatch')
    identity = source_identity(store.task, source)
    run = store.db.execute('SELECT identity,members FROM runs WHERE source=?',(source,)).fetchone()
    if payload['source_identity'] != identity or not run or run[0] != identity:
        raise ValueError('archive checkpoint source identity drift')
    for key in ('compressed_bytes', 'tar_offset', 'member_count'):
        if type(payload[key]) is not int or payload[key] <= 0:
            raise ValueError('archive checkpoint position invalid')
    if (payload['tar_offset'] % 512 or payload['member_count'] != run[1]
            or payload['compressed_bytes'] > store.task.config['sources'][source]['size_bytes']):
        raise ValueError('archive checkpoint position/member ledger mismatch')
    last = payload['last_member']
    if (not isinstance(last, list) or len(last) != 3
            or any(not isinstance(value, str) for value in last)
            or store.seen(source,last[0]) != tuple(last[1:])):
        raise ValueError('archive checkpoint last member mismatch')
    for algorithm in ('md5', 'sha256'):
        state = ArchiveHash.restore(payload[algorithm])
        if state.algorithm != algorithm or state.size != payload['compressed_bytes']:
            raise ValueError('archive checkpoint hash position mismatch')
    if 'seek_state' in payload:
        from .source_http import ArchiveSeekReader, PublicHTTP
        reader = ArchiveSeekReader(PublicHTTP(store.task),store.task.config['sources'][source],
                                   state=payload['seek_state'])
        try:
            if (reader.highwater != payload['compressed_bytes']
                    or any(payload[name] != payload['seek_state'][name] for name in ('md5','sha256'))):
                raise ValueError('archive checkpoint transport/hash mismatch')
        finally:reader.close()


def save_decoder_checkpoint(store, source, reader, decoder, last_member):
    """Assemble a real index/transport checkpoint inside the member transaction.

    Reject sparse indexes before publication. The caller must invoke only at a
    complete tar-member boundary, with that member included in this transaction.
    """
    if not store.db.in_transaction:
        raise ValueError('decoder checkpoint requires member transaction')
    position = decoder.tell()
    if type(position) is not int or position <= 0 or position % 512:
        raise ValueError('decoder checkpoint requires aligned tar member boundary')
    points = [point for point in decoder.seek_points() if point[0] <= position]
    floor = min(reader.blocks) if reader.blocks else 0
    if not points or max(points,key=lambda point:point[0])[1]//reader.BLOCK*reader.BLOCK < floor:
        raise ValueError('decoder index lacks a point in the bounded replay window')
    class BoundedIndex(io.BytesIO):
        def write(self, value):
            if self.tell()+len(value)>MAX_CHECKPOINT_INDEX:
                raise ValueError('decoder checkpoint index exceeds storage bound')
            return super().write(value)
    with BoundedIndex() as output:
        decoder.export_index(fileobj=output)
        index = output.getvalue()
    state = reader.snapshot()
    count = store.db.execute('SELECT members FROM runs WHERE source=?',(source,)).fetchone()
    if not count:raise ValueError('decoder checkpoint source not started')
    payload = {'schema':'archive-stream-checkpoint/v1',
               'source_identity':source_identity(store.task,source),
               'compressed_bytes':reader.highwater,'tar_offset':position,
               'member_count':count[0],'last_member':list(last_member),
               'md5':state['md5'],'sha256':state['sha256'],
               'index_sha256':hashlib.sha256(index).hexdigest(),'seek_state':state}
    save_stream_checkpoint(store,source,payload,index)


def save_stream_checkpoint(store, source, payload, index):
    """Caller owns writer lock and the SAME transaction as member/event writes.

    A bounded single row replaces the previous state. No external index file
    can become newer or older than the member commit. No production caller is
    connected until decoder-position restoration is independently verified.
    """
    from .task import canonical
    if not store.db.in_transaction:
        raise ValueError('archive checkpoint requires member transaction')
    _validate_checkpoint(store,source,payload,index)
    encoded = canonical(payload)
    if len(encoded) > 16384:
        raise ValueError('archive checkpoint manifest exceeds bound')
    store.task.check(len(encoded)+len(index)+1024**2)
    store.db.execute('CREATE TABLE IF NOT EXISTS '+_CHECKPOINT_TABLE+
        '(source TEXT PRIMARY KEY,payload TEXT NOT NULL,index_bytes BLOB NOT NULL,sha256 TEXT NOT NULL)')
    old = store.db.execute('SELECT payload,sha256,index_bytes FROM '+_CHECKPOINT_TABLE+' WHERE source=?',(source,)).fetchone()
    if old:
        if hashlib.sha256(old[0].encode()).hexdigest() != old[1]:
            raise ValueError('existing archive checkpoint manifest corrupted')
        prior = json.loads(old[0])
        if (prior['source_identity'] != payload['source_identity']
                or hashlib.sha256(old[2]).hexdigest() != prior['index_sha256']):
            raise ValueError('existing archive checkpoint identity/index corrupted')
        if any(payload[k] < prior[k] for k in ('compressed_bytes','tar_offset','member_count')):
            raise ValueError('archive checkpoint cannot move backwards')
        if payload == prior and index == old[2]:
            return
        if payload['tar_offset'] == prior['tar_offset']:
            raise ValueError('archive checkpoint changed at same member position')
    store.db.execute('INSERT INTO '+_CHECKPOINT_TABLE+' VALUES(?,?,?,?) ON CONFLICT(source) DO UPDATE SET '
        'payload=excluded.payload,index_bytes=excluded.index_bytes,sha256=excluded.sha256',
        (source,encoded.decode(),index,hashlib.sha256(encoded).hexdigest()))
    store.task.check_memory()


def load_stream_checkpoint(store, source):
    """Read only; absent means no direct seek, invalid means stop, never replay silently."""
    if store.db.in_transaction:
        raise ValueError('archive checkpoint load requires settled transaction')
    exists = store.db.execute('SELECT 1 FROM sqlite_master WHERE type=\'table\' AND name=?',
                             (_CHECKPOINT_TABLE,)).fetchone()
    if not exists:
        return None
    row = store.db.execute('SELECT payload,index_bytes,sha256 FROM '+_CHECKPOINT_TABLE+' WHERE source=?',
                          (source,)).fetchone()
    if not row:
        return None
    encoded,index,checksum = row
    if len(encoded) > 16384 or hashlib.sha256(encoded.encode()).hexdigest() != checksum:
        raise ValueError('archive checkpoint manifest checksum mismatch')
    payload = json.loads(encoded)
    _validate_checkpoint(store,source,payload,index)
    return payload,index


def queue_archive_rejections(store, source, *, after_rowid=0, max_members=10000, max_seconds=60):
    """Caller owns the existing writer lock; the cursor covers committed groups."""
    if source not in orcid_archive_sources(store.task):
        raise ValueError('follow-up requires a frozen annual ORCID source')
    if store.task.config['sources'].get('orcid-public-api',{}).get('kind') != 'orcid-public-api':
        raise ValueError('ORCID public API is not registered in the frozen task')
    if type(after_rowid) is not int or after_rowid < 0:
        raise ValueError('invalid archive follow-up cursor')
    if type(max_members) is not int or not 1 <= max_members <= 100000:
        raise ValueError('invalid archive follow-up member limit')
    if type(max_seconds) not in (int,float) or not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('invalid archive follow-up deadline')
    if store.db.in_transaction:
        raise ValueError('archive follow-up requires a settled transaction')
    spec = store.task.config['sources'][source]
    identity = source_identity(store.task, source)
    run = store.db.execute('SELECT identity FROM runs WHERE source=?',(source,)).fetchone()
    if not run or run[0] != identity:
        raise ValueError('archive follow-up source identity is missing or changed')
    pattern = summary_member_pattern(spec)
    cursor = after_rowid
    scanned = eligible = inserted = invalid = 0
    deadline = time.monotonic()+max_seconds
    while scanned < max_members and time.monotonic() < deadline:
        store.task.check_memory()
        rows = store.db.execute('''SELECT rowid,locator,sha256 FROM members NOT INDEXED
            WHERE rowid>? AND source=? AND status='rejected'
            ORDER BY rowid LIMIT ?''',(cursor,source,min(128,max_members-scanned))).fetchall()
        if not rows:
            break
        values = []
        for rowid,locator,checksum in rows:
            match = pattern.fullmatch(locator) if isinstance(locator,str) else None
            if (not match or not valid_orcid(match[1]) or not isinstance(checksum,str)
                    or not re.fullmatch('[0-9a-f]{64}',checksum)):
                invalid += 1
                continue
            query = digest({'kind':'archive-rejection-followup/v1','source':source,
                            'source_identity':identity,
                            'locator_sha256':hashlib.sha256(locator.encode('utf-8')).hexdigest()})
            values.append(pack_row(('ingest-orcid-api',match[1],query,checksum)))
        if values:
            with store.member('archive-follow-up',source,max_growth_bytes=8*1024**2):
                added = store.db.executemany('INSERT OR IGNORE INTO discovery VALUES(?,?,?,?)',values).rowcount
                store.task.check_memory()
            inserted += added
        eligible += len(values)
        scanned += len(rows)
        cursor = rows[-1][0]
    return {'source':source,'scanned_rejections':scanned,'eligible_members':eligible,
            'new_candidates':inserted,'existing_candidates':eligible-inserted,
            'invalid_members_retained':invalid,'after_rowid':cursor,
            'annual_members_modified':0,'canonical_events_written':0,
            'independent_evidence_promotions':0,
            'scope':'candidate-follow-up-only; annual rejection settlement remains incomplete'}
