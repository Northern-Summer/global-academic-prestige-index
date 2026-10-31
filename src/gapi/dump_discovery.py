"""Official bulk candidate discovery independent of public SPARQL/search services.

No raw bytes are persisted. Unverified candidates are NOT canonical career events:
the existing entity/ORCID adapters must independently fetch and validate each record.
Wikidata dump and Wikidata API remain the same evidence source.
"""
from __future__ import annotations

import bz2
import gzip
import hashlib
import json
import re
import time
from contextlib import nullcontext
from pathlib import Path

from .global_sources import wd_value
from .orcid_stream import CHUNK
from .career_rules import valid_orcid
from .task import BudgetStop, canonical, digest
from .source_http import StageDeadline

MAX_LINE = 2*1024**2


def load_feed(path: Path):
    spec = json.loads(path.read_text(encoding='utf-8'))
    name = spec.get('id', '')
    if not re.fullmatch(r'wikidata-\d{8}-all', name) or spec.get('kind') != 'wikidata-json-discovery/v1':
        raise ValueError('unsupported discovery feed identity')
    compression = spec.get('compression')
    suffix = {'bz2':'bz2', 'gzip':'gz'}.get(compression)
    date = name.split('-')[1]
    expected = f'https://dumps.wikimedia.org/wikidatawiki/entities/{date}/{name}.json.{suffix}'
    if spec.get('url') != expected or not re.fullmatch(r'[a-f0-9]{40}', spec.get('sha1','')):
        raise ValueError('feed requires a fixed official URL and upstream SHA-1')
    if not isinstance(spec.get('size_bytes'),int) or spec['size_bytes'] <= 0 or not suffix:
        raise ValueError('feed requires exact compressed size and known compression')
    if spec.get('purpose') != 'candidate-discovery-only-not-career-evidence' or spec.get('license') != 'CC0-1.0':
        raise ValueError('feed purpose/license mismatch')
    return spec


class FeedReader:
    def __init__(self, raw, task, *, max_bytes=None, deadline=None, resume=None, expected_bytes=None,
                 max_reconnects=256, max_stalled_reconnects=8):
        self.raw, self.task = raw, task
        self.max_bytes, self.deadline = max_bytes, deadline
        self.size = 0
        self.sha1 = hashlib.sha1(usedforsecurity=False)
        self.sha256 = hashlib.sha256()
        self.resume, self.expected_bytes = resume, expected_bytes
        self.max_reconnects, self.reconnects = max_reconnects, 0
        self.max_stalled_reconnects, self.stalled_reconnects = max_stalled_reconnects, 0

    def check_deadline(self):
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise BudgetStop('bounded discovery time reached; replay from start required')

    def check_budget(self):
        self.task.check_memory()
        self.check_deadline()
        if self.max_bytes is not None and self.size >= self.max_bytes:
            raise BudgetStop('bounded compressed discovery bytes reached; replay required')

    def read(self, size=-1):
        if size == 0:
            return b''
        self.check_deadline()
        # The complete frozen representation needs no extra socket read or
        # reconnect. An exact byte budget must still allow EOF validation.
        if self.expected_bytes is not None and self.size == self.expected_bytes:
            return b''
        self.check_budget()
        size = CHUNK if size < 0 else min(size,CHUNK)
        if self.max_bytes is not None:
            size = min(size,self.max_bytes-self.size)
        if self.expected_bytes is not None:
            size = min(size,self.expected_bytes-self.size)
        while True:
            self.check_budget()
            try:
                chunk = self.raw.read(size)
                self.check_deadline()
                if chunk or self.expected_bytes is None or self.size >= self.expected_bytes:
                    if chunk:
                        self.stalled_reconnects = 0
                    break
                failure = EOFError('compressed source ended before frozen length')
            except StageDeadline:
                raise
            except OSError as exc:
                failure = exc
            self.check_budget()
            if (not self.resume or self.reconnects >= self.max_reconnects
                    or self.stalled_reconnects >= self.max_stalled_reconnects):
                raise failure
            self.raw.close()
            self.reconnects += 1
            self.stalled_reconnects += 1
            self.raw = self.resume(self.size)
        self.size += len(chunk)
        self.sha1.update(chunk)
        self.sha256.update(chunk)
        return chunk

    def close(self):
        self.raw.close()


def bounded_lines(stream):
    """Never retain an oversized line, even when a hostile record lacks newlines."""
    pending = bytearray()
    oversized = False
    line_hash = hashlib.sha256()
    while chunk := stream.read(CHUNK):
        parts = chunk.split(b'\n')
        for i, part in enumerate(parts):
            line_hash.update(part)
            if not oversized:
                if len(pending)+len(part) > MAX_LINE:
                    pending.clear()
                    oversized = True
                else:
                    pending.extend(part)
            if i < len(parts)-1:
                yield None if oversized else bytes(pending), line_hash.hexdigest()
                pending.clear(); oversized = False; line_hash = hashlib.sha256()
    if pending or oversized:
        yield None if oversized else bytes(pending), line_hash.hexdigest()


def discover_dump(store, http, spec, *, max_seconds=300, max_bytes=None, progress=None,
                  stage_careers=False, max_reconnects=256):
    if type(max_reconnects) is not int or not 1 <= max_reconnects <= 4096:
        raise ValueError('total reconnect limit must be between 1 and 4096')
    feed = spec['id']
    identity = canonical(spec).decode()
    previous = store.db.execute('SELECT spec,status,lines,candidates,rejected,prefix_sha256,compressed_bytes,sha256 FROM discovery_feeds WHERE id=?',(feed,)).fetchone()
    if previous and previous[0] != identity:
        raise ValueError('discovery feed changed; frozen replay forbidden')
    if previous and previous[1] == 'verified' and not stage_careers:
        return {'status':'verified', 'feed':feed, 'already_complete':True}
    if stage_careers:
        from .dump_landing import initialize, stage_entity
        initialize(store)
    store.task.check(4*1024**2)
    with store.db:
        store.db.execute('INSERT OR IGNORE INTO discovery_feeds(id,spec,status) VALUES(?,?,?)',(feed,identity,'provisional'))
        if stage_careers:
            store.db.execute("UPDATE discovery_feeds SET status='provisional' WHERE id=?",(feed,))
    replay_until = previous[2] if previous else 0
    candidates, rejected = (previous[3], previous[4]) if previous else (0,0)
    lines, batch, status, detail = 0, [], 'provisional', 'bounded partial scan; candidates only'
    reader = None
    prefix = hashlib.sha256()
    start_marker = end_marker = False
    def flush():
        store.task.check(1024**2)
        # A short replay or failed open must not shrink the next scan ceiling.
        # Keep the byte count and its matching hash as one observed-prefix pair;
        # the returned compressed_bytes still describes only this attempt.
        old_bytes = previous[6] if previous else 0
        if reader is not None and reader.size >= old_bytes:
            observed_bytes, observed_hash = reader.size, reader.sha256.hexdigest()
        else:
            observed_bytes, observed_hash = old_bytes, previous[7] if previous else None
        with store.db:
            store.db.executemany('INSERT OR IGNORE INTO discovery VALUES(?,?,?,?)',batch)
            store.db.execute('''UPDATE discovery_feeds SET status=?,compressed_bytes=?,lines=?,candidates=?,rejected=?,sha256=?,detail=?,prefix_sha256=? WHERE id=?''',
                (status,observed_bytes,max(lines,replay_until),candidates,rejected,
                 observed_hash,detail,
                 prefix.hexdigest() if lines > replay_until or not previous else previous[5],feed))
        batch.clear()
    deadline = time.monotonic()+max_seconds if max_seconds else None
    window = http.within(max_seconds) if max_seconds and hasattr(http,'within') else nullcontext()
    try:
        with window, http.open(spec['url']) as raw:
            if raw.headers.get('Content-Encoding','identity') != 'identity':
                raise ValueError('compressed dump transfer encoding changed')
            advertised = raw.headers.get('Content-Length')
            if advertised and int(advertised) != spec['size_bytes']:
                raise ValueError('discovery source Content-Length changed')
            def resume(offset):
                if deadline is None:
                    return http.open_range(spec['url'],offset,spec['size_bytes'],raw.headers)
                with http.within(max(0,deadline-time.monotonic())):
                    return http.open_range(spec['url'],offset,spec['size_bytes'],raw.headers)
            reader = FeedReader(raw,store.task,max_bytes=max_bytes, deadline=deadline,
                resume=resume if hasattr(http,'open_range') else None, expected_bytes=spec['size_bytes'],
                max_reconnects=max_reconnects)
            decompressor = bz2.BZ2File(reader) if spec['compression']=='bz2' else gzip.GzipFile(fileobj=reader)
            try:
                for lines, (line, checksum) in enumerate(bounded_lines(decompressor),1):
                    prefix.update(bytes.fromhex(checksum))
                    text = line.strip() if line is not None else b''
                    is_start, is_end = text == b'[', text == b']'
                    start_marker, end_marker = start_marker or is_start, end_marker or is_end
                    if line is None:
                        store.failure('wikidata-dump',f'{feed}:line:{lines}',
                            f'oversized-candidate-record;sha256={checksum}')
                    # Backfill landing even in a previously scanned prefix. A
                    # restart must replay compressed bytes for checksum anyway.
                    # Never store raw entities, and never create ranking events.
                    if stage_careers and text and not (is_start or is_end):
                        try:
                            landing_entity = json.loads(text.removesuffix(b','))
                            landing_claims = landing_entity.get('claims',{})
                            if landing_claims.get('P69') and landing_claims.get('P108'):
                                with store.db:
                                    stage_entity(store,feed,landing_entity,checksum)
                        except (ValueError,TypeError,AttributeError,RecursionError) as exc:
                            store.failure('wikidata-dump-landing',f'{feed}:line:{lines}',
                                f'{type(exc).__name__}: {str(exc)[:160]};sha256={checksum}')
                    if lines <= replay_until:
                        if lines == replay_until and previous[5] != prefix.hexdigest():
                            raise ValueError('discovery replay prefix changed')
                        if progress and lines % 10000 == 0:
                            progress({'feed':feed,'phase':'replay-verification','lines':lines,
                                'replay_until':replay_until,'compressed_bytes':reader.size,'reconnects':reader.reconnects})
                        continue
                    store.task.check_memory()
                    if line is None:
                        rejected += 1
                    else:
                        if text and not (is_start or is_end):
                            try:
                                entity = json.loads(text.removesuffix(b','))
                                claims = entity.get('claims',{})
                                qid = entity.get('id','')
                                if not re.fullmatch(r'Q[1-9]\d*',qid):
                                    continue
                                if claims.get('P69') and claims.get('P108'):
                                    batch.append(('ingest-wikidata',qid,digest(spec),checksum))
                                    candidates += 1
                                values = (wd_value(c.get('mainsnak',{})) for c in claims.get('P496',[]) if c.get('rank')!='deprecated')
                                ids = {v for v in values if valid_orcid(v)}
                                if len(ids) == 1 and (claims.get('P69') or claims.get('P108')):
                                    batch.append(('ingest-orcid-api',next(iter(ids)),digest(spec),checksum))
                            except (ValueError,TypeError,AttributeError,RecursionError):
                                rejected += 1
                                store.failure('wikidata-dump',f'{feed}:line:{lines}',
                                    f'invalid-candidate-json-or-schema;sha256={checksum}')
                    if len(batch) >= 256 or lines % 10000 == 0:
                        flush()
                        if progress:
                            progress({'feed':feed,'lines':lines,'compressed_bytes':reader.size,'candidates':candidates,
                                'rejected':rejected,'reconnects':reader.reconnects})
                # On replay the initial marker may have been in the skipped prefix.
                if not (start_marker or replay_until) or not end_marker:
                    raise ValueError('missing JSON-array framing')
                if reader.size != spec['size_bytes'] or reader.sha1.hexdigest() != spec['sha1']:
                    raise ValueError('full discovery source checksum/length mismatch')
                reader.check_deadline()
                status, detail = 'verified', ('complete source bytes/checksum verified; '
                    f'{rejected} record lines rejected; candidates still require individual evidence acquisition')
            finally:
                decompressor.close()
                reader.close()
    except (BudgetStop,StageDeadline) as exc:
        detail = str(exc)
    except (OSError,ValueError,EOFError,RuntimeError) as exc:
        status, detail = 'failed', str(exc)[:256]
        flush()
        raise
    flush()
    return {'feed':feed,'status':status,'lines':max(lines,replay_until),'compressed_bytes':reader.size if reader else 0,
        'candidates':candidates,'rejected':rejected,'detail':detail,'canonical_events_written':0,
        'reconnects':reader.reconnects if reader else 0}
