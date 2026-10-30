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
from pathlib import Path

from .global_sources import wd_value
from .orcid_stream import CHUNK
from .career_rules import valid_orcid
from .task import BudgetStop, canonical, digest

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
    def __init__(self, raw, task, *, max_bytes=None, deadline=None):
        self.raw, self.task = raw, task
        self.max_bytes, self.deadline = max_bytes, deadline
        self.size = 0
        self.sha1 = hashlib.sha1(usedforsecurity=False)
        self.sha256 = hashlib.sha256()

    def read(self, size=-1):
        self.task.check_memory()
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise BudgetStop('bounded discovery time reached; replay from start required')
        if self.max_bytes is not None and self.size >= self.max_bytes:
            raise BudgetStop('bounded compressed discovery bytes reached; replay required')
        size = CHUNK if size < 0 else min(size,CHUNK)
        if self.max_bytes is not None:
            size = min(size,self.max_bytes-self.size)
        chunk = self.raw.read(size)
        self.size += len(chunk)
        self.sha1.update(chunk)
        self.sha256.update(chunk)
        return chunk


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


def discover_dump(store, http, spec, *, max_seconds=300, max_bytes=None, progress=None):
    feed = spec['id']
    identity = canonical(spec).decode()
    previous = store.db.execute('SELECT spec,status,lines,candidates,rejected,prefix_sha256,compressed_bytes,sha256 FROM discovery_feeds WHERE id=?',(feed,)).fetchone()
    if previous and previous[0] != identity:
        raise ValueError('discovery feed changed; frozen replay forbidden')
    if previous and previous[1] == 'verified':
        return {'status':'verified', 'feed':feed, 'already_complete':True}
    store.task.check(4*1024**2)
    with store.db:
        store.db.execute('INSERT OR IGNORE INTO discovery_feeds(id,spec,status) VALUES(?,?,?)',(feed,identity,'provisional'))
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
    try:
        with http.open(spec['url']) as raw:
            advertised = raw.headers.get('Content-Length')
            if advertised and int(advertised) != spec['size_bytes']:
                raise ValueError('discovery source Content-Length changed')
            reader = FeedReader(raw,store.task,max_bytes=max_bytes,
                deadline=time.monotonic()+max_seconds if max_seconds else None)
            decompressor = bz2.BZ2File(reader) if spec['compression']=='bz2' else gzip.GzipFile(fileobj=reader)
            try:
                for lines, (line, checksum) in enumerate(bounded_lines(decompressor),1):
                    prefix.update(bytes.fromhex(checksum))
                    if lines <= replay_until:
                        if lines == replay_until and previous[5] != prefix.hexdigest():
                            raise ValueError('discovery replay prefix changed')
                        continue
                    store.task.check_memory()
                    if line is None:
                        rejected += 1
                    else:
                        text = line.strip()
                        if text == b'[':
                            start_marker = True
                        elif text == b']':
                            end_marker = True
                        elif text:
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
                    if len(batch) >= 256 or lines % 10000 == 0:
                        flush()
                        if progress:
                            progress({'feed':feed,'lines':lines,'compressed_bytes':reader.size,'candidates':candidates,'rejected':rejected})
                # On replay the initial marker may have been in the skipped prefix.
                if not (start_marker or replay_until) or not end_marker:
                    raise ValueError('missing JSON-array framing')
                if reader.size != spec['size_bytes'] or reader.sha1.hexdigest() != spec['sha1']:
                    raise ValueError('full discovery source checksum/length mismatch')
                status, detail = 'verified', 'complete source enumeration verified; queue members still require individual evidence acquisition'
            finally:
                decompressor.close()
    except BudgetStop as exc:
        detail = str(exc)
    except (OSError,ValueError,EOFError,RuntimeError) as exc:
        status, detail = 'failed', str(exc)[:256]
        flush()
        raise
    flush()
    return {'feed':feed,'status':status,'lines':max(lines,replay_until),'compressed_bytes':reader.size if reader else 0,
        'candidates':candidates,'rejected':rejected,'detail':detail,'canonical_events_written':0}
