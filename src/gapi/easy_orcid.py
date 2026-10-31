"""Bounded candidate discovery from the public Easy ORCID derivative.

The record is derived from ORCID, so it is an enumeration aid rather than an
independent corroborating source. Only ORCID identifiers are retained; the
remote JSONL and its gzip stream never touch disk.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
import zlib

from .career_rules import valid_orcid
from .source_http import StageDeadline
from .task import digest

SOURCE_ID = "easy-orcid-2023.6-hq"
SOURCE_URL = "https://zenodo.org/records/13333068/files/records_hq.jsonl.gz?download=1"
MAX_LINE_BYTES = 2 * 1024 * 1024


def _lines(response, *, limit, deadline, task, gzip_stream=False):
    encoding = response.headers.get("Content-Encoding", "identity").lower()
    if encoding not in ("identity", "gzip"):
        raise ValueError("Easy ORCID requires identity or gzip transport")
    decoder = zlib.decompressobj(31) if gzip_stream or encoding == "gzip" else None
    pending = bytearray()
    oversized = False
    compressed_pending = b""
    while True:
        if time.monotonic() >= deadline:
            raise StageDeadline("Easy ORCID bounded stage ended")
        task.check_memory()
        from_source = not compressed_pending
        if compressed_pending:
            chunk, compressed_pending = compressed_pending, b""
        else:
            chunk = response.read(65536)
        if not chunk:
            if not from_source:
                continue
            # unconsumed_tail was drained in bounded chunks before EOF.
            # flush() without a limit would reintroduce unbounded expansion.
            if decoder and not decoder.eof:
                raise ValueError("truncated Easy ORCID gzip stream")
            if pending and not oversized:
                line = bytes(pending)
                if len(line) > limit:
                    yield None
                else:
                    yield line
            return
        if decoder:
            try:
                chunk = decoder.decompress(chunk, 65536)
            except zlib.error as exc:
                raise ValueError("invalid Easy ORCID gzip stream") from exc
            if decoder.unused_data:
                raise ValueError("trailing Easy ORCID gzip data")
            compressed_pending = decoder.unconsumed_tail
        if oversized:
            if b"\n" in chunk:
                _, _, chunk = chunk.partition(b"\n")
                oversized = False
            else:
                continue
        pending.extend(chunk)
        while b"\n" in pending:
            line, _, rest = pending.partition(b"\n")
            pending = bytearray(rest)
            if len(line) > limit:
                yield None
            else:
                yield bytes(line)
        if len(pending) > limit:
            pending.clear()
            oversized = True
            yield None


def discover(store, http, *, max_records=1000, max_seconds=120):
    if type(max_records) is not int or not 1 <= max_records <= 100000:
        raise ValueError("Easy ORCID record bound must be between 1 and 100000")
    if type(max_seconds) not in (int, float) or not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError("Easy ORCID time bound must be positive")
    # A separate cursor namespace makes reruns idempotent and does not alter
    # the existing ORCID API source identity or its frozen queue semantics.
    row = store.db.execute(
        "SELECT cursor_after FROM discovery_pages WHERE source=? ORDER BY rowid DESC LIMIT 1",
        (SOURCE_ID,),
    ).fetchone()
    cursor = int(row[0]) if row else 0
    started = time.monotonic()
    selected = set()
    rejected = 0
    seen_records = 0
    response_sha = hashlib.sha256()
    with http.open(SOURCE_URL, "application/json", headers={"Accept-Encoding": "identity"}) as response:
        for raw in _lines(response, limit=MAX_LINE_BYTES, deadline=started + max_seconds, task=store.task, gzip_stream=True):
            if seen_records < cursor:
                seen_records += 1
                continue
            if seen_records >= cursor + max_records:
                break
            if raw is None:
                rejected += 1
                seen_records += 1
                continue
            try:
                record = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("invalid Easy ORCID JSONL record") from exc
            if not isinstance(record, dict) or not isinstance(record.get("orcid"), str):
                raise ValueError("Easy ORCID record lacks ORCID identifier")
            orcid = record["orcid"]
            if valid_orcid(orcid):
                selected.add(orcid)
            response_sha.update(raw)
            response_sha.update(b"\n")
            seen_records += 1
    query_hash = digest({"source": SOURCE_ID, "url": SOURCE_URL, "cursor": cursor, "limit": max_records})
    page_hash = response_sha.hexdigest()
    # Include queue indexes and rollback journal growth before admission. The
    # established queue owner commits candidates and cursor in one transaction.
    store.task.check(len(selected) * 1024 + 1024**2)
    before_changes = store.db.total_changes
    store.queue_page(SOURCE_ID, 'ingest-orcid-api', str(cursor), str(seen_records),
                     query_hash, page_hash, selected, seen_records - cursor)
    inserted = store.db.total_changes - before_changes - 1
    return {"source": SOURCE_ID, "status": "candidate-discovery-only", "records": seen_records - cursor,
            "cursor": seen_records, "queued": inserted, "valid_unique_candidates": len(selected),
            "rejected_oversized": rejected,
            "response_sha256": page_hash, "raw_saved": False, "independent_corroboration": False}
