"""Zero-raw-disk ORCID summary ingestion, with bounded SAX extraction and replay."""
from __future__ import annotations

import gzip
import hashlib
import re
import tarfile
import time
from contextlib import nullcontext
from xml.parsers import expat

from .career_rules import classify, institution_id, year
from .event_storage import MAX_BYTES
from .task import BudgetStop, canonical, digest

CHUNK = 65536
MAX_COMMIT_MEMBERS = 128
ORCID = r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]"


def validate_commit_members(value):
    if type(value) is not int or not 1 <= value <= MAX_COMMIT_MEMBERS:
        raise ValueError(f'commit_members must be between 1 and {MAX_COMMIT_MEMBERS}')
    return value

def summary_member_pattern(spec):
    """Accept documented roots and the exact frozen archive's actual root.

    Publication-year roots are bound to the registered filename, rather than
    accepting any vintage or arbitrary leading directories. ORCID's check digit
    may be X, including in the final three-character directory component.
    """
    roots=['summaries']
    filename=spec.get('filename','')
    if isinstance(filename,str) and re.fullmatch(r'ORCID_\d{4}_\d{2}_summaries\.tar\.gz',filename):
        roots.append(filename.removesuffix('.tar.gz'))
    root='|'.join(re.escape(value) for value in roots)
    return re.compile(rf"(?:\./)?(?:{root})/\d{{2}}[\dX]/({ORCID})\.xml")


def private_person(orcid):
    # Pseudonymous linkage key, never a public output field.
    return hashlib.sha256(("gapi-person/v1/orcid/" + orcid).encode()).hexdigest()


class SummaryParser:
    """Retains whitelisted affiliation fields only, never an XML/person tree."""
    def __init__(self, person, source, locator, emit, max_events=4096, *, section=None):
        self.person, self.source, self.locator, self.emit = person, source, locator, emit
        candidate = locator.rsplit("/", 1)[-1].removesuffix(".xml")
        self.expected_orcid = candidate if re.fullmatch(ORCID, candidate) else None
        if section not in (None,'educations','employments'):
            raise ValueError('unsupported affiliation section')
        self.section=section
        self.path, self.current = [], None
        self.max_events, self.count = max_events, 0
        self.parser = expat.ParserCreate(namespace_separator="}")
        # Coalesce fragmented text before Python callbacks. Keep the buffer
        # bounded; affiliation field limits and structural handlers still apply.
        self.parser.buffer_size = 8192
        self.parser.buffer_text = True
        self.parser.StartElementHandler = self.start
        self.parser.EndElementHandler = self.end
        self.parser.CharacterDataHandler = self.characters
        self.parser.StartDoctypeDeclHandler = self.forbidden
        self.parser.EntityDeclHandler = self.forbidden
        self.parser.ExternalEntityRefHandler = self.forbidden

    def forbidden(self, *args):
        raise ValueError("DTD/entities are forbidden")

    def start(self, tag, attrs):
        local = tag.rsplit("}", 1)[-1]
        if not self.path and self.expected_orcid:
            root=self.section or 'record'
            expected_path='/'+self.expected_orcid+('/'+self.section if self.section else '')
            if local!=root or attrs.get('path')!=expected_path:
                # Keep a bounded diagnostic without copying the unexpected
                # identity (which could be a different person's identifier).
                shape = 'missing' if 'path' not in attrs else 'empty' if not attrs['path'] else 'different'
                raise ValueError("ORCID XML root identity does not match frozen member locator"
                    f"; element={local[:64]!r}; identity_path={shape}")
        self.path.append(local)
        if len(self.path) > 64:
            raise ValueError("XML nesting exceeds bound")
        if local in ("education-summary", "employment-summary"):
            if self.section and local != self.section.removesuffix('s')+'-summary':
                raise ValueError('affiliation appears in the wrong section')
            if self.current is not None:
                raise ValueError("nested affiliation")
            self.current = {"kind": local.split("-", 1)[0], "put_code": attrs.get("put-code", "")[:128], "fields": {}}

    def characters(self, text):
        if self.current is None:
            return
        path = "/".join(self.path[-3:])
        leaf = self.path[-1]
        if leaf not in {"role-title", "department-name", "name", "city", "country", "year", "disambiguated-organization-identifier", "disambiguation-source", "path", "credit-name"}:
            return
        fields = self.current["fields"]
        if len(fields.get(path, "")) + len(text) > 2048:
            raise ValueError("affiliation field exceeds bound")
        fields[path] = fields.get(path, "") + text

    def end(self, tag):
        local = tag.rsplit("}", 1)[-1]
        if local in ("education-summary", "employment-summary"):
            c = self.current
            f = c["fields"]
            def get(suffix):
                return next((v.strip() for k, v in f.items() if k.endswith(suffix)), "")
            role = get("/role-title")
            kind, stage = classify(c["kind"], role)
            inst = institution_id(get("/disambiguated-organization-identifier"), get("/disambiguation-source"))
            start, end = year(get("/start-date/year")), year(get("/end-date/year"))
            assertion = get("/source-orcid/path") or get("/source-client-id/path") or "unknown"
            record = {"person": self.person, "institution": inst, "kind": kind,
                "stage": stage, "year_start": start, "year_end": end, "role": role,
                "institution_name": get("/organization/name"), "country": get("/address/country") or None,
                "put_code": c["put_code"], "assertion_origin": assertion,
                "source": self.source, "locator": self.locator,
                "url": "https://orcid.org/" + self.locator.rsplit("/", 1)[-1].removesuffix(".xml"),
                "status": "observed" if inst and stage and (start if kind == "employment" else end) else "unresolved"}
            self.count += 1
            if self.count > self.max_events:
                raise ValueError("too many affiliations in member")
            self.emit(record)
            self.current = None
        self.path.pop()

    def feed(self, chunk, final=False):
        self.parser.Parse(chunk, final)


class HashReader:
    def __init__(self, raw):
        self.raw, self.size = raw, 0
        self.sha256, self.md5 = hashlib.sha256(), hashlib.md5(usedforsecurity=False)

    def read(self, size):
        if size < 0:
            raise ValueError("unbounded compressed reads forbidden")
        data = self.raw.read(min(size, CHUNK))
        self.size += len(data)
        self.sha256.update(data)
        self.md5.update(data)
        return data


def exact(stream, size):
    result = bytearray()
    while len(result) < size:
        chunk = stream.read(size - len(result))
        if not chunk:
            raise EOFError("truncated tar header")
        result.extend(chunk)
    return bytes(result)


def read_mtime_pax(stream, size):
    """Accept only one bounded, non-semantic PAX modification timestamp.

    No PAX path, link, size, encoding or sparse override reaches the parser.
    Record lengths include their own prefix and final newline (POSIX PAX).
    """
    if not 1 <= size <= 128:
        raise ValueError('PAX timestamp exceeds bound')
    body = exact(stream, size)
    prefix, separator, record = body.partition(b' ')
    if (not separator or not prefix.isdigit() or int(prefix) != size
            or not re.fullmatch(rb'mtime=-?[0-9]{1,20}(?:\.[0-9]{1,20})?\n', record)):
        raise ValueError('unsupported or malformed PAX metadata')
    if size % 512:
        padding = exact(stream, 512-size % 512)
        if any(padding):
            raise ValueError('nonzero PAX padding')


class _ArchiveMemberBatch:
    """Bound complete normalized members; commit their ledgers and events together.

    No raw XML is retained. Uncommitted members are replayed after interruption,
    exactly as a single interrupted member was before. Budget scans remain at
    both transaction boundaries; SQLite's physical growth cap remains active.
    """
    def __init__(self, store, source, limit):
        self.store, self.source, self.limit = store, source, limit
        self.rows, self.known, self.events = [], {}, 0

    def flush(self):
        if not self.rows:
            return
        with self.store.member(self.source, 'archive-commit-batch'):
            for locator, sha256, status, reason, events in self.rows:
                if status == 'rejected':
                    self.store.db.execute('DELETE FROM events WHERE source=? AND locator=?',
                                          (self.source, locator))
                else:
                    for record in events:
                        self.store.event(record)
                self.store.finish_member(self.source, locator, sha256, status, reason)
            self.store.task.check_memory()
        self.rows.clear()
        self.known.clear()
        self.events = 0

    def add(self, locator, sha256, status, reason, events):
        maximum = self.store.task.config['batch_events']
        if len(events) > maximum:
            raise ValueError('archive commit exceeds event limit')
        if self.events + len(events) > maximum:
            self.flush()
        self.rows.append((locator, sha256, status, reason, events))
        self.known[locator] = (sha256, status)
        self.events += len(events)
        self.store.task.check_memory()
        if len(self.rows) >= self.limit or self.events >= maximum:
            self.flush()


def stream_archive(store, source, raw, *, max_members=None, max_new_members=None, max_seconds=None, progress=None, commit_members=1):
    """Replay from the start, optionally bounding new commits instead of scans.

    Previously committed members still consume stream bytes and are hashed to
    detect changed input, but require no write transaction. Neither member limit
    proves archive completion; only EOF, CRC and the upstream checksum do that.
    """
    if max_new_members is not None and (type(max_new_members) is not int or max_new_members <= 0):
        raise ValueError('max_new_members must be a positive integer')
    validate_commit_members(commit_members)
    spec = store.task.config["sources"][source]
    member_pattern=summary_member_pattern(spec)
    store.start(source)
    store.status(source, 'provisional', detail='archive scan active; full verification pending')
    reader = HashReader(raw)
    started, processed, added, replayed = time.monotonic(), 0, 0, 0
    compressed = gzip.GzipFile(fileobj=reader, mode="rb")
    status, detail = "provisional", None
    pending_pax = False
    batch = _ArchiveMemberBatch(store, source, commit_members) if commit_members > 1 else None
    try:
        while True:
            if ((max_members is not None and processed >= max_members)
                    or (max_new_members is not None and added >= max_new_members)
                    or (max_seconds is not None and time.monotonic()-started >= max_seconds)):
                detail = "explicit bounded batch; archive NOT fully read or verified"
                break
            block = exact(compressed, 512)
            if block == b"\0"*512:
                if pending_pax:
                    raise ValueError('PAX timestamp has no following member')
                if exact(compressed, 512) != block:
                    raise ValueError("invalid tar end marker")
                # Drain padding and require the gzip CRC/trailer. No full reads.
                while tail := compressed.read(CHUNK):
                    if any(tail):
                        raise ValueError("nonzero data after tar end")
                expected = spec.get("checksum")
                if spec.get("size_bytes") is not None and reader.size != spec["size_bytes"]:
                    raise ValueError("compressed source length mismatch")
                if not expected:
                    detail = "complete bytes read but upstream checksum unavailable"
                    break
                algorithm, hexdigest = expected.split(":", 1)
                actual = {"md5": reader.md5, "sha256": reader.sha256}.get(algorithm)
                if actual is None or actual.hexdigest() != hexdigest:
                    raise ValueError("upstream checksum mismatch")
                status = "verified"
                break
            header = tarfile.TarInfo.frombuf(block, "utf-8", "strict")
            if header.type == tarfile.XHDTYPE:
                if pending_pax:
                    raise ValueError('repeated PAX timestamp header')
                read_mtime_pax(compressed, header.size)
                pending_pax = True
                continue
            pending_pax = False
            # No extraction, links, semantic PAX/sparse expansion or long names.
            if header.isdir() and header.size == 0:
                continue
            if not header.isreg() or header.size < 0:
                raise ValueError("unsupported tar type (links, PAX, sparse and special records forbidden)")
            locator = header.name
            match = member_pattern.fullmatch(locator)
            reason = None
            if not match:
                reason = "invalid_summary_path"
            elif header.size > store.task.config["max_member_bytes"]:
                reason = "member_exceeds_size_limit"
            previous = (batch.known.get(locator) if batch else None) or store.seen(source, locator)
            h = hashlib.sha256()
            events = []
            def collect(record):
                # Preserve the existing per-event rejection at parser ingress.
                if len(canonical(record)) > MAX_BYTES:
                    raise ValueError('normalized event exceeds 16 KiB')
                events.append(record)
                store.task.check_memory()
            with nullcontext() if previous or batch else store.member(source, locator):
                parser = None if reason or previous else SummaryParser(private_person(match[1]), source, locator, collect if batch else store.event,
                    min(store.task.config["max_events_per_member"], store.task.config["batch_events"]))
                remaining = header.size
                while remaining:
                    store.task.check_memory()
                    if max_seconds is not None and time.monotonic()-started >= max_seconds:
                        raise BudgetStop("bounded stream time reached inside member; replay required")
                    chunk = compressed.read(min(CHUNK, remaining))
                    if not chunk:
                        raise EOFError("truncated tar member")
                    remaining -= len(chunk)
                    h.update(chunk)
                    if parser:
                        try:
                            parser.feed(chunk)
                        except (ValueError, expat.ExpatError) as exc:
                            reason, parser = str(exc)[:256], None
                if parser:
                    try:
                        parser.feed(b"", final=True)
                    except (ValueError, expat.ExpatError) as exc:
                        reason = str(exc)[:256]
                if reason and not previous and batch is None:
                    store.db.execute("DELETE FROM events WHERE source=? AND locator=?", (source, locator))
                if header.size % 512:
                    exact(compressed, 512 - header.size % 512)
                if previous:
                    if previous[0] != h.hexdigest():
                        raise ValueError('member changed within frozen source')
                else:
                    if batch:
                        batch.add(locator, h.hexdigest(), 'rejected' if reason else 'observed',
                                  reason, [] if reason else events)
                    else:
                        store.finish_member(source, locator, h.hexdigest(), "rejected" if reason else "observed", reason)
            if previous:
                replayed += 1
            else:
                added += 1
            processed += 1
            if progress and processed % 100 == 0:
                if batch:
                    batch.flush()
                progress({"processed_this_pass": processed, "new_members_this_pass": added,
                    "replayed_members_this_pass": replayed,
                    "compressed_bytes": reader.size, "storage_bytes": store.task.used()})
        if batch:
            batch.flush()
    except BudgetStop as exc:
        status, detail = "budget_stopped", str(exc)
    except BaseException as exc:
        status, detail = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed", f"{type(exc).__name__}: {exc}"
        store.status(source, status, byte_count=reader.size, detail=detail)
        raise
    finally:
        compressed.close()
    store.status(source, status, byte_count=reader.size, sha256=reader.sha256.hexdigest() if status == "verified" else None, detail=detail)
    return store.report()
