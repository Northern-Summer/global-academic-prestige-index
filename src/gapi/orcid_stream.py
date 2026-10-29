"""Zero-raw-disk ORCID summary ingestion, with bounded SAX extraction and replay."""
from __future__ import annotations

import gzip
import hashlib
import re
import tarfile
import time
from xml.parsers import expat

from .career_rules import classify, institution_id, year
from .task import BudgetStop, digest

CHUNK = 65536
ORCID = r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]"
MEMBER = re.compile(rf"(?:\./)?summaries/\d{{3}}/({ORCID})\.xml")


def private_person(orcid):
    # Pseudonymous linkage key, never a public output field.
    return hashlib.sha256(("gapi-person/v1/orcid/" + orcid).encode()).hexdigest()


class SummaryParser:
    """Retains whitelisted affiliation fields only, never an XML/person tree."""
    def __init__(self, person, source, locator, emit, max_events=4096):
        self.person, self.source, self.locator, self.emit = person, source, locator, emit
        candidate = locator.rsplit("/", 1)[-1].removesuffix(".xml")
        self.expected_orcid = candidate if re.fullmatch(ORCID, candidate) else None
        self.path, self.current = [], None
        self.max_events, self.count = max_events, 0
        self.parser = expat.ParserCreate(namespace_separator="}")
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
        if not self.path and self.expected_orcid and (local != "record" or attrs.get("path") != "/" + self.expected_orcid):
            raise ValueError("ORCID XML root identity does not match frozen member locator")
        self.path.append(local)
        if len(self.path) > 64:
            raise ValueError("XML nesting exceeds bound")
        if local in ("education-summary", "employment-summary"):
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


def stream_archive(store, source, raw, *, max_members=None, max_seconds=None, progress=None):
    """Replay from the start; complete source validation is separate from member commits."""
    spec = store.task.config["sources"][source]
    store.start(source)
    reader = HashReader(raw)
    started, processed = time.monotonic(), 0
    compressed = gzip.GzipFile(fileobj=reader, mode="rb")
    status, detail = "provisional", None
    try:
        while True:
            if (max_members is not None and processed >= max_members) or (max_seconds is not None and time.monotonic()-started >= max_seconds):
                detail = "explicit bounded batch; archive NOT fully read or verified"
                break
            block = exact(compressed, 512)
            if block == b"\0"*512:
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
            # No extraction, links, PAX/sparse expansion or long-name allocation.
            if header.isdir() and header.size == 0:
                continue
            if not header.isreg() or header.size < 0:
                raise ValueError("unsupported tar type (links, PAX, sparse and special records forbidden)")
            locator = header.name
            match = MEMBER.fullmatch(locator)
            reason = None
            if not match:
                reason = "invalid_summary_path"
            elif header.size > store.task.config["max_member_bytes"]:
                reason = "member_exceeds_size_limit"
            previous = store.seen(source, locator)
            h = hashlib.sha256()
            with store.member(source, locator):
                parser = None if reason or previous else SummaryParser(private_person(match[1]), source, locator, store.event,
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
                if reason:
                    store.db.execute("DELETE FROM events WHERE source=? AND locator=?", (source, locator))
                if header.size % 512:
                    exact(compressed, 512 - header.size % 512)
                store.finish_member(source, locator, h.hexdigest(), "rejected" if reason else "observed", reason)
            processed += 1
            if progress and processed % 100 == 0:
                progress({"processed_this_pass": processed, "compressed_bytes": reader.size, "storage_bytes": store.task.used()})
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
