"""Deterministic, resumable institution-directory ingestion from OpenAlex."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .storage import require_external_cache, temporary_raw_cache


API = "https://api.openalex.org/institutions"
FIELDS = (
    "id,ror,display_name,display_name_acronyms,alternate_names,country_code,"
    "type,geo,works_count,cited_by_count,created_date,updated_date"
)
USER_AGENT = "GAPI-OpenData-Ingest/0.1 (https://github.com/global-academic-prestige-index)"


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(content)
    os.replace(temporary, path)


def _retry_after(error: HTTPError) -> float | None:
    value = error.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def _request_page(url: str, *, opener: Callable[..., Any] = urlopen, sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    for attempt in range(8):
        request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        try:
            with opener(request, timeout=60) as response:
                return json.loads(response.read())
        except HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 7:
                error.close()
                raise
            delay = _retry_after(error)
            error.close()
            if delay is not None and delay > 300:
                raise RuntimeError(f"OpenAlex asked clients to retry after {int(delay)} seconds; import is paused") from error
            sleep(delay if delay is not None else min(60.0, 2**attempt + random.random()))
        except (TimeoutError, URLError):
            if attempt == 7:
                raise
            sleep(min(60.0, 2**attempt + random.random()))
    raise RuntimeError("OpenAlex request retries exhausted")


def normalize_institution(record: dict[str, Any]) -> dict[str, Any]:
    openalex_id = record.get("id", "").rsplit("/", 1)[-1]
    if not openalex_id.startswith("I"):
        raise ValueError(f"invalid OpenAlex institution id: {record.get('id')!r}")
    return {
        "id": openalex_id,
        "openalex_url": record["id"],
        "ror": record.get("ror"),
        "name": record.get("display_name") or "",
        "acronyms": sorted(set(record.get("display_name_acronyms") or [])),
        "aliases": sorted(set(record.get("alternate_names") or [])),
        "country_code": record.get("country_code"),
        "type": record.get("type"),
        "geo": record.get("geo"),
        "works_count": record.get("works_count", 0),
        "cited_by_count": record.get("cited_by_count", 0),
        "created_date": record.get("created_date"),
        "updated_date": record.get("updated_date"),
    }


def _ingest_institutions_cached(
    output: Path,
    checkpoint_dir: Path,
    *,
    per_page: int = 200,
    max_pages: int | None = None,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
    on_page: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    if not 1 <= per_page <= 200:
        raise ValueError("per_page must be between 1 and 200")
    cache = checkpoint_dir
    cache.mkdir(parents=True, exist_ok=True)
    state_path = cache / "checkpoint.json"
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {
        "next_cursor": "*", "page": 0, "expected_count": None, "retrieved_at": None,
    }
    page_number = int(state["page"])
    expected_count = state.get("expected_count")

    while state.get("next_cursor") and (max_pages is None or page_number < max_pages):
        params = urlencode({"per-page": per_page, "select": FIELDS, "cursor": state["next_cursor"]})
        payload = _request_page(f"{API}?{params}", opener=opener, sleep=sleep)
        rows = payload.get("results")
        if not isinstance(rows, list):
            raise ValueError("OpenAlex response is missing a results array")
        meta = payload.get("meta", {})
        expected_count = expected_count if expected_count is not None else meta.get("count")
        page_number += 1
        page_path = cache / f"page-{page_number:06d}.json.gz"
        _atomic_write(page_path, gzip.compress(_json_bytes(rows), mtime=0))
        state = {
            "next_cursor": meta.get("next_cursor"),
            "page": page_number,
            "expected_count": expected_count,
            "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        _atomic_write(state_path, _json_bytes(state))
        if on_page:
            on_page(page_number, expected_count or 0)
        if state["next_cursor"]:
            sleep(1.0)

    complete = not state.get("next_cursor")
    records: list[dict[str, Any]] = []
    for page_path in sorted(cache.glob("page-*.json.gz")):
        with gzip.open(page_path, "rt", encoding="utf-8") as stream:
            records.extend(normalize_institution(row) for row in json.load(stream))
    records.sort(key=lambda row: (row["name"].casefold(), row["id"]))
    ids = [row["id"] for row in records]
    if len(ids) != len(set(ids)):
        raise ValueError("OpenAlex returned duplicate institution IDs across pages")
    if complete and expected_count is not None and len(records) != expected_count:
        raise ValueError(f"OpenAlex count mismatch: expected {expected_count}, got {len(records)}")

    payload = {
        "schema_version": "gapi-openalex-institutions/v1",
        "dataset_kind": "real/openalex-institution-directory",
        "source": "OpenAlex",
        "source_url": API,
        "source_license": "CC0-1.0",
        "retrieved_at": state.get("retrieved_at"),
        "complete": complete,
        "expected_count": expected_count,
        "record_count": len(records),
        "records": records,
    }
    encoded = _json_bytes(payload)
    output_bytes = gzip.compress(encoded, mtime=0) if output.suffix == ".gz" else encoded
    _atomic_write(output, output_bytes)
    manifest = {
        "schema_version": "gapi-data-manifest/v1",
        "dataset_kind": payload["dataset_kind"],
        "source_url": API,
        "source_license": "CC0-1.0",
        "retrieved_at": payload["retrieved_at"],
        "complete": complete,
        "expected_count": expected_count,
        "record_count": len(records),
        "output": output.name,
        "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
        "openalex_ids_unique": True,
    }
    _atomic_write(output.with_suffix(output.suffix + ".manifest.json"), _json_bytes(manifest))
    return manifest


def ingest_institutions(
    output: Path,
    *,
    checkpoint_dir: Path | None = None,
    per_page: int = 200,
    max_pages: int | None = None,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
    on_page: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Ingest to a temporary page cache unless resumable caching is requested."""
    if checkpoint_dir is not None:
        return _ingest_institutions_cached(
            output,
            require_external_cache(checkpoint_dir),
            per_page=per_page,
            max_pages=max_pages,
            opener=opener,
            sleep=sleep,
            on_page=on_page,
        )
    with temporary_raw_cache("gapi-openalex-") as temporary_cache:
        return _ingest_institutions_cached(
            output,
            Path(temporary_cache),
            per_page=per_page,
            max_pages=max_pages,
            opener=opener,
            sleep=sleep,
            on_page=on_page,
        )
