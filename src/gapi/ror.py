"""Fetch and normalize the versioned ROR organization registry dump."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import random
import re
import time
import zipfile
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .storage import require_external_cache, temporary_raw_cache


ZENODO_API = "https://zenodo.org/api/records/?communities=ror-data&sort=mostrecent&size=1"
USER_AGENT = "GAPI-ROR-Registry-Ingest/0.1 (https://github.com/global-academic-prestige-index)"


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


def _request(url: str, *, opener: Callable[..., Any] = urlopen, sleep: Callable[[float], None] = time.sleep) -> bytes:
    for attempt in range(8):
        request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json, application/zip"})
        try:
            with opener(request, timeout=120) as response:
                return response.read()
        except HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 7:
                error.close()
                raise
            delay = _retry_after(error)
            error.close()
            if delay is not None and delay > 300:
                raise RuntimeError(f"ROR source asked clients to retry after {int(delay)} seconds; import is paused") from error
            sleep(delay if delay is not None else min(60.0, 2**attempt + random.random()))
        except (TimeoutError, URLError):
            if attempt == 7:
                raise
            sleep(min(60.0, 2**attempt + random.random()))
    raise RuntimeError("ROR request retries exhausted")


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    ror_url = record.get("id", "")
    ror_id = ror_url.rstrip("/").rsplit("/", 1)[-1]
    if not re.fullmatch(r"0[a-hj-km-np-z0-9]{6}[0-9]{2}", ror_id):
        raise ValueError(f"invalid ROR identifier: {ror_url!r}")
    names = record.get("names", [])
    primary = next((name["value"] for name in names if "ror_display" in name.get("types", [])), None)
    primary = primary or next((name["value"] for name in names if "label" in name.get("types", [])), None)
    primary = primary or (names[0]["value"] if names else ror_id)
    aliases = sorted({name["value"] for name in names if "alias" in name.get("types", []) and name.get("value") != primary})
    acronyms = sorted({name["value"] for name in names if "acronym" in name.get("types", [])})
    locations = record.get("locations", [])
    country_codes = sorted({location.get("geonames_details", {}).get("country_code") for location in locations if location.get("geonames_details", {}).get("country_code")})
    primary_location = next((location.get("geonames_details", {}) for location in locations if location.get("geonames_details")), {})
    admin = record.get("admin", {})
    external_ids = {
        item.get("type"): item.get("all", [])
        for item in record.get("external_ids", [])
        if item.get("type") and item.get("all")
    }
    return {
        "id": ror_id,
        "ror": f"https://ror.org/{ror_id}",
        "name": primary,
        "aliases": aliases,
        "acronyms": acronyms,
        "country_code": primary_location.get("country_code"),
        "country_codes": country_codes,
        "type": ", ".join(sorted(set(record.get("types", [])))) or "Unspecified",
        "types": sorted(set(record.get("types", []))),
        "geo": {key: primary_location.get(key) for key in ("city", "country_name", "continent_name", "latitude", "longitude") if primary_location.get(key) is not None},
        "status": record.get("status"),
        "updated_date": admin.get("last_modified"),
        "established": record.get("established"),
        "external_ids": external_ids,
    }


def _ingest_ror_cached(
    output: Path,
    cache: Path,
    *,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    cache.mkdir(parents=True, exist_ok=True)
    metadata = json.loads(_request(ZENODO_API, opener=opener, sleep=sleep))
    hits = metadata.get("hits", {}).get("hits", [])
    if len(hits) != 1:
        raise ValueError("ROR Zenodo endpoint did not return exactly one latest record")
    release = hits[0]
    release_meta = release.get("metadata", {})
    license_id = (release_meta.get("license") or {}).get("id")
    if license_id != "cc-zero":
        raise ValueError(f"expected CC0 ROR release, got license {license_id!r}")
    files = release.get("files", [])
    archive = next((item for item in files if item.get("key", "").endswith("-ror-data.zip")), None)
    if not archive:
        raise ValueError("latest ROR release does not include a ROR data ZIP")

    archive_path = cache / archive["key"]
    if not archive_path.exists():
        content = _request(archive["links"]["self"], opener=opener, sleep=sleep)
        checksum_kind, expected_checksum = archive["checksum"].split(":", 1)
        digest = hashlib.new(checksum_kind, content).hexdigest()
        if digest != expected_checksum:
            raise ValueError(f"ROR archive {checksum_kind} mismatch: expected {expected_checksum}, got {digest}")
        _atomic_write(archive_path, content)
    archive_bytes = archive_path.read_bytes()
    checksum_kind, expected_checksum = archive["checksum"].split(":", 1)
    if hashlib.new(checksum_kind, archive_bytes).hexdigest() != expected_checksum:
        raise ValueError("cached ROR archive checksum mismatch")

    with zipfile.ZipFile(archive_path) as bundle:
        json_members = [name for name in bundle.namelist() if name.endswith("-ror-data.json")]
        if len(json_members) != 1:
            raise ValueError(f"expected one ROR JSON data file in archive, found {len(json_members)}")
        with bundle.open(json_members[0]) as stream:
            source_records = json.load(stream)
    if not isinstance(source_records, list) or not source_records:
        raise ValueError("ROR data file must contain a non-empty JSON array")
    records = [normalize_record(record) for record in source_records]
    records.sort(key=lambda row: (row["name"].casefold(), row["id"]))
    ids = [row["id"] for row in records]
    if len(ids) != len(set(ids)):
        raise ValueError("ROR snapshot contains duplicate organization identifiers")

    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    dataset = {
        "schema_version": "gapi-institution-directory/v1",
        "dataset_kind": "real/ror-institution-directory",
        "source": {
            "name": "Research Organization Registry (ROR)",
            "version": release_meta.get("version"),
            "publication_date": release_meta.get("publication_date"),
            "record_url": release.get("doi_url"),
            "archive_url": archive["links"]["self"],
            "archive_checksum": archive["checksum"],
            "license": "CC0-1.0",
        },
        "retrieved_at": retrieved_at,
        "complete": True,
        "record_count": len(records),
        "records": records,
    }
    encoded = json.dumps(dataset, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    output_bytes = gzip.compress(encoded, mtime=0) if output.suffix == ".gz" else encoded
    _atomic_write(output, output_bytes)
    manifest = {
        "schema_version": "gapi-data-manifest/v1",
        "dataset_kind": dataset["dataset_kind"],
        "source": dataset["source"],
        "publication_date": release_meta.get("publication_date"),
        "retrieved_at": retrieved_at,
        "complete": True,
        "record_count": len(records),
        "output": output.name,
        "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
        "source_archive_sha256": hashlib.sha256(archive_bytes).hexdigest(),
        "ror_ids_unique": True,
    }
    _atomic_write(output.with_suffix(output.suffix + ".manifest.json"), json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"))
    return manifest


def ingest_ror(
    output: Path,
    *,
    cache_dir: Path | None = None,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Import ROR while keeping raw archives out of the project by default."""
    if cache_dir is not None:
        return _ingest_ror_cached(output, require_external_cache(cache_dir), opener=opener, sleep=sleep)
    with temporary_raw_cache("gapi-ror-") as temporary_cache:
        return _ingest_ror_cached(output, Path(temporary_cache), opener=opener, sleep=sleep)
