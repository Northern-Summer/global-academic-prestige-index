"""One-pass, zero-raw-storage feasibility census of a pinned career derivative.

Counts rows and minimal normalized affiliation bytes, not people deduplicated
across the source. No careers are promoted and no personal identifiers exported.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
import sqlite3
from pathlib import Path
import time
import zlib

from .career_rules import classify, institution_id, valid_orcid, year
from .easy_orcid import SOURCE_ID, SOURCE_URL, MAX_LINE_BYTES, _lines
from .source_http import StageDeadline
from .task import canonical, digest, BudgetStop


ARCHIVES={SOURCE_ID:(SOURCE_URL,662112167,'e18decdd695473e5c6b040f699e63d04'),
          'easy-orcid-2023.6-full':('https://zenodo.org/records/13333068/files/records.jsonl.gz?download=1',920836363,'8829b83ba498b901fc7a1d7320040520')}


def load_spec(path: Path):
    spec = json.loads(path.read_text(encoding='utf-8'))
    known=ARCHIVES
    if (spec.get('id') not in known or (spec.get('url'),spec.get('size_bytes'),spec.get('md5')) != known.get(spec.get('id'))
            or spec.get('kind') != 'easy-orcid-jsonl-gzip/v1'
            or spec.get('version') != '2023.6'
            or spec.get('publication_date') != '2024-08-17'
            or spec.get('derived_from') != '10.23640/07243.24204912.v1'
            or spec.get('license') != 'CC0-1.0'
            or spec.get('independent_corroboration') is not False):
        raise ValueError('unsupported or changed Easy ORCID source')
    return spec


class HashStream:
    def __init__(self, response, expected_size, *, task=None, http=None, url=None, md5=None):
        self.response, self.headers = response, response.headers
        if self.headers.get('Content-Encoding', 'identity').lower() != 'identity':
            raise ValueError('archive transport representation must be identity')
        length=self.headers.get('Content-Length')
        if length is not None and (not str(length).isdigit() or int(length)!=expected_size):
            raise ValueError('archive response length differs from frozen size')
        if http is not None and hasattr(http,'open_range'):
            from .dump_discovery import FeedReader
            initial_headers=self.headers
            self.response=FeedReader(response,task,expected_bytes=expected_size,
                max_reconnects=8,max_stalled_reconnects=3,
                resume=lambda offset:http.open_range(url,offset,expected_size,initial_headers,expected_md5=md5))
        self.size, self.expected_size = 0, expected_size
        self.md5 = hashlib.md5(usedforsecurity=False)
        self.sha256 = hashlib.sha256()

    def read(self, size):
        chunk = self.response.read(min(size, 65536))
        self.size += len(chunk)
        if self.size > self.expected_size:
            raise ValueError('archive exceeds frozen compressed size')
        self.md5.update(chunk)
        self.sha256.update(chunk)
        return chunk

    def close(self):self.response.close()


def affiliation_projection(record):
    """Minimal measurement projection, using shared classification owners.

    RORs in this derivative may be publisher-grounded, not ORCID declarations;
    their presence is measured, never elevated to independently verified identity.
    """
    rows = []
    for field, kind in (('educations', 'education'), ('employments', 'employment')):
        items = record.get(field) or []
        if not isinstance(items, list) or len(items) > 4096:
            raise ValueError('affiliation list exceeds measurement bound')
        for item in items:
            if not isinstance(item, dict):
                raise ValueError('invalid affiliation shape')
            role, refs = item.get('role') or '', item.get('xrefs') or {}
            if not isinstance(role, str) or len(role) > 2048 or not isinstance(refs, dict):
                raise ValueError('invalid role or references')
            normalized_kind, stage = classify(kind, role, completed_only=True)
            inst = None
            for authority in ('ror', 'grid', 'ringgold'):
                value = refs.get(authority)
                if isinstance(value, str):
                    inst = institution_id(value, authority)
                    if inst:
                        break
            dates = []
            for name in ('start', 'end'):
                date = item.get(name) or {}
                if not isinstance(date, dict):
                    raise ValueError('invalid affiliation date')
                dates.append(year(date.get('year')))
            rows.append([normalized_kind, stage, inst, *dates, role])
    return rows


def census(task, http, spec, *, max_records=None, max_seconds=1800, progress=None, consume=None):
    if max_records is not None and (type(max_records) is not int or max_records < 1):
        raise ValueError('record bound must be positive or absent')
    if type(max_seconds) not in (int, float) or not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('time bound must be finite and positive')
    started = time.monotonic()
    counts, kinds, stages, missing = Counter(), Counter(), Counter(), Counter()
    stream, complete, error = None, False, None
    def report():
        return {'source': spec['id'], 'spec_sha256': digest(spec),
                'status': 'verified-source-census' if complete else 'partial-unverified-census',
                'source_complete': complete, 'global_complete': False,
                'scope': spec['scope'], 'independent_corroboration': False,
                'compressed_bytes': stream.size if stream else 0,
                'expected_compressed_bytes': spec['size_bytes'],
                'compressed_md5': stream.md5.hexdigest() if stream else None,
                'compressed_sha256': stream.sha256.hexdigest() if stream else None,
                'counts': dict(counts), 'kind_counts': dict(kinds),
                'stage_counts': dict(stages), 'missing_fields': dict(missing),
                'elapsed_seconds': round(time.monotonic()-started, 3),
                'transport_reconnects':getattr(stream.response,'reconnects',0) if stream else 0,
                'error': error, 'raw_saved': False, 'personal_records_saved': False,
                'measurement': 'source rows, not distinct people; projected payload bytes exclude database indexes and evidence'}
    try:
        with http.within(max_seconds):
            with http.open(spec['url'], 'application/octet-stream',
                           headers={'Accept-Encoding': 'identity'}) as response:
                stream = HashStream(response, spec['size_bytes'],task=task,http=http,
                                    url=spec['url'],md5=spec['md5'])
                for raw in _lines(stream, limit=MAX_LINE_BYTES,
                                  deadline=started+max_seconds, task=task, gzip_stream=True):
                    counts['source_rows'] += 1
                    if raw is None:
                        counts['oversized_rows'] += 1
                    else:
                        try:
                            record = json.loads(raw)
                            if not isinstance(record, dict) or not valid_orcid(record.get('orcid')):
                                raise ValueError('invalid source identifier')
                            rows = affiliation_projection(record)
                        except (ValueError, TypeError, UnicodeDecodeError, RecursionError):
                            counts['invalid_rows'] += 1
                        else:
                            if consume:
                                consume(counts['source_rows'], record['orcid'], rows,
                                        hashlib.sha256(raw).digest())
                            counts['valid_rows'] += 1
                            counts['affiliations'] += len(rows)
                            counts['rows_with_education_and_employment'] += bool(record.get('educations') and record.get('employments'))
                            if rows:
                                minimal = canonical(rows)
                                counts['minimal_affiliation_json_bytes'] += len(minimal)
                                counts['minimal_affiliation_zlib_bytes'] += len(zlib.compress(minimal, 6))
                            for kind, stage, inst, start, end, role in rows:
                                kinds[kind] += 1
                                stages[stage or 'unknown'] += 1
                                missing['institution_identifier'] += not bool(inst)
                                missing['stage'] += not bool(stage)
                                missing['required_year'] += not bool(start if kind == 'employment' else end)
                    if progress and counts['source_rows'] % 10000 == 0:
                        progress(report())
                    if max_records is not None and counts['source_rows'] >= max_records:
                        break
                else:
                    if stream.size != spec['size_bytes'] or stream.md5.hexdigest() != spec['md5']:
                        raise ValueError('frozen archive length/checksum mismatch')
                    complete = True
    except (OSError, EOFError, ValueError, RuntimeError, BudgetStop, sqlite3.Error) as exc:
        error = type(exc).__name__ + ': ' + str(exc)[:256]
    finally:
        if stream:stream.close()
    return report()
