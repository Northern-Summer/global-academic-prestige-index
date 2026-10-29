"""Official, resumable award inventory. No guessed institution or person joins.

API pages are bounded in memory, discarded after normalization, and atomically
committed with their cursor/hash. The separate frozen source contract extends the
task registry without rewriting previous acquisition identities. This inventory
does not stand in for complete award coverage or create a composite score.
"""
from __future__ import annotations

import json
import hashlib
import math
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from .source_http import SourcePause
from .task import canonical, digest, BudgetStop

TABLES = """
CREATE TABLE IF NOT EXISTS award_sources(
 id TEXT PRIMARY KEY, spec TEXT NOT NULL, status TEXT NOT NULL,
 expected_total INTEGER, completed_rows INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS award_pages(
 source TEXT NOT NULL, offset INTEGER NOT NULL, rows INTEGER NOT NULL,
 sha256 TEXT NOT NULL, url TEXT NOT NULL, retrieved_at TEXT NOT NULL,
 PRIMARY KEY(source,offset));
CREATE TABLE IF NOT EXISTS official_awards(
 source TEXT NOT NULL, event_key TEXT NOT NULL, recipient_key TEXT NOT NULL,
 award_year INTEGER NOT NULL, category TEXT NOT NULL, recipient_type TEXT NOT NULL,
 portion TEXT NOT NULL, affiliations TEXT NOT NULL, page_offset INTEGER NOT NULL,
 PRIMARY KEY(source,event_key));
"""
CATEGORIES = {'Physics', 'Chemistry', 'Physiology or Medicine', 'Literature',
              'Peace', 'Economic Sciences'}
ENDPOINT = 'https://api.nobelprize.org/2.1/laureates'


def load_source(path: Path):
    spec = json.loads(path.read_text(encoding='utf-8'))
    if (not re.fullmatch(r'nobel-official-\d{8}', spec.get('id', ''))
            or spec.get('kind') != 'nobel-laureate-pages/v1'
            or spec.get('url') != ENDPOINT or type(spec.get('page_size')) is not int
            or not 1 <= spec['page_size'] <= 50
            or spec.get('license') != 'CC0-1.0-with-service-terms'
            or spec.get('rule_version') != 'official-nobel-affiliation-inventory/1'
            or spec.get('allow_institution_aggregates') is not True):
        raise ValueError('unsupported official award source contract')
    return spec


def text(value):
    result = value.get('en') if isinstance(value, dict) else None
    if not isinstance(result, str) or not result.strip() or len(result) > 500:
        raise ValueError('missing/invalid official English label')
    return result.strip()


def normalize_laureate(row):
    if not isinstance(row, dict) or not re.fullmatch(r'[1-9]\d*', str(row.get('id', ''))):
        raise ValueError('invalid official laureate identifier')
    recipient = 'nobel:' + str(row['id'])
    if 'orgName' in row and 'knownName' not in row:
        kind = 'institutional'
    elif 'knownName' in row and 'orgName' not in row:
        kind = 'personal'
    else:
        raise ValueError('ambiguous official recipient type')
    prizes = row.get('nobelPrizes')
    if not isinstance(prizes, list) or not 1 <= len(prizes) <= 10:
        raise ValueError('invalid official award list')
    seen = set()
    result = []
    for prize in prizes:
        if not isinstance(prize, dict): raise ValueError('invalid official award')
        category = text(prize.get('category'))
        date = str(prize.get('awardYear', ''))
        if category not in CATEGORIES or not re.fullmatch(r'(19|20)\d{2}', date):
            raise ValueError('unsupported award category/year')
        portion = prize.get('portion')
        if not isinstance(portion, str) or not re.fullmatch(r'1(?:/[1-9]\d?)?', portion):
            raise ValueError('invalid official award share')
        key = digest((recipient, date, category))
        if key in seen: raise ValueError('duplicate award in official recipient')
        seen.add(key)
        affiliation_rows = prize.get('affiliations', [])
        if not isinstance(affiliation_rows, list) or len(affiliation_rows) > 50:
            raise ValueError('invalid official affiliation list')
        affiliations = {}
        for affiliation in affiliation_rows:
            if not isinstance(affiliation, dict): raise ValueError('invalid affiliation')
            # These are award-time labels, not nameNow/cityNow or guessed ROR IDs.
            value = {'name': text(affiliation.get('name')),
                     'city': text(affiliation['city']) if affiliation.get('city') else None,
                     'country': text(affiliation['country']) if affiliation.get('country') else None}
            affiliations[digest(value)] = value
        result.append((key, recipient, int(date), category, kind, portion,
                       canonical([affiliations[k] for k in sorted(affiliations)]).decode()))
    return recipient, result


def ingest_nobel(store, http, spec, *, max_pages=100, max_seconds=300, progress=None):
    if not 1 <= max_pages <= 1000 or not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('invalid official award work budget')
    store.task.check(4 * 1024**2)
    store.db.executescript(TABLES)
    source = spec['id']
    identity = canonical(spec).decode()
    old = store.db.execute('SELECT spec,status FROM award_sources WHERE id=?', (source,)).fetchone()
    if old and old[0] != identity: raise ValueError('award source changed; frozen resume forbidden')
    if old and old[1] == 'enumerated-live-selection': return public_inventory(store)
    if not store.source_ready(source): return {'status': 'server-backoff-pending'}
    with store.db:
        store.db.execute('INSERT OR IGNORE INTO award_sources(id,spec,status) VALUES(?,?,?)',
                         (source, identity, 'partial'))
    started = time.monotonic()
    for _ in range(max_pages):
        if time.monotonic() - started >= max_seconds: break
        store.task.check(); store.task.check_memory()
        offset = store.db.execute('SELECT COALESCE(sum(rows),0) FROM award_pages WHERE source=?', (source,)).fetchone()[0]
        url = ENDPOINT + '?' + urlencode({'offset': offset, 'limit': spec['page_size'], 'sort': 'asc'})
        try:
            with http.within(max_seconds - (time.monotonic() - started)):
                data, sha = http.json(url, limit=2*1024**2)
            if not isinstance(data, dict) or not isinstance(data.get('meta'), dict):
                raise ValueError('invalid official award envelope')
            rows, meta = data.get('laureates'), data['meta']
            if not isinstance(rows, list) or len(rows) > spec['page_size']:
                raise ValueError('official award page exceeds contract')
            total = meta.get('count')
            if type(total) is not int or total < offset + len(rows) or total > 100000:
                raise ValueError('invalid official total count')
            if meta.get('offset') != offset or meta.get('limit') != spec['page_size']:
                raise ValueError('official award cursor mismatch')
            before = store.db.execute('SELECT expected_total FROM award_sources WHERE id=?', (source,)).fetchone()[0]
            if before is not None and before != total:
                raise ValueError('official award population changed during enumeration; new source id required')
            if not rows and offset < total: raise ValueError('premature empty award page')
            if len(rows) < spec['page_size'] and offset + len(rows) < total:
                raise ValueError('short nonterminal official award page')
            normalized = [normalize_laureate(row) for row in rows]
            recipients = [recipient for recipient, _events in normalized]
            if len(set(recipients)) != len(recipients): raise ValueError('duplicate recipient in page')
            for recipient in recipients:
                if store.db.execute('SELECT 1 FROM official_awards WHERE source=? AND recipient_key=? LIMIT 1', (source, recipient)).fetchone():
                    raise ValueError('recipient repeated across pages; source enumeration drift')
            retrieved = datetime.now(timezone.utc).isoformat()
            with store.member('award', source):
                for _recipient, events in normalized:
                    store.db.executemany('INSERT INTO official_awards VALUES(?,?,?,?,?,?,?,?,?)',
                        [(source, *event, offset) for event in events])
                store.db.execute('INSERT INTO award_pages VALUES(?,?,?,?,?,?)',
                    (source, offset, len(rows), sha, url, retrieved))
                complete = offset + len(rows) == total
                store.db.execute('UPDATE award_sources SET expected_total=?,completed_rows=?,status=? WHERE id=?',
                    (total, offset+len(rows), 'enumerated-live-selection' if complete else 'partial', source))
            if progress: progress({'stage': 'official-nobel', 'rows': offset+len(rows), 'expected': total,
                                   'raw_bytes_stored': 0, 'storage_bytes': store.task.used()})
            if complete: break
        except BudgetStop:
            raise
        except (OSError, ValueError, RuntimeError) as exc:
            if isinstance(exc, SourcePause): store.pause_source(source, exc.retry_at, str(exc))
            store.failure(source, 'offset:' + str(offset), str(exc))
            # Keep committed pages and exact cursor; no other source is stopped.
            return {'status': 'resumable-source-boundary', 'reason': str(exc)[:256],
                    'inventory': public_inventory(store)}
    return public_inventory(store)


def public_inventory(store):
    exists = store.db.execute("SELECT 1 FROM sqlite_master WHERE name='award_sources'").fetchone()
    if not exists: return None  # Previous frozen inputs retain byte-identical output.
    sources = list(store.db.execute('SELECT id,spec,status,expected_total,completed_rows FROM award_sources ORDER BY id'))
    if not sources: return None
    # Multiple versions must never double-count. One explicit source owns the view.
    if len(sources) != 1: raise ValueError('select a single frozen award version before export')
    source, spec_text, status, total, rows = sources[0]
    categories, types, affiliations = Counter(), Counter(), {}
    no_affiliation = multiple = 0
    index = hashlib.sha256()
    years = []
    for event in store.db.execute('''SELECT event_key,recipient_key,award_year,category,recipient_type,portion,affiliations,page_offset
        FROM official_awards WHERE source=? ORDER BY event_key''', (source,)):
        index.update(canonical(event))
        _key, _recipient, award_year, category, recipient_type, portion, labels, _page = event
        categories[category] += 1; types[recipient_type] += 1; years.append(award_year)
        labels = json.loads(labels)
        no_affiliation += not bool(labels); multiple += len(labels) > 1
        for label in labels:
            key = digest(label)
            item = affiliations.setdefault(key, {**label, 'official_label_id': key, 'award_mentions': 0,
                                                  'categories': Counter(), 'identity_status': 'official-label-not-canonical-institution'})
            item['award_mentions'] += 1; item['categories'][category] += 1
    return {'source': json.loads(spec_text), 'source_sha256': digest(json.loads(spec_text)),
        'status': status, 'expected_recipients': total, 'processed_recipients': rows,
        'award_events': sum(categories.values()), 'categories': dict(sorted(categories.items())),
        'recipient_types': dict(sorted(types.items())), 'year_range': [min(years), max(years)] if years else None,
        'events_without_affiliation': no_affiliation, 'events_with_multiple_affiliations': multiple,
        'affiliations': [affiliations[k] for k in sorted(affiliations)],
        'normalized_award_index_sha256': index.hexdigest(),
        'page_index_sha256': digest(list(store.db.execute('SELECT offset,rows,sha256,url,retrieved_at FROM award_pages WHERE source=? ORDER BY offset', (source,)))),
        'coverage_note': 'Nobel official API live offset enumeration; not all academic awards or a frozen upstream census. Labels may denote the same institution.',
        'scoring_status': 'inventory-only; canonical affiliation mapping and other award families pending; no composite imputation',
        'individual_records_published': False}
