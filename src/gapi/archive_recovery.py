"""Queue bounded follow-up for rejected annual members, without promoting facts.

The member ledger retains each rejection and its source digest. The existing
ORCID API discovery queue owns subsequent fetches and backoff; this module
creates no second retry ledger, independent evidence or ranking qualification.
"""
import hashlib
import math
import re
import time

from .career_rules import valid_orcid
from .career_store import orcid_archive_sources
from .orcid_stream import summary_member_pattern
from .queue_storage import pack_row
from .task import digest


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
    identity = digest({'source':spec,'rules':store.task.config['rules_version']})
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
