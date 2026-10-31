"""Bounded normalized dump landing; never persists entity bodies or raw lines.

The discovery feed owns source integrity. Until its complete checksum passes,
these records are isolated from events, members, ranking and candidate progress.
"""
from datetime import datetime, timezone
import json
import math
import time

from .global_sources import wikidata_events
from .task import canonical


def initialize(store):
    store.db.execute('''CREATE TABLE IF NOT EXISTS dump_landing(
        feed TEXT NOT NULL, locator TEXT NOT NULL, sha256 TEXT NOT NULL,
        payload TEXT NOT NULL, PRIMARY KEY(feed,locator))''')
    store.db.commit()


def stage_entity(store, feed, entity, checksum, *, retrieved_at=None):
    """Caller owns commit, so landing and scan checkpoint can commit together."""
    if not store.db.execute('SELECT 1 FROM discovery_feeds WHERE id=?', (feed,)).fetchone():
        raise ValueError('unknown frozen discovery feed')
    events = []
    retrieved_at = retrieved_at or datetime.now(timezone.utc).isoformat()
    for event in wikidata_events(entity, checksum):
        if len(events) >= store.task.config['batch_events']:
            raise ValueError('dump entity exceeds bounded event transaction')
        event.update(retrieved_at=retrieved_at, transport='frozen-dump/v1', dump_feed=feed)
        if len(canonical(event)) > 16384:
            raise ValueError('normalized event exceeds 16 KiB')
        events.append(event)
    locator = f"{entity['id']}@{entity['lastrevid']}"
    payload = canonical(events).decode()
    store.task.check(len(payload.encode()) * 3 + 65536)
    previous = store.db.execute('SELECT sha256 FROM dump_landing WHERE feed=? AND locator=?',
                                (feed, locator)).fetchone()
    if previous and previous[0] != checksum:
        raise ValueError('dump member changed within frozen source')
    store.db.execute('INSERT OR IGNORE INTO dump_landing VALUES(?,?,?,?)',
                     (feed, locator, checksum, payload))
    return len(events)


def promote_one(store, feed):
    """Move at most one bounded member after full-source verification.

    Re-running is safe; an interrupted member rolls back both insert and delete.
    Promotion does not count as a second independent evidence source.
    """
    verified = store.db.execute('SELECT status FROM discovery_feeds WHERE id=?', (feed,)).fetchone()
    if not verified or verified[0] != 'verified':
        raise ValueError('dump promotion requires complete source checksum verification')
    row = store.db.execute('SELECT locator,sha256,payload FROM dump_landing WHERE feed=? ORDER BY locator LIMIT 1',
                           (feed,)).fetchone()
    if row is None:
        return False
    locator, checksum, payload = row
    with store.member('wikidata', locator):
        old = store.seen('wikidata', locator)
        if old is None:
            for event in json.loads(payload):
                store.event(event)
            store.finish_member('wikidata', locator, checksum)
        # A validated API snapshot at the same revision already owns its events.
        # Keep its evidence identity; the dump does not replace or duplicate it.
        store.db.execute('DELETE FROM dump_landing WHERE feed=? AND locator=?', (feed, locator))
    return True


def promote_batch(store, feed, *, max_members=1000, max_seconds=60):
    """Bounded resumable promotion, under CareerStore's exclusive writer lock."""
    if type(max_members) is not int or not 1 <= max_members <= 10000:
        raise ValueError('promotion member limit must be between 1 and 10000')
    if not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('promotion time limit must be positive')
    verified=store.db.execute('SELECT status FROM discovery_feeds WHERE id=?',(feed,)).fetchone()
    if not verified or verified[0] != 'verified':
        raise ValueError('dump promotion requires complete source checksum verification')
    store.start('wikidata')
    started=time.monotonic()
    promoted=0
    while promoted < max_members and time.monotonic()-started < max_seconds:
        store.task.check_memory()
        if not promote_one(store,feed):break
        promoted+=1
    pending=store.db.execute('SELECT count(*) FROM dump_landing WHERE feed=?',(feed,)).fetchone()[0]
    return {'feed':feed,'promoted_members':promoted,'pending_members':pending,
            'promotion_complete':pending==0,'independent_source_count_added':0}
