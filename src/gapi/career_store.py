"""Transactional, size-limited normalized landing store. Never stores raw bodies.

SQLite owns ingest/checkpoints because max_page_count bounds durable database growth.
DuckDB owns bounded analysis/Parquet export; it never downloads raw input or spills.
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from contextlib import contextmanager

from .task import Task, canonical, digest

DDL = """
CREATE TABLE IF NOT EXISTS runs(
 source TEXT PRIMARY KEY, identity TEXT NOT NULL, status TEXT NOT NULL,
 bytes INTEGER DEFAULT 0, members INTEGER DEFAULT 0, sha256 TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS members(
 source TEXT NOT NULL, locator TEXT NOT NULL, sha256 TEXT NOT NULL,
 status TEXT NOT NULL, reason TEXT, PRIMARY KEY(source,locator));
CREATE TABLE IF NOT EXISTS events(
 id TEXT PRIMARY KEY, person TEXT NOT NULL, institution TEXT, kind TEXT NOT NULL,
 stage TEXT, year_start INTEGER, year_end INTEGER, role TEXT, country TEXT,
 source TEXT NOT NULL, locator TEXT NOT NULL, status TEXT NOT NULL,
 payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS events_person ON events(person);
CREATE INDEX IF NOT EXISTS events_member ON events(source,locator);
CREATE TABLE IF NOT EXISTS entities(
 id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT, stable INTEGER NOT NULL,
 payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS discovery(
 source TEXT NOT NULL, identifier TEXT NOT NULL, query_sha256 TEXT NOT NULL,
 response_sha256 TEXT NOT NULL, PRIMARY KEY(source,identifier));
CREATE TABLE IF NOT EXISTS discovery_pages(
 source TEXT NOT NULL, cursor_before TEXT NOT NULL, cursor_after TEXT NOT NULL,
 query_sha256 TEXT NOT NULL, response_sha256 TEXT NOT NULL, retrieved_at TEXT NOT NULL,
 row_count INTEGER NOT NULL, PRIMARY KEY(source,cursor_before));
CREATE TABLE IF NOT EXISTS discovery_retries(
 source TEXT NOT NULL, identifier TEXT NOT NULL, attempts INTEGER NOT NULL,
 next_attempt_at REAL NOT NULL, reason TEXT NOT NULL,
 PRIMARY KEY(source,identifier));
CREATE TABLE IF NOT EXISTS source_pauses(
 source TEXT PRIMARY KEY, retry_at REAL NOT NULL, reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS request_reservations(
 id INTEGER PRIMARY KEY, source TEXT NOT NULL, reserved_at REAL NOT NULL,
 requests INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS requests_window ON request_reservations(source,reserved_at);
CREATE TABLE IF NOT EXISTS member_parts(
 source TEXT NOT NULL, locator TEXT NOT NULL, part TEXT NOT NULL,
 url TEXT NOT NULL, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL,
 retrieved_at TEXT NOT NULL, PRIMARY KEY(source,locator,part));
CREATE TABLE IF NOT EXISTS discovery_feeds(
 id TEXT PRIMARY KEY, spec TEXT NOT NULL, status TEXT NOT NULL,
 compressed_bytes INTEGER NOT NULL DEFAULT 0, lines INTEGER NOT NULL DEFAULT 0,
 candidates INTEGER NOT NULL DEFAULT 0, rejected INTEGER NOT NULL DEFAULT 0,
 sha256 TEXT, detail TEXT, prefix_sha256 TEXT);
CREATE TABLE IF NOT EXISTS institution_aliases(
 alias TEXT PRIMARY KEY, institution TEXT NOT NULL, provenance TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS official_checks(
 event_id TEXT NOT NULL, url TEXT NOT NULL, content_sha256 TEXT NOT NULL,
 rule_version TEXT NOT NULL, retrieved_at TEXT NOT NULL, status TEXT NOT NULL,
 origin_relation TEXT NOT NULL, field_hash TEXT NOT NULL,
 PRIMARY KEY(event_id,url));
CREATE TABLE IF NOT EXISTS official_profile_subjects(
 url TEXT NOT NULL, content_sha256 TEXT NOT NULL, rule_version TEXT NOT NULL,
 person TEXT NOT NULL, PRIMARY KEY(url,content_sha256,rule_version));
CREATE TABLE IF NOT EXISTS failures(
 source TEXT NOT NULL, locator TEXT NOT NULL, reason TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(source,locator,reason));
"""


class CareerStore:
    def __init__(self, task: Task):
        self.task = task
        task.require_frozen()
        task.check(4 * 1024**2)
        self.lock = (task.root / "writer.lock").open("a+b")
        try:
            self.lock.seek(0)
            if self.lock.read(1) == b"":
                self.lock.write(b"0")
                self.lock.flush()
            self.lock.seek(0)
            import os
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise RuntimeError("another task writer is active")
        self.db = sqlite3.connect(task.root / "careers.sqlite", timeout=0)
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA temp_store=MEMORY")
        self.db.execute("PRAGMA cache_size=-8192")
        self.db.execute("PRAGMA hard_heap_limit=134217728")
        # Include existing runtime and auxiliary files in the cap, and leave journal reserve.
        size = (task.root / "careers.sqlite").stat().st_size
        available = task.config["budget_bytes"] - task.used() + size - task.config["reserve_bytes"]
        self.db.execute(f"PRAGMA max_page_count={max(32, available // 4096)}")
        self.db.executescript(DDL)
        self.db.commit()

    def close(self):
        self.db.close()
        self.lock.close()

    def start(self, source: str) -> None:
        self.task.require_frozen()
        spec = self.task.config["sources"][source]
        identity = digest({"source": spec, "rules": self.task.config["rules_version"]})
        old = self.db.execute("SELECT identity FROM runs WHERE source=?", (source,)).fetchone()
        if old and old[0] != identity:
            raise ValueError("source identity changed; resume forbidden")
        self.db.execute("INSERT OR IGNORE INTO runs(source,identity,status) VALUES(?,?,?)", (source, identity, "provisional"))
        self.db.commit()

    def status(self, source: str, status: str, *, byte_count=0, sha256=None, detail=None):
        self.db.execute("UPDATE runs SET status=?, bytes=?, sha256=?, detail=? WHERE source=?",
                        (status, byte_count, sha256, str(detail)[:1024] if detail else None, source))
        self.db.commit()

    def failure(self, source, locator, reason):
        self.task.check(64*1024)
        self.db.execute("INSERT INTO failures(source,locator,reason) VALUES(?,?,?) ON CONFLICT(source,locator,reason) DO UPDATE SET attempts=attempts+1",
                        (source,locator,str(reason)[:512]))
        self.db.commit()

    def seen(self, source, locator):
        return self.db.execute("SELECT sha256,status FROM members WHERE source=? AND locator=?", (source, locator)).fetchone()

    def pending(self, command, limit=200, *, now=None):
        sources={'ingest-wikidata':'wikidata','ingest-orcid-api':'orcid-public-api'}
        if command not in sources or not 1<=limit<=1000:
            raise ValueError('invalid pending source/batch limit')
        query='''SELECT d.identifier FROM discovery d
            LEFT JOIN discovery_retries r ON r.source=d.source AND r.identifier=d.identifier
            WHERE d.source=? AND COALESCE(r.next_attempt_at,0)<=?
            AND NOT EXISTS(SELECT 1 FROM members m WHERE m.source=?
            AND (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%'))'''
        parameters=(command,time.time() if now is None else now,sources[command])
        # Reserve a quarter of a normal batch for due retries so a continuing
        # stream of fresh candidates cannot starve recovered records forever.
        due=[r[0] for r in self.db.execute(query+''' AND r.identifier IS NOT NULL
            ORDER BY r.next_attempt_at,d.identifier LIMIT ?''',(*parameters,limit))]
        reserved=min(len(due),max(1,limit//4))
        fresh=[r[0] for r in self.db.execute(query+''' AND r.identifier IS NULL
            ORDER BY d.identifier LIMIT ?''',(*parameters,limit-reserved))]
        return due[:reserved]+fresh+due[reserved:limit-len(fresh)]

    def defer(self, command, identifier, reason, *, now=None):
        """Persist finite backoff without dropping evidence or blocking fresh candidates."""
        if command not in ('ingest-wikidata','ingest-orcid-api'):
            raise ValueError('unsupported retry command')
        self.task.check(64*1024)
        row=self.db.execute('SELECT attempts FROM discovery_retries WHERE source=? AND identifier=?',
                            (command,identifier)).fetchone()
        attempts=(row[0] if row else 0)+1
        delay=min(21600,60*2**min(attempts-1,9))
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO discovery_retries VALUES(?,?,?,?,?)',
                (command,identifier,attempts,(time.time() if now is None else now)+delay,str(reason)[:512]))

    def queue_page(self, feed, command, before, after, query_hash, response_hash, identifiers, row_count):
        """The candidate queue and its discovery cursor share one commit boundary."""
        self.task.check(1024**2)
        with self.db:
            self.db.executemany('INSERT OR IGNORE INTO discovery VALUES(?,?,?,?)',
                [(command,value,query_hash,response_hash) for value in sorted(identifiers)])
            self.db.execute('INSERT INTO discovery_pages VALUES(?,?,?,?,?,?,?)',
                (feed,before,after,query_hash,response_hash,datetime.now(timezone.utc).isoformat(),row_count))

    def source_ready(self, source, *, now=None):
        row=self.db.execute('SELECT retry_at FROM source_pauses WHERE source=?',(source,)).fetchone()
        return not row or row[0]<=(time.time() if now is None else now)

    def pause_source(self, source, retry_at, reason):
        self.task.check(64*1024)
        with self.db:
            self.db.execute('''INSERT INTO source_pauses VALUES(?,?,?) ON CONFLICT(source)
                DO UPDATE SET retry_at=max(retry_at,excluded.retry_at),reason=excluded.reason''',
                (source,retry_at,str(reason)[:512]))

    @contextmanager
    def request_budget(self, source, maximum, capacity, http):
        """Reserve worst-case retries before a member transaction/network request.

        A crash retains the conservative reservation. A completed/rolled-back
        member settles to attempted requests. This bounds this task's rolling
        day, not traffic sent by other clients sharing its IP address.
        """
        from .source_http import SourcePause
        now=time.time()
        used=self.db.execute('''SELECT COALESCE(sum(requests),0),min(reserved_at) FROM request_reservations
            WHERE source=? AND reserved_at>? AND requests>0''',(source,now-86400)).fetchone()
        if used[0]+maximum>capacity:
            raise SourcePause(429,(used[1] or now)+86400,'local rolling-day request allowance exhausted')
        self.task.check(64*1024)
        with self.db:
            reservation=self.db.execute('INSERT INTO request_reservations(source,reserved_at,requests) VALUES(?,?,?)',
                (source,now,maximum)).lastrowid
        before=getattr(http,'requests_count',None)
        try:yield
        finally:
            # Never assume an uninstrumented transport made no requests.
            actual=maximum if before is None else http.requests_count-before
            if not 0<=actual<=maximum:
                raise RuntimeError('transport exceeded its reserved request budget')
            with self.db:self.db.execute('UPDATE request_reservations SET requests=? WHERE id=?',(actual,reservation))

    @contextmanager
    def member(self, source, locator):
        self.task.check(self.task.config["max_member_bytes"] * 3)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.task.check(1024**2)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def event(self, record: dict):
        if len(canonical(record)) > 16384:
            raise ValueError("normalized event exceeds 16 KiB")
        key = digest({k: record.get(k) for k in ("person", "institution", "kind", "stage", "year_start", "year_end", "source", "locator", "put_code")})
        self.db.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            key, record["person"], record.get("institution"), record["kind"], record.get("stage"),
            record.get("year_start"), record.get("year_end"), record.get("role"), record.get("country"),
            record["source"], record["locator"], record["status"], canonical(record).decode()))

    def finish_member(self, source, locator, sha256, status="observed", reason=None):
        old = self.seen(source, locator)
        if old and old[0] != sha256:
            raise ValueError("member changed within frozen source")
        self.db.execute("INSERT OR IGNORE INTO members VALUES(?,?,?,?,?)", (source, locator, sha256, status, reason))
        self.db.execute("UPDATE runs SET members=(SELECT count(*) FROM members WHERE source=?) WHERE source=?", (source, source))

    def report(self):
        return {
            "sources": [dict(zip(("source", "identity", "status", "bytes", "members", "sha256", "detail"), row)) for row in self.db.execute("SELECT * FROM runs ORDER BY source")],
            "people": self.db.execute("SELECT count(DISTINCT person) FROM events").fetchone()[0],
            "events": self.db.execute("SELECT count(*) FROM events").fetchone()[0],
            "countries": [row[0] for row in self.db.execute("SELECT DISTINCT country FROM events WHERE country IS NOT NULL ORDER BY country")],
            "event_status": dict(self.db.execute("SELECT status,count(*) FROM events GROUP BY status")),
            "member_status": dict(self.db.execute("SELECT status,count(*) FROM members GROUP BY status")),
            "failure_counts":dict(self.db.execute("SELECT source,sum(attempts) FROM failures GROUP BY source")),
            "source_pauses":[{'source':r[0],'retry_at':r[1],'reason':r[2]} for r in self.db.execute(
                'SELECT source,retry_at,reason FROM source_pauses WHERE retry_at>? ORDER BY source',(time.time(),))],
            "task_requests_rolling_day":dict(self.db.execute('''SELECT source,sum(requests) FROM request_reservations
                WHERE reserved_at>? GROUP BY source''',(time.time()-86400,))),
            "deferred_candidates":dict(self.db.execute('''SELECT r.source,count(*) FROM discovery_retries r
                WHERE r.next_attempt_at>? AND NOT EXISTS(SELECT 1 FROM members m
                WHERE m.source=CASE WHEN r.source='ingest-wikidata' THEN 'wikidata' ELSE 'orcid-public-api' END
                AND (m.locator=r.identifier OR m.locator LIKE r.identifier||'@%')) GROUP BY r.source''',(time.time(),))),
            "discovery_feeds":[dict(zip(('id','status','compressed_bytes','lines','candidates','rejected','sha256','detail'), row))
                for row in self.db.execute('SELECT id,status,compressed_bytes,lines,candidates,rejected,sha256,detail FROM discovery_feeds ORDER BY id')],
            "storage_bytes": self.task.used(), "storage_budget": self.task.config["budget_bytes"],
            "complete_global_census": False,
        }
