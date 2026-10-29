"""Transactional, size-limited normalized landing store. Never stores raw bodies.

SQLite owns ingest/checkpoints because max_page_count bounds durable database growth.
DuckDB owns bounded analysis/Parquet export; it never downloads raw input or spills.
"""
from __future__ import annotations

import json
import sqlite3
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
CREATE TABLE IF NOT EXISTS discovery_feeds(
 id TEXT PRIMARY KEY, spec TEXT NOT NULL, status TEXT NOT NULL,
 compressed_bytes INTEGER NOT NULL DEFAULT 0, lines INTEGER NOT NULL DEFAULT 0,
 candidates INTEGER NOT NULL DEFAULT 0, rejected INTEGER NOT NULL DEFAULT 0,
 sha256 TEXT, detail TEXT, prefix_sha256 TEXT);
CREATE TABLE IF NOT EXISTS institution_aliases(
 alias TEXT PRIMARY KEY, institution TEXT NOT NULL, provenance TEXT NOT NULL);
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
            "discovery_feeds":[dict(zip(('id','status','compressed_bytes','lines','candidates','rejected','sha256','detail'), row))
                for row in self.db.execute('SELECT id,status,compressed_bytes,lines,candidates,rejected,sha256,detail FROM discovery_feeds ORDER BY id')],
            "storage_bytes": self.task.used(), "storage_budget": self.task.config["budget_bytes"],
            "complete_global_census": False,
        }
