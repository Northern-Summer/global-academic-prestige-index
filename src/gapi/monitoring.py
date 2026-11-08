"""Aggregate monitoring on an in-memory snapshot, never a long live read lock."""
from __future__ import annotations

import sqlite3
import time
import math

from .career_store import CareerStore, exclusive_writer
from .event_storage import configure_reader
from .task import BudgetStop


REPORT_COLUMNS = {
    'runs':'source,identity,status,bytes,members,sha256,detail',
    'events':'person,country,status', 'members':'source,locator,status',
    'failures':'source,attempts', 'source_pauses':'source,retry_at,reason',
    'request_reservations':'source,reserved_at,requests',
    'discovery_retries':'source,identifier,next_attempt_at',
    'discovery_feeds':'id,status,compressed_bytes,lines,candidates,rejected,sha256,detail',
    'institution_alias_issues':'reason',
    'discovery':'source,identifier,query_sha256,response_sha256',
}


def settled_report(task, *, max_seconds=60, candidate_selection=None):
    """Read the report owner directly while the existing exclusion is held.

    Explicit settled operation only: refuses an active collector, opens SQLite
    read-only and allocates no database projection. The deadline covers queries
    and Python-side candidate hashing; all exits release the source and lock.
    """
    if not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('monitor timeout must be positive')
    deadline=time.monotonic()+max_seconds
    def check():
        task.check_memory()
        if time.monotonic()>=deadline:
            raise BudgetStop('settled report time budget exhausted')
    stopped=None
    def observe():
        nonlocal stopped
        try:
            check()
            return 0
        except BudgetStop as exc:
            stopped=exc
            return 1
    with exclusive_writer(task):
        db=sqlite3.connect((task.root/'careers.sqlite').as_uri()+'?mode=ro',uri=True,timeout=1)
        try:
            configure_reader(db)
            db.execute('PRAGMA query_only=ON')
            db.set_progress_handler(observe,10000)
            check()
            view=CareerStore.__new__(CareerStore)
            view.db,view.task=db,task
            result=view.report()
            if candidate_selection is not None:
                from .candidate_selection import processing
                result['frozen_candidate_processing']=processing(view,candidate_selection)
            check()
            return result
        except sqlite3.OperationalError:
            if stopped is not None:raise stopped from None
            raise
        finally:
            db.close()


def report_projection(snapshot, task, check):
    """Copy only report-owned columns; large private landings stay on disk.

    A single read transaction fixes one source view. A two-second admission
    deadline releases the read lock before the writer's five-second timeout.
    Aggregate queries run after detaching the live source, as on the backup path.
    """
    until=time.monotonic()+2
    stopped=None
    def observe():
        nonlocal stopped
        try:
            check()
            if time.monotonic()>until:raise BudgetStop('monitor projection read-lock deadline exhausted')
            return 0
        except BudgetStop as exc:
            stopped=exc
            return 1
    snapshot.execute('ATTACH DATABASE ? AS live',((task.root/'careers.sqlite').as_uri()+'?mode=ro',))
    snapshot.set_progress_handler(observe,10000)
    try:
        snapshot.execute('BEGIN')
        for table,columns in REPORT_COLUMNS.items():
            check()
            if time.monotonic()>until:raise BudgetStop('monitor projection read-lock deadline exhausted')
            selected=('rowid AS rowid,' if table=='discovery' else '')+columns
            snapshot.execute(f'CREATE TABLE main.{table} AS SELECT {selected} FROM live.{table}')
        if snapshot.execute("SELECT 1 FROM live.sqlite_master WHERE name='candidate_selections' AND type='table'").fetchone():
            snapshot.execute('CREATE TABLE main.candidate_selections AS SELECT id,payload FROM live.candidate_selections')
        snapshot.commit()
    except BaseException:
        snapshot.rollback()
        if stopped:raise stopped
        raise
    finally:
        snapshot.set_progress_handler(None,0)
        snapshot.execute('DETACH DATABASE live')


def snapshot_report(task, *, max_seconds=60, candidate_selection=None):
    """Release the live connection before running potentially expensive counts.

    SQLite backup copies bounded page steps and releases its source read lock
    between steps. No second durable database, raw input or task writer is opened.
    The report remains the CareerStore-owned calculation on one consistent view.
    """
    if not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('monitor timeout must be positive')
    deadline = time.monotonic() + max_seconds
    def check():
        task.check_memory()
        if time.monotonic() >= deadline:
            raise BudgetStop('monitor snapshot/report time budget exhausted')
    snapshot = sqlite3.connect(':memory:',uri=True)
    configure_reader(snapshot)
    interruption = None
    def query_progress():
        nonlocal interruption
        try:
            check()
            return 0
        except BudgetStop as exc:
            interruption = exc
            return 1
    try:
        snapshot.execute('PRAGMA temp_store=MEMORY')
        snapshot.set_progress_handler(query_progress, 10000)
        try:size=(task.root/'careers.sqlite').stat().st_size
        except FileNotFoundError:size=0
        if size > task.config['memory_bytes']//2:
            report_projection(snapshot,task,check)
            snapshot.set_progress_handler(query_progress,10000)
            snapshot.execute('CREATE INDEX report_discovery ON discovery(source,identifier)')
            snapshot.execute('CREATE INDEX report_members ON members(source,locator)')
            # report_projection intentionally copies columns, not source indexes.
            # candidate_progress checks retry membership once per candidate;
            # without this key that correlated lookup scans all retries each time.
            snapshot.execute('CREATE INDEX report_retries ON discovery_retries(source,identifier)')
        else:
            source = sqlite3.connect((task.root/'careers.sqlite').as_uri()+'?mode=ro',
                                     uri=True, timeout=1, isolation_level=None)
            try:
                source.backup(snapshot, pages=64, sleep=0.05, progress=lambda *args:check())
            finally:
                source.close()
        check()
        # Report methods do not write or acquire the CareerStore writer lock.
        view = CareerStore.__new__(CareerStore)
        view.db, view.task = snapshot, task
        result = view.report()
        if candidate_selection is not None:
            from .candidate_selection import processing
            result['frozen_candidate_processing']=processing(view,candidate_selection)
        check()
        return result
    except sqlite3.OperationalError:
        if interruption is not None:
            raise interruption from None
        raise
    finally:
        snapshot.close()
