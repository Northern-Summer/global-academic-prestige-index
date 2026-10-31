"""Transactional, size-limited normalized landing store. Never stores raw bodies.

SQLite owns ingest/checkpoints because max_page_count bounds durable database growth.
DuckDB owns bounded analysis/Parquet export; it never downloads raw input or spills.
"""
from __future__ import annotations

import json
import hashlib
import sqlite3
import time
from datetime import datetime, timezone
from contextlib import contextmanager

from .task import Task, BudgetStop, canonical, digest
from .event_storage import pack_event, configure_reader, DICTIONARY_DDL, source_dictionary
from .queue_storage import pack_row

CANDIDATE_SOURCES={'ingest-wikidata':'wikidata','ingest-orcid-api':'orcid-public-api'}


def bounded_run_detail(detail):
    """Keep control JSON valid; full acquisition errors belong to failures/retries."""
    if not detail:return None
    text=str(detail)
    if len(text)<=1024:return text
    try:value=json.loads(text)
    except ValueError:return text[:1024]
    if not isinstance(value,dict):
        raise ValueError('oversized structured run detail requires an object')
    failures=value.get('failures')
    if not isinstance(failures,list):
        raise ValueError('oversized structured run detail has no reducible failure summary')
    value=dict(value)
    value['failure_summary_count']=len(failures)
    value['failure_summary_truncated']=True
    value['failures']=list(failures)
    while True:
        text=json.dumps(value,ensure_ascii=True,separators=(',',':'))
        if len(text)<=1024:return text
        if not value['failures']:
            raise ValueError('run control fields exceed the status detail bound')
        value['failures'].pop()


def member_evidence_sql(*, observed_only=True):
    """Membership predicate for an events table aliased as e.

    Indexed exact/base matches cover canonical locators. The original predicate
    remains authoritative for nested fragments and historical LIKE semantics.
    Quality auditing can inspect all member states without promoting them.
    """
    state = " AND m.status='observed'" if observed_only else ''
    prefix = f"SELECT 1 FROM members m WHERE m.source=e.source{state} AND "
    return ("(CASE WHEN EXISTS(" + prefix + "m.locator=e.locator) THEN 1 "
        "WHEN instr(e.locator,'#')>0 AND EXISTS(" + prefix +
        "m.locator=substr(e.locator,1,instr(e.locator,'#')-1)) THEN 1 "
        "ELSE EXISTS(" + prefix +
        "(m.locator=e.locator OR e.locator LIKE m.locator||'#%')) END)")

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
CREATE TABLE IF NOT EXISTS institution_alias_issues(
 alias TEXT NOT NULL, policy TEXT NOT NULL, reason TEXT NOT NULL,
 evidence TEXT NOT NULL, PRIMARY KEY(alias,policy));
CREATE VIEW IF NOT EXISTS active_institution_aliases AS
 SELECT a.* FROM institution_aliases a WHERE NOT EXISTS(
 SELECT 1 FROM institution_alias_issues i WHERE i.alias=a.alias);
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


@contextmanager
def exclusive_writer(task):
    """Existing task writer exclusion, also usable without opening SQLite.

    Immutable-file maintenance must still exclude snapshot readers/collectors,
    but does not need the database constructor's 4 MiB schema allowance.
    Each maintenance writer separately admits its actual file allocations.
    """
    task.require_frozen()
    task.check(64*1024)
    lock = (task.root / 'writer.lock').open('a+b')
    try:
        try:
            lock.seek(0)
            if lock.read(1) == b'':
                lock.write(b'0');lock.flush()
            lock.seek(0)
            import os
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError('another task writer is active') from None
        yield lock
    finally:
        lock.close()


class CareerStore:
    def __init__(self, task: Task, *, source_compression=True):
        if type(source_compression) is not bool:raise ValueError('invalid source compression option')
        self.source_compression=source_compression
        self.task = task
        task.require_frozen()
        task.check(4 * 1024**2)
        self._writer_context = exclusive_writer(task)
        self.lock = self._writer_context.__enter__()
        # The OS lock above excludes other writers. Short read-only monitoring
        # transactions can still hold SQLite's shared lock during a commit;
        # wait at most five seconds instead of aborting an otherwise valid batch.
        self.db = sqlite3.connect(task.root / "careers.sqlite", timeout=5)
        configure_reader(self.db)
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
        self.db.executescript(DDL+DICTIONARY_DDL)
        self.db.commit()

    def close(self):
        seeds=getattr(self,'_event_codec_seeds',None)
        if seeds is not None:seeds.clear()
        self.db.close()
        self._writer_context.__exit__(None,None,None)

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
                        (status, byte_count, sha256, bounded_run_detail(detail), source))
        self.db.commit()

    def failure(self, source, locator, reason):
        self.task.check(64*1024)
        self.db.execute("INSERT INTO failures(source,locator,reason) VALUES(?,?,?) ON CONFLICT(source,locator,reason) DO UPDATE SET attempts=attempts+1",
                        (source,locator,str(reason)[:512]))
        self.db.commit()

    def seen(self, source, locator):
        return self.db.execute("SELECT sha256,status FROM members WHERE source=? AND locator=?", (source, locator)).fetchone()

    def pending(self, command, limit=200, *, now=None, priority_source=None):
        sources=CANDIDATE_SOURCES
        if command not in sources or not 1<=limit<=1000:
            raise ValueError('invalid pending source/batch limit')
        if priority_source is not None and (command!='ingest-orcid-api' or priority_source!='wikidata'):
            raise ValueError('cross-source priority requires explicit Wikidata-to-ORCID identity')
        member_keys='''WITH member_keys AS MATERIALIZED (
            SELECT locator,CASE WHEN instr(locator,'@')>0 AND
                (instr(locator,char(0))=0 OR instr(locator,'@')<instr(locator,char(0)))
                THEN lower(substr(locator,1,instr(locator,'@')-1)) END AS prefix
            FROM members WHERE source=?) '''
        eligible=''' WHERE d.source=? AND COALESCE(r.next_attempt_at,0)<=?
            AND CASE WHEN d.identifier<>'' AND instr(d.identifier,char(0))=0
                AND d.identifier NOT GLOB '*[^A-Za-z0-9-]*'
                THEN d.identifier NOT IN (SELECT locator FROM member_keys)
                    AND lower(d.identifier) NOT IN (SELECT prefix FROM member_keys WHERE prefix IS NOT NULL)
                ELSE NOT EXISTS(SELECT 1 FROM members m WHERE m.source=?
                    AND (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%')) END'''
        relationship='r.source=d.source AND r.identifier=d.identifier'
        query=(member_keys+'SELECT d.identifier FROM discovery d '
               'LEFT JOIN discovery_retries r ON '+relationship+eligible)
        # CROSS JOIN fixes the small retry table as the outer loop. An ordinary
        # join let SQLite scan every discovery candidate even for one due retry.
        # Eligibility is shared with fresh and linked selection, not redefined.
        retry_query=(member_keys+'SELECT d.identifier FROM discovery_retries r '
                     'CROSS JOIN discovery d ON '+relationship+eligible)
        # IN builds each membership set once. LEFT JOINs on materialized keys
        # can make SQLite choose repeated scans of either exact or prefix keys.
        # Both sets exclude NULL, preserving the original NOT EXISTS predicate.
        parameters=(sources[command],command,time.time() if now is None else now,sources[command])
        # Reserve a quarter of a normal batch for due retries so a continuing
        # stream of fresh candidates cannot starve recovered records forever.
        due=[r[0] for r in self.db.execute(retry_query+''' AND r.identifier IS NOT NULL
            ORDER BY r.next_attempt_at,d.identifier LIMIT ?''',(*parameters,limit))]
        reserved=min(len(due),max(1,limit//4))
        fresh=[r[0] for r in self.db.execute(query+''' AND r.identifier IS NULL
            ORDER BY d.identifier LIMIT ?''',(*parameters,limit-reserved))]
        self.last_priority_deferred = False
        if priority_source and len(fresh)>1:
            # Optional priority must not consume the acquisition deadline. Source
            # facts and queue membership remain untouched when it is deferred.
            until = time.monotonic() + 2
            timed_out = False
            def limit_priority():
                nonlocal timed_out
                timed_out = time.monotonic() >= until
                return int(timed_out)
            self.db.set_progress_handler(limit_priority, 1000)
            try:
                priority = self._linked_priority(query, parameters, priority_source, len(fresh)//2)
            except sqlite3.OperationalError as exc:
                if not timed_out or getattr(exc, 'sqlite_errorcode', None) != sqlite3.SQLITE_INTERRUPT:
                    raise
                priority = []
                self.last_priority_deferred = True
            finally:
                self.db.set_progress_handler(None, 0)
            preferred=set(priority)
            fresh=(priority+[identifier for identifier in fresh if identifier not in preferred])[:len(fresh)]
            if not self.last_priority_deferred:
                # A time-bounded transport often consumes only a short prefix.
                # Interleave the reserved pools so late positions are not the
                # only chance for ordinary fresh candidates to be requested.
                # Full-batch membership and quotas are unchanged.
                from itertools import zip_longest
                missing=object()
                pools=(due[:reserved],[value for value in fresh if value in preferred],
                       [value for value in fresh if value not in preferred])
                prefix=[value for group in zip_longest(*pools,fillvalue=missing)
                        for value in group if value is not missing]
                return prefix+due[reserved:limit-len(fresh)]
        return due[:reserved]+fresh+due[reserved:limit-len(fresh)]

    def _linked_priority(self, query, parameters, priority_source, slots):
        from .orcid_stream import private_person
        from .career_rules import valid_orcid
        official=set()
        for (person,) in self.db.execute('SELECT DISTINCT person FROM official_profile_subjects'):
            official.add(person)
            if len(official)>200000:raise BudgetStop('linked-person priority exceeds bounded selection')
        self.task.check_memory()
        # Scheduling, not corroboration: rotate through a bounded candidate
        # window rather than sorting the entire queue by a Python SQL UDF.
        # Fresh/retry quotas remain owned by pending(). Restarting may revisit
        # a window, but never changes evidence or excludes ordinary candidates.
        cursor=getattr(self,'_priority_scan_after','')
        candidates=[row[0] for row in self.db.execute(query+''' AND r.identifier IS NULL
            AND d.identifier>? ORDER BY d.identifier LIMIT 8192''',(*parameters,cursor))]
        priority=[]
        # Only ask about this bounded window. Enumerating every linked source
        # person first grows with acquisition and can exhaust optional priority
        # before any candidate is examined. Reuse the existing person index;
        # the same evidence predicate remains authoritative.
        linked_query=('SELECT 1 FROM events e INDEXED BY events_person '
                      'WHERE e.person=? AND e.source=? AND '+member_evidence_sql()+' LIMIT 1')
        for identifier in candidates:
            if not isinstance(identifier,str):continue
            person=private_person(identifier)
            linked=person in official or self.db.execute(linked_query,(person,priority_source)).fetchone() is not None
            if linked and valid_orcid(identifier):
                priority.append((2 if person in official else 1,identifier))
        self.task.check_memory()
        self._priority_scan_after=candidates[-1] if len(candidates)==8192 else ''
        return [identifier for _,identifier in sorted(priority,key=lambda row:(-row[0],row[1]))[:slots]]

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
                [pack_row((command,value,query_hash,response_hash)) for value in sorted(identifiers)])
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
    def member(self, source, locator, *, max_growth_bytes=None):
        # The sole-writer OS lock keeps these free pages available to this
        # connection. SQLite can reuse them without growing the database file.
        # Keep the fixed task reserve intact for rollback journals/exports and
        # at least 1 MiB of physical headroom; max_page_count still caps growth.
        size=self.db.execute('PRAGMA page_size').fetchone()[0]
        reusable=self.db.execute('PRAGMA freelist_count').fetchone()[0]*size
        maximum=self.task.config['max_member_bytes']*3
        if max_growth_bytes is not None:
            if type(max_growth_bytes) is not int or not 0<max_growth_bytes<=maximum:
                raise ValueError('invalid bounded transaction growth')
            maximum=max_growth_bytes
        growth=max(0,maximum-reusable)
        constrained=False
        if max_growth_bytes is None:
            allowed=self.task.available_growth(growth)
            constrained=allowed<growth
            growth=allowed
        else:
            self.task.check(max(1024**2,growth))
        original_cap=None
        if max_growth_bytes is not None or constrained:
            original_cap=self.db.execute('PRAGMA max_page_count').fetchone()[0]
            pages=self.db.execute('PRAGMA page_count').fetchone()[0]
            self.db.execute('PRAGMA max_page_count='+str(min(original_cap,pages+(growth+size-1)//size)))
        try:
            self.db.execute("BEGIN IMMEDIATE")
            yield
            self.task.check(1024**2)
            self.db.commit()
        except BaseException as exc:
            self.db.rollback()
            if (constrained and isinstance(exc,sqlite3.OperationalError)
                    and getattr(exc,'sqlite_errorcode',0)&255==sqlite3.SQLITE_FULL):
                raise BudgetStop('available SQLite growth exhausted; member rolled back and remains pending') from exc
            raise
        finally:
            if original_cap is not None:self.db.execute('PRAGMA max_page_count='+str(original_cap))

    def event(self, record: dict):
        text=canonical(record).decode()
        if len(text.encode('utf-8')) > 16384:
            raise ValueError("normalized event exceeds 16 KiB")
        seed=source_dictionary(self,text,record) if self.source_compression else None
        key = digest({k: record.get(k) for k in ("person", "institution", "kind", "stage", "year_start", "year_end", "source", "locator", "put_code")})
        self.db.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            key, record["person"], record.get("institution"), record["kind"], record.get("stage"),
            record.get("year_start"), record.get("year_end"), record.get("role"), record.get("country"),
            record["source"], record["locator"], record["status"], pack_event(self.db,text,record,source_seed=seed)))

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
            "acquisition_progress":self.candidate_progress(),
            "institution_alias_issues":dict(self.db.execute('SELECT reason,count(*) FROM institution_alias_issues GROUP BY reason')),
            "complete_global_census": False,
        }

    def candidate_progress(self, *, allowed_sources=None):
        """Counts committed candidate selection, not unknown world personnel."""
        from .ranking_snapshot import candidate_progress
        frozen=candidate_progress(self,allowed_sources=allowed_sources)
        if frozen is not None:return frozen
        rows=[]
        for command,source in CANDIDATE_SOURCES.items():
            if allowed_sources is not None and source not in allowed_sources:continue
            index=hashlib.sha256()
            for row in self.db.execute('SELECT identifier,query_sha256,response_sha256 FROM discovery WHERE source=? ORDER BY identifier',(command,)):
                index.update(canonical(row))
            # Materialize each source's member keys once instead of scanning all
            # members for every candidate. Exact locators retain binary equality;
            # @-prefix matching retains SQLite LIKE's ASCII case behavior. Only
            # wildcard-free ASCII IDs use that projection. Legacy/unusual IDs
            # still execute the original predicate, including its wildcards.
            total,processed,observed,failed_pending=self.db.execute('''WITH
                member_keys AS MATERIALIZED (
                    SELECT locator,status,CASE WHEN instr(locator,'@')>0 AND
                        (instr(locator,char(0))=0 OR instr(locator,'@')<instr(locator,char(0)))
                        THEN lower(substr(locator,1,instr(locator,'@')-1)) END AS prefix
                    FROM members WHERE source=?),
                exact_keys AS MATERIALIZED (
                    SELECT locator,1 AS present,max(status='observed') AS observed
                    FROM member_keys GROUP BY locator),
                prefix_keys AS MATERIALIZED (
                    SELECT prefix,1 AS present,max(status='observed') AS observed
                    FROM member_keys WHERE prefix IS NOT NULL GROUP BY prefix)
                SELECT count(*),COALESCE(sum(committed),0),
                COALESCE(sum(observed),0),COALESCE(sum(attempted AND NOT committed),0) FROM (
                SELECT CASE WHEN d.identifier<>'' AND instr(d.identifier,char(0))=0
                    AND d.identifier NOT GLOB '*[^A-Za-z0-9-]*'
                    THEN COALESCE(e.present,p.present,0)
                    ELSE EXISTS(SELECT 1 FROM members m WHERE m.source=? AND
                        (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%')) END AS committed,
                    CASE WHEN d.identifier<>'' AND instr(d.identifier,char(0))=0
                    AND d.identifier NOT GLOB '*[^A-Za-z0-9-]*'
                    THEN max(COALESCE(e.observed,0),COALESCE(p.observed,0))
                    ELSE EXISTS(SELECT 1 FROM members m WHERE m.source=? AND m.status='observed' AND
                        (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%')) END AS observed,
                    EXISTS(SELECT 1 FROM discovery_retries r WHERE r.source=d.source AND r.identifier=d.identifier) AS attempted
                FROM discovery d LEFT JOIN exact_keys e ON e.locator=d.identifier
                LEFT JOIN prefix_keys p ON p.prefix=lower(d.identifier)
                WHERE d.source=?)''',(source,source,source,command)).fetchone()
            if total:
                rows.append({'source':source,'candidates':total,'processed':processed,
                    'observed_members':observed,'rejected_members':processed-observed,
                    'pending':total-processed,'pending_with_errors':failed_pending,
                    'selection_sha256':index.hexdigest(),'enumeration_complete':processed==total})
        return {'scope':'current-discovered-candidates-only','complete_global_census':False,
                'world_personnel_denominator':None,'sources':rows}
