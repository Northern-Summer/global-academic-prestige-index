"""Streaming task commands, sharing one frozen task/source contract."""
from __future__ import annotations

import json
from pathlib import Path

from .career_store import CareerStore
from .global_sources import discover_wikidata, ingest_orcid_api, ingest_wikidata
from .orcid_stream import stream_archive
from .source_http import PublicHTTP
from .task import Task


def register(sub):
    sources = sub.add_parser("sources", help="freeze the source registry and task resource contract")
    sources.add_argument("action", choices=["freeze"])
    sources.add_argument("--task", type=Path, default=Path("configs/streaming.json"))
    for name in ("ingest-orcid", "ingest-wikidata", "ingest-orcid-api", "quality", "corroborate", "rank-global", "export-release"):
        parser = sub.add_parser(name)
        parser.add_argument("--task", type=Path, default=Path("configs/streaming.json"))
        if name.startswith("ingest-"):
            parser.add_argument("--resume", action="store_true")
            parser.add_argument("--max-members", type=int)
            parser.add_argument("--max-seconds", type=float, default=300)
        if name in ("ingest-wikidata", "ingest-orcid-api"):
            parser.add_argument("--ids", nargs="*", default=[])
            parser.add_argument("--discover", action="store_true")
        if name == "export-release":
            parser.add_argument("--out", type=Path, default=Path("outputs/global"))


def execute(args):
    names = {"sources", "ingest-orcid", "ingest-wikidata", "ingest-orcid-api", "quality", "corroborate", "rank-global", "export-release"}
    if args.command not in names:
        return False
    task = Task(args.task)
    task.activate_runtime()
    try:
        return execute_task(task, args)
    finally:
        task.restore_runtime()


def execute_task(task, args):
    if hasattr(args,'max_members') and args.max_members is not None and args.max_members < 1:
        raise ValueError('max-members must be positive')
    if hasattr(args,'max_seconds') and args.max_seconds <= 0:
        raise ValueError('max-seconds must be positive')
    if args.command == "sources":
        print(json.dumps(task.freeze(), indent=2))
        return True
    store = CareerStore(task)
    try:
        http = PublicHTTP(task)
        if args.command == "ingest-orcid":
            source = "orcid-2026-summary"
            store.start(source)
            try:
                with http.open(task.config["sources"][source]["url"]) as response:
                    result = stream_archive(store, source, response, max_members=args.max_members,
                        max_seconds=args.max_seconds, progress=lambda r: print(json.dumps(r), flush=True))
            except (OSError, RuntimeError, ValueError) as exc:
                store.status(source, "access_failed", detail=f"{type(exc).__name__}: {exc}")
                store.failure(source,'download',f"{type(exc).__name__}: {exc}")
                result = store.report()
        elif args.command in ("ingest-wikidata", "ingest-orcid-api"):
            ids = args.ids
            if args.discover:
                existing = [r[0] for r in store.db.execute("SELECT identifier FROM discovery WHERE source=? ORDER BY identifier",(args.command,))]
                page = None
                if not existing:
                    page = discover_wikidata(http, limit=min(args.max_members or 100, 200), degree_employer=args.command == "ingest-wikidata")
                # Compact discovery cursor/provenance; never saves fetched bodies.
                    task.check(1024**2)
                    ids = sorted({row["qid" if args.command == "ingest-wikidata" else "orcid"] for row in page["rows"] if row["qid" if args.command == "ingest-wikidata" else "orcid"]})
                    with store.db:
                        store.db.executemany("INSERT OR IGNORE INTO discovery VALUES(?,?,?,?)",[(args.command,i,page["query_sha256"],page["response_sha256"]) for i in ids])
                else:
                    ids = existing
            if args.max_members:
                ids = ids[:args.max_members]
            result = (ingest_wikidata if args.command == "ingest-wikidata" else ingest_orcid_api)(store, http, ids,
                max_seconds=args.max_seconds, progress=lambda r: print(json.dumps(r),flush=True))
        elif args.command == "quality":
            result = store.report()
            result["sqlite_integrity"] = store.db.execute("PRAGMA quick_check").fetchone()[0]
            result['events_without_member_evidence'] = store.db.execute('''SELECT count(*) FROM events e
                WHERE NOT EXISTS(SELECT 1 FROM members m WHERE m.source=e.source
                AND (m.locator=e.locator OR e.locator LIKE m.locator||'#%'))''').fetchone()[0]
            result['provisional_archive_events'] = store.db.execute('''SELECT count(*) FROM events e JOIN runs r ON r.source=e.source
                WHERE e.source='orcid-2026-summary' AND r.status!='verified' ''').fetchone()[0]
            raw_files = [p for p in task.root.rglob('*') if p.is_file() and p.suffix.lower() in ('.gz','.zip','.tar','.xml')]
            result["raw_source_files_detected"] = len(raw_files)
            result["raw_source_bytes_detected"] = sum(p.stat().st_size for p in raw_files)
            result["pending_discovery"] = dict(store.db.execute('''SELECT d.source,count(*) FROM discovery d
                WHERE NOT EXISTS(SELECT 1 FROM members m WHERE m.source=CASE WHEN d.source='ingest-wikidata' THEN 'wikidata' ELSE 'orcid-public-api' END
                AND (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%')) GROUP BY d.source'''))
        else:
            from .global_analysis import corroborate, export_release, export_local
            result = {"corroborate": corroborate, "rank-global": export_local,
                      "export-release": lambda s: export_release(s, args.out)}[args.command](store)
        print(json.dumps(result, indent=2, sort_keys=True))
        if args.command == 'ingest-orcid' and any(r['source']=='orcid-2026-summary' and r['status'] in ('access_failed','failed') for r in result['sources']):
            raise SystemExit(2)
    finally:
        store.close()
    return True
