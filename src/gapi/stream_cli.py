"""Streaming task commands, sharing one frozen task/source contract."""
from __future__ import annotations

import json
import math
from pathlib import Path

from .career_store import CareerStore
from .global_sources import enqueue_discovery, ingest_orcid_api, ingest_wikidata
from .orcid_stream import stream_archive
from .source_http import PublicHTTP
from .task import Task


def register(sub):
    sources = sub.add_parser("sources", help="freeze the source registry and task resource contract")
    sources.add_argument("action", choices=["freeze"])
    sources.add_argument("--task", type=Path, default=Path("configs/streaming.json"))
    for name in ("project-awards", "normalize-careers", "ingest-nobel", "discover-crossref", "discover-openalex", "collect-global", "discover-wikidata-dump", "ingest-orcid", "ingest-wikidata", "ingest-orcid-api", "quality", "corroborate", "rank-global", "export-release"):
        parser = sub.add_parser(name)
        parser.add_argument("--task", type=Path, default=Path("configs/streaming.json"))
        if name in ('discover-wikidata-dump','collect-global'):
            parser.add_argument('--feed', type=Path, default=Path('configs/discovery-dump.json'))
            parser.add_argument('--max-seconds', type=float, default=300)
        if name == 'discover-wikidata-dump':
            parser.add_argument('--max-compressed-bytes', type=int)
        if name == 'collect-global':
            parser.add_argument('--batch-members',type=int,default=50)
            parser.add_argument('--allow-dump-discovery',action='store_true',
                help='explicitly enable optional compressed-stream candidate discovery; default uses finite APIs only')
        if name == 'ingest-nobel':
            parser.add_argument('--source', type=Path, default=Path('configs/nobel-20261029.json'))
            parser.add_argument('--max-pages', type=int, default=100)
        if name in ('discover-openalex','discover-crossref'):
            parser.add_argument('--pages',type=int,default=1)
            parser.add_argument('--per-page',type=int,default=50 if name=='discover-crossref' else 200)
        if name.startswith("ingest-"):
            parser.add_argument("--resume", action="store_true")
            parser.add_argument("--max-members", type=int)
            parser.add_argument("--max-seconds", type=float, default=300)
        if name in ("ingest-wikidata", "ingest-orcid-api"):
            parser.add_argument("--ids", nargs="*", default=[])
            parser.add_argument("--discover", action="store_true")
            parser.add_argument("--discovery-limit", type=int, default=200)
            parser.add_argument("--discovery-scope", choices=['academic','degree-employer'], default='academic')
        if name == "export-release":
            parser.add_argument("--out", type=Path, default=Path("outputs/global"))
            parser.add_argument('--publication-policy',type=Path)
            group=parser.add_mutually_exclusive_group()
            group.add_argument('--freeze-input',action='store_true')
            group.add_argument('--frozen-input',help='private normalized snapshot filename, never a public file')
        if name == 'corroborate':
            parser.add_argument('--official',action='store_true')
            parser.add_argument('--max-pages',type=int,default=20)


def execute(args):
    names = {"sources", "project-awards", "normalize-careers", "ingest-nobel", "discover-crossref", "discover-openalex", "collect-global", "discover-wikidata-dump", "ingest-orcid", "ingest-wikidata", "ingest-orcid-api", "quality", "corroborate", "rank-global", "export-release"}
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
    if hasattr(args,'max_seconds') and (not math.isfinite(args.max_seconds) or args.max_seconds <= 0):
        raise ValueError('max-seconds must be positive')
    if args.command == "sources":
        print(json.dumps(task.freeze(), indent=2))
        return True
    store = CareerStore(task)
    try:
        http = PublicHTTP(task)
        if args.command == 'project-awards':
            from .award_projection import project_awards
            result = project_awards(store)
        elif args.command == 'normalize-careers':
            from .career_normalization import normalize_careers
            result = normalize_careers(store)
        elif args.command == 'ingest-nobel':
            from .nobel import ingest_nobel, load_source
            result = ingest_nobel(store, http, load_source(args.source), max_pages=args.max_pages,
                max_seconds=args.max_seconds, progress=lambda r: print(json.dumps(r), flush=True))
        elif args.command in ('discover-openalex','discover-crossref'):
            if args.command=='discover-openalex':
                from .openalex_discovery import discover_openalex as discover
            else:
                from .crossref_discovery import discover_crossref as discover
            result=discover(store,http,pages=args.pages,per_page=args.per_page,
                progress=lambda r:print(json.dumps(r),flush=True))
        elif args.command == 'collect-global':
            from .collection import collect
            from .dump_discovery import load_feed
            result=collect(store,http,load_feed(args.feed) if args.allow_dump_discovery else None,max_seconds=args.max_seconds,
                batch_members=args.batch_members,progress=lambda r:print(json.dumps(r),flush=True))
        elif args.command == 'discover-wikidata-dump':
            from .dump_discovery import discover_dump, load_feed
            if args.max_compressed_bytes is not None and args.max_compressed_bytes <= 0:
                raise ValueError('max-compressed-bytes must be positive')
            result = discover_dump(store,http,load_feed(args.feed),max_seconds=args.max_seconds,
                max_bytes=args.max_compressed_bytes,progress=lambda r:print(json.dumps(r),flush=True))
        elif args.command == "ingest-orcid":
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
            if args.discover or args.resume and not ids:
                if args.discover:
                    try:
                        print(json.dumps(enqueue_discovery(store, http, args.command,
                            limit=args.discovery_limit, scope=args.discovery_scope)), flush=True)
                    except (OSError, RuntimeError, ValueError) as exc:
                        store.failure('wikidata', 'discovery:'+args.command, str(exc))
                        print(json.dumps({'discovery_failed': str(exc)[:256], 'continuing_committed_queue': True}), flush=True)
                ids=store.pending(args.command,min(args.max_members or 200,1000))
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
        elif args.command=='export-release':
            from .global_analysis import export_release
            from .frozen_input import freeze_input, frozen_reader
            from contextlib import nullcontext
            policy=json.loads(args.publication_policy.read_text(encoding='utf-8')) if args.publication_policy else None
            snapshot=freeze_input(store) if args.freeze_input else None
            if args.frozen_input:
                import re
                if not re.fullmatch(r'[a-f0-9]{64}\.sqlite',args.frozen_input):
                    raise ValueError('frozen input requires a content-addressed filename')
                path=task.root/'frozen-inputs'/args.frozen_input
                snapshot={'sha256':args.frozen_input.removesuffix('.sqlite'),'filename':args.frozen_input,
                    'bytes':path.stat().st_size,'contains_personal_input':True,'publicly_bundled':False}
            with frozen_reader(store,snapshot['filename']) if snapshot else nullcontext():
                result=export_release(store,args.out,publication_policy=policy,frozen_input=snapshot)
        else:
            from .global_analysis import corroborate, export_release, export_local
            result = {"corroborate": corroborate, "rank-global": export_local}[args.command](store)
            if args.command=='corroborate' and args.official:
                from .official_profiles import check_official_profiles
                result['official_profiles']=check_official_profiles(store,http,limit=args.max_pages)
                # Consume the fresh official checks in the same invocation.
                result.update(corroborate(store))
        print(json.dumps(result, indent=2, sort_keys=True))
        if args.command == 'ingest-nobel' and result and result.get('status') == 'resumable-source-boundary':
            raise SystemExit(2)
        if args.command == 'ingest-orcid' and any(r['source']=='orcid-2026-summary' and r['status'] in ('access_failed','failed') for r in result['sources']):
            raise SystemExit(2)
    finally:
        store.close()
    return True
