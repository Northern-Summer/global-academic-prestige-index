"""Streaming task commands, sharing one frozen task/source contract."""
from __future__ import annotations

import json
import math
from pathlib import Path
from urllib.error import HTTPError

from .career_store import CareerStore, member_evidence_sql, CANDIDATE_SOURCES, orcid_archive_sources, verified_archive_sql
from .global_sources import enqueue_discovery, ingest_orcid_api, ingest_wikidata
from .orcid_stream import stream_archive, validate_commit_members
from .source_http import PublicHTTP, StageDeadline, WIKIDATA_BATCH_LIMIT
from .task import Task


def register(sub):
    local_ingest=sub.add_parser('ingest-orcid-local',help='continue verified local archive from transaction-bound tar cursor')
    local_ingest.add_argument('--task',type=Path,default=Path('configs/orcid-2026-production.json'))
    local_ingest.add_argument('--source',default='orcid-2026-summary')
    local_ingest.add_argument('--max-seconds',type=float,default=21600)
    local_ingest.add_argument('--max-new-members',type=int)
    local_ingest.add_argument('--commit-members',type=int,default=1024)
    local_index=sub.add_parser('index-orcid-local',help='one authorized local index pass; no career ingestion')
    local_index.add_argument('--task',type=Path,default=Path('configs/orcid-2026-production.json'))
    local_index.add_argument('--source',default='orcid-2026-summary')
    local_index.add_argument('--max-seconds',type=float,default=21600)
    selection=sub.add_parser('freeze-candidates',help='freeze a finite selection in the existing candidate ledger without copying IDs')
    selection.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    rejected=sub.add_parser('queue-archive-rejections',help='queue registered ORCID API follow-up without changing annual rejections')
    rejected.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    rejected.add_argument('--source',default='orcid-2026-summary')
    rejected.add_argument('--after-rowid',type=int,default=0)
    rejected.add_argument('--max-members',type=int,default=10000)
    rejected.add_argument('--max-seconds',type=float,default=60)
    members=sub.add_parser('compact-member-digests',help='losslessly compact member hashes under the writer lock')
    members.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    members.add_argument('--after-rowid',type=int,default=0)
    members.add_argument('--max-rows',type=int,default=100000)
    members.add_argument('--max-seconds',type=float,default=60)
    evidence=sub.add_parser('repack-evidence-urls',help='losslessly compress evidence URLs under the writer lock')
    evidence.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    evidence.add_argument('--after',nargs=3,metavar=('SOURCE','LOCATOR','PART'))
    evidence.add_argument('--max-rows',type=int,default=10000)
    evidence.add_argument('--max-seconds',type=float,default=30)
    full_rows=sub.add_parser('repack-full-input',help='losslessly repack a retained full-row input under the writer lock')
    full_rows.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    full_rows.add_argument('--filename',required=True)
    full_rows.add_argument('--dry-run',action='store_true')
    full_rows.add_argument('--max-decode-seconds',type=float)
    projections=sub.add_parser('repack-projection-pages',help='losslessly pack private derivative payloads under the writer lock')
    projections.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    projections.add_argument('--table',choices=('easy_careers','easy_full_delta'),default='easy_careers')
    projections.add_argument('--after-row',type=int,default=0)
    projections.add_argument('--max-rows',type=int,default=10000)
    projections.add_argument('--max-seconds',type=float,default=60)
    projections.add_argument('--codec',choices=('blocks','dictionary'),default='blocks')
    repack=sub.add_parser('repack-event-pages',help='rebalance event pages without changing stored rows or rowids')
    repack.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    repack.add_argument('--after-rowid',type=int,default=0)
    repack.add_argument('--max-events',type=int,default=20000)
    repack.add_argument('--max-seconds',type=float,default=60)
    repack_codec = repack.add_mutually_exclusive_group()
    repack_codec.add_argument('--share-sources',action='store_true',help='losslessly reuse bounded source seed dictionaries')
    repack_codec.add_argument('--blocks',action='store_true',help='opt in to lossless bounded event blocks')
    compact_snapshots=sub.add_parser('compact-frozen-inputs',help='losslessly pack selected private derived snapshots under the writer lock')
    compact_snapshots.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    compact_snapshots.add_argument('--filenames',nargs='+',required=True)
    compact_snapshots.add_argument('--codec',choices=('gzip','xz'),default='gzip')
    browser = sub.add_parser('export-easy-orcid-web',help='export verified aggregate scores as bounded browser assets')
    browser.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    browser.add_argument('--out',type=Path,default=Path('outputs/easy-orcid-web'))
    browser.add_argument('--include-full',action='store_true')
    full = sub.add_parser('ingest-easy-orcid-full',help='stream full derivative, reusing exact HQ projections')
    full.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    full.add_argument('--source',type=Path,default=Path('configs/easy-orcid-2023-full.json'))
    full.add_argument('--max-records',type=int)
    full.add_argument('--max-seconds',type=float,default=1800)
    bulk_rank = sub.add_parser('rank-easy-orcid',help='score only the completely verified derivative projection')
    bulk_rank.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    bulk_rank.add_argument('--out',type=Path,default=Path('outputs/easy-orcid-observed.json.gz'))
    bulk_rank.add_argument('--include-full',action='store_true')
    bulk = sub.add_parser('ingest-easy-orcid',help='stream compact private career projections, gated on full archive verification')
    bulk.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    bulk.add_argument('--source',type=Path,default=Path('configs/easy-orcid-2023-hq.json'))
    bulk.add_argument('--max-records',type=int)
    bulk.add_argument('--max-seconds',type=float,default=1800)
    census = sub.add_parser('census-easy-orcid',help='stream a frozen derivative census without storing individual records')
    census.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    census.add_argument('--source',type=Path,default=Path('configs/easy-orcid-2023-hq.json'))
    census.add_argument('--max-records',type=int)
    census.add_argument('--max-seconds',type=float,default=1800)
    compaction = sub.add_parser('compact-events',help='losslessly compress normalized events within the task budget')
    compaction.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    compaction.add_argument('--max-events',type=int,default=100000)
    compaction.add_argument('--max-seconds',type=float,default=120)
    promotion = sub.add_parser('promote-wikidata-dump',help='promote bounded normalized members from a fully verified dump')
    promotion.add_argument('--task',type=Path,default=Path('configs/streaming.json'))
    promotion.add_argument('--feed',type=Path,default=Path('configs/discovery-dump.json'))
    promotion.add_argument('--max-members',type=int,default=1000)
    promotion.add_argument('--max-seconds',type=float,default=60)
    sources = sub.add_parser("sources", help="freeze the source registry and task resource contract")
    sources.add_argument("action", choices=["freeze"])
    sources.add_argument("--task", type=Path, default=Path("configs/streaming.json"))
    for name in ("acquisition-status", "resolve-institutions-online", "project-awards", "normalize-careers", "ingest-nobel", "discover-crossref", "discover-openalex", "discover-easy-orcid", "collect-global", "discover-wikidata-dump", "ingest-orcid", "ingest-wikidata", "ingest-orcid-api", "quality", "corroborate", "rank-global", "export-release"):
        parser = sub.add_parser(name)
        parser.add_argument("--task", type=Path, default=Path("configs/streaming.json"))
        if name == 'rank-global':
            parser.add_argument('--second-task',type=Path,
                help='unify two existing frozen stores without copying source rows')
            parser.add_argument('--analysis-seconds',type=float,default=14400,
                help='remaining unified computation wall-time allowance')
            parser.add_argument('--progress',action='store_true',
                help='emit analysis phase JSON lines on stderr; final JSON stays on stdout')
            parser.add_argument('--chunked',action='store_true',
                help='write full local aggregate as verified bounded chunks; not a browser/public release')
        if name == 'acquisition-status':
            parser.add_argument('--max-seconds',type=float,default=60)
            parser.add_argument('--settled',action='store_true',
                help='read directly under the existing writer exclusion; refuses active collection')
            parser.add_argument('--candidate-selection',help='include verified finite candidate processing')
        if name == 'resolve-institutions-online':
            parser.add_argument('--max-aliases',type=int,default=100)
            parser.add_argument('--max-seconds',type=float,default=300)
            parser.add_argument('--candidate-manifest',type=Path,
                help='hash-verified public aggregate institution directory; candidates only, never identity evidence')
        if name in ('discover-wikidata-dump','collect-global'):
            parser.add_argument('--feed', type=Path, default=Path('configs/discovery-dump.json'))
            parser.add_argument('--max-seconds', type=float, default=300)
        if name == 'discover-wikidata-dump':
            parser.add_argument('--max-compressed-bytes', type=int)
            parser.add_argument('--max-reconnects',type=int,default=256,
                help='bounded total reconnect allowance, 1–4096; consecutive stalled limit remains eight')
            parser.add_argument('--stage-careers',action='store_true',
                help='retain normalized careers in isolated landing; replays old prefix, never stores raw entities')
        if name == 'collect-global':
            parser.add_argument('--bounded-report',action='store_true',
                help='source-scoped stage counts and bounded settlement; omit full-database aggregates')
            parser.add_argument('--orcid-record',action='store_true',
                help='opt in to one bounded record XML response with truthful whole-response evidence; default uses two affiliation sections')
            parser.add_argument('--candidate-selection',help='verified immutable selection ID; requires --queued-only')
            parser.add_argument('--reuse-verified-annual',action='store_true',
                help='within a frozen selection, reuse verified annual records and retain error-document failures; never count either as API success')
            parser.add_argument('--batch-members',type=int,default=50)
            parser.add_argument('--allow-dump-discovery',action='store_true',
                help='explicitly enable optional compressed-stream candidate discovery; default uses finite APIs only')
            parser.add_argument('--queued-only',action='store_true',
                help='finish committed candidates and their stable-identifier followups without refilling discovery pages')
        if name == 'ingest-nobel':
            parser.add_argument('--source', type=Path, default=Path('configs/nobel-20261029.json'))
            parser.add_argument('--max-pages', type=int, default=100)
        if name in ('discover-openalex','discover-crossref'):
            parser.add_argument('--pages',type=int,default=1)
            parser.add_argument('--per-page',type=int,default=50 if name=='discover-crossref' else 200)
        if name == 'discover-easy-orcid':
            parser.add_argument('--max-records',type=int,default=1000)
            parser.add_argument('--max-seconds',type=float,default=120)
        if name.startswith("ingest-"):
            parser.add_argument("--resume", action="store_true")
            parser.add_argument("--max-members", type=int)
            parser.add_argument("--max-seconds", type=float, default=300)
        if name == 'ingest-orcid':
            parser.add_argument('--source',default='orcid-2026-summary',
                help='annual archive source key in the frozen task registry')
            parser.add_argument('--max-new-members',type=int,
                help='stop after this many previously uncommitted members; gzip still replays from the start')
            parser.add_argument('--commit-members',type=int,default=1,
                help='atomically commit up to 1–128 complete members within the existing event limit')
        if name in ("ingest-wikidata", "ingest-orcid-api"):
            parser.add_argument("--ids", nargs="*", default=[])
            parser.add_argument("--discover", action="store_true")
            parser.add_argument("--discovery-limit", type=int, default=200)
            parser.add_argument("--discovery-scope", choices=['academic','degree-employer'], default='academic')
        if name == 'ingest-wikidata':
            parser.add_argument('--batch-size',type=int,default=WIKIDATA_BATCH_LIMIT,
                help='1 uses EntityData; larger batches use bounded official claims/info requests')
        if name == "export-release":
            parser.add_argument("--out", type=Path, default=Path("outputs/global"))
            parser.add_argument('--publication-policy',type=Path)
            parser.add_argument('--freeze-format',choices=('sqlite','rows'),default='sqlite')
            parser.add_argument('--freeze-base',help='retained full HASH.rows baseline for bounded row differences')
            parser.add_argument('--allow-delta-base',action='store_true',help='explicitly permit a retained delta base; dependency depth capped at four')
            parser.add_argument('--compress-snapshot',action='store_true',help='keep a hash-bound gzip aggregate in the repository; build serves decoded JSON')
            parser.add_argument('--compact-nodes',action='store_true',help='losslessly share repeated institution fields in the public transport')
            group=parser.add_mutually_exclusive_group()
            group.add_argument('--freeze-input',action='store_true')
            group.add_argument('--frozen-input',help='private normalized snapshot filename, never a public file')
        if name == 'corroborate':
            parser.add_argument('--official',action='store_true')
            parser.add_argument('--max-pages',type=int,default=20)


def execute(args):
    if args.command=='ingest-orcid-local':
        from .local_archive_index import LocalArchive
        task=Task(args.task)
        if args.source not in orcid_archive_sources(task):raise ValueError('local source must be a frozen annual archive')
        validate_commit_members(args.commit_members,local=True)
        archive=task.resource_root.parent/'archive-download'/task.config['sources'][args.source]['filename']
        store=CareerStore(task)
        try:
            with LocalArchive(store,args.source,archive,task.root/(args.source+'.gzi'),max_seconds=args.max_seconds) as local:
                result=stream_archive(store,args.source,None,local=local,
                    max_new_members=args.max_new_members,max_seconds=args.max_seconds,
                    commit_members=args.commit_members,full_report=False,
                    progress=lambda r:print(json.dumps(r),flush=True))
        finally:store.close()
        print(json.dumps(result,indent=2))
        return True
    if args.command == 'index-orcid-local':
        task=Task(args.task)
        from .career_store import exclusive_writer
        from .local_archive_index import build_local_index, local_decoder, committed_prefix
        source=args.source
        if source not in orcid_archive_sources(task):
            raise ValueError('local index requires a frozen annual archive')
        archive=task.resource_root.parent/'archive-download'/task.config['sources'][source]['filename']
        output=task.root/(source+'.gzi')
        with exclusive_writer(task):
            with committed_prefix(task,source) as prefix:
                result=build_local_index(task,source,archive,output,local_decoder,
                    max_seconds=args.max_seconds,prefix=prefix)
        print(json.dumps(result,indent=2))
        return True
    names = {"sources", "acquisition-status", "resolve-institutions-online", "project-awards", "normalize-careers", "ingest-nobel", "discover-crossref", "discover-openalex", "discover-easy-orcid", "collect-global", "discover-wikidata-dump", "ingest-orcid", "ingest-wikidata", "ingest-orcid-api", "quality", "corroborate", "rank-global", "export-release"}
    if args.command not in names and args.command not in ('freeze-candidates','queue-archive-rejections','compact-member-digests','repack-evidence-urls','repack-full-input','repack-projection-pages','repack-event-pages','compact-frozen-inputs','promote-wikidata-dump','compact-events','census-easy-orcid','ingest-easy-orcid','rank-easy-orcid','ingest-easy-orcid-full','export-easy-orcid-web'):
        return False
    if args.command=='rank-global' and getattr(args,'second_task',None):
        from .unified_analysis import export_unified
        result=export_unified((args.task,args.second_task),seconds=args.analysis_seconds,
            progress=(lambda item:print(json.dumps({'analysis_progress':item}),
                file=__import__('sys').stderr,flush=True)) if args.progress else None)
        print(json.dumps(result,indent=2,sort_keys=True))
        return True
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
    if args.command == 'acquisition-status':
        from .monitoring import snapshot_report, settled_report
        report=settled_report if getattr(args,'settled',False) else snapshot_report
        print(json.dumps(report(task,max_seconds=args.max_seconds,
            candidate_selection=getattr(args,'candidate_selection',None)),indent=2))
        return True
    if args.command == 'census-easy-orcid':
        from .easy_orcid_census import census, load_spec
        task.require_frozen()
        result=census(task,PublicHTTP(task),load_spec(args.source),
                      max_records=args.max_records,max_seconds=args.max_seconds,
                      progress=lambda r:print(json.dumps({'census_progress':r}),flush=True))
        print(json.dumps({'census_result':result}),flush=True)
        if result['error'] is not None:
            raise SystemExit(2)
        return True
    store = CareerStore(task)
    http=None
    try:
        http = PublicHTTP(task,reuse_connections=True)
        if args.command == 'freeze-candidates':
            from .candidate_selection import freeze
            result=freeze(store)
        elif args.command == 'queue-archive-rejections':
            from .archive_recovery import queue_archive_rejections
            result=queue_archive_rejections(store,args.source,after_rowid=args.after_rowid,
                max_members=args.max_members,max_seconds=args.max_seconds)
        elif args.command == 'compact-member-digests':
            from .queue_storage import compact_members
            result=compact_members(store,after_rowid=args.after_rowid,
                max_rows=args.max_rows,max_seconds=args.max_seconds)
        elif args.command == 'repack-evidence-urls':
            from .evidence_storage import repack_urls
            result=repack_urls(store,after=args.after,max_rows=args.max_rows,max_seconds=args.max_seconds)
        elif args.command == 'repack-full-input':
            from .row_packing import repack_full
            result=repack_full(store,args.filename,dry_run=args.dry_run,max_decode_seconds=args.max_decode_seconds)
        elif args.command == 'compact-frozen-inputs':
            from .frozen_input import compress_snapshot,repack_snapshot,snapshot_path
            # Validate every requested identity before the first mutation.
            for filename in args.filenames:snapshot_path(store,filename)
            compact=repack_snapshot if getattr(args,'codec','gzip')=='xz' else compress_snapshot
            result={'snapshots':[compact(store,filename) for filename in args.filenames]}
        elif args.command == 'export-easy-orcid-web':
            from .easy_orcid_landing import rank_projection
            from .derived_export import write_bundle
            if args.out.resolve().parent != Path('outputs').resolve():
                raise ValueError('browser bundle must be a direct outputs child')
            result=write_bundle(task,rank_projection(store,stream_edges=True,include_full=args.include_full),args.out)
        elif args.command == 'ingest-easy-orcid-full':
            from .easy_orcid_census import load_spec
            from .easy_orcid_full import ingest_full
            result=ingest_full(store,http,load_spec(args.source),max_records=args.max_records,
                              max_seconds=args.max_seconds,
                              progress=lambda r:print(json.dumps({'full_progress':r}),flush=True))
        elif args.command == 'rank-easy-orcid':
            from .easy_orcid_landing import write_projection
            if args.out.resolve().parent != Path('outputs').resolve():
                raise ValueError('derivative ranking output must be a direct outputs child')
            result=write_projection(store,args.out,include_full=args.include_full)
        elif args.command == 'ingest-easy-orcid':
            from .easy_orcid_census import load_spec
            from .easy_orcid_landing import ingest
            result=ingest(store,http,load_spec(args.source),max_records=args.max_records,
                          max_seconds=args.max_seconds,
                          progress=lambda r:print(json.dumps({'bulk_progress':r}),flush=True))
        elif args.command == 'repack-projection-pages':
            from .projection_storage import repack, repack_dictionary
            repacker=repack_dictionary if args.codec=='dictionary' else repack
            result=repacker(store,table=args.table,after_row=args.after_row,
                          max_rows=args.max_rows,max_seconds=args.max_seconds)
        elif args.command == 'repack-event-pages':
            from .event_storage import repack_event_pages, repack_event_blocks
            if getattr(args, 'blocks', False):
                result=repack_event_blocks(store,after_rowid=args.after_rowid,
                    max_events=args.max_events,max_seconds=args.max_seconds)
            else:
                result=repack_event_pages(store,after_rowid=args.after_rowid,
                    max_events=args.max_events,max_seconds=args.max_seconds,share_sources=args.share_sources)
        elif args.command == 'compact-events':
            from .event_storage import compact_events
            result=compact_events(store,max_events=args.max_events,max_seconds=args.max_seconds)
        elif args.command == 'promote-wikidata-dump':
            from .dump_landing import promote_batch
            from .dump_discovery import load_feed
            result=promote_batch(store,load_feed(args.feed)['id'],
                max_members=args.max_members,max_seconds=args.max_seconds)
        elif args.command == 'resolve-institutions-online':
            from .institution_bridge import resolve_online
            result=resolve_online(store,http,max_aliases=args.max_aliases,max_seconds=args.max_seconds,
                progress=lambda r: print(json.dumps(r),flush=True),candidate_manifest=args.candidate_manifest)
        elif args.command == 'project-awards':
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
        elif args.command == 'discover-easy-orcid':
            from .easy_orcid import discover
            result=discover(store,http,max_records=args.max_records,max_seconds=args.max_seconds)
        elif args.command == 'collect-global':
            from .collection import collect
            from .dump_discovery import load_feed
            result=collect(store,http,load_feed(args.feed) if args.allow_dump_discovery else None,max_seconds=args.max_seconds,
                batch_members=args.batch_members,progress=lambda r:print(json.dumps(r),flush=True),discovery=not args.queued_only,
                candidate_selection=getattr(args,'candidate_selection',None),
                orcid_record=getattr(args,'orcid_record',False),
                reuse_annual=getattr(args,'reuse_verified_annual',False),
                full_report=not getattr(args,'bounded_report',False))
            result['transport']={'mode':'single-persistent-https',
                'opened_connections':http.transport.opened_connections,
                'reused_connections':http.transport.reused_connections}
        elif args.command == 'discover-wikidata-dump':
            from .dump_discovery import discover_dump, load_feed
            if args.max_compressed_bytes is not None and args.max_compressed_bytes <= 0:
                raise ValueError('max-compressed-bytes must be positive')
            result = discover_dump(store,http,load_feed(args.feed),max_seconds=args.max_seconds,
                max_bytes=args.max_compressed_bytes,progress=lambda r:print(json.dumps(r),flush=True),
                stage_careers=args.stage_careers,max_reconnects=args.max_reconnects)
        elif args.command == "ingest-orcid":
            source = args.source
            validate_commit_members(getattr(args, 'commit_members', 1))
            if source not in orcid_archive_sources(task):
                raise ValueError('source must be an annual ORCID archive in the frozen registry')
            # This adapter cannot restore decoder state, regardless of CLI flags.
            committed=store.db.execute(
                'SELECT 1 FROM members WHERE source=? LIMIT 1',(source,)).fetchone()
            previous=store.db.execute(
                'SELECT members FROM runs WHERE source=?',(source,)).fetchone()
            if committed or (previous and previous[0]>0):
                raise ValueError('annual archive resume requires a verified decoder checkpoint; '
                                 'current adapter would replay committed input; stopped before HTTP')
            store.start(source)
            scanning = False
            try:
                with http.within(args.max_seconds), http.open_archive(source) as response:
                    scanning = True
                    result = stream_archive(store, source, response, max_members=args.max_members,
                        max_new_members=args.max_new_members,
                        commit_members=getattr(args, 'commit_members', 1),
                        max_seconds=args.max_seconds, progress=lambda r: print(json.dumps(r), flush=True),
                        full_report=False)
            except (OSError, RuntimeError, ValueError) as exc:
                # The stream owns parse/checksum status; an available download
                # with invalid content must not be reported as access failure.
                if not scanning:
                    store.status(source, "access_failed", detail=f"{type(exc).__name__}: {exc}")
                store.failure(source,'archive-scan' if scanning else 'download',f"{type(exc).__name__}: {exc}")
                # Settlement must not aggregate the entire growing database,
                # especially after a refusal or damaged archive.
                result = store.archive_report(source)
                if isinstance(exc, HTTPError):
                    # Preserve the refusal's stage without disclosing signed
                    # redirect URLs, query parameters or response bodies.
                    endpoint = (exc.filename or '').split('?', 1)[0].split('#', 1)[0]
                    result['archive_http_failure'] = {
                        'status': exc.code,
                        'stage': 'archive-scan' if scanning else 'download',
                        'request_kind': ('robots' if endpoint.endswith('/robots.txt') else
                            'frozen-archive' if endpoint == task.config['sources'][source]['url'] else
                            'other-endpoint'),
                    }
            if scanning:
                result['transport_reconnects'] = getattr(response, 'reconnects', 0)
        elif args.command in ("ingest-wikidata", "ingest-orcid-api"):
            ids = args.ids
            if args.discover or args.resume and not ids:
                if args.discover:
                    try:
                        print(json.dumps(enqueue_discovery(store, http, args.command,
                            limit=args.discovery_limit, scope=args.discovery_scope)), flush=True)
                    except StageDeadline:
                        print(json.dumps({'discovery_status':'resumable-stage-time-boundary',
                            'continuing_committed_queue':True}),flush=True)
                    except (OSError, RuntimeError, ValueError) as exc:
                        store.failure('wikidata', 'discovery:'+args.command, str(exc))
                        print(json.dumps({'discovery_failed': str(exc)[:256], 'continuing_committed_queue': True}), flush=True)
                ids=store.pending(args.command,min(args.max_members or 200,1000))
            if args.max_members:
                ids = ids[:args.max_members]
            options={'batch_size':getattr(args,'batch_size',WIKIDATA_BATCH_LIMIT)} if args.command=='ingest-wikidata' else {}
            result = (ingest_wikidata if args.command == "ingest-wikidata" else ingest_orcid_api)(store, http, ids,
                max_seconds=args.max_seconds, progress=lambda r: print(json.dumps(r),flush=True),**options)
        elif args.command == "quality":
            result = store.report()
            result["sqlite_integrity"] = store.db.execute("PRAGMA quick_check").fetchone()[0]
            result['events_without_member_evidence'] = store.db.execute(
                'SELECT count(*) FROM events e WHERE NOT ' + member_evidence_sql(observed_only=False)).fetchone()[0]
            archive_predicate,archive_params=verified_archive_sql(task)
            result['provisional_archive_events'] = store.db.execute(
                'SELECT count(*) FROM events e WHERE NOT '+archive_predicate,archive_params).fetchone()[0]
            from .frozen_input import verified_packed_snapshot
            archives = [p for p in task.root.rglob('*') if p.is_file() and p.suffix.lower() in ('.gz','.xz','.zip','.tar','.xml')]
            packed_inputs=[p for p in archives if verified_packed_snapshot(store,p)]
            raw_files=[p for p in archives if p not in packed_inputs]
            result['verified_private_packed_inputs']=len(packed_inputs)
            result["raw_source_files_detected"] = len(raw_files)
            result["raw_source_bytes_detected"] = sum(p.stat().st_size for p in raw_files)
            commands={source:command for command,source in CANDIDATE_SOURCES.items()}
            result["pending_discovery"] = {
                commands[row['source']]:row['pending']
                for row in result['acquisition_progress']['sources'] if row['pending']}
        elif args.command=='export-release':
            from .global_analysis import export_release
            from .frozen_input import freeze_input, frozen_reader
            from contextlib import nullcontext
            policy=json.loads(args.publication_policy.read_text(encoding='utf-8')) if args.publication_policy else None
            fresh_rows=args.freeze_input and getattr(args,'freeze_format','sqlite')=='rows'
            base=getattr(args,'freeze_base',None)
            if base and not fresh_rows:raise ValueError('freeze-base requires freeze-input with row format')
            allow_delta=getattr(args,'allow_delta_base',False)
            if allow_delta and (not fresh_rows or not base):raise ValueError('allow-delta-base requires a fresh row freeze and explicit base')
            if fresh_rows:
                from .row_snapshot import freeze_rows
                snapshot=freeze_rows(store,base_filename=base,allow_delta_base=allow_delta)
            else:snapshot=freeze_input(store,scope='global-ranking') if args.freeze_input else None
            if args.frozen_input:
                import re
                if not re.fullmatch(r'[a-f0-9]{64}\.(sqlite|rows)',args.frozen_input):
                    raise ValueError('frozen input requires a content-addressed filename')
                snapshot={'sha256':args.frozen_input.rsplit('.',1)[0],'filename':args.frozen_input,
                    'contains_personal_input':True,'publicly_bundled':False}
            # A fresh row archive was frozen under this sole writer lock. Score
            # the same unchanged live tables without allocating a second database.
            with frozen_reader(store,snapshot['filename']) if snapshot and not fresh_rows else nullcontext():
                if snapshot is not None and not fresh_rows and not snapshot['filename'].endswith('.rows'):
                    snapshot['bytes']=store.db.execute('PRAGMA page_count').fetchone()[0]*store.db.execute('PRAGMA page_size').fetchone()[0]
                result=export_release(store,args.out,publication_policy=policy,frozen_input=snapshot,
                    compress_snapshot=getattr(args,'compress_snapshot',False),compact_nodes=getattr(args,'compact_nodes',False))
        else:
            from .global_analysis import corroborate, export_release, export_local
            result = {"corroborate": corroborate,
                "rank-global": lambda s: export_local(s, compress_snapshot=True,
                    chunked=getattr(args,'chunked',False),
                    progress=(lambda item:print(json.dumps({'analysis_progress':item}),
                        file=__import__('sys').stderr,flush=True))
                        if getattr(args,'progress',False) else None)}[args.command](store)
            if args.command=='corroborate' and args.official:
                from .official_profiles import check_official_profiles
                result['official_profiles']=check_official_profiles(store,http,limit=args.max_pages)
                # Consume the fresh official checks in the same invocation.
                result.update(corroborate(store))
        print(json.dumps(result, indent=2, sort_keys=True))
        if args.command in ('ingest-easy-orcid','ingest-easy-orcid-full') and result['error'] is not None:
            raise SystemExit(2)
        if args.command == 'ingest-nobel' and result and result.get('status') == 'resumable-source-boundary':
            raise SystemExit(2)
        if args.command == 'ingest-orcid' and any(r['source']==args.source and r['status'] in ('access_failed','failed') for r in result['sources']):
            raise SystemExit(2)
    finally:
        try:
            if http is not None:http.close()
        finally:store.close()
    return True
