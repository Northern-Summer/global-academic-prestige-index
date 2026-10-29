# Bounded real-career ingestion

The task contract is `configs/streaming.json`. It limits new data to 2 GiB (including
the task-local optional DuckDB runtime, journals, release staging and builds), keeps
a 256 MiB write reserve, limits each transaction to 1,000 events, reads compressed
data in chunks no larger than 64 KiB, and samples process resident memory against
512 MiB. Defaults use the D drive on Windows. No cloud accounts or billing are used.

## Reproduce and resume

```powershell
$env:PYTHONPATH='src'
python -m gapi sources freeze
python -m gapi ingest-orcid --resume --max-members 100 --max-seconds 300
python -m gapi ingest-wikidata --discover --resume --max-members 200 --max-seconds 300
python -m gapi ingest-orcid-api --discover --resume --max-members 200 --max-seconds 300
python -m gapi corroborate
python -m gapi quality
python -m gapi rank-global
npm run dev
```

Repeat a discovery command to finish its **persisted discovery queue**, not to claim
another independent sample. The query selects a bounded live set, not a stable global
enumeration. Each Wikidata response has its own revision and hash. ORCID API snapshots
are separate from the annual archive. Previously committed API records are immutable
within this work directory; refreshing them requires an explicit source revision.

The annual ORCID archive is opened directly over HTTPS and gzip is decoded without
saving compressed or uncompressed material. A small tar reader processes headers
and members sequentially without extraction, accepts only regular summary XML members,
and rejects links, special headers, traversal paths, DTD/entities, excessive depth,
large fields and oversized records. SAX callbacks emit only education/employment
fields; no document tree, works, contacts or biography is retained. The public API
adapter also requests XML and incrementally parses it, rather than accumulating a
person's complete JSON record. Older bounded API JSON acquisition is preserved as
predecessor evidence; it is not evidence of the new XML streaming implementation.

SQLite owns the ingest log and atomic member transactions: `max_page_count`, small
transactions, memory-only temporary storage, explicit admission checks and the reserve
bound durable growth and rollback journals. An OS writer lock prevents competing
ingest processes and releases after process death. DuckDB is an optional analysis
dependency (`pip install .[analysis]`); Parquet export has 128 MiB RAM, one thread and
disk spill disabled. This split avoids claiming DuckDB's memory setting limits its
durable database or transaction-log size.

A partial gzip run reopens the same frozen source **from the beginning**. Previously
committed members are hashed again but not reinserted. Full CRC/trailer, upstream
checksum and advertised byte count must agree before the run becomes `verified`.
No checksum, truncated input, changed member hash or an incomplete stream cannot
produce a verified annual source. Normalized provisional rows can be retained for
replay, but the scoring reader excludes them.

## Actual source boundaries

The 2026 archive is [ORCID's frozen release](https://orcid.figshare.com/articles/dataset/ORCID_Public_Data_File_2026/34697766/1),
snapshot 2026-10-13. The public file locator observed on its UI is 70342863. At this
implementation checkpoint the local download/robots route returned HTTP 202 with an
empty body, and the public metadata API returned HTTP 403. Exact byte count and
upstream checksum remain unresolved. No attempt is made to defeat those controls.

ORCID's [Public API terms](https://info.orcid.org/public-client-terms-of-service/) grant
noncommercial API access; Wikidata [documents programmatic EntityData and SPARQL
access](https://www.wikidata.org/wiki/Wikidata:Data_access). Those exact API routes are
handled as documented interfaces rather than website crawls. Other routes obey robots;
authentication/access refusals are never retried via another host or impersonation.
ORCID API-derived publication remains disabled pending source-specific reuse review.
Wikidata is CC0. Copies and mirrored declarations are not independent corroboration.

## Local view and publication

### Bulk discovery independent of SPARQL

```powershell
python -m gapi discover-wikidata-dump --max-seconds 300 --max-compressed-bytes 268435456
python -m gapi ingest-wikidata --resume --max-members 200 --max-seconds 300
python -m gapi ingest-orcid-api --resume --max-members 200 --max-seconds 300
```

`configs/discovery-dump.json` binds the official 2026-10-26 Wikidata JSON dump,
exact size and upstream SHA-1. HTTP → streaming bzip2/gzip → bounded lines →
candidate ID queue avoids both SPARQL and search. The official documentation
describes [line-wise entity processing](https://www.wikidata.org/wiki/Wikidata:Database_download).
No archive, raw JSON line or person body is saved. The 2 MiB line cap drains larger
entities, records a rejected count and retains the coverage gap. Queue entries are
candidate IDs and line hashes; the ordinary adapters fetch their evidence separately.
Dump/API are still the same source, never independent corroboration.

Replay begins at the start, validates the committed prefix hash and skips previous
lines; arbitrary compressed-byte resume is not claimed. Only complete decoder EOF,
framing, compressed size and upstream SHA-1 agreement can mark enumeration verified.
Bounded scans remain provisional. Metadata is a separate immutable discovery-feed
record in the same budget/OS-writer boundary; it does not mutate the source lock.
The first real 32 MiB compressed batch processed 6,475 entity lines and queued 167
career candidates. This confirms a real bulk ingress, not whole-source completion.
Wikidata search and unauthenticated ORCID search were refused under current access
rules; no robots exception, account change or alternate protected host was used.

For a finite programmatic collection run use:

```powershell
python -m gapi collect-global --max-seconds 1800 --batch-members 50
python -m gapi corroborate --official --max-pages 20
```

The collection loop has one writer and one HTTP stream. It alternates bounded ORCID
and Wikidata queue work, suspends a source for the current invocation when no progress
is possible, and continues the other source. When queues cannot advance it attempts
the bulk discovery route once, increasing the previous compressed prefix bound.
The process ends at its time/resource/source boundary; it creates no scheduler or
background timer. Every invocation resumes the existing database. Access denials
remain denials and cannot change source trust or trigger authentication workarounds.

The official-profile adapter currently supports the exact TAMU-CC business-directory
layout and existing Wikidata P854 profile references. It incrementally reads HTML,
extracts only education lines and explicit academic/start-year statements, checks
stable institution crosswalks, and retains field/content hashes. Unsupported layouts
remain unsupported. Two real fields were verified in the initial official profile.
Because the Wikidata statements already cite that page, the check is explicitly
same-origin confirmation; neither copied evidence nor the dump/API paths become two
independent source classes. No formal-eligibility promotion is made.

Discovery uses ordered live Wikidata pages, atomically committing each page's query
hash, response hash, retrieval time, queue and last person identifier to SQLite.
Each `--discover --resume` invocation advances one page (at most 200 people) and
processes only unfinished members. A failed query leaves the cursor and existing
queue intact. Multiple conflicting ORCIDs for a person are not selected. Repeated
calls can expand beyond the initial selection; they are not a frozen dump scan:
upstream changes behind the cursor may be missed, and query exhaustion means only
that this live query returned no later rows. It never establishes global completeness.
Full scans and refreshes need a new explicitly versioned source selection.
Wikidata discovery defaults to explicitly declared university-teacher candidates;
their occupation is never converted into a job or education fact. The optional
`--discovery-scope degree-employer` selection has a separate cursor because its
more expensive join may time out on the public service. ORCID discovery groups
identifiers per person to avoid splitting a conflicting identity across pages.

`rank-global` creates a compact institution-only local view in the external task
directory. Its manifest pins the exact JSON bytes. It may contain observed scores
but never a formal rank without the required evidence and external review. Unknown
award and composite scores are null. A network's frozen node universe includes all
accepted relation endpoints, irrespective of formal eligibility. Directory-only
institutions have missing scores, not zero scores.

```powershell
# A local production build, including the explicitly selected local-only data:
$env:GAPI_RELEASE_MANIFEST='D:/GAPI/global-academic-prestige-index/global-local.json.manifest.json'
$env:GAPI_LOCAL_BUILD='1'
npm run build

# A public eligible observation release (refuses no-edge/unlicensed inputs):
python -m gapi export-release
# Unset GAPI_LOCAL_BUILD and select outputs/global/manifest.json before public build.
```

`GAPI_CODE_ONLY=1 npm run build` (PowerShell: set the environment variable first)
builds code/docs without publishing a score dataset. The public CI explicitly uses
this mode until an allowed real release manifest is selected. US research sample
data remain local; neither it nor personal trajectories are included by the release
plugin. Missing manifests, invalid paths, checksum mismatch and restricted datasets
fail the ranking build. The optional 3D module loads on demand and has a complete
table-based entry point; its displayed subset is explicitly limited.

## Remaining acceptance gates

The current source sweep is a bounded engineering proof. It is **not** a completed
global census. Completing source enumeration, independently confirming careers from
official institutional evidence, full stage/source/region/discipline/time sensitivity,
official award attribution and external dual-track review remain required. Parameter
rank ranges are conditional scenarios, not statistical intervals for unknown missing
global personnel. No external reviewer has passed this release.
