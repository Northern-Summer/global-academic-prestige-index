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
python -m gapi collect-global --max-seconds 1200 --batch-members 50
python -m gapi ingest-wikidata --discover --resume --max-members 200 --max-seconds 300
python -m gapi ingest-orcid-api --discover --resume --max-members 200 --max-seconds 300
python -m gapi normalize-careers
python -m gapi corroborate --official
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
adapter requests only `/educations` and `/employments` XML. Both root names and
exact ORCID paths are checked. Each section has a URL, retrieval time, byte count
and SHA-256 in `member_parts`; their canonical hash manifest identifies the member.
Both sections commit atomically: failure of the second rolls back the first's
events and evidence. Earlier whole-record snapshots remain immutable, and are
not silently refreshed. Older bounded API JSON acquisition is preserved as
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

### Bounded Wikibase entity batches

Wikidata ingestion now uses the official read-only
[Wikibase Action API](https://www.mediawiki.org/wiki/Wikibase/API), with
`action=wbgetentities`, `props=claims|info`, `maxlag=5` and at most **10 IDs**
per request. The HTTP reader keeps its existing **2 MiB response limit** and
64 KiB read chunks. This bounded JSON buffer is distinct from the incremental
XML/archive parser: it never holds the whole source, writes raw JSON to disk or
opens parallel HTTP streams. Each entity still has its own atomic transaction;
one invalid entity rolls back without losing its valid neighbors. Already committed
EntityData members retain their original evidence and are not downloaded again.

For new batch members, the ledger retains the exact response SHA-256, batch URL,
response byte count and retrieval time in `member_parts`. The member hash is the
SHA-256 of the canonical entity JSON; event evidence includes both hashes and the
entity revision/statement locator. Neither hash is described as the other. Batch
transport is another access channel to Wikidata, **not an independent source**.

An oversized batch may use the existing individually bounded EntityData reader.
HTTP 401/403/429, `maxlag`, access refusals and server pauses never trigger that
fallback. The documented-route exception admits only the exact claims/info query;
it does not authorize search, editing or extra parameters. `--batch-size 1` selects
the predecessor single-entity transport for reproducible diagnostics.

Caller time exhaustion has its own `StageDeadline` signal. Reading/connecting
across the deadline, or having insufficient time for the ordinary request interval,
leaves the member pending without recording a source failure or increasing its
retry penalty. A genuine earlier socket timeout still records failure; server
`Retry-After` still pauses the source. A partially read ORCID affiliation pair
continues to roll back both sections. Historical failures remain in the ledger.

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
The frozen acquisition lock retains its initial restrictive publication default.
The separate reviewed release policy below permits noncommercial institution aggregates.
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
and Wikidata queue work. Empty queues trigger repeated committed discovery pages,
not just one page per invocation. OpenAlex, Crossref and Wikidata discovery have
separate cursors and failure isolation. Duplicate-only pages can advance their
cursor without fabricating evidence. A failed discovery route is paused for the
invocation while other routes and existing queues continue.

Failed individuals receive persistent retry delays (60 seconds doubling to six
hours). One quarter of normal batch capacity is reserved for due retries; the
rest accepts fresh candidates, preventing starvation on either side. They remain pending/deferred, not
deleted or promoted to verified members. A 401/403 or exhausted 429 pauses that
source for the invocation; another person is never tried to work around an access
refusal. Source stages bound request/retry admission to their time window; an
in-flight read can still take its admitted socket timeout.

Long or HTTP-date `Retry-After` instructions persist as whole-source cooldowns,
not individual failures. Restarting the collector cannot erase that pause. Invalid
server dates cause a disclosed conservative six-hour pause. ORCID's
[anonymous allowance](https://info.orcid.org/ufaqs/what-are-the-api-limits/) is
25,000 reads/day per IP. A rolling-day task ledger reserves worst-case section
retries before any request, then settles attempted counts; a crash retains the
conservative reservation. It cannot account for other software on the same IP or
retroactively reconstruct earlier uninstrumented calls. The service's own limits
still apply. No credentials, additional IPs or paid access are used.

When queues cannot advance the loop attempts
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

### Independent candidate discovery with OpenAlex

```powershell
python -m gapi discover-openalex --pages 5 --per-page 200
python -m gapi ingest-orcid-api --resume --max-members 200 --max-seconds 300
```

The public OpenAlex author endpoint supplies only `id,orcid` with an opaque saved
cursor. No account/key is used, and paid access is never requested. Its own API
documentation permits basic [unauthenticated access](https://help.openalex.org/api/).
Every page validates ASCII ORCID formatting and ISO 7064 MOD 11-2 check digits,
commits queue/cursor atomically, and does not create career facts. Publication
affiliations are never converted to degrees/employment or independent confirmation.
OpenAlex's author count is a discovery-search count, not an academic population
denominator. Live cursor scans have update drift and cannot establish frozen coverage.
`collect-global` repeatedly advances this route when its ORCID queue is empty,
independently of Wikidata services, and continues other routes on failure. On
2026-10-29 the local first 200-row page succeeded and its next cursor returned HTTP
401. That page/cursor are retained; no key, account or protected-route workaround
was used. First-page success does not establish unlimited free pagination.

### Crossref candidate discovery independent of OpenAlex

```powershell
python -m gapi discover-crossref --pages 3 --per-page 50
```

The [official public REST API](https://github.com/CrossRef/rest-api-doc) documents
free unauthenticated metadata access, `has-orcid:true`, minimal-field selection and
cursor pagination. Only checksum-valid author ORCIDs enter the existing candidate
queue. DOI/author payloads, names and publication affiliations are not saved or
converted into careers or corroboration. The work count is not a people count or
global coverage denominator. Response hashes/cursors commit with candidates. The
response is capped at 2 MiB, pages at 100 works, and ordinary rate/retry rules apply.

Ascending-index feed `crossref-orcid-candidates/v1` returned three pages without
usable author ORCIDs despite filter hits; it remains as predecessor evidence.
`v2` uses a separate descending-index cursor: three real pages yielded 148, 150 and
158 valid identifiers per page (456 before cross-page/queue deduplication). Recency
selection and publisher deposits introduce bias; live index changes can cause
omissions/duplicates. This is not frozen enumeration.

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
python -m gapi export-release --publication-policy configs/publication-20261029.json --freeze-input
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

For historical reproduction, `--freeze-input` creates a content-addressed private
SQLite copy of normalized events/evidence/crosswalks in the external task's
`frozen-inputs` directory. These are structured computation inputs, not original
XML or archives. The copy is admitted against the same 2 GiB cap with write reserve;
if it cannot fit, freezing stops rather than silently increasing storage. Existing
snapshots are hash-verified and reused. They contain controlled personal inputs
and must never be uploaded as release assets. The manifest contains only the
snapshot filename/hash/size and its private status. Authorized reviewers need
controlled access to these inputs for exact numerical reconstruction.

```powershell
python -m gapi export-release --publication-policy configs/publication-20261029.json --frozen-input HASH.sqlite
```

The reader rejects hash mismatches, paths and writes. Later ingestion can change the
live database while an older selected input still reconstructs its original output.
The frozen copy is useful for the current bounded release; repeated copies remain
subject to the same budget and are not claimed to scale to unlimited version retention.

The explicit `--publication-policy configs/publication-20261029.json` release
selection permits noncommercial ORCID public-record institution derivatives under
the reviewed official terms. Its rationale, scope and hash are recorded in the
manifest; the acquisition source lock is not edited. Omitting the policy preserves
the predecessor restrictive default. This source-policy review is not an external
legal opinion or independent red-team approval. The legacy US study remains local.

The current source sweep is a bounded engineering proof. It is **not** a completed
global census. Completing source enumeration, independently confirming careers from
official institutional evidence, full stage/source/region/discipline/time sensitivity,
official award attribution and external dual-track review remain required. Parameter
rank ranges are conditional scenarios, not statistical intervals for unknown missing
global personnel. No external reviewer has passed this release.

## Independent official award inventory

```powershell
python -m gapi ingest-nobel --resume --source configs/nobel-20261029.json --max-pages 100 --max-seconds 300
```

The official developer zone documents a key-free REST API. Its current terms allow
CC0 use subject to service terms and minimizing server load. This adapter requests
at most 25 recipients per page, caps responses at 2 MiB, obeys the shared rate,
deadline and source-pause rules, and stores no original response. A separate frozen
source specification in the same budgeted task database extends the registry
without rewriting previous acquisition identities. A single writer still owns
acquisition; run this after the career collector releases its lock.

Private normalized events retain a source-specific recipient key, award category,
year, official prize portion and reported award-time affiliations. Page, hash and
cursor commit atomically. Duplicate recipients/awards, count drift, incorrect
cursors and malformed pages stop that source without erasing prior pages. Resume
uses the committed offset; completed selections make no network calls. Multiple
versions are never silently added together.

The public inventory includes category/type counts, affiliation labels/mentions
and evidence-index hashes. Labels are not guessed canonical institution IDs:
historical names may describe the same organization. Multiple affiliations are
disclosed without invented fractional institutional attribution. Nobel coverage
alone cannot satisfy the complete award catalogue, and no university award or
composite score is imputed. A live offset selection has temporal drift limits even
when its processed unique recipient count equals the API's reported total.
# Primary continuation: finite-response APIs

The active `collect-global` route uses Crossref/OpenAlex/Wikidata as independent
candidate-discovery queues and ORCID education/employment XML endpoints plus
Wikidata EntityData for career observations. Each response is read in bounded
chunks, normalized immediately and discarded; only selected events, evidence
hashes and checkpoints persist. A single explicit Wikidata ORCID also queues a
separate-source fetch atomically. Conflicting identifiers do not trigger a fetch
or an entity merge. A discovery lead is never corroboration by itself.

No large compressed archive is a prerequisite for this route. The upstream 2026
summary's known 57,290,954,546-byte size/MD5 is metadata, not a local download or
a completed scan. Optional old archive code is not invoked by the active API loop.
Raw XML/archive files stay absent from the external task directory. The 2 GiB
budget includes normalized databases, journals, frozen inputs and release outputs.

After a finite collection batch, run `normalize-careers`, `corroborate`,
`project-awards`, `quality`, and freeze/export a new institution-only release.
`project-awards` is deterministic and does not fetch/download source material.
Only a verified committed manifest goes to the automatic deployment route in
[CI_CD.md](CI_CD.md). Full source enumeration and world coverage remain distinct.

The collector now defaults to finite APIs even when every API is temporarily
unavailable. It never falls through to a compressed dump automatically. The legacy
streaming dump-discovery route requires explicit `--allow-dump-discovery`; the
archive itself still never lands on disk. Interrupted API batches preserve the
committed queue and retry deadlines instead of requiring a large download.

`collect-global --queued-only` drains committed discovery candidates and their
explicit stable-identifier followups without adding new discovery pages. It never
starts dump discovery. Temporarily unavailable records remain pending, including
those under persisted retry deadlines; they are not counted as received. The
quality and public observation summaries hash the current candidate selection and
separately report received, rejected and pending members for each source. A source
queue reaching zero is completion of that named selection, never evidence of a
complete world personnel census. A world denominator is explicitly unavailable.

The public page's expandable acquisition panel reads those same frozen selection
counts. Each source gets its own native accessible progress bar, received/rejected
totals and outstanding retry count. It describes that release's snapshot, not a
live connection to private inputs. Even a fully processed candidate selection
cannot claim global coverage. Browser validation rejects inconsistent totals,
duplicate source rows and a fabricated world denominator before rendering.

The writer's SQLite connection allows up to five seconds for a brief read-only
monitor transaction to release its shared lock. The separate OS writer lock still
rejects a second writer immediately. Longer reader locks can still interrupt a
batch; the uncommitted member rolls back and remains queued. Monitor readers
should use bounded queries and close their transaction promptly, preferably
reading frozen inputs when they do not need current progress.

## Evidence decisions and qualification checks

`corroboration.py` derives a versioned, hashed decision from frozen structured
evidence; acquisition events are never rewritten. For the supported university
profile layout, the official page must explicitly link the same valid ORCID as
the person's self assertion. A Wikidata reference supplies only a candidate URL.
Missing, ambiguous or different official ORCIDs cannot promote a fact. University
pages and their Wikidata transcriptions remain one origin. Client assertions,
uncommitted records, unhashed API affiliation parts and old profile rule versions
cannot satisfy this route.

Required matching fields are canonical ROR institution, event kind, degree or job
stage and graduation year / employment start year. Other dates must not conflict
when both sources provide them. Single-source optional dates are not claimed as
corroborated. Each decision lists its supported fields. The profile adapter's
completed-qualification check shares the ORCID normalization exclusion owner;
unfinished, candidate and honorary degrees cannot be confirmed. The acquisition
v1 contract remains frozen; these stricter checks belong to the derived view.

One person/stage/employer/training relation contributes once. All valid alternative
sources survive deduplication; evidence selection and qualification do not depend
on event ID order. Source deletion replays the join after removing that source,
including evidence decisions. Missing real independent evidence remains missing;
the existence of this mechanism does not establish global corroboration coverage.

## Reciprocal institution identifiers

```powershell
python -m gapi resolve-institutions-online --max-aliases 1000 --max-seconds 1200
```

`institution_bridge.py` processes at most 20 observed Ringgold IDs per query. A
Wikidata query discovers candidates; it cannot authorize a merge. The subsequent
revision-pinned entity must carry one current unqualified Ringgold ID (P3500) and
one ROR ID (P6782). The frozen ROR directory must independently point back to that
same Wikidata entity and ROR ID. Qualified/historical statements, multiple candidate
entities, multiple current IDs, missing reciprocal IDs and existing alias conflicts
stay unresolved. Institution names never decide these matches.

The normalized ledger keeps the versioned specification, query/response hashes,
entity revision/hash, statement IDs and frozen ROR identity. The acquisition sweep
receipt bounds the retrieval time window; it does not invent an exact timestamp
for older individual queries. No original response or personal trajectory is saved.
Every completed alias decision, including unmatched or ambiguous, resumes without
refetching within that version. A network failure leaves the current batch pending.
The adapter shares WDQS and EntityData pause keys with the existing source workers.
Its output is institution identity evidence, not independent confirmation of a
person's degree or employment, and enumerates only observed IDs rather than the
world's institutions. A new source/identity version is required to refresh completed
decisions. Public manifests bind the alias projection's checksum separately.

Bridge v2 consumes only the current frozen-directory scan's unambiguous,
conflict-free reciprocal results. Conflicting alias rows are preserved as history
and excluded from the active projection used by ranking, official checks and
corroboration. Neither stale rows nor old provenance can restore eligibility.
Predecessor bridge decisions are rechecked against that same rule without
refetching already stored snapshots; their original evidence remains in the
versioned ledger. Each rejected predecessor is excluded from active aliases.
Source snapshots and issue indexes are hashed in the release manifest.

## ORCID identity redirects

The HTTP boundary stops ORCID redirects before following them, closes the old
response and preserves request accounting. Only HTTP 301/308 from the exact
requested public v3 affiliation URL to another checksum-valid ORCID on that same
host, version and section can discover a new candidate. Temporary redirects,
changed sections, query/fragment URLs, credentials and invalid identifiers cannot
queue targets. This rule never merges people or treats a redirect as career evidence.

The existing discovery-page ledger stores normalized redirect metadata: the
original URL, status, destination URL, retrieval time and metadata SHA-256. This
is explicitly a header-metadata hash, not an unseen raw response-body checksum.
Repeated observations are idempotent. Any partial education/employment transaction
for the old identity is rolled back. The target requires its own complete verified
affiliation pair and retains its own pseudonymous person key. The original stays
pending with its failure and retry deadline, so redirects cannot manufacture a
100% successful retrieval rate. Nothing is written to a raw archive/XML cache.

A live bulk `api.php` experiment was rejected by robots before the entity request;
it created no bridge decisions. This route has been removed. Entity acquisition
continues only through the previously allowed individual `Special:EntityData`
URLs. No authentication, robot rule or access limit is bypassed. Oversized entity
responses receive an explicit unresolved rejection count, allowing later entities
to continue without sampling or promoting the rejected identity.
