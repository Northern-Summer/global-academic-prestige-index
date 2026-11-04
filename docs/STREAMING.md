# Bounded real-career ingestion

The task contract is `configs/streaming.json`. Its authorized total is
32,000,000,000 bytes (32 GB, not 32 GiB), including source databases, retained
inputs, the complete checkout, dependencies, Git history, declared external
deliveries, journals, release staging and builds. It keeps
a 256 MiB write reserve, limits each transaction to 1,000 events, reads compressed
data in chunks no larger than 64 KiB, and samples process resident memory against
512 MiB. Defaults use the D drive on Windows. No cloud accounts or billing are used.

The original 2 GiB limit remains the historical baseline for earlier measurements.
On 2026-11-01, after lossless primary-key delta repacks saved 9,544,716 bytes,
freezing the new real input still failed the 2 GiB admission check. The user then
authorized a conditional increase to 4 GiB. The configured ceiling was raised;
the 256 MiB reserve, 512 MiB process limit, single writer, no raw-file storage and
incremental transaction limits remain unchanged. Older sections recording 2 GiB
describe their original test conditions rather than a second active limit.
On 2026-11-03 the user authorized the current 32 GB total. This ceiling is an
admission limit, not an estimate or proof that complete acquisition fits.

`Task.used` owns storage measurement. It scans the complete project directory
and shared resource tree on each admission, plus optional absolute directory
paths in `additional_storage_roots`. Overlapping directory roots count once;
resource and extra delivery trees retain their link/reparse rejection even
when nested under another counted root. Their registered paths and ancestors
are checked before overlap removal, so a junction alias cannot bypass this
restriction or count the same physical tree twice. Project file links retain target-size
accounting; linked project directories are not traversed and any task-owned
external content must be explicitly registered. Counts are ordinary logical
file sizes, not physical disk allocation or an atomic filesystem snapshot.
Missing files during observation may reflect committed journal removal;
permission failures stop admission. Separate physical volumes retain their
own free-space constraints; the total budget does not pool their free space.

Additional roots are part of the frozen configuration. A nested source must
declare the same roots as its owner. Register a new delivery only at a settled
writer boundary using the existing source-freeze transition procedure, preserving
the preceding freeze and the nested source's owner hash. Editing a running
collector's config does not update its in-memory accounting.

## Reproduce and resume

### Separate annual-source freezes within one resource allowance

A task may set `resource_root` to its existing frozen parent's external work
directory and `resource_freeze_sha256` to the SHA-256 of that parent's
`sources.lock.json`. Its own `work_dir` must be a strict descendant. Budget,
reserve and memory limits must equal the parent's frozen limits; nested resource
owners are rejected. `Task.require_resource_contract` checks the pinned owner
on admission and resume. All sibling source databases and retained inputs count
toward the same allowance, and `exclusive_writer` uses the parent's writer lock.

This permits a new annual archive identity and database without copying or
mutating the previous source freeze. It does not increase capacity or verify
the archive. The annual stream must still reach EOF and pass the upstream
checksum before it contributes relationships or denominators. Separate-source
records are not automatically merged into the older production database or
published scoring snapshot. A stopped gzip stream must replay from the start.

`configs/orcid-2026-production.json` pins the 2026 summary filename, official
download URL, advertised length and MD5 from the recorded metadata, while
retaining the original task freeze. Start its single stream with
`python -B -m gapi ingest-orcid --task configs/orcid-2026-production.json --source orcid-2026-summary --resume --max-seconds 7200`.
The first official tar header uses PAX `mtime` metadata. `read_mtime_pax` accepts
only one length-checked timestamp record of at most 128 bytes, with zero padding;
path, link, size, encoding, sparse and global PAX overrides remain rejected.
An accessible gzip prefix is not full enumeration or checksum verification.

`SummaryParser` enables Expat text buffering with an explicit 8 KiB buffer.
This reduces fragmented Python character callbacks while retaining the existing
2,048-character affiliation-field limit, structural handlers and DTD/entity
rejection. Differential tests compare buffered and unbuffered parsing across
single-byte and normal chunks, entities, CDATA, multilingual text and malformed
inputs. See [Python's Expat buffering contract](https://docs.python.org/3/library/pyexpat.html#xmlparser.buffer_text).
The local synthetic microbenchmark is not a full-archive throughput measurement;
running collectors retain the parser implementation loaded at startup.

### Optional bounded archive commits

`ingest-orcid --commit-members 32` groups complete normalized members into one
atomic transaction. The default remains one member; the explicit limit is
1–128. Each group also obeys the existing `batch_events` event limit. A pending
group and the current member can each hold at most `batch_events` normalized
events; each event retains the existing 16 KiB limit. No raw XML is buffered or
written to disk. Process memory checks remain active during parsing and commit.

Storage is fully measured before and after every transaction. The existing
sole-writer lock, SQLite growth cap, FULL synchronous commits and rollback
reserve remain in force; no cached storage estimate substitutes for admission.
Events, member digests and source member counts commit together. Rejected
members discard partial events; duplicate buffered members still require exact
digest agreement. Progress callbacks flush pending groups before reporting.

A normal bounded stop flushes complete pending members. An exception or process
exit may leave the last group uncommitted; restart replays the gzip stream and
those members under the same frozen identity. Counts reflect committed rows.
EOF, gzip trailer, advertised length and upstream checksum gates remain
mandatory, including when the final group fails to commit. Successful group
commits alone never verify the annual source.

Use the explicit batching option only on the next normal collector start,
after the preceding writer settles. It changes no source identity or ranking
rule and requires no database migration. Do not restart a healthy full read
to enable it. Same-input synthetic benchmarks exercise storage admission and
fsync but do not establish full-archive throughput or capacity.

On 2026-11-03, five alternating same-input runs of 128 synthetic one-event
members measured median times of 2.361 seconds with individual commits and
0.126 seconds with groups of 32. Complete storage scans fell from 257 to 9,
while logical event hashes and all member counts matched. The isolated recheck
ran after the local regression finished; the existing production collector
continued unchanged. This 18.7-fold synthetic result isolates commit overhead
and must not be presented as a measured full-archive speedup.

### Optional event block maintenance

Bounded real-event query experiments found slower ID and person traversal with
128-event blocks. Production migration is therefore deferred; payload savings
alone do not justify adopting this representation. Group-specific dictionary
experiments also did not establish sufficient benefit for migration.

`repack-event-pages --blocks` groups at most 128 normalized event payloads into
one independently hashed block. Event columns, rowids, indexes and provenance
remain intact. The existing writer lock, task budget and transaction admission
apply; run maintenance only after the collector has safely settled. The default
page-repack mode and ingestion format remain unchanged. `--blocks` and
`--share-sources` are mutually exclusive.

```powershell
python -B -m gapi repack-event-pages --task configs/orcid-2026-production.json --blocks --max-events 20000 --max-seconds 60
```

Resume with the returned `after_rowid`. Each batch commits its block and
references together, verifies every original payload and skips existing block
references on replay. Readers check the stored block bytes before consulting a
single-block decode cache; missing, changed, truncated and corrupt dependencies
are rejected. Full database backups retain the blocks; ranking SQLite and row
snapshots convert them to self-contained legacy payloads. Existing freezes
remain readable. No blocks or dictionaries are deleted by this command.

Payload savings and reusable SQLite pages are distinct from filesystem space
reclaimed. Neither bounded sample measurements nor successful maintenance prove
that the complete annual source fits within the task budget.

### Bounded dictionary decompression reuse

`event_storage.load_dictionary` reads the current compressed dictionary bytes
from SQLite on every lookup. Up to eight exact compressed byte strings and
their decoded dictionaries are cached; each compressed input is limited to
`MAX_DICTIONARY_BYTES + 64` bytes and each decoded dictionary to
`MAX_DICTIONARY_BYTES`. The event decoder still checks the requested SHA-256.
Deletion, replacement, rollback and different database contents cannot reuse a
previous value merely because the dictionary key matches. Invalid decoding is
not cached. This changes no stored data and requires no migration.

Paired local experiments on three 2,048-event annual-source samples compared
the same SQLite readers with and without decompression reuse. Across rowid,
ID and person order, median speed ratios were 1.14–1.31 over five alternating
repeats; every returned row matched. These in-memory sample results do not
measure full-archive throughput, disk performance or production capacity.
Running collectors retain their startup implementation until a normal restart.

### Compact member digests

New member commits encode lowercase 64-character SHA-256 values using the same
bounded, reversible digest codec as the candidate queue. Owned readers still
return the exact hexadecimal text. Uppercase or legacy arbitrary values retain
their original TEXT representation; member locators, statuses, rejection reasons
and replay checks are unchanged. The digest codec and column names have one
owner in `queue_storage.py`; member and candidate maintenance share its bounded
transaction implementation.

`compact-member-digests` converts existing member hashes in batches of at most
128 rows under the existing writer lock, budget and reserve. Resume using the
returned `after_rowid`. Repeated compaction does not write already encoded rows.
Oversized legacy rows are retained and counted rather than widening the memory
or journal bound. Triggers, incoming foreign keys and unexpected ledger columns
are rejected before mutation.

```powershell
python -B -m gapi compact-member-digests --task configs/orcid-2026-production.json --max-rows 100000 --max-seconds 60
```

Run this only after the collector settles. Logical member hashes and all three
snapshot routes remain readable; this changes storage bytes, not the upstream
checksum algorithm or the source-integrity gate. Reusable-page savings are not
filesystem reclamation or proof of full-source capacity.

### Read acquisition progress

`acquisition-status` reports `candidate_processing` separately from
`source_integrity`. Candidate processing fractions use only the currently
discovered selection, identified by its selection hash. A finished selection
does not prove complete source enumeration. The legacy
`acquisition_progress.sources[].enumeration_complete` field is retained for
snapshot compatibility; it means that selection has no pending candidates.
New consumers should use `candidate_processing.sources[].processing_complete`.

`source_integrity.sources` lists every configured annual ORCID archive,
including sources without a run. Full enumeration and integrity are true only
for a parser-verified run whose source identity matches the configured frozen
source and rules. `all_archives_verified` covers those annual archives only;
it does not establish completion of other discovery feeds, independent fact
corroboration, or world coverage. No global personnel percentage is available.

Freeze a finite API candidate set only after the active writer has settled:

```text
python -m gapi freeze-candidates --task configs/streaming.json
python -m gapi acquisition-status --task configs/streaming.json --candidate-selection SELECTION_ID
python -m gapi collect-global --task configs/streaming.json --queued-only --candidate-selection SELECTION_ID --max-seconds 900
```

The returned selection ID binds the frozen task configuration, a discovery
rowid upper boundary, and each source's candidate count and provenance hash.
The small manifest lives in the existing database; candidate IDs are not copied.
Repeated freezing of unchanged inputs reuses the same record. Later discovery
is outside the original denominator. Candidate deletion, identity/provenance
changes or configuration drift invalidate that selection rather than silently
changing its denominator. Lossless digest maintenance preserves its identity.

After a settled storage-root registration, the former selection still binds its
old configuration. Use `candidate_selection.rebind_storage_roots` with the
retained preceding source freeze under the existing writer lock to inherit that
selection. It verifies the stored predecessor, original rowid boundary, source
counts and provenance before atomically storing a successor for the new frozen
configuration. Only additional accounting roots and the nested resource-owner
pin may differ; roots cannot be removed and allowances, source policy and rules
must remain identical. It does not perform the multi-file resource transition.
The preceding record remains intact, retries reuse the successor, and later
discoveries stay outside the original denominator. Do not use a fresh
`freeze-candidates` operation to inherit an earlier selection.

Frozen collection disables discovery, filters both fresh work and due retries
through the same boundary, and verifies the selection before requests and again
at settlement. `frozen_candidate_processing` reports per-source processed and
observed fractions with the fixed candidate counts. A rejected member is counted
as processed but prevents `processing_complete`; pending retries remain pending.
With a progress callback, collection also emits `frozen-candidate-processing`
at startup and after a settled batch when at least 900 seconds have passed.
Each interval update reruns the selection owner, including its provenance checks;
it does not extrapolate from event or member deltas. There is no background timer,
second database or live reader snapshot. No interval work is done without a
callback, after storage contention, or once the collection deadline is reached.
An admission or integrity failure propagates with existing commits preserved.
Running processes retain their loaded code; these updates begin on the next
normal invocation rather than through a collector restart.
The legacy current-queue report is retained. Neither finite candidate completion
nor an empty frozen queue establishes full-source enumeration, verified facts,
annual archive integrity, or global coverage. A rejected-member API redirect
outside the frozen set requires a later selection; it cannot expand this one.

Each annual source also exposes `member_processing`: committed, parsed,
rejected and unclassified member counts, plus reconciliation against the
transactional `runs.members` counter. `all_archive_members_processed` requires
verified archive integrity, reconciled counts, and no rejected or unclassified
members in any configured annual source. It covers annual member parsing only,
not dynamic API queues, independent facts or ranking qualification.

A fully checksum-verified archive may contain `<error>` documents instead of
summary records. These remain rejected and unsettled. Oversized members and
other parse failures also remain unsettled; no rejection reason is automatically
promoted to a legal exclusion. `legal_exclusions_verified` is unknown (`null`)
because the current ledger has no independently verified exclusion decision.
The parsed fraction has a denominator only after archive integrity completes;
partial scans return `null`. Legacy integrity fields retain their byte-level
meaning and cannot substitute for member processing.

After the active collector releases its writer lock, rejected annual members
can nominate follow-up candidates in the existing ORCID public API queue:

```text
python -m gapi queue-archive-rejections --task configs/orcid-2026-production.json --max-members 10000 --max-seconds 60
```

Use the returned `after_rowid` with `--after-rowid` to continue committed groups.
Only frozen annual source identities, matching member paths, valid ORCID IDs
and SHA-256 digests qualify. This command performs no HTTP requests and retains
existing candidate provenance. Repeated scans insert no duplicate candidates;
it never changes annual rejection reasons, creates career events, or establishes
independent corroboration. Subsequent API retrieval uses the existing queue,
backoff and source policy. A retrieved API record does not retroactively repair
the annual member or complete its settlement.

Member-ledger progress uses the authoritative `members` table. A source start
reconciles its `runs.members` counter once; each newly inserted member advances
the counter in the same transaction. Repeated members do not advance it, and a
rollback undoes both changes. This avoids recounting an entire source ledger
after every person. It changes neither source evidence nor qualification rules.

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

`collect-global --orcid-record` explicitly selects the optional single `/record`
XML response for new API members. The default remains the two affiliation routes.
The SAX parser still emits only education/employment fields; other record fields
are neither retained nor used. This route keeps the same decoded byte/event caps,
single connection, request interval, rolling-day allowance and atomic member
transaction. It reserves one request group instead of two, including retries.
Already committed members are skipped regardless of the selected route; this
does not refresh a frozen record or create duplicate careers. A refused, malformed,
oversized or interrupted response never falls back to another endpoint.

Record events declare `incremental-record-xml/v1` and the real `/record` URL and
locator. One `member_parts` row named `record` hashes the entire decoded response;
the member digest binds that representation and hash. It is never described as
two section response hashes. The corroboration reader requires the exact same
person's record URL, locator and committed member manifest before applying the
unchanged fact/independent-origin rules. Existing section evidence remains valid.
Different transport evidence has different evidence digests even for equal facts.
This option halves nominal requests per new member, not necessarily elapsed time:
whole-record bodies may be much larger. Production adoption needs a current bounded
comparison under the ordinary writer exclusion; local equivalence tests alone do
not establish live throughput or full-source completion.

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

For annual archive replay, `ingest-orcid --max-new-members 500` stops after 500
previously uncommitted members, including rejected members. The existing
`--max-members` remains a bound on all members scanned in this pass; if both are
given, the first reached limit stops the pass. Previously committed members are
read and hashed without a member write transaction. Their stored hash must match
before replay advances. Reaching a member limit leaves the archive provisional,
even when the last data member has been consumed; full EOF and checksum validation
still require a complete pass. The time limit also includes prefix replay, so a
short time window can stop before any new member. This reduces redundant writes
but does not remove gzip replay costs or provide random access.

The CLI defaults to `orcid-2026-summary`. Use `--source SOURCE_KEY --task TASK_CONFIG`
to select another annual archive already present in that task's frozen registry.
Only sources with kind `orcid-summary-tar-gzip` are accepted. Source selection does
not edit a freeze or merge work directories. The separately frozen 2025 pilot
remains isolated; it is not merged into production or treated as fully verified
archive data.

Relation scoring, observed-training denominators and the quality audit share the
same registry-based archive verification predicate. Every configured annual
archive requires a `verified` run, regardless of its source key or publication year.
A missing, partial or failed run cannot contribute relationships or denominator
observations. This applies to both original and versioned event interpretations;
member evidence is also required in both denominator paths. Per-record API sources
retain their existing evidence requirements.

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

The discovery ledger's `compressed_bytes` and `sha256` retain the longest observed
compressed prefix as a matching count/hash pair. A shorter replay or failed open
must not reduce that continuation ceiling or replace its hash with an empty one.
The command result's `compressed_bytes` reports bytes from the current attempt;
neither quantity implies a full-source checksum or random-access resumability.
The committed line prefix is checked separately before new candidates are queued.

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
directory. The CLI stores deterministic gzip and its manifest pins both stored
and decoded bytes. The development server and local production builds use the
same bounded snapshot reader, while retaining support for legacy JSON inputs.
The browser URL remains `global-local.json`; no personal input is exposed.
It may contain observed scores
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

The official-profile adapter now supports the exact CU Experts
`https://experts.colorado.edu/display/fisid_<digits>` route in addition to the
existing TAMU-CC directory. It reads only the dedicated education list's explicit
degree, schema-marked university name and year, and the dedicated ORCID list.
Literal HTTP ORCID identifiers in official HTML are parsed as identifiers without
following an HTTP link. Fetches still use HTTPS, robots checks, finite timeouts and
the existing 2 MiB response bound. Shared completed-qualification rules own degree
interpretation; the adapter never treats publication affiliations as jobs.

Profile rule version 5 invalidates older derived checks until refreshed. In
addition to checking the original cited event, an official page with exactly one
valid ORCID can support a degree explicitly present on that page against that
ORCID's self assertion. Matching requires a resolved ROR institution, exact
normalized institution name, completed stage and degree year. The corroboration
owner separately requires committed member evidence, the affiliation part hash,
an ORCID-owned assertion and the current official person binding. An employment
citation is only a discovery route and never becomes degree evidence itself.
This direct official/self support survives removal of Wikidata from a sensitivity
scenario; removing ORCID removes that support. No original event is rewritten.

Unique official ORCIDs enter the existing discovery queue atomically with their
profile hash and identity binding. Repeat reads do not duplicate candidates.
The existing bounded half of fresh priority slots first selects official-bound
identities, then Wikidata-linked identities; ordinary fresh work, due retries and
source backoff retain their reserved capacity. No separate queue is created.
Successful parsing, stable identity and independent corroboration remain separate
results. Adapter tests alone do not establish real independent confirmations.

For storage-constrained publication, `--freeze-input --freeze-format rows` streams
normalized table rows directly into a private `HASH.rows.xz` archive without a
temporary SQLite copy. The logical row stream is SHA-256 addressed; its schema,
table counts, frozen acquisition progress and original binary payloads are retained.
Every line is bounded to 1 MiB. The archive is fully decoded and hash checked
before promotion. A fresh export scores the unchanged live tables under the same
exclusive writer lock. It does not restore a second database locally.

`--frozen-input HASH.rows` restores the verified archive into an owned temporary
SQLite file only after separate disk and memory admission. Failure cleans only
that temporary file and its journal. Restoration may require more space than is
available beside the live database; the archive is not a claim that full-scale
local restoration has passed. Raw source responses and excluded acquisition tables
are not included. This format is a private ranking input, not an acquisition backup.

For later releases, add `--freeze-base HASH.rows` to retain an exact row-stream
difference against an existing full baseline. `HASH` must identify a retained
full `HASH.rows.xz`; delta chains are refused. The baseline is never deleted.
The archive stores copy, skip and exact added-line commands, synchronized with
at most 32 lines / approximately 5 MiB of lookahead on each side. The difference
does not infer, merge or alter scientific events. Full baseline and reconstructed
target SHA-256 identities must both pass before promotion. Missing, changed or
truncated inputs, malformed commands, trailing data and budget failures reject
the candidate; owned staging alone is removed. No SQLite staging copy is needed.

The logical filename remains `TARGET_HASH.rows`, physically represented by
`TARGET_HASH.rows.delta.xz`. Existing restore and quality commands verify the
reconstructed logical stream. Restoring SQLite still requires separate resource
admission. A changed scientific record is preserved literally, and skipped rows
remain recoverable in the immutable baseline. Repeated identical inputs preserve
the archive identity and mtime. No additional compression benefit is guaranteed;
admission remains subject to the same task budget even when a difference grows.

For historical reproduction, `--freeze-input` creates a content-addressed private
SQLite projection of normalized events/evidence/crosswalks in the external task's
`frozen-inputs` directory. These are structured computation inputs, not original
XML or archives. The copy is admitted against the same 2 GiB cap with write reserve;
if it cannot fit, freezing stops rather than silently increasing storage. Existing
snapshots are hash-verified and reused. They contain controlled personal inputs
and must never be uploaded as release assets. The manifest contains only the
snapshot filename/hash/size and its private status. Authorized reviewers need
controlled access to these inputs for exact numerical reconstruction.

New releases use `global-ranking-private-snapshot/1`: the current-events ranking
does not read the independently scored Easy ORCID `easy_careers`/`easy_full_delta`
tables or provisional `dump_landing`, so those tables are excluded. The live
candidate queue is replaced by the exact selection hashes/counts produced by
`CareerStore.candidate_progress` inside the same read transaction. All other tables,
optional award/identity inputs, indexes and views are copied in primary-key order,
at most 100 rows or 2 MiB per batch. Event text uses the existing lossless GAPIz1
storage owner. Triggers and virtual tables are rejected. Every batch must pass the
same full storage admission; no full-database staging copy is needed. The private
manifest records scope and excluded row counts. This is a ranking input snapshot,
not a recoverable acquisition backup and not an Easy ORCID input snapshot.

After integrity verification the projection is deterministically gzip-packed and
verified before retiring its uncompressed successor. `--frozen-input HASH.sqlite`
still reads existing full snapshots and new scoped snapshots; exact scores and
candidate selection metadata must replay with the pinned code/configuration.

```powershell
python -m gapi export-release --publication-policy configs/publication-20261029.json --frozen-input HASH.sqlite
```

The reader rejects hash mismatches, paths and writes. Later ingestion can change the
live database while an older selected input still reconstructs its original output.
The frozen copy is useful for the current bounded release; repeated copies remain
subject to the same budget and are not claimed to scale to unlimited version retention.

Private historical snapshots can be packed without changing their logical filename
or original SHA-256:

```powershell
python -m gapi compact-frozen-inputs --filenames HASH.sqlite OTHER_HASH.sqlite
```

This command takes the existing exclusive writer lock, validates the requested
filenames, streams deterministic gzip into a staging file, verifies the complete
decompressed SHA-256, atomically publishes `HASH.sqlite.gz`, and only then removes
the verified raw SQLite predecessor. Repeating it verifies the packed successor
without creating another copy. This is compression of derived private inputs,
not storage of upstream raw archives. Never upload these files.

An existing verified gzip can be streamed directly into a smaller XZ successor:

```powershell
python -m gapi compact-frozen-inputs --codec xz --filenames HASH.sqlite OTHER_HASH.sqlite
```

This optional maintenance keeps the same writer exclusion and original logical
input identity. It never creates an uncompressed SQLite staging file. The gzip
is fully hashed while streaming; the XZ successor is fully decoded and hashed
before atomic publication, and the predecessor is reverified before retirement.
If XZ is not smaller, gzip remains. Preset 3 bounds compression memory; XZ reads
limit the decoder dictionary to 32 MiB and each output chunk to 1 MiB. Truncation,
trailing bytes, a wrong hash, existing foreign staging and budget refusal fail
closed. A crash after successor publication retains both verified representations
until the same command settles retirement. Repeated completed runs only verify
the successor. These files remain private, structured derived inputs.

`--frozen-input HASH.sqlite` accepts raw SQLite, gzip and XZ representations. Packed reads
restore a temporary SQLite file within the same external task budget, verify its
original hash, and remove it on normal exit or a handled exception. A process kill
may leave `frozen-read-*.sqlite`; inspect active readers before retiring a leftover.
A failed compression staging file is preserved after a process kill and blocks
another attempt until inspected. Unpacking can stop at the budget boundary;
compression does not promise that an arbitrarily large snapshot can be restored.
Pinned older code that predates a storage reader requires explicitly decompressing
the gzip or XZ under the same budget and verifying the original filename SHA-256
first. Full acquisition snapshots retain their full-copy admission rule. Public
current-event freezes use the scoped ranking projection described above. Packing
old snapshots does not waive either release gate or provide unlimited retention.

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

An earlier bulk `api.php` experiment was rejected by the website-crawling gate
before the entity request; it created no bridge decisions. That bridge experiment
remains retired. The later bounded `wbgetentities` ingestion route described above
uses the documented read-only API contract, alongside `Special:EntityData`;
it does not restore the retired bridge route or change any observed access denial.
No authentication or server access limit is bypassed. Oversized entity
responses receive an explicit unresolved rejection count, allowing later entities
to continue without sampling or promoting the rejected identity.

Evidence-membership SQL is owned by `career_store.member_evidence_sql`. Canonical
exact/base locators use the existing `(source,locator)` primary-key index; nested
and historical locators retain the original predicate as a fallback. Ranking and
corroboration require observed members; quality auditing can inspect all member
states without changing that admission rule. The paired same-frozen-input
benchmark is `python scripts/benchmark-member-evidence.py`. It checks the output
hash against the current public manifest and saves aggregate timings, never raw
responses or person-level rows. A timing observation is not a throughput guarantee.

## Fixed dump transport reconnection

The candidate-discovery reader can reconnect a broken HTTP body while the same
process retains its bz2/gzip decompressor and cumulative SHA-1/SHA-256 state. It
requests the next compressed byte that has not been delivered to that decompressor.
The shared HTTP boundary permits this only for the fixed official Wikidata dump
route, with robots checks, existing rate limits and finite retries. It requires a
strong ETag or a valid Last-Modified validator, sends If-Range, and accepts only
HTTP 206 with the exact expected Content-Range, length, URL and validator. Ignored
ranges, changed files, missing validators and access refusals stop that attempt.

There are at most eight consecutive body reconnections without delivered bytes
and at most 256 total per invocation. Successful continuation resets only the
stalled count: intermittent healthy progress cannot consume a tiny failure budget
intended for a dead endpoint, while tiny responses still cannot cause unlimited
requests. Time, memory, disk and optional transport-byte budgets still apply.
An exact frozen-length EOF needs no
additional request. Raw source bytes are never written to disk. Progress reports
separate replay verification from new discovery and expose reconnection counts.
Process restart still replays from the beginning and verifies the committed line
prefix: this is not arbitrary compressed-offset recovery across processes. Only
complete framing, frozen compressed length and the upstream full checksum can
verify source enumeration. Discovered IDs remain candidates requiring individual
career evidence, and the dump/API are the same evidence source.
Oversized and malformed candidate records are recorded in the existing failure
ledger with the fixed feed identity, line number, reason and record SHA-256; raw
record contents are omitted. Replayed oversized records can fill missing legacy
diagnostics without incrementing the committed rejected-record count. Historical
malformed records lacking locators remain a disclosed gap. A verified source
checksum does not mean every record was successfully interpreted.

### Normalized career landing from the frozen dump

Continue the existing candidate queue first:
`python -m gapi collect-global --queued-only --max-seconds 3600 --batch-members 200`.
Wikidata requests contain at most 50 entities, with bounded splitting for oversized
responses. ORCID keeps its independent request budget. Collection generates the
full queue audit once at exit; each stage reports new members, new events, elapsed
seconds and members per second. These rates describe source records, not verified
people or global completion. Queued-only completion does not finish source discovery.

Do not run long SQL/profiling reads directly against the live acquisition database:
DELETE journal mode readers can block the sole writer's commit. Use the existing
bounded `acquisition-status` projection, a frozen ranking input, or a writer-free
maintenance window. If a member transaction encounters SQLite BUSY/LOCKED despite
the five-second wait, collection rolls back that unfinished member and returns
`resumable-storage-contention`, retaining the candidate without source suspension
or a fabricated source failure. It does not loop/replay the request automatically.
After the competing reader ends, rerun the same queued-only command. Other SQLite
errors still fail visibly and require investigation. Existing request reservations
remain conservative if their own settlement cannot commit.

The frozen-dump route below is optional, and is not the active acquisition path.
Defaults remain 300 seconds and 256 reconnects. The eight consecutive no-byte
reconnect cap, individual request deadlines, memory and storage limits still apply.
Historical long-run feasibility receipts are retained under `outputs/`.

Use `python -m gapi discover-wikidata-dump --stage-careers --max-seconds 300`
to retain normalized education/employment events while streaming. The default
candidate-only command remains compatible. No raw entity, label or compressed
body is stored. The landing table shares the task's disk/memory limits and sole
writer lock; it is excluded from canonical events, candidate processing counts
and ranking. Both paths use `wikidata_events` as their interpretation owner.

Enabling landing after an earlier candidate scan replays the compressed source
from its beginning and backfills normalized careers even in the old prefix.
The persisted discovery ceiling is not the replay's current position. Landing
inserts are idempotent; a crash between landing commit and source checkpoint
does not lose records because replay repeats the prefix. A changed replay prefix
or invalid full checksum leaves the feed unverified and prohibits promotion.
Malformed or over-limit normalized members are recorded as
`wikidata-dump-landing` failures, not silently counted as processed careers.

After full verification, stop the scanning writer and run
`python -m gapi promote-wikidata-dump --max-members 1000 --max-seconds 60`.
Repeat until `promotion_complete` is true. Each member's event insertion and
landing removal are one transaction. An already acquired API member at the same
revision retains its existing events. Dump and API remain one evidence source;
promotion does not establish independent corroboration. A verified archive with
landing errors still has explicitly unresolved interpretation gaps. Resource
limits may stop landing before full enumeration; preserve that checkpoint and
report the capacity boundary rather than dropping evidence or claiming completion.

For aggregate monitoring while a writer is active, use
`python -m gapi acquisition-status --max-seconds 60`. It copies the structured
database into memory using 64-page SQLite backup steps, closes the live read-only
connection, then runs the existing report calculations. This avoids holding a
long live read transaction during candidate counts. It creates no disk snapshot
or second writer, and stops on memory/time bounds. For very large stores where
an in-memory snapshot does not fit, use the bounded acquisition progress log
until the writer ends; do not run full live-table counts in parallel.

Candidate completion statistics are owned by `CareerStore.candidate_progress`.
It materializes each source's exact locators and revision-prefix keys once, then
joins candidates to those keys. Exact matches retain binary equality and `@`
prefixes retain SQLite LIKE's ASCII case behavior. Wildcard-bearing, non-ASCII
and unusual historical candidate IDs keep the original predicate as a fallback.
Multiple revisions never multiply candidate counts, and an observed revision can
coexist with a rejected exact record. Source selection hashes and public progress
fields remain unchanged. This avoids rescanning every member for every pending ID.

`python scripts/benchmark-candidate-progress.py --baseline 8c2636d --out <receipt>`
compares the pinned predecessor on one bounded RAM snapshot. It checks identical
aggregate output, selection hashes and repeatability on up to 2,000 candidates
per source, and separately times the full current calculation. The bounded
comparison is not a full-source equivalence proof or a ranking benchmark; the
mixed-locator regression also covers wildcard, Unicode, case and multi-revision
records. This benchmark creates no persistent database copy or raw-source cache.


### Lossless event storage (GAPIz1)

New normalized events use a compressed SQLite BLOB only when it saves at least
32 bytes; small events remain TEXT. The uncompressed canonical UTF-8 event is
limited to 16 KiB. GAPIz1 records carry a version marker and original byte length;
the decoder bounds expansion, validates the zlib checksum and rejects truncation,
trailing bytes and unknown encodings. Source hashes and event identifiers are
calculated before storage encoding.

CareerStore, monitoring snapshots and frozen readers use
`event_storage.configure_reader`. A column named `payload` is returned as its
exact original text. Direct SQL JSON operations must use
`gapi_event_text(payload)`. Alias `payload AS stored_payload` to inspect physical
storage. External readers must use this reader contract; old software cannot
interpret compressed BLOBs. Existing private snapshots remain untouched and readable.

Stop the collector before running `python -m gapi compact-events --max-events
100000 --max-seconds 120`. This shares the sole writer lock and resource gates,
updates at most 100 events in each transaction and verifies exact round trips.
Interrupted transactions roll back; another run can repeat safely. Unchanged
small TEXT records may be rescanned under the bounded limit. Compaction scans
at most the requested count per invocation; its counters do not certify that
all events were compressed. New collection can then reuse freed database pages.
There is no VACUUM or full database copy. Payload savings and SQLite free pages
must not be described as filesystem bytes reclaimed or proof that all global
data will fit the budget. Logical event hashes, ranking outputs and historical
snapshot reads are the compatibility checks.

### Bounded JSON transport compression

Public JSON requests advertise `Accept-Encoding: gzip`; plain responses remain
supported. Both received and expanded bodies are limited to the existing 2 MiB
JSON bound (or a smaller caller bound). Decompression is incremental and rejects
truncation, invalid gzip checksums, concatenated members, trailing bytes and
unsupported content encodings. No compressed response is written to disk.
`bytes_received` counts received wire bytes; evidence SHA-256 still covers the
exact decoded JSON bytes, before parsing. Archive range requests retain their
separate identity encoding and fixed upstream checksum contract. Compression
does not change access, retry, rate-limit or maxlag handling. A maxlag probe is
not a successful entity acquisition or evidence of improved normal throughput.

### Easy ORCID candidate discovery

`gapi discover-easy-orcid` reads the public Zenodo Easy ORCID 2023.6
`records_hq.jsonl.gz` derivative as a bounded JSONL stream. It retains only valid
ORCID identifiers and queues them for the existing ORCID API adapter; no source
record, name, affiliation or compressed body is stored. The derivative is CC0 and
is derived from the ORCID public dump, so it is an enumeration aid rather than an
independent corroborating source. Each bounded page has a cursor, URL-derived
query hash and response hash. Re-running a page is idempotent. This route does
not change the ORCID API evidence tier or turn the derivative into a second source.

Easy ORCID queue admission reserves 1 KiB per candidate plus 1 MiB before the
existing `CareerStore.queue_page` transaction. Queue inserts and the cursor roll
back together on write failure. Decompression produces at most 64 KiB per step;
an oversized line is discarded incrementally and counted once, including at EOF.
`valid_unique_candidates` counts valid identifiers found in a page; `queued`
counts only newly inserted identifiers. Earlier receipts used `queued` for valid
identifiers, so they must not be added together as distinct new candidates.
Page hashes cover accepted normalized lines and do not verify the full upstream
archive. This remains provisional candidate discovery, with an unknown full-record
denominator; it is not a checksum-verified career-data source or global completion.

### Bulk-career feasibility census

`python -m gapi census-easy-orcid --max-seconds 1800` scans the frozen HQ
derivative in `configs/easy-orcid-2023-hq.json` with no database writes or raw
files. It reports source rows, invalid and oversized rows, education/employment
field availability, shared-rule stage classification, and the byte size of a
minimal normalized affiliation projection, both plain and compressed per record.
Those byte counts omit identities, provenance, indexes and journals; they are a
lower bound for storage planning, not a claim that a full evidence database fits.
Progress every 10,000 rows contains aggregates only. `--max-records` is an optional
prefix bound; any prefix or transport failure remains unverified. Only EOF, gzip
integrity, exact compressed length and the frozen upstream MD5 together establish
`source_complete`. A SHA-256 is calculated on the complete compressed stream too.
No completed census alone adds careers to scoring or proves a world census;
duplicate ORCIDs are not deduplicated by this constant-memory measurement.

`--source configs/easy-orcid-2023-full.json` selects the separately pinned full
published derivative (920,836,363 compressed bytes), rather than HQ. This provides
the source denominator and visibility into HQ selection bias. It still describes
the historical derivative, not current ORCID or all world personnel. It does not
replace the identity of an existing HQ private landing or promote census rows.

### Compact batch career input

`python -m gapi ingest-easy-orcid --max-seconds 1800` streams the same frozen
archive into a private, compressed SQLite landing under the existing exclusive
writer and 2 GiB budget. The scoring projection retains rows containing both
education/research training and employment, including unknown stages, missing
identifiers and missing dates. Rows lacking either side are counted by the full
census but not persisted as scoring inputs. This is a disclosed network-input
projection, not a complete career database or a random sample.

Each retained row has its source line number, exact line SHA-256, the shared
pseudonymous person key and compressed normalized affiliations. Names, ORCID
strings, works and source bodies are not saved. Institution display names are
not retained; unresolved identifiers require re-reading the frozen source. A
source spec plus policy hash prevents mixed-version resume. Stable row keys and
content comparison make replay idempotent. Every resume re-reads the gzip from
the beginning; it never seeks to arbitrary compressed offsets. At most 1,000
projections are committed together after a conservative storage admission check.
On failure, prior committed batches remain; the current failed batch is discarded.

The landing is separate from canonical events, members and current scores.
`verified_rows` refuses reads until complete gzip, length and upstream checksum
verification passes. Retained source rows are not distinct people: downstream
consumers must deduplicate persons and relation events. Declaration origins are
unknown in this derivative; ROR grounding by its publisher and its ORCID lineage
are not independent confirmation. Full-source verification is transport integrity,
not verification of the truth of self-reported degrees or jobs. No formal ranking
qualification is granted by this import.

After verification, `python -m gapi rank-easy-orcid` generates a separate
institution-level observation JSON under `outputs/`. The default output is
`easy-orcid-observed.json.gz`, written incrementally with a deterministic gzip
header; no uncompressed duplicate is staged. Explicit `--out
outputs/easy-orcid-observed.json` retains the uncompressed export interface.
Decompression recovers the same JSON bytes. Gzip receipt bytes and SHA-256 refer
to the compressed file. Both export formats compare bounded file hashes before
replacement: byte-identical reruns preserve the existing file and modification
time. A changed result is still published atomically after complete staging;
an interrupted or budget-rejected export keeps the predecessor. Non-file and
symbolic-link targets are rejected.

The export uses the same relation
owner as the canonical analysis for chronological order, self-edge exclusion,
joint-appointment exclusion and per-person/stage/institution deduplication. The
existing exact ROR crosswalk resolves stable identifiers; names never merge
institutions. A private person index supports deduplication across repeated source
rows without retaining a population-sized Python set. Index creation is budgeted.
The real 2.3-million-row index exceeded SQLite's 128 MiB heap when sorting in
memory. Its external merge sort now uses an explicitly verified temporary
directory under the D-drive task root, checks disk and process memory during
execution, rolls back on budget cancellation, and removes its temporary directory
after success or failure. The process heap and total storage limits stay in force.
PageRank uses the shared implementation and reference stage weights; alpha 0.65
and 0.95 comparisons accompany the reference 0.85. Awards, composite score and
formal ranks remain missing. This output is separate from the current public
release; publication and expanded robustness checks remain later acceptance gates.

The real derivative exceeds the old 200,000-edge sample guard. This separate
view uses packed four-stage edge accumulators and packed PageRank edge arrays,
with resident-memory checks during aggregation and every iteration. The packed
kernel implements the same uniform-prior, row-normalized probability model as
the original function; deterministic random-graph comparisons check equivalence.
The export streams edges and JSON tokens to an exclusive temporary file, checks
the task budget during writing, then atomically replaces the output. Existing
temporary files are preserved rather than deleted. No edges are truncated or
sampled to fit a rendering limit.

Status monitoring copies only the report owner's columns when the database is
too large for a full RAM backup. The private bulk table and its indexes stay on
disk. The projection fixes one read transaction with a two-second read-lock
deadline, releases the live connection, then runs the existing report owner in
memory. This preserves the report contract without allowing a large new landing
to exhaust parser memory or hold the writer throughout aggregate queries.

### Full derivative without duplicating HQ

`python -m gapi ingest-easy-orcid-full --max-seconds 1800` requires the verified
HQ landing and its unchanged source identity. It streams the separately pinned
full derivative. For every scoring-capable source row, it looks up the shared
private person key and compares the exact compressed normalized projection.
An exact match reuses HQ storage; a new or changed projection enters the private
`easy_full_delta` table with its full-file row number and exact source-line hash.
No name matching or evidence upgrade is involved. HQ's frozen state stays intact.

The complete ordered reuse proof is hashed; a bounded bitmap counts distinct HQ
rows encountered so duplicate full rows cannot fake coverage. The full-file
checksum gates its own state. Full scoring additionally requires every HQ
projection to have been encountered exactly in the complete full source. A
duplicate HQ row with the same person and projection is covered by the same
full-source content; row multiplicity does not create additional talent events.
The scorer also checks that the full report is bound to the current HQ identity.
A verified full source that fails that subset check remains an independently
observed delta, but cannot be presented by the union scorer as the full source.
Partial, corrupt, identity-changed or capacity-stopped runs cannot enter full
scoring. Only completed transactions survive; reruns compare existing delta
payloads and ignore unchanged rows, then recompute the whole-source proof.

`python -m gapi rank-easy-orcid --include-full` reads the verified HQ plus full
delta as two indexed streams, merges them by private person key and applies the
same per-person relation owner. It does not concatenate their scores or counts.
The default output path is shared with HQ scoring; select `--out` explicitly to
retain both snapshots, subject to the same storage admission. The source and
scope fields distinguish the resulting file. The full-source view remains a
historical, uncorroborated observation product, not a formal global ranking.

### Interrupted archive connections

The full derivative connection ended early during a real run. Its committed
delta remained provisional; it did not become a scoring input. The shared
`FeedReader` now permits at most eight range reconnections (three consecutive
without progress) while keeping the same in-memory gzip decoder and hashes.
`PublicHTTP.open_range` requires status 206, the exact remaining Content-Range,
length, URL and identity representation. An ignored range or changed validator
fails closed. All sockets, including replacement sockets, close on exit.

Zenodo's pinned Easy ORCID files advertise byte ranges but currently provide no
ETag or Last-Modified. For these two exact archives only, the URL/length/MD5 tuple
must match the archive registry owned by `easy_orcid_census.ARCHIVES`. The entire
gzip, length and upstream checksum remain mandatory after reassembly. Until then
all projections remain provisional. This does not establish the identity of each
segment separately; final whole-file verification is the promotion boundary.
Wikidata's existing validator requirement is unchanged. A restarted process
still replays from the beginning; compressed offsets are never durable resume
checkpoints. The real range probe is recorded in
`outputs/EASY_ORCID_RANGE_PROBE.txt`.

### Bounded browser export

`python -m gapi export-easy-orcid-web --include-full` computes the verified full
network and exports institution aggregates into `outputs/easy-orcid-web`.
The existing single JSON export remains available independently. The browser
contract is separate from the old small-network schema; its limits are not
relaxed to admit a larger monolithic file.

The directory contains a column-defined institution table, 10,000-edge chunks,
an institution-to-chunk index, and a clearly labelled overview of up to 2,000
strongest edges. Every asset is content-addressed with SHA-256. The manifest is
replaced only after all assets have been written. Identical reruns preserve
existing bytes and modification times. Corrupt existing content addresses and
pre-existing temporary files fail closed. Failed exports may leave reusable
content-addressed assets, but cannot replace the previous manifest.

The overview is for rendering; all edges remain available through the chunk
index and all scores come from the complete network. Institution identity and
unranked reasons are retained. No person table or per-person evidence enters
these assets. Each edge asset is capped at 2 MiB and the institution asset at
32 MiB, under the same task-wide storage and process-memory budget. This export
does not itself publish the files or wire them into the production frontend.
Acquisition duration and connection-retry counts stay in operational reports;
they are excluded from score exports so identical frozen inputs do not acquire
different release hashes merely because download conditions changed.

### Evidence-oriented queue scheduling

The finite `collect-global` loop reserves up to half of ordinary fresh ORCID
slots for valid ORCID identifiers already linked to committed Wikidata event
persons. Linkage uses the existing stable pseudonym owner, not names or inferred
affiliations. Remaining fresh slots continue ordinary queue order, and the
existing due-retry reservation and backoff stay intact. A single fresh slot
continues ordinary order. All candidates remain queued; prioritization neither
samples away records nor elevates evidence status. Selection uses bounded
in-memory linkage, creates no persistent index or copied source file, and fails
at the resource bound rather than retaining an unbounded person set.

Source selection uses `least-source-service-time/1`: after a productive batch,
the finite invocation prefers the ready source with the least cumulative queue
selection and acquisition time. A faster source may finish several bounded
batches before the slower source gets another turn. Ties retain ORCID-first
order. Empty, paused and invocation-suspended sources are skipped. Existing
60-second stage windows, batch caps, request spacing, retry reservations and
single-writer rules are unchanged. Returned service times describe this
invocation only; they are neither global coverage nor a throughput guarantee.

### Slow HTTP body reads

The shared HTTP boundary uses the standard library's `HTTPResponse.read1` for
available body bytes, including the single reusable HTTPS connection. Ordinary
file-like adapters retain their existing read contract. Filling a 64 KiB buffer
with `read` can hide several successful socket reads from the stage deadline;
short reads allow the existing consumer loop to recheck its deadline between
body fragments. This does not increase concurrency, request frequency, retry
limits or source permissions. A blocked socket and chunked-protocol framing can
still overrun a stage window by their admitted timeout; this is not a hard total
wall-clock guarantee.

`tests/test_http_trickle.py` exercises the actual standard-library HTTP buffer
with deterministic, one-byte simulated arrivals and no sleeps or source files.
The old filling read consumed ten arrivals before returning; the new path checks
the two-tick stage limit between arrivals for both connection types. Complete
identity, chunked and gzip bodies retain their bytes and hashes. Corrupt gzip
trailers and truncated chunked bodies fail closed and close the connection.
This establishes the buffering mechanism, not the cause of every slow upstream
batch or a measured production throughput improvement.

### Compressed repository aggregates

`export-release --compress-snapshot` stores the institution aggregate as a
deterministic gzip file. The manifest keeps the decoded JSON filename and SHA-256
for browser readers, and adds `repository_snapshot` with encoding, exact packed
filename, packed hash and both byte sizes. It never compresses or publishes
individual trajectories. The build and standalone graph replay check the packed
identity, bound decompression to 32 MiB, then verify the decoded identity. Vite
emits the same JSON URL and bytes as an uncompressed input. Build provenance also
checks the committed packed Git object, so storage compression cannot weaken the
publication identity gate. Existing uncompressed manifests remain supported.

The exporter does not delete a preceding JSON or gzip file. Any migration must
first verify decoded equality, update the manifest atomically and confirm that
no current source reader needs the obsolete representation. Preserve a recoverable
archive or its committed original before removing the redundant copy. This saves
repository working space; the static build still needs bounded decoded copies.

### Reusing event pages without an extra database

`python -m gapi repack-event-pages --max-events 20000 --max-seconds 60`
rebalances at most 128 contiguous event rows per transaction under the ordinary
exclusive writer lock. It preserves SQLite rowids, every stored value and payload
byte, checks the full batch before committing, and rolls back an unfinished
batch. It refuses event triggers and incoming foreign keys. Source facts,
interpretations, schemas and codecs are unchanged. Resume only from a successfully
returned `--after-rowid`; starting again is logically idempotent.

This operation reuses the existing member write admission and transaction
reserve. It creates no second database, raw file, checkpoint table or temporary
sort. It cannot promise savings: allocated pages can remain unchanged and later
rows may reduce the freelist. Report net reusable pages across the whole run;
do not describe them as filesystem bytes reclaimed. New ingestion can consume
the freed pages while retaining the same total file size.

### Primary-key alignment in private deltas

New row freezes can align rows by the existing table's ascending BINARY primary
key, after both archive headers match its exact table declaration. This prevents
a same-table insertion longer than the bounded exact-line lookahead from causing
unchanged rows to be encoded again. Composite and BLOB keys use SQLite storage
class ordering. NULL, unknown declarations, custom collation, descending keys
and mismatched schemas retain the previous exact-line synchronization.

The decoder and archive formats do not change: the writer still emits only
copy, skip and exact additions. Complete baseline and reconstructed target hashes
remain mandatory. This changes neither evidence nor ranking rules. The existing
`rebase_delta` owner accepts `primary_key_alignment=True` for a separately
admitted, sole-writer retained-input replacement; dry-run output must fit staging,
and size regressions retain the predecessor. Repetition may recompute a candidate
but never rewrites a non-smaller successor. Production savings must be measured
separately from a memory-only preview.

### Inline evidence URLs

`member_parts.url` may contain legacy TEXT or a `GAPIu1` compressed BLOB.
The ordinary row reader exposes the exact original URL string; raw inspections
must use `url AS stored_url` or a cursor without the configured row factory.
SQL functions applied directly to `url`, such as `length(url)`, measure the
physical representation and must not be interpreted as the original URL length.
No URL-access, evidence-confidence or source-independence rule is changed.

The frame stores the decoded UTF-8 byte count and a zlib stream. Both compressed
and decoded frames are bounded to 65,536 bytes, with complete stream and CRC
checks. Unprofitable, empty or unusually large legacy strings remain TEXT.
New private row archives emit the exact restored URL text so a physical URL
repack cannot change their logical input identity or hide repetition from the
outer archive compressor. No dictionary table is needed. Readers also support
historical TEXT and inline-compressed archives.

The optional command uses the existing sole-writer lock and budget checks:

```text
python -m gapi repack-evidence-urls --max-rows 10000 --max-seconds 30
python -m gapi repack-evidence-urls --after SOURCE LOCATOR PART --max-rows 10000 --max-seconds 30
```

Each transaction changes at most 128 rows and checks the complete restored row
and original rowid before commit. Batches with shorter URLs are deleted and
reinserted inside that same transaction with their original rowids, allowing
whole pages to become reusable. Unknown table columns, triggers and incoming
foreign keys fail closed before mutation. `net_reusable_bytes` measures the
decrease in occupied SQLite pages, not physical file shrinkage; fragmentation
can make that value differ from payload savings or even become negative.
Only a successful returned checkpoint may
advance the next call. Repeating the same range is a no-op after conversion.
Checkpoints can contain private evidence locators: keep them outside public
assets. The command preserves all URLs and evidence fields. Its reported payload
reduction is not filesystem deletion, and page reuse must be measured separately
from the change in field bytes. Do not migrate beside an active collector.

### Reuse historical institution candidates

The online identifier bridge normally selects Ringgold identifiers from current
career events. To include the already processed historical aggregate directory:

```text
python -m gapi resolve-institutions-online --candidate-manifest outputs/easy-orcid-web/manifest.json --max-aliases 100 --max-seconds 180
```

Run this only after the collector has released its sole-writer lock. The reader
checks the public manifest schema, node count, content-addressed filename, decoded
size and SHA-256 before making requests. Existing gzip repository assets are read
in memory with a 32 MiB decoded bound; no decoded file or personal projection is
saved. A selection can contain at most 200,000 institution identifiers. The result
records both manifest and node hashes, so the historical input can be identified.
Repeated decisions under the same bridge version are not fetched again.

An aggregate identifier only nominates a candidate. The existing revision-pinned
Wikidata statements and reciprocal frozen ROR checks still own identity promotion;
neither names nor historical network scores confirm an identity. This does not
independently verify anyone's career or change the source's coverage claim. To
show confirmed mappings in historical scores, explicitly rerun the historical
ranking and export; this command never rewrites existing published scores.

Institution identity reads now reuse the same bounded read-only claims/info URL
builder as career acquisition. Each discovered group contains at most 20 entity
IDs; a normal group needs one HTTP response rather than one per institution.
Response keys must match the requested group exactly. Per-entity revision and
identifier validation remain the existing identity policy. Evidence retains the
actual response URL and hash as well as the canonical entity hash.

Only an explicit structured-response size overflow permits splitting the group
into smaller requests. An oversized single entity remains a bounded unresolved
identity record. Authentication/access refusals, service pauses, malformed JSON
and missing/unexpected response keys do not trigger alternate-route attempts.
Current source backoff and the sole-writer boundary still apply. Local transport
fixtures establish request-count reduction and content consistency; actual
network throughput must be measured during a later exclusive identity batch.
