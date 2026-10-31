# Storage capacity investigation — 2026-10-31

The collection goal remains active. The current event acquisition and the fully
scanned Easy ORCID 2023 derivative are separate datasets. Neither queue progress
nor a completed source scan is a denominator for the world's academic personnel.
At the initial investigation, the selected public slice contained 101,915 events.
Release 0.2.12 subsequently advanced that slice to 123,077 events using a verified
streaming row archive; see its release notes and manifest for the frozen identity.

## Measured publication constraint

At 120,133 retained events, the live SQLite table/index allocation included about
272,920,576 bytes in the current global-ranking projection, compared with
230,503,275 bytes of task headroom after the mandatory reserve. This is a physical
allocation estimate, not the precise size of a newly built snapshot. It already
shows that a raw projection plus its packed successor cannot be assumed to fit.
The collection process was stopped at a transaction boundary for this bounded
measurement, then resumed under its existing writer exclusion.

`outputs/SNAPSHOT_ADMISSION_ESTIMATE_032.json` records the per-table allocation.
No full database copy, VACUUM, table removal or deletion of historical inputs was
performed. The source database and its existing recovery inputs remain intact.

## Tested alternatives

| Alternative | Actual result | Decision |
| --- | --- | --- |
| Page-wise XOR between the two largest historical private inputs | 136,030,000 compressed difference bytes versus 98,579,348 existing packed target bytes; 95.484 seconds; no raw landing | Reject for this pair: increases space by 37,450,652 bytes |
| Whole ranking projection in RAM | SQLite `MemoryError` during bounded copy with a 320 MiB heap limit and the unchanged 512 MiB process ceiling; handles closed | Do not adopt or increase the user budget |
| Blocks of 32 already-compressed normalized rows | Three 2,000-row prototypes grew page allocation and released no freelist pages; all rows reconstructed exactly | Reject this representation |
| Blocks of 32 decoded normalized rows | Three 2,000-row samples estimated 129,387–132,353 fewer payload/reference bytes; all compressed rows reconstructed on the current zlib runtime | Candidate only: not deployed, no claim of reclaimed disk space |

The evidence files are respectively
`outputs/SNAPSHOT_DELTA_FEASIBILITY_032.json`,
`outputs/MEMORY_FREEZE_FEASIBILITY_032.json`,
`outputs/BLOCK32_STORAGE_FEASIBILITY_032.json`, and
`outputs/DECODED_BLOCK32_FEASIBILITY_032.json`.

The samples are bounded source-order slices, not representative estimates for
the entire population. The block prototypes use only temporary in-memory tables.
Exact compressed-byte reconstruction was checked on zlib 1.3.2; this is not proof
of cross-version compatibility. No production reader, format migration or
performance claim follows from those experiments.

## Next implementation gate

Before adopting a new representation, demonstrate all of the following with the
same retained inputs: lossless logical rows and evidence, idempotent interrupted
migration, bounded extra allocation, actual reusable SQLite pages (or verified
filesystem byte reduction), acceptable person-order and source-order traversal,
and unchanged scoring of a pinned dataset. Payload compression alone does not
prove usable capacity. Historical recovery inputs must remain readable.

Do not retry the rejected whole-memory or page-XOR routes without new evidence.
Continue the sole finite collector while headroom permits; inspect its actual
terminal state before starting another writer. At a capacity stop, retain its
valid progress and report the remaining source scope. Do not silently discard
evidence, sample the queue or label partial source processing as 100% complete.

## Operational evidence

The first monitoring attempt in the preceding turn hit its two-second read-lock
deadline and safely released the database. A fresh attempt in this turn completed
and counted 120,149 events and 34,307 people; collection later advanced further.
See `outputs/ACQUISITION_STATUS_032.json` and the current continuation settlement
for the exact later counts and active process handle. The monitoring timeout
neither proved collection failure nor authorized a duplicate collector.

This investigation does not change the scientific ranking policy, publication
rights, web typography, external-review status or resource ceilings. No new
release is needed solely to publish these local capacity diagnostics.

## Follow-up measurements

Content-defined binary chunks were also tested read-only on the two largest
historical SQLite inputs. Full decoded hashes matched. Chunk/reference storage
was estimated at 255,725,863 bytes, exceeding the existing 173,766,712 bytes by
81,959,151 bytes even before container overhead. Reject this representation;
no archive migration was performed (`outputs/CHUNK_DEDUP_FEASIBILITY_034.json`).

A RAM-only pilot on 20,000 retained Easy ORCID rows grouped decoded payloads in
32-row blocks, deleting and reinserting at most 1,024 contiguous rows per batch.
All logical rows and 1,000 sampled person lookups matched. SQLite pages remained
1,471 total but gained 176 reusable pages (720,896 bytes). This is internal page
capacity, not reclaimed filesystem space. A complete source-order decode took
1.188 seconds and sampled lookups took 0.078 seconds in this pilot. No production
migration or population-wide saving claim follows from this small ordered sample
(`outputs/BLOCK_PAGE_REUSE_034.json`).

Two inactive, publicly versioned institution slices were instead losslessly
archived as gzip after decoded SHA-256 and original public Git object checks.
The active slice and every private recovery input were retained. The measured
logical reduction was 19,742,001 bytes, with both exact compressed restoration
and the original public commit available (`outputs/PUBLIC_ARCHIVE_CLEANUP_034.json`).

Subsequent row differences use the same logical input stream as the full row
archive, with one immutable retained full baseline, exact line operations and
complete baseline/target hash checks. This avoids another full copy when most
rows are unchanged. Test and real-scale results must be recorded separately;
implementation alone does not establish a capacity gain.

## Release refresh regression and repair

The next real refresh exposed a long-insertion alignment failure: a new delta
grew to 140,613,280 packed bytes, and export was refused after file close.
The old public manifest remained intact. The newly generated, unreferenced delta
was removed after recording its packed hash; the live input, full baseline and
every historical recovery input remain. See `outputs/EXPORT_FAILURE_038.json`.

Windows directory enumeration had not reflected the full open-file growth.
Full and delta row writers now supply initial usage plus bytes actually written
as a lower bound to the existing task budget gate, including compressor flush.
No budget or reserve was increased. Delta alignment also preserves table-name
boundaries across insertions longer than its bounded lookahead, so unchanged
following tables can be copied. Reconstruction and both complete hashes remain
the acceptance boundary; the optimization never infers or drops input facts.

Regression coverage includes stale enumeration and insertion of 199 rows before
an unchanged following table. All 347 Python tests passed after the repair.
Real-scale retry and publication outcomes must be recorded separately; these
test results alone do not establish the new archive size or successful release.

The real retry reconstructed the same logical input hash and produced an
17,973,232-byte delta, versus the failed 140,613,280-byte representation.
Scoring then reached a previously hidden SQLite memory ceiling: its fallback
label query sorted complete event payloads by institution and event ID. Reading
the existing event-ID index instead preserves the first eligible ID per
institution, while avoiding that population-sized payload sort. A differential
test compares complete outputs with the previous query and verifies the new
query plan has no temporary sort. All 348 Python tests passed. The verified
delta is retained; the failed scoring attempt did not replace the public manifest.

## Verified event-page reuse

After a RAM-only simulated-layout pilot, the production event table was
rebalanced in three finite passes of 5,000, 40,000 and 95,723 rows, with no
concurrent collector. Every rowid and stored field was preserved, including exact
compressed payload bytes. The complete 140,723-event physical-row hash matched
before and after each pass. The first pass also recomputed the complete public
score output on both sides and matched; later passes checked exact unchanged
input bytes without claiming another score recomputation. SQLite quick_check
passed after every pass. All 350 Python regression tests passed before migration,
including interrupted-batch rollback, repeated execution, committed resume and
schema-side-effect rejection.

Allocated pages stayed at 312,488. Freelist pages changed from 0 to 1,011 to
10,332 and finally 10,292: net reusable capacity is 42,156,032 bytes (about
40.2 MiB). The last pass consumed 40 free pages rather than saving space; the
reported benefit uses the complete sequence. No filesystem bytes were reclaimed,
no VACUUM or second database was used, and historical recovery inputs remain.
Evidence: `outputs/EVENT_REPACK_REAL_042.json`,
`outputs/EVENT_REPACK_REAL_043.json`, `outputs/EVENT_REPACK_REAL_044.json`.


## Capacity continuation 070

The collector in native session 5028 terminated with BudgetStop. A short,
writer-exclusive read established 172,983 committed events, including the
partially completed final batch. The database had 316,345 allocated 4-KiB pages
and no freelist pages. No uncommitted work was counted.

`outputs/STORAGE_TABLES_070.json` records a completed read-only table allocation
scan: easy_careers 584,093,696 bytes; events 213,803,008; discovery 156,618,752.
These are allocated SQLite pages, not compressed upstream source sizes.

A superseded 11,122,267-byte browser node asset, absent from the active manifest,
was losslessly compressed to 1,556,867 bytes. The original decoded SHA-256 is
2ddae1c284e1cae1783624302d65b7260a5034781dceab8983d963dd235c1825.
The exclusive source handle was retired only after the successor was flushed,
fully decoded and checked. A second run was a verified no-op. The retained
receipt is `outputs/INACTIVE_BROWSER_ASSET_ARCHIVE_070.json`; the exact archive
is beside the active browser assets. Logical occupied bytes decreased by
9,565,400. No private recovery input was removed.

The active historical bundle was independently read through readDerivedBundle:
manifest SHA-256 c591594a32b240e15bf2fefd5e04df5a5b5359fc148a50a12d806325e97d0a08,
62,103 nodes, 487,234 edges, 22,616,790 total bytes. All selected asset hashes,
lengths, endpoint indices and score mass checks passed after retirement.

A separate RAM-only pilot retained the first 10,000 candidate rows exactly after
converting lowercase hexadecimal evidence digests to tagged binary values. It
made 552,960 bytes reusable in the in-memory database. This is neither a deployed
codec nor reclaimed filesystem capacity. Production adoption requires reader
compatibility, unchanged candidate hashes/order, rollback/resume tests, and a
bounded write-admission proof for reusable pages. The live database was not
modified by this pilot (`outputs/QUEUE_DIGEST_PILOT_070.json`).

The sole collector resumed using outputs/continue-070.py, native session 16281.
Its start does not establish a completed source scan. Budget/reserve, scientific
eligibility, personal-data publication boundaries, live typography, and external
review status remain unchanged. The capacity bottleneck is not resolved by this
archive; do not claim global completion or publish the pilot as a measured
population-wide storage improvement.


## Queue representation correction 071-073

The user challenged the relation between visible event counts and storage. A
completed allocation scan distinguishes 2,306,992 historical HQ projection rows
(and their 105,095,168-byte person index), 881,061 current discovery candidates,
and 177,015 current events. These populations must never be described as one
small event table. The actual frozen candidate counts are 191,845 Wikidata and
689,216 ORCID candidates; their pending counts were 144,819 and 680,527 before
the capacity pause. They are not the denominator of world academic personnel.

The first live digest migration was rejected: replacing each row immediately
left no freelist pages after at least 194,400 checkpointed rows. Its exact process
was stopped; SQLite retained committed transactions and rolled back unfinished
work. The negative result and checkpoint remain in
`outputs/QUEUE_STORAGE_MIGRATION_071.json`; its stale runner is disabled.
Mixed TEXT/tagged-digest readers remain compatible. This failure was not source
loss or a reason to lower evidence standards.

The revised implementation deletes and reinserts at most 128 contiguous short
queue rows per transaction, with an 8 MiB admission bound. It retains rowids,
source evidence and the complete decoded candidate content. Oversized legacy
rows are preserved. A bounded real pilot scanned 10,000 rows, of which 2,576 were
not already converted by the first attempt, and yielded 118,784 reusable bytes.
Full candidate progress/hashes and the complete event hash were unchanged;
SQLite quick_check passed (`outputs/QUEUE_STORAGE_MIGRATION_072.json`).

Candidate digests have one owner, queue_storage.py. Only lowercase 64-character
hex SHA-256 strings use the tagged 39-byte representation. Other legacy strings
remain exact TEXT. All four acquisition write sites use that owner; the shared
reader exposes exact original strings at the two named digest columns. Other
BLOB columns remain untouched. Corrupt frames fail closed.

Member admission now subtracts actual SQLite freelist capacity from anticipated
physical database growth, with a minimum 1 MiB admission and the unchanged fixed
task reserve, disk-free checks, and max_page_count ceiling. The sole-writer lock
prevents another writer consuming that capacity. A real SQLite fixture proves
the same budget rejects an empty freelist but admits insertion into existing
pages without file growth; the fixed-reserve rejection still holds. Restoring
the previous admission makes that same fixture fail, as recorded in
`outputs/QUEUE_STORAGE_NEGATIVE_CONTROL_071.json`.

The revised Python regression run passed 365 tests in 31.374 seconds. The earlier
row-wise implementation's 365 passes established content compatibility but did
not establish real space recovery; the new test also requires positive reusable
page capacity. An intermediate expanded fixture failed because Q999 already
existed; a new non-colliding ID fixes that test setup without changing its
assertions (`QUEUE_STORAGE_TARGETED_071_REVISED.txt`, then
`QUEUE_STORAGE_TARGETED_071_RETEST.txt`). All failures remain available.

An ordered sample of the first/last 500 events shows why event cost is not that
of a short spreadsheet row: decoded JSON averages 2,146 bytes; references,
qualifiers and response URLs together occupy about 67% of sampled value bytes
relative to complete payload bytes. The event payloads are already compressed;
these decoded figures are not projected disk savings. Repeated batch response
URLs also exist in member_parts. Normalizing shared transport evidence is a
concrete next candidate, requiring exact reconstruction and reader/snapshot
compatibility rather than deleting evidence. Sample field sizes, without
personal values, are in `outputs/EVENT_FIELD_COST_073.json`.

The resumed whole-queue migration uses `outputs/migrate-queue-073.py` and must
complete its full before/after comparison before being called verified. SQLite
reusable pages and filesystem bytes reclaimed are reported separately. No claim
of scalable global completion follows from this bounded storage correction.


The full continuation 073 completed and verified all candidate selection hashes,
progress counts and the complete 177,015-event logical hash unchanged. SQLite
quick_check returned ok. It added 29,024,256 reusable bytes; including the pilot,
the database ended with 7,115 free 4-KiB pages (29,143,040 bytes). Its allocated
file stayed at 318,174 pages; filesystem bytes reclaimed remain zero. The full
second pass scanned all 881,061 queue rows, changed zero rows and changed zero
pages (`outputs/QUEUE_STORAGE_IDEMPOTENCE_073.json`). This is now measured live
reusable capacity, not the earlier failed row-wise estimate.


## Shared response evidence pilot 075-076

An ordered RAM-only pilot over the latest 2,000 events found 1,940 eligible batch
responses and 13 distinct URL dictionaries. Exact UTF-8 reconstruction passed;
net sample savings after estimated dictionary overhead were 376,827 bytes.
This sample was not extrapolated as a proven population result.

GAPIz2 stores a digest-addressed, bounded shared zlib dictionary containing the
exact repeated batch response URL. It removes no event fields and preserves the
entire original UTF-8 event representation. Event/dictionary insertion shares
one transaction; dictionary bytes and compressed framing are verified on every
read. Missing, corrupt, trailing or oversized dictionary inputs fail closed.
GAPIz1 and legacy TEXT remain readable. Dictionaries contain minimal structured
transport evidence, not raw upstream bodies, and remain private.

A sole-writer live pilot scanned the last 10,000 of 179,624 committed events;
9,637 payloads converted. Payloads shrank 1,914,569 bytes; page rebalancing yielded
2,568,192 net reusable bytes including dictionary overhead. Allocated database
pages did not grow; filesystem bytes reclaimed are zero. The full logical event
hash remained a61e34da84c25e7f3713bd476b15fc50be3cd7075678a061a10e5b373982bc46;
quick_check returned ok and process memory was 34,639,872 bytes. See
`outputs/RESPONSE_DICTIONARY_LIVE_076.json`.

Full database backups retain dictionaries. Ranking-only SQLite/row snapshots
instead restore the existing self-contained GAPIz1 payload representation and
exclude the local dictionary table, avoiding storage-codec dependencies and
unnecessary physical changes in historical logical row deltas. Tests check both
full backup and scoped snapshot restoration against complete event hashes and
public scores. Existing frozen inputs remain unchanged.

The first test invocation incorrectly expected frozen_reader to yield a store;
its contract temporarily replaces the existing store reader. This fixture was
corrected. A later run exposed two explicit snapshot expectations: full backups
retain dictionaries, whereas scoped inputs exclude them; the old exclusion list
also needed to include the new storage-only table. These failed logs are retained
in outputs/RESPONSE_DICTIONARY_TARGETED_075.txt and
outputs/RESPONSE_DICTIONARY_FULL_075_FINAL.txt. The final regression result must
be recorded after execution; these fixes alone are not a passing receipt.

The collector session 94173 was stopped by exact process identity for exclusive
pilot verification. It was not described as an upstream failure. All completed
transactions were retained. Full-population response migration and publication
remain pending until regression and actual storage evidence support expansion.


Final snapshot-contract regressions passed: 369 tests in 44.906 seconds
(outputs/RESPONSE_DICTIONARY_FULL_076.txt); after hoisting the event payload-column
lookup outside the row loop, 11 response/row-snapshot tests passed in 1.863 seconds.

The next live bounded pass scanned the first 76,416 events and converted 46,802.
Its 45-second migration deadline bounded conversion, with full before/after hash
and quick_check afterward. Event payloads shrank 9,194,583 bytes and 10,887,168
net bytes became reusable across the complete database, with all 179,624 event
bytes unchanged. Allocated file size stayed fixed. Process memory was 34,582,528
bytes. See outputs/RESPONSE_DICTIONARY_BATCH_077.json. The difference between
payload savings and net reusable page capacity includes existing fragmentation;
it must not be attributed entirely to the codec.

A further bounded pass starts at the committed rowid 76,416 using
outputs/migrate-response-078.py. It must be read back before restarting the sole
collector. No new database copy, raw source file, personal public export or
external publication was made by these storage passes.


Pass 078 completed: 58,880 further rows scanned through rowid 135,296;
52,323 converted, 10,987,271 payload bytes saved and 14,069,760 net reusable
bytes gained. Complete event hash and quick_check passed again. The database
ended with 12,651 free pages (51,818,496 reusable bytes), file size unchanged.
The unprocessed response-sharing cursor is 135,296; source data and acquisition
progress were not discarded. The remaining storage conversion is optional and
must not be confused with remaining source enumeration. Collection resumes
sequentially with the tested codec, using outputs/continue-079.py.


## Publication capacity stop 080-081

The next data release was attempted under the unchanged task budget. Normalization
interpreted 7,133 events without canonical fact mutation; corroboration retained
four independent official confirmations. Frontend checks passed 42 tests.
Freezing the private row delta then stopped with BudgetStop before completion:
used=1,878,977,112, anticipated=94,212, reserve=268,435,456. The writer removed
its unfinished partial and left the public manifest untouched. No new data was
published and no remote ref was changed. The complete failure log remains in
outputs/EXPORT_080.txt; outputs/RELEASE_080_CAPACITY_STOP.json records settlement.

A discriminating comparison of the first 1,000 old events with the live snapshot
representation found 1,000 identical stored payloads and 1,000 identical decoded
payloads. No changed fields were observed in this ordered sample. Thus the new
shared URL codec does not explain a wholesale rewrite of that sample; this is
not proof about all rows. The next capacity investigation should measure delta
additions by table and their actual compression contribution in memory, rather
than rerunning the same oversized disk archive. See
outputs/FROZEN_REPRESENTATION_DIAGNOSTIC_081.json.

The collector resumes using outputs/continue-082.py after the failed export has
terminated and its writer lock is released. Global completion and the next data
release remain unverified. All historical private inputs are retained.


## Exact row-delta compression correction 083-084

A complete RAM-only replay of the failed delta produced 56,578,532 XZ bytes
without writing an archive. Its 107,914 additions included 56,808 event rows;
those event additions contained 83,835,275 row bytes and alone took 55,502,499
bytes under a separate diagnostic zlib stream. Actual joint XZ and per-table
zlib are different measurements. The complete target stream was 337,478,641
bytes, SHA-256 891b1524979fdd505f24a5ef179525ee96906526ae078d977e821a8fbd9a342d.
The process used 65,888,256 bytes. See outputs/DELTA_TABLE_COST_083.json.

These numbers support the nested-compression hypothesis: newly added events
are already individually compressed GAPIz1 BLOBs, then Base64-encoded inside
row JSON before XZ, which prevents shared JSON patterns from compressing across
records. The former inference of a generic alignment failure is not supported
by this observation: the actual run copied 290,417 lines and skipped 5,278.

Row-delta format /2 adds an E command for the existing 13-column events contract.
It stores normalized event text and its original compressed BLOB digest inside
the private XZ archive. Restore deterministically
reconstructs the exact GAPIz1 bytes and checks their SHA-256 before reconstructing
the original row line. The final baseline and complete target stream checks
remain mandatory. This is not a new raw-source cache or public person export.
Unknown row layouts, TEXT, other codecs and noncanonical lines retain A commands.
Legacy /1 inputs remain readable and reject unknown E commands. No chains are
introduced and no baseline/historical archive is deleted or rewritten.

Targeted tests passed all 11 row-delta cases, including exact event reconstruction,
modified digest, oversized expanded text, legacy-reader rejection, score restore,
truncation, changed baseline, admission failure and repeated-run identity.
A complete regression and a real archive size comparison are required before
calling this correction a successful release-capacity solution.


The actual format-2 freeze succeeded: 18,193,464 bytes versus the 56,578,532-byte
format-1 RAM replay, a reduction of 38,385,068 bytes (67.84%). Both reconstructed
the identical 337,478,641-byte target, SHA-256
891b1524979fdd505f24a5ef179525ee96906526ae078d977e821a8fbd9a342d.
Complete baseline/target reconstruction and an independent second live logical
stream comparison passed. The existing baseline and every historical input were
retained. This corrects the publication-capacity stop for this frozen input; it
does not prove unlimited storage growth or complete global coverage.

The 371-test Python regression passed in 56.504 seconds. Public score export,
standalone arithmetic replay, hosted build/browser acceptance and deployment
readback remain distinct required publication gates.


## Release 0.2.20 candidate 085

Export completed within budget: public coverage 179,885 events / 52,661 people,
34,508 directory institutions / 1,832 scored institutions, 2,058 aggregate edges.
The gzip aggregate is 968,537 bytes. Independent stdlib replay converged in 131
iterations, mass 0.9999999999999999, max score error 1.734723475976807e-18 and
L1 residual 9.24234538728086e-13. Public output hash:
e6a7e607995700819ca908e7844ba39c18b365f7e0f29c547dd8371c557afc9f.
Replay does not establish underlying facts or external auditor independence.

The first public-upload runner stopped before mutation because replacing a run
number had also changed its Git file-mode literal. The allowed regular-file
mode was corrected explicitly to 100644, and the remote parent was reread before
resuming. No force push or private local ancestry was uploaded. Verified public
head is 676894b3ff2b11ab0a022573a6abae320aace079. Only the reviewed output allowlist
was included, and non-output source bytes matched the tested local source.
GitHub run 41208705569 was observed in progress; deployment and release settlement
are still pending at this record. The active collector is continue-086.py,
native session 88336, and is separate from this frozen candidate.

## Current storage explanation and directory correction (087)

Fresh short read-only queries found 183,163 current events, 2,306,992 retained
historical career rows and 881,062 discovery candidates. These are separate
populations; the people shown by the current release are not the database row
denominator. The external task tree contained 1,783,805,002 bytes, including
1,303,240,704 bytes of SQLite and 435,403,728 bytes of frozen recovery inputs.
The existing authoritative budget counter, which also includes repository
outputs/dist, measured 1,846,080,176 bytes. After the fixed 268,435,456-byte
reserve, admission headroom was 32,968,016 bytes. SQLite had 45,944,832 bytes
of internal reusable pages, not physical bytes returned to the filesystem.
Collection continues, so counts and free pages are point-in-time observations.
See private outputs/STORAGE_READBACK_087.json for the exact inventory.

The earlier table-allocation inventory (070) measured 584,093,696 bytes for
easy_careers and 105,095,168 bytes for its person index. Those are historical
allocation measurements, not fresh per-table measurements. No long dbstat scan,
VACUUM, raw download, database copy or history deletion was performed while
the current collector was writing.

The prior publication CI failed at real-observation-build: 34,508 directory
institutions exceeded the old 32,768-node input guard. The shared directory
owner now allows at most 65,536 institutions, independently of unchanged
500-node/2,000-edge rendering caps and the 32 MiB decoded-input bound. This is
a bounded immediate correction, not an unlimited directory architecture.
An integrated regression now validates the actual committed release, its hash,
all directory rows and the rendered graph limits. The 42-test frontend baseline
passed after the guard change; after adding this regression, all 12 focused
global tests passed. The real 24,632,711-byte decoded snapshot validated in
93.0 ms, sorted in 20.7 ms and used a 143,347,712-byte Node process, with
500 rendered nodes / 547 edges. This is local Node evidence, not hosted browser
performance or visual acceptance. Hosted CI/build/browser and online readback
remain required before claiming deployment.

The collector session 88336 subsequently ended normally with a resumable source
boundary at 183,163 events; its terminal handle and absence of another matching
collector were checked before taking the existing sole-writer maintenance lock.
The remaining shared-response pass (088) scanned 47,867 rows through rowid
183,163 and converted 30,479 events, saving 6,138,583 payload bytes and producing
7,917,568 additional internal reusable bytes. The physical file remained
1,303,240,704 bytes; free pages increased from 11,217 to 13,150. Full logical
event hashes before/after matched, quick_check returned ok, and the maintenance
process measured 35,000,320 bytes. No historical input was removed. The sole
collector was then resumed using continue-089.py (native session 1250).

Public correction commit d283bad4d45fe439d48df33d02d77a2cc026ac3b retains
public-only ancestry. GitHub run 41209396416 completed all four jobs successfully:
test, real-observation-build, production-browser, deploy-observations. Online
source/data readback and release creation are recorded separately in the
087 publication receipts, not inferred from transport success.

## Acquisition continuation and alternate-source check (090)

Fresh SQLite readback confirmed both easy_career_state and easy_full_state are
verified with source_complete=true. The full derivative selected 2,473,948
two-sided source rows, reused all 2,306,992 exact HQ projections and retained
only 166,956 delta rows. These are source-row counts, not independently
corroborated people or current-world coverage. The existing 2023.6 historical
view remains separate from present API snapshots.

The resumed collector (native session 1250) was observed live and advancing.
Readback 090 measured 183,633 events, 9,065 ORCID members, 48,733 Wikidata
members, 689,218 ORCID candidates and 191,845 Wikidata candidates. API member
counts cannot be divided by candidate counts to claim global coverage: those
populations, temporal revisions and identifiers have distinct contracts.
SQLite still had 53,325,824 reusable bytes. Actual Wikidata run state reported
the service's requested source pause; ORCID continued independently. No new
collector or download stream was created during this continuation.

Bounded live Firecrawl searches checked ORCID career/education/employment bulk
alternatives. The publisher's older Easy ORCID page was read directly:
https://zenodo.org/records/11371268 (version 2023.2, published 2024-05-28).
It describes education/employment projections and publisher ROR grounding;
its older release is not a new independent source, nor a replacement for the
already verified 2023.6 file. Search snippets mentioning bibliographic indexes,
general employment resumes and regional ORCID datasets were not promoted into
the global source registry without a compatible contract and verification.
No new raw archive, cloud account or paid service was activated.
The current event/candidate counts and verified source states are retained in
private outputs/ACQUISITION_SCOPE_090.json. This is progress toward the source
scope, not a claim of 100 percent current global collection.

## Retry-queue admission efficiency (091)

A fresh EXPLAIN QUERY PLAN showed the due-retry selector scanning the large
discovery table first and probing discovery_retries per candidate. On the live
ORCID queue, an initial observation took 0.891 seconds for one due result.
The selector now starts with discovery_retries CROSS JOIN discovery, sharing
the exact existing member-key CTE, eligibility predicate, retry time boundary,
ordering and LIMIT. Fresh and optional linked priority selection retain their
existing contracts. No index, materialized person table or persistent cache was
added, and no evidence or qualification rule changed.

The same-real-query ABBA comparison at a fixed query time produced identical
selection digests: old mean 0.9391344 seconds versus new mean 0.00430355 seconds
(218.22x faster for this local query). At the fixed comparison time no retries
were due; the preceding observation and the new fixture cover nonempty results.
This is not an end-to-end network acquisition speedup. The fixture also covers
same-time ordering, future retries, orphan retry rows, already acquired exact and
revision-prefixed members, wildcard-bearing legacy identifiers, repeatability,
and verifies that the actual query plan starts from the retry table. All 24
collection tests passed. Full regression and live process adoption remain
separate gates, recorded below when executed. Raw query text and identifiers
remain in memory; outputs/RETRY_OUTER_REAL_091.json contains only durations,
counts, selection hashes and scope labels.

Full Python regression passed 372 tests in 58.631 seconds. To adopt the corrected
selector, the exact collector PID/parent/command were rechecked, then the old
child was intentionally terminated. Its wrapper handle returned terminal exit
1 (controlled restart, not a successful full scan). CareerStore reacquired the
sole-writer lock, recovered SQLite normally and returned quick_check=ok with
183,871 committed events. Previously committed members/checkpoints were retained;
any interrupted member remains resumable. The loaded pending owner contained
the CROSS JOIN implementation, SHA-256
b191b305c26924935d2376235da8465c298ce20c6e3c3997cdd462eaeadac053.
The sole collector resumed as continue-092.py, native session 73243. Its entry
receipt measured 1,846,123,240 budgeted bytes and 53,039,104 internal reusable
bytes. See private outputs/RETRY_OUTER_ADOPTION_092.json. No new raw data cache,
database replica or persistent priority index was created.

## Retained delta representation upgrade (093–094)

Four retained historical deltas still used format /1, with packed sizes
26,094,336 / 22,598,968 / 17,973,232 / 7,258,116 bytes. The existing /2 event
addition representation can also reduce their nested-compression overhead.
repack_delta upgrades one retained delta under the existing writer exclusion,
first fully verifying its old reconstruction. It writes an exclusively created
partial, verifies complete successor reconstruction against the same logical
hash/byte count and baseline, verifies the predecessor's packed hash has not
changed, and atomically replaces only when the successor is smaller. Otherwise
the predecessor remains. No source record or historical version is removed;
old manifest packed-size/format fields remain historical storage observations.
Repeated /2 calls verify and reuse the existing bytes. An existing unknown
partial is never deleted. New partials are cleaned on write/admission/validation
failure. Baseline chains remain prohibited.

All 374 Python tests passed in 60.352 seconds, including original score restore,
logical identity, repeated-run byte preservation, nonempty event compression,
corrupt/truncated predecessors, successor mismatch, preexisting partial
preservation and capacity failure during writing. The first real conversion
uses only the largest retained /1 archive. Collector 73243 was intentionally
stopped after its identity check; the returned terminal exit was 1 and committed
members remain resumable. Real replacement results are recorded separately;
test success alone does not establish real reclaimed capacity.

The real replacement succeeded in 97.890 seconds: 26,094,336 bytes became
8,385,680 bytes, reclaiming 17,708,656 physical file bytes (67.86 percent).
Both archives reconstruct the identical 280,862,320-byte logical input,
SHA-256 55929c91d9bdb17213c72f2a99f3f50f3459c36860e9c894990b2533e0503e8f.
Its retained baseline and historical logical filename are unchanged. The old
packed SHA was ac77c85d0aaae8172eaf95371de736eed408b37645e8d061d6b7d01144b1ae90;
the new packed SHA is 7f5fde4220400eb0069b8cea13b57df35b95a1b026b235d82ebc0f1577d2b671.
Total accounted storage decreased from 1,846,172,917 to 1,828,466,791 bytes.
Final maintenance memory measured 34,246,656 bytes, database quick_check=ok,
187,236 committed events remained, and zero source records were deleted.
This physical reduction is distinct from prior SQLite internal page reuse.
Private receipt: outputs/RETAINED_DELTA_REPACK_094.json. A subsequent bounded
normalization/freeze/export is in progress for the next observation release;
no new release or deployment is implied by this storage result.

## Frozen observation candidate 095 and resource contention

The next freeze/export succeeded: logical input 351,091,116 bytes, saved as a
23,393,156-byte format-2 delta, SHA-256
23cf11956042693b13c8d7c65709e4f42a350310605058e7ec75b2917ba869e9.
Complete reconstruction and a second live logical-stream hash comparison matched.
The public snapshot contains 187,236 events, 54,837 people, 35,397 directory
institutions, 1,881 scored institutions and 2,140 aggregate edges. Its gzip is
993,046 bytes; decoded size 25,263,333 bytes. Public decoded SHA-256:
8bbff95c5859346b3e802aca93df157a6329826415f167831766395208231ecf.
Independent stdlib replay passed (131 iterations, mass 1.0, maximum score error
1.734723475976807e-18, L1 residual 9.017302963348905e-13). All 43 frontend tests
passed; Node validation/sort measured 107.56/19.71 ms, 500 rendered nodes and
565 edges. These are local evidence, not hosted or visual acceptance.

The collector was initially resumed before the full frontend regression. This
ordering caused avoidable resource contention: temporary test fixtures raised
accounted usage to 1,883,436,124 bytes, where the collector correctly refused to
consume the fixed 256 MiB reserve. Native session 40606 ended with exit 1 after
21 committed additional events, which are outside the frozen release. The exact
failure was retained in ACQUISITION_RESUMED_096.txt. All frontend tests completed
and removed their temporary fixtures; usage then fell to 1,852,950,170 bytes,
leaving 26,098,022 admission bytes. After confirming the old handle terminal,
the collector resumed as continue-097.py, native session 12698. Future local
full frontend regressions must finish before resuming collection in this tight
budget; short public replay checks do not require the large fixtures. No budget
was increased, no safety check was disabled, and no recovery input was deleted
to hide this admission failure.
