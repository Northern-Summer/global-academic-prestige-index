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
