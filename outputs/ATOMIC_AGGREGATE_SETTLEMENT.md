# Atomic aggregate publication

The active collection process is unchanged. No ranking rule, evidence gate or source permission was changed.

## Observed defect and correction

The aggregate snapshot already used replacement, but its public manifest and local manifest were written directly. A failed write could leave a damaged reader pointer. All writes owned by global_analysis now use its _write_aggregate helper: exclusive staging, flush/fsync, atomic replacement, and cleanup limited to newly owned staging. An existing recovery file is preserved. Byte-identical content retains its prior mtime. The public manifest is written after its snapshot and checksum file. This is atomic per file, not a multi-file filesystem transaction.

## Executed verification

- ATOMIC_AGGREGATE_TARGETED.txt: 16 tests passed, including interrupted replacement, recovery-file preservation, budget failure, successful replacement and repeated local export timestamps.
- Full regression outcome is recorded in ATOMIC_AGGREGATE_REGRESSION.txt.
- CONTINUATION_028_BASELINE.txt: predecessor baseline 319 tests passed.
- CONTINUATION_028_FRONTEND.txt: 39 frontend tests passed; frontend code unchanged at this step.

## Storage decision

EASY_BLOCK_COMPRESSION_FEASIBILITY.json records four ordered windows of 1,000 historical rows. Reads were bounded by a one-second SQLite progress deadline and released before compression. No private row was written to an artifact. Each exact in-memory block round trip passed; packed bytes were 58.56 to 59.49 percent of the current logical row sizes. These windows are not a representative census and exclude SQLite page/index overhead.

Do not infer that a full successor database fits the remaining budget. No historical table has been migrated or deleted. A storage change must preserve verified row-order replay, random person lookup, scoring deduplication, source hashes and recoverability; file-level savings and payload compression are separate measurements.

## Actual resource admission

rank_global is read-only and does not allocate a disk workspace. The inherited fixed 64 MiB disk admission prevented valid read-only replay close to the storage boundary. It now checks the complete task disk budget without fictional extra bytes, and samples process memory separately. Public export likewise waits for known output bytes; the existing four-times-content admission and atomic writer byte admission remain. No budget, reserve, event evidence or ranking rule changed.

ACTUAL_RESOURCE_ADMISSION_TARGETED.txt: 17 tests passed, including read-only scoring with only 8 KiB unreserved headroom, rejection of actual export, and rejection of scoring after the total budget is exceeded. Full regression is ACTUAL_RESOURCE_ADMISSION_REGRESSION.txt.

HISTORICAL_AGGREGATE_COMPACTION_028.json records 17 old, unselected institution slices. Every original matched committed Git bytes; no source/test/document consumer referenced its name. Each deterministic gzip was fully decoded and hash-checked before retiring the original. Logical space released: 31,979,427 bytes. The current selected slice, all 14 private frozen inputs and the live database were preserved. These are archived results, not discarded evidence.

## Real export boundary correction

The first 0.2.8 export successfully froze and verified input 7a49110628bb2040e9de1d7a8c7399702ad378c458779b32189df8f33a04552f.sqlite (192,020,480 bytes). Its frozen-reader replay reached the inherited 4x-output disk gate: used 1,819,203,657; anticipated 62,473,548; reserve 268,435,456. The output was not written and the predecessor manifest remained selected. RELEASE_028_EXPORT_ADMISSION_FAILURE.json retains this failure.

The redundant 4x disk check is removed from public/local export. Canonical serialization now samples actual process memory; the shared atomic writer still requires 2x-content plus 64 KiB on top of the full measured tree and unchanged 256 MiB reserve. A real-sized 1 MiB fixture succeeds at 3 MiB headroom and rejects replacement at 1 MiB headroom, preserving the predecessor. ACTUAL_EXPORT_ADMISSION_REGRESSION.txt records the full regression.
