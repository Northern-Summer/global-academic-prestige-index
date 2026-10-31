# Streamed frozen-input storage improvement

## Baseline and rejected route

SCHEMA_DICTIONARY_FEASIBILITY_029.json records four bounded windows of 1,000 historical rows. A schema-only preset dictionary saved approximately 29 payload bytes per row. This does not prove SQLite can reclaim or reuse its table pages. No historical table was migrated or dropped.

## Frozen input transformation

FROZEN_XZ_FEASIBILITY_029.json measures two complete structured snapshots with compressed output discarded; original decompressed hashes were verified. The measured gains supported implementation in the existing frozen_input owner, rather than changing facts, rankings or live event storage.

The existing compact-frozen-inputs command accepts --codec xz. Existing logical HASH.sqlite identity remains. Its new reader accepts raw SQLite, gzip and XZ; streaming XZ output is limited to 1 MiB chunks with a 32 MiB decoder memory limit. Compressor preset 3 is explicit. Publication requires source and successor full hashes, exclusive staging and byte-dependent capacity admission including the final compressor flush. The old representation is retired only after verification, under the existing writer lock. No-op reruns verify without rewriting; historical older code can decode and check the same original hash first.

FROZEN_XZ_MIGRATION_029.json: all 15 inputs migrated and fully re-read, 43,064,085 bytes saved, 152.093 seconds, no raw SQLite materialization. The production collector was settled through session 84322 before maintenance; no concurrent writer was started. Its committed event count was 108,473 and is preserved.

FROZEN_XZ_REAL_REPLAY_029.json: the largest published input was restored under the same task budget and recomputed to public score SHA-256 90463d6a2aa66f8c39ba268d7f3f0652de756306d318e1bae878352b97074b5e, identical to 0.2.8. Repeat maintenance preserved packed mtime; reader temporaries remaining zero.

## Verification and limits

FROZEN_XZ_TARGETED_029.txt retains the initial Namespace compatibility failure; it was fixed without changing the default gzip command contract. FROZEN_XZ_TARGETED_FINAL_029.txt passed 16 snapshot tests. FROZEN_XZ_FINAL_REGRESSION_029.txt records the final full Python regression; FROZEN_XZ_FRONTEND_029.txt passed 40 frontend tests. Fixtures exercise truncated streams, trailing data, incorrect hashes, dictionaries exceeding the decode memory limit, insufficient storage and foreign staging preservation.

FROZEN_XZ_QUALITY_029.json confirms SQLite integrity, no events without committed member evidence, no upstream raw archives, and 15 verified compressed private inputs. These checks do not establish external source truth, full global coverage or independent third-party audit. Compression is a measured finite capacity improvement; no unlimited retention claim.
