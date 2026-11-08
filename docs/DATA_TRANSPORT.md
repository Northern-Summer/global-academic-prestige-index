# Public institution transport

The logical dataset remains `gapi-global-observed/v1`. The optional physical
representation `gapi-institution-defaults/1` removes repeated institution
fields without changing any observation, score, reason or missing value.

The envelope has exactly four keys:

```json
{
  "schema_version": "gapi-institution-defaults/1",
  "payload": {"schema_version": "gapi-global-observed/v1"},
  "node_defaults": {"formal_rank": null},
  "node_rows": [{"institution_id": "example:A", "name": "Example"}]
}
```

`payload` contains all original top-level fields except `nodes`. Each node is
reconstructed by copying `node_defaults` and overriding it with the corresponding
`node_rows` object. A value is omitted only if the same field exists in every
original node and equals the explicit default, including its JSON type. Absent
fields stay absent; null, false and zero remain distinct. Institution identity
and name are always explicit. These are encoding defaults, not statistical
imputation or assumptions about missing evidence.

The writer verifies canonical byte equality after reconstruction before accepting
the encoding. It retains the legacy format when the envelope saves no bytes.
The manifest's `output_sha256` identifies the uncompressed transport bytes;
`repository_snapshot.sha256` identifies the retained gzip bytes.
`logical_output_sha256`, when present, identifies reconstructed logical JSON
using sorted keys, ASCII escaping, compact separators and no non-finite numbers.
All earlier logical snapshots remain readable.

Readers reject extra envelope keys, a nested `nodes` payload, identity defaults,
prototype-related keys, over 131,072 nodes, over 64 fields per reconstructed node,
and defaults exceeding the 65,536-byte bound. Existing browser byte limits apply
before expansion; the full original schema, score and duplicate-identity checks
apply after expansion. The directory cap bounds row expansion. These guards do
not claim that every possible decoded JavaScript object consumes the same memory
as its JSON representation.

The Python writer/reader, JavaScript reader and standalone public verifier share
a protocol fixture and are tested for exact reconstruction, invalid input and
independent graph-to-score replay. The standalone verifier imports no GAPI
ranking implementation. Its verification concerns arithmetic and byte identity;
it does not establish the truth of career claims or independent audit status.

On the frozen v0.2.28 dataset, 44,821 nodes account for 30,637,716 of 31,871,202
logical bytes. Repeated per-field reason objects account for 7,601,519 bytes
(field totals include their own object overhead and are not additive).
The measured transport is 13,424,433 bytes and its gzip is 1,046,105 bytes.
The original logical SHA-256 remains
`71b392d3e7916ed0273c0d0cacd58561d963123af4ecfdf5d0aaa31e042e9d5a`.
These are measurements of one frozen public dataset, not future capacity claims.

## Full local aggregate chunks

`rank-global --chunked` retains the full logical `gapi-global-observed/v1`
aggregate in the task storage's `global-local-chunks` directory. This is a local
transport, `gapi-local-aggregate-chunks/1`; it is not a browser release and does
not replace the existing single-file local or public pointers.

`gapi.aggregate_chunks.write` owns serialization under the existing writer
exclusion. Canonical JSON bytes are streamed into at most 1 MiB decoded chunks,
each stored as deterministic gzip with compressed and decoded SHA-256 digests.
Chunks may split JSON tokens: only their ordered concatenation is a complete
JSON document. Identical content reuses the existing bytes. The manifest is
committed last, after each chunk is read back, so a failed write preserves the
previous manifest. Partial content-addressed chunks remain reusable on retry;
they are not an accepted result without a committed manifest.

`gapi.aggregate_chunks.chunks` checks descriptors, paths, lengths, per-chunk
hashes and the concatenated logical hash while yielding bounded byte blocks.
Consumers must exhaust it before accepting a result. Materializing the complete
JSON object requires its own memory admission. The task's storage and memory
budgets remain authoritative; this protocol does not raise them. Publication,
browser loading and independent graph replay retain their separate contracts.

`python scripts/verify-public-release.py --local-chunks <directory>` provides
an independent standard-library check of this local transport. It checks every
compressed and decoded chunk identity and the final concatenation without
materializing the whole JSON document. Its result explicitly excludes JSON
semantics, graph-to-score reproduction and publication eligibility. The default
public single-file graph replay remains unchanged.

### Recovery of local graph construction

The chunked local command also enables a private first-pass graph checkpoint.
`gapi.analysis_checkpoint.GraphCheckpoint` stores all graph accumulators and the
last fully consumed person group, normally every 60 seconds and at EOF. The
next invocation seeks strictly after that person using the existing person
index. A completed first pass is loaded without repeating its population scan;
ranking and sensitivity calculations retain their existing definitions.

The recovery identity binds the settled database file generation, task config,
allowed source selection and calculation code bytes. Database modification,
restoration or code drift rejects the checkpoint; it is not a portable frozen
input or permission to discard a predecessor. The compressed checkpoint carries
a decoded length and SHA-256, and uses the existing fsync/atomic aggregate
replacement. Failed writes retain the preceding checkpoint. A crash can require
reprocessing the unsaved person groups since that checkpoint, never recollecting
their source records. Existing count-only progress logs cannot be converted
into a recoverable graph.

This checkpoint covers first-pass graph construction. Later graph ranking,
directory and sensitivity work is still recomputed after interruption. It is
private, is not bundled with public releases and does not establish scientific
or publication acceptance. All checkpoint and temporary bytes count toward the
same task cap; encoding and restoration retain the task's memory admission.

### Compact completed graph checkpoint

`GraphCheckpoint.write_compact` converts a validated, completed first-pass state
under the same writer exclusion. It retains the original `.checkpoint` and
atomically writes its `.compact` sibling. `GraphCheckpoint.load` prefers that
sibling when present; a corrupt or mismatched compact file raises an error
instead of silently falling back or starting a population scan.

The private `GAPICMP1` format starts with a 32-byte little-endian
`<8sIIIIII` header: magic, node count, edge count, stage-row count, recent-edge
count, metadata byte count and format version. An 8-byte big-endian decoded
length and a 32-byte SHA-256 precede the zlib stream. Edge and recent-edge rows
use `<IId`; stage rows use `<II4d` with bachelor, master, doctorate and postdoc
weights in that order. Integer endpoints index the metadata node list.
Metadata also retains counts, source classes, independent origins, relation
count, input binding, completion flag and person cursor. The implementation's
`MAX_PACKED` and `MAX_DECODED` bounds apply before accepting the stream.

`PackedRows` retains byte-backed records; it does not create a permanent Python
tuple for every row. `PackedMap` exposes the existing graph interfaces and
missing-stage zero behavior. PageRank still consumes edges in sorted endpoint
order. This representation changes neither source eligibility nor edge
direction, weights, evidence levels or formal-ranking eligibility.

Code changes normally invalidate recovery. A reviewed representation change
can instead use `GraphCheckpoint.migrate_compact(predecessor_code,
original_sha256=...)` after the active owner has settled and the existing writer
lock is acquired. The supplied complete predecessor module-hash manifest must
reproduce the old binding with the current database generation, configuration
and allowed sources. Migration verifies the exact original file hash and full
decoded stream, then reconstructs that original canonical stream from compact
rows and checks its length and hash before updating the compact binding.
It preserves the original checkpoint; an interrupted atomic replacement leaves
the previous compact readable. A repeated migration rechecks equivalence and
reuses identical output. There is no unconditional historical-binding bypass.

Migration receipts identify the predecessor and current bindings, original
file hash, logical hash, compact hash and relation count. Retain them in the
existing task progress record. Recheck identities when executing a migration;
an earlier receipt does not authorize loading changed input. Completing the
first pass or converting its representation does not prove that subsequent
ranking, sensitivity, output or publication checks have completed.

### Completed training denominators

`analysis_checkpoint.TrainingDenominatorCheckpoint` retains exact, completed
institution training counts for chunked ranking resumes. It uses the same local
database/configuration/code and allowed-source binding as the graph checkpoint.
The source-selection key names the gzip file; a missing file permits calculation,
while an invalid existing file stops without silently recalculating. Partial
counts are never saved. The reader bounds compressed input to 4 MiB and decoded
input to 16 MiB, validates gzip EOF/trailing bytes, rejects duplicate JSON keys
and requires positive integer counts.

`import_completed` explicitly imports the predecessor
`analysis-training-denominators.json.gz` using its recorded SHA-256 and complete
predecessor code manifest. It recomputes that predecessor binding against the
current database, configuration and source selection before preserving its
counts under the current binding. The original file remains available for
recovery. Identical retries reuse the same bytes without timestamp churn.
The caller holds the existing writer exclusion; no personal input rows change.
When a completed graph or denominator file has already been explicitly
migrated, the same migration entry accepts its predecessor receipt. The receipt
must match the original input binding and original file identity, and the exact
current predecessor file digest. Graph migration additionally reconstructs the
original logical stream; denominator import compares every retained count with
the original. A receipt does not exempt changed data or unknown code generations
from validation. Ordinary loads continue to require the current binding.
Adding these counts to the resume path does not establish whole-ranking
completion. `InstitutionDirectoryCheckpoint` separately saves the completed
reported-name directory and observed institution endpoint set. It uses the same
exact input/code/source-selection binding, a source-selection-specific filename,
atomic publication, 16 MiB compressed and 64 MiB decoded limits, gzip stream
boundary checks and duplicate-key rejection. A cached directory skips both
event-label decoding and endpoint enumeration; invalid existing bytes stop the
resume. Only complete directory calculations are saved. No checkpoint can
recover labels or sensitivity results lost when an older process exited before
persisting them. Sensitivity results are not retained by this directory file.

`CoverageCheckpoint` stores completed scalar coverage under the same exact
input/code/source binding. The local JSON file is capped at 1 MiB; duplicate
keys, changed bindings, invalid scalar types and event/status total disagreement
are rejected. Only full completion is saved atomically. Coverage computation
reports separate event-status/country, unique-person and member-status passes.
Resume skips these scans only after a valid complete checkpoint is loaded;
invalid existing files do not silently trigger a new scan. This checkpoint
does not establish worldwide coverage or settle rejected source records.

The standalone verifier's `replay(data, edge_limit=...)` can explicitly admit
larger graphs using numeric edge arrays. Its default remains 200,000 edges;
the implementation bounds opt-in capacity to 1,000,000. This is an input-count
limit, not a memory allowance, browser capability or whole-document parser.
Chunked local ranking requests repeatable node and edge row producers from
`rank_global(stream_rows=True)`. The existing chunk writer emits those arrays
one row at a time instead of materializing both complete row lists. Sorted
fields, row order, numbers and logical JSON bytes remain the same as the list
path; ordinary `rank_global` results retain their list interface. A producer
must yield its declared number of rows; mismatches and interrupted iteration
fail before committing a new manifest. This reduces output materialization,
but does not prove that all upstream graph, directory or scenario state fits.

`--local-chunks` continues to verify transport bytes only. The separate
`--replay-local-chunks <directory>` entry validates the complete canonical ASCII
JSON stream and manifest node/edge counts, retaining only scored node fields,
edge endpoints/weights and algorithm metadata for the existing independent
graph-to-score replay. It admits at most 600,000 edges and 200,000 scored nodes;
individual retained values are capped at 8 MiB, string tokens at 1 MiB, nesting
at 64 levels and object keys at 200,000. Duplicate keys, nonfinite numbers,
truncated input, changed manifests/chunks and mismatched stored scores fail.
This entry does not establish personal fact truth, source independence, global
coverage, source rights or publication eligibility. Production output still
requires its own successful readback; fixture verification is not production
acceptance.

### Lossless row partitions of a completed aggregate

`gapi.aggregate_partitions.repartition` converts an already committed local
chunk manifest into `gapi-aggregate-partitions/1`. It reads no person database
and recalculates no scores. Calling it without a target measures actual
compressed asset sizes in bounded memory; writing requires the existing writer
exclusion and the unchanged task capacity checks.

Node and edge assets use `gapi-aggregate-row-columns/1`: sorted column names
and corresponding value arrays preserve absent fields, nulls, numbers and row
order. Each decoded row asset is at most 1 MiB. Edge source/target indices may
refer to the exact `algorithm.network_universe` array; the explicit reference
contract and index bounds are checked during reconstruction. Sensitivity
scenarios are separate value assets, preserving every scenario and result.
Other top-level values retain a 32 MiB per-value bound. These are asset bounds,
not a higher process-memory or storage allowance.

Every asset has separate compressed and decoded hashes and byte lengths.
The manifest records its source-manifest hash and the original full logical
hash, byte count, node count and edge count. It is committed only after the
entire input is consumed and exact canonical reconstruction matches the source
identity. Interrupted or failed replacement leaves the previous manifest
selected. Repeated identical writes reuse the same assets and pointer.

`gapi.aggregate_partitions.logical_chunks` restores the canonical logical
stream with bounded assets and verifies hashes, row widths, reference indices,
counts and the final logical identity on exhaustion. This representation
remains `local-only-source-license-review`. It is preparation for a bounded
consumer, not a new public-data permission, an implemented website adapter or
an independent verification of personal facts.

`python scripts/verify-public-release.py --local-partitions <directory>
--source-local-chunks <original-directory>` independently verifies both asset
representations, reconstructs the exact original logical hash and checks both
source and partition pointers for drift. It does not import the canonical
partition codec. This equivalence check connects the successor bytes to the
previously verified source aggregate; it does not upgrade source rights or
personal-fact evidence.

The existing development page can explicitly select these local partitions
with `GAPI_LOCAL_PARTITIONS=1`. Its Vite middleware reads the annual task
configuration and serves only the selected manifest and its exact asset names
on loopback. It checks stored hashes before serving, rejects linked paths and
unselected assets, and does not enable a public build or change publication
policy. The ordinary single-file development and production selection remains
the default.

`web/global-partitions.js` verifies the complete manifest and sequential gzip
assets, including both hashes, declared byte counts, row counts, reference
indices and scenario identities. The browser uses at most one asset's decoded
text at a time. Reused immutable label arrays and stage-weight objects reduce
allocation; every value is preserved. The full in-memory directory and graph
still require a measured runtime memory check. Loader count/asset limits are
declared by `PARTITION_CAPACITY`; these do not change the task's 512 MiB process
budget. The original single-file validation limits remain unchanged.

After a complete load, the existing table and network use the same authoritative
selectors. Network rendering and pagination reuse one ordered edge selection;
table-only search, sort and scenario changes do not recompute unrelated network
filters. Graph display limits remain distinct from the complete table scope.

### Retiring duplicate legacy aggregate blocks

`gapi.aggregate_chunks.select_successor` may select a sibling partition
directory under the existing writer exclusion. Before it writes `successor.json`,
it reconstructs every original decoded block and recompresses it with the
original gzip level and timestamp. Compressed and decoded hashes and lengths
must match every entry of the unchanged original manifest. Selection deletes
nothing. Physical legacy blocks qualify for retirement only after this proof,
the independent verifier, consumer readback and fresh per-file checks succeed.

The original manifest and the small successor link remain. Existing
`aggregate_chunks.chunks` readers reconstruct the original boundaries through
the pinned successor, checking both manifests and every original block
identity. They fail on drift instead of silently recomputing inputs.
`materialize_original` can restore the exact original gzip files when their
actual space is admitted. An explicitly retired generation cannot be
overwritten by a fresh legacy export. Ranking checkpoints and source archives
are outside this retirement operation.

The standalone `--local-chunks` and `--replay-local-chunks` routes also accept
an explicitly selected successor. Their independent implementation reconstructs
the original compressed identities; the replay route additionally runs its
existing graph-to-score check. The separate partition check now needs the
retained original manifest, not a second physical copy of every legacy block.
Recovery equivalence is verified against actual gzip bytes in the execution
runtime, never inferred from a decoded hash alone.

The Vite build also accepts the partition transport as an explicitly selected
local build. Build provenance binds every compressed asset, verifies both stored
and decoded lengths and hashes, and checks that the payload publication status
matches the manifest. Output verification and committed-input checks cover all
selected assets. This adapter does not promote the local annual result to a
public release; changing its manifest label alone remains invalid.

`aggregate_partitions.write_result` writes a completed local aggregate directly
to the same row/value transport. It retains `source-manifest.json`, describing
the exact legacy gzip boundaries generated in memory, without storing another
full gzip set. `aggregate_chunks.select_successor` can bind this anchor to the
new partitions and prove exact reconstruction through the existing independent
verification route. No source database or previous generation is rewritten.

Append-only recovery uses `global_analysis.complete_additive_result`, the pinned
private additions and saved base scores. It checks the actual person events,
member hashes, prior graph and denominator identities, source deletion bounds,
institution aliases and label winners. Replacements, additional source support
or previously absent directory nodes are refused rather than silently treated
as additive. `AdditiveGraph` stores only added weights and an integer order index
over the retained compact graph. Scenario calculation shares the full-analysis
owner. The new generation stays local until the separate publication gate passes.

The local preview reads `global-local-partitions-current.json` from the configured
task directory when present. This small selector pins one sibling directory and
manifest hash; absent selectors retain the original directory. Selection changes
the current reading surface and preserves the predecessor. It grants no public
rights and rejects unsafe directory names or manifest drift.

Public partition candidates are prepared by `publication.prepare_partition_release`.
The gate binds the existing noncommercial institution-aggregate source policy,
verified annual source identity, actual API run and complete independent replay.
Only the logical publication-status value changes; every other field and row is
bound to the retained local input manifest and its original transport manifest.
The candidate does not replace `outputs/global/manifest.json` until publication
settlement. `verify-public-release.py --manifest` verifies this transform and
replays the complete graph. `GAPI_PARTITION_ASSETS` supplies a frozen local asset
directory for candidate checks/builds without duplicating its assets; committed
CI reads the same assets from the reviewed release directory.

The existing `publish_code` accepts explicitly bounded content-addressed aggregate
inputs from one declared frozen root. It hashes them directly into the temporary
Git index, checks identity again before preparing the commit, preserves reviewed
public ancestry, and uses the same HTTPS push/readback receipt. It never publishes
private delta inputs or includes unrelated local ancestry.

The optional local archive index supports a selected gzip storage descriptor.
`local_archive_index.index_bytes` verifies compressed and decoded identities before
native import. `compress_index` selects this representation without deleting the
original; retirement requires separate verified cleanup. This preserves the exact
index bytes and cursor identity and does not read or reacquire the source archive.
