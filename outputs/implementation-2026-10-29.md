# GAPI implementation checkpoint — 2026-10-29

This is a partial implementation and source scan. It is not completion of the global
statistics objective or independent third-party approval.

## Delivered

The predecessor is commit `e2a0d8197863bc45d901709c8884813c553d68a4`. The preserved
146,716-row ROR directory and separate 368-node US sample retain the hashes recorded
in `implementation-checkpoint-2026-10-29.json`. They are not global career coverage.

Implemented a D-drive task with a 2 GiB storage cap, 256 MiB reserve, 512 MiB resident
memory admission, single OS-locked writer, SQLite page cap and atomic member commits.
HTTP/gzip/tar/SAX input never writes source archives/XML to disk. ORCID's public API
also has an executed incremental XML path. The annual stream requires complete gzip
CRC/trailer, frozen source bytes/checksum and length before promotion; partial work
is replayed from the beginning with member deduplication. DuckDB Parquet export was
executed with one thread, 128 MiB RAM and disabled disk spill (3,211-byte output).

Independent Wikidata statements retain revision, qualifiers, references and response
hash. Names do not merge identities. Reported labels are only display labels;
unresolved external IDs remain unresolved. A discovered ambiguous ROR/Wikidata
crosswalk (`Q1636837`) is retained rather than forced. Matching copied statements
does not count as independently corroborated evidence.

Current normalized local acquisition: **1,835 events, 404 pseudonymous person keys,
72 observed country/territory codes**, including 623 structurally accepted observations
and 1,212 unresolved events. These are bounded discovery selections, not representative
samples or completed enumerations. The local view has **968 institution identifiers**
(including unresolved IDs which may duplicate institutions), **132 scored endpoints**
and **97 weighted directed edges**. Forty-six committed ORCID records exercised the
strict XML transport. Predecessor API records used bounded JSON and are not claimed
as proof of the later SAX implementation.

Source-permitted public view: **797 Wikidata career events, 204 person keys,
478 institution identifiers, 2 qualifying directed edges, 3 scored endpoints**.
The other institution IDs have missing scores. Public data exclude restricted ORCID
API contributions and the US study. This small score-bearing subset is disclosed;
it is not presented as a completed worldwide ranking.

Network direction remains employer → training. Fixed the shared PageRank denominator
to use the same valid edges as propagation, preventing probability loss. Global
exports keep awards/composites missing, qualification reasons explicit, and all valid
edge endpoints in the network universe. Scenario calculations cover alpha, isolated
stages, source deletion, observed-region prior balancing and a 2016 employment window.
Ranges exclude prior-only scenarios and cannot quantify unknown global missingness.

## Executed verification

- 60 Python tests and 3 subtests passed; 13 Node tests passed. Python unittest discovery
  is also compatible. Regression cases cover malicious XML/tar, oversize fields/members,
  CRC/truncation/hash errors, source/member changes, replay, privacy/source filtering,
  deterministic bytes, budget refusal, process death rollback, writer recovery,
  PageRank direction/zero/boundary edges/mass, HTTP denial/retries/robots and CSV injection.
- Actual source calls: Figshare robots/download returned 202 with no body, metadata API
  403. Recorded as failure, without auth bypass. A complex SPARQL discovery query timed
  out; changed to bounded queries and persisted queues. One mismatched ORCID response
  was rejected, rather than attached to the requested person.
- An initial Windows ctypes handle conversion and an isolated test temp-directory
  lifecycle defect caused failing tests; both were repaired and the failed cases rerun.
- Real browser: global load, search, country/sort filters, keyboard institution details,
  CSV download, Three.js rendering, directional relationship table, WebGL-unavailable
  fallback, mobile page width and no page exceptions passed. Public production build
  was separately exercised, not just the dev middleware. Screenshots were inspected.
- Public manifest explicitly selects the licensed snapshot. Static build rejects
  unsafe paths, oversized slices, hash mismatch and restricted-data publication.
  Missing data builds fail unless code-only mode is explicitly selected.

Detailed status, browser evidence and preserved hashes are adjacent JSON files. Storage
was approximately **42 MiB** at this checkpoint, including runtime/build/release assets;
the changing measured total is in the status JSON. No raw material was downloaded to
the project or task work tree. Old, policy-denied legacy caches were left untouched.

## Remaining gates and next continuation

The ORCID 2026 summary file locator is known, but its usable stream, exact bytes and
upstream checksum are unavailable on the current local route. Full source enumeration
cannot be claimed. Persisted API discovery queues are bounded and do not provide an
arbitrary global resume cursor. Full-scale scan/official corroboration, robust discipline
and English-source OOD checks, official awards, and independent external dual-track
review are unfinished. No reviewer has been hired/contacted and no independent pass
is asserted. The prepared review handoff identifies these gates.

The code and source-permitted observation subset can be published as a prerelease.
The formal global ranking remains withheld. Continue from the external source lock,
SQLite members/events/discovery/failures and current release manifest; do not rebuild
the project or reinterpret a partial source as a global census.

## Continuation: real page processing and public preview

The complete source history was uploaded without force to the public repository
`Northern-Summer/global-academic-prestige-index`; its default source branch is
`codex/global-streaming`. The original bootstrap branch and local history are
preserved. Publication uses SHA-addressed Git objects and remote ref readback;
an ambiguous network write was reconciled before continuing.

Replaced the one-shot first-200 discovery queue with ordered live keyset pages.
Each page atomically commits its query/response hashes, timestamp, queue and
cursor. A failed query does not advance the cursor; resume processes unfinished
members rather than slicing the already-completed queue. Independent scope
cursors separate university-teacher candidates from degree/employer selection.
Conflicting ORCIDs are not selected and occupational candidates never create
employment or degree facts.

Actual execution: one new 200-person candidate page was committed and fully
processed across a bounded ingest and a resume. Total local structured inputs
are now **2,269 events, 572 person keys, 72 observed country/territory codes**;
637 events are observed and 1,632 unresolved. The current local aggregate has
1,162 institution identifiers, 98 weighted edges and converges in 26 iterations.
The source-permitted public slice has **1,231 Wikidata events, 372 person keys,
677 institution identifiers, 3 directed edges and 5 score-bearing endpoints**.
Identifiers are not a deduplicated census. No independent official confirmations
exist; awards, composites and all formal ranks remain missing.

The next real keyset request timed out. Its cursor and the completed first page
remain intact. The degree/employer ordered join also timed out; the simpler
university-teacher candidate query succeeded. These observations establish a
first-page processing/recovery mechanism, **not a successful complete multi-page
scan**. The annual ORCID access/checksum boundary remains unchanged.

Executed verification after corrections: **67 Python tests, 13 Node tests**;
production browser checks bind the fetched snapshot SHA to its manifest, exercise
filters/sorting/keyboard/details/CSV/WebGL/fallback/mobile, and confirm that the
public build makes no restricted US-sample request. Browser testing exposed an
invalid production mount dependency and a long scenario select overflowing at
390px; both were fixed and the same acceptance checks rerun. A preview-base
mismatch was corrected. A quality command attempted during ingestion was correctly
refused by the existing writer lock; it was rerun after the writer exited.

At 2,269 events, SQLite uses 5,832,704 bytes; normalized payloads total 3,050,465
bytes, averaging 1,344 bytes per event. Including current indexes/queues/entities,
the database ratio is about 2,571 bytes/event. This small-source mix is not a
global sizing model. At that observed ratio, the remaining authorized budget
after runtime and journal/export reserve would hold roughly 0.7 million additional
events; actual capacity may vary materially. Total accounted task/build/release
storage remains about 45 MiB, with zero detected raw source archives/XML. No
increase to the 2 GiB cap or reduction in evidence requirements was made.

Publication is an observation/code **prerelease only**. Source/ref, release/tag,
site-tree and live-site checks have separate task-local receipts; repository
creation or a successful upload alone is not evidence of a live site. External
dual-track review, official-profile adapters, complete enumeration and fuller OOD
analysis remain unfinished. The user's complete-global-statistics objective has
not been achieved by this preview.

## Continuation: source-independent discovery, affiliation streams and fair retry

Preserved predecessor commit `0f120ef` before further changes. The prior collector
was allowed to finish; no second real-data writer was started concurrently. Added
Crossref's public metadata cursor as another candidate source. Its initial ascending
index selection returned three pages without usable ORCIDs; these pages remain as
feed v1 evidence. A distinct descending-index v2 selection committed three pages,
yielding 148, 150 and 158 valid identifiers (456 before queue/cross-page dedup).
OpenAlex's first page committed 200 candidates, but its second page returned 401.
No account, key or alternate protected route was used. Candidate sources never
become education/employment assertions or independent corroboration.

ORCID ingestion now streams only education and employment XML sections. The 109
members acquired by the new section path transferred 847,393 total body bytes,
about 7.6 KiB/member in this observed mix; this is not a population-wide estimate.
Both sections, their exact identities, hashes, URLs and events commit atomically.
Truncation of the second section rolls back the first. Earlier whole-record members
remain immutable. All raw XML, archives and author response bodies remain unsaved.

The finite collection loop repeatedly replenishes pages, isolates discovery and
career sources, retains bounded progress history, and persists individual backoff.
One quarter of normal batches is reserved for due retries, preventing permanent
starvation by newly discovered candidates. Long/date-form Retry-After instructions
persist as source cooldowns across invocations. HTTP stage deadlines bound retry
admission. A rolling-day task ledger reserves worst-case ORCID requests before the
network and settles actual attempts; crashes retain conservative reservations. It
does not account for other software on the same IP or earlier uninstrumented calls.

One read-only internal review found three P1 defects and a P2 starvation risk:
source-wide rate-limit signaling, malformed discovery containers, deletion-source
sensitivity erasing surviving alternate evidence, and due-retry starvation. All
have deterministic regression fixtures. Source deletion now filters and rejoins
person events before deduplication. A transient disappeared-journal stat during
concurrent tests was observed in the real collector and fixed without suppressing
permission errors. The internal review is not an independent third-party audit.

Executed full verification: 112 Python tests with 23 subtests and 13 Node tests.
Real 15-minute continuation ended at its declared time boundary, not a source
failure. At that frozen checkpoint: 3,612 events; 954 person keys; 76 reported
country/territory codes; 772 observed and 2,840 unresolved events; ORCID 350 and
Wikidata 689 committed members. 134 deduplicated qualifying relations form 119
weighted edges over 162 scored endpoints. The directory contains 1,582 observed
institution identifiers, not necessarily 1,582 distinct resolved institutions.
PageRank converged in 27 iterations, residual 3.6716636675482306e-13. Recalculation
produced the same local snapshot SHA-256:
`01574652825a6e29b68a2615548ac2d2cc4c3563910d10d3ebe03e205564cf38`.

Browser runtime checks bound this exact snapshot to the served manifest and passed
search/filter/sort, keyboard details, CSV, Three.js, WebGL fallback and mobile layout.
Desktop/mobile screenshots were actually viewed. Task/build/release data accounted
about 48 MiB before final publication; SQLite integrity was OK, and raw source-file
bytes detected were zero. The frozen ORCID annual route and checksum remain
unavailable, while Wikidata dump discovery has processed only its 256 MiB prefix.
No global source-enumeration completeness is asserted.

The official ORCID terms were reviewed live on 2026-10-29. Terms of Use sections 3
and 4 explicitly permit distribution/public display of public Record Data; API
terms still constrain acquisition to noncommercial use. A separately versioned
`configs/publication-20261029.json` therefore permits institution aggregates for this
noncommercial research release, without editing the acquisition lock or labeling
API data CC0. Manifest policy hashes preserve the exact decision. Public histories,
linked works, the US study, missing awards and formal ranking remain excluded.

The global-design target remains open: complete frozen enumeration, substantially
wider independent official corroboration, official awards, full OOD analysis and
external dual-track review are not achieved by these implementation tests.

Historical reproduction now supports a budget-admitted private normalized SQLite
snapshot. It freezes the input events/member hashes/crosswalks without preserving
original source XML or copying personal inputs into the project. A regression
changes live event statuses after freezing and confirms the original public score
bytes reproduce from the read-only snapshot. Tampering, path traversal, write
attempts and insufficient budget are rejected. This adds one small private input
copy for the current release, not unlimited snapshot retention outside the cap.
