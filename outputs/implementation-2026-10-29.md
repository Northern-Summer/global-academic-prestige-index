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
476 institution identifiers, 2 qualifying directed edges, 3 scored endpoints**.
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
