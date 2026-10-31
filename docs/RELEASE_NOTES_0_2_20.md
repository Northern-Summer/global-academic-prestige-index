# GAPI 0.2.20 — real observations and bounded evidence storage

This prerelease refreshes the public institution-level observation network.
It remains an observation release pending independent external review, not a
complete global census or a formal university ranking.

The acquisition database now stores repeated batch response URLs through exact
shared compression dictionaries. Candidate evidence digests also use a compact
lossless representation. Every original normalized event remains recoverable;
none of these storage changes alter facts, evidence levels, ranking eligibility
or PageRank direction. The fixed 2 GiB task budget and 256 MiB reserve remain.

Private row-delta format 2 avoids compressing Base64 representations of already
compressed event payloads. Expanded normalized event additions reconstruct the
original compressed BLOB and verify its digest before full-stream hash checking.
Legacy snapshots remain readable. Historical recovery inputs are retained.

Validation: 371 Python tests and 42 frontend tests passed locally. Hosted checks
must also pass before deployment, including production build and browser tests.
The independent public replay checks graph-to-score arithmetic; it does not
validate personal facts, source independence, source rights or world coverage.
Personal careers and private acquisition diagnostics are not bundled publicly.

The separate verified Easy ORCID 2023.6 historical source view remains separately
labelled, with its own census and denominator. Awards/composite values remain
missing where no defensible attribution data exist. External dual-track review
remains pending.

Frozen public scope: 179885 events, 52661 people, 34508 directory institutions and 2058 directed aggregate edges. These are source observations, not world-population totals.

The same-input private freeze fell from 56,578,532 bytes (old-format RAM replay) to 18,193,464 verified bytes, a 67.84% reduction with identical complete input hash.
