# API expansion settlement — 2026-10-30

This extends, rather than replaces, `CONTINUATION_20261030.md`. Its 15,917-event
predecessor and private frozen inputs remain available for recovery.

## Frozen observed scope

| Measurement | New freeze | Predecessor |
| --- | ---: | ---: |
| Internal pseudonymous people | 4,509 | 4,130 |
| Career events | 17,207 | 15,917 |
| Country/region codes | 96 | 90 |
| Deduplicated talent relations | 351 | 302 |
| Scored institution endpoints | 366 | 323 |

The new institution identifier directory contains 4,962 entries; this is not a
count of all universities worldwide. There are 303 aggregated directed edges,
1,733 raw observed events and 15,474 unresolved events. Independent corroborated
career events remain zero. Observed scores are not formal global ranks. Official
Nobel inventory remains 1,028/1,028 recipients with 384 institution attribution
labels; canonical university award/composite scores remain missing.

Wikidata's expanded candidate selection is 3,865/3,865 committed (100% of that
selection). ORCID is 1,285/1,291 committed (99.535%); all six remaining candidates
have recorded errors: two HTTP 409, one HTTP 404, three XML root identity-path
mismatches. These are not counted as received or adjudicated records. Retry
deadlines remain persisted. The global personnel denominator is unknown and
`complete_global_census` is false.

## Acquisition and resource evidence

The API expansion, subsequent queue drain and due-error retry all exited normally.
Crossref supplied independent candidate discovery while OpenAlex returned HTTP
401; no authentication changes or bypasses were attempted. Discovery is not
career evidence or independent corroboration. Full response bodies, raw XML and
archives were not saved. The quality report found SQLite integrity `ok`, zero
events without member evidence and zero raw files. Pre-freeze task storage was
249,454,562 bytes; this freeze adds 37,629,952 bytes, within the 2 GiB budget with
256 MiB reserved. Observed collector working-set peak was 49,082,368 bytes.

## Identity and reproducibility

- Source commit: `2df5c6ea4fd11b5080d68397847af484e887ead2`.
- Private input SHA-256: `86ed9ad19983f5458de5867d3a36b4b9dab7c724cd906d7e594d4f3ecd2644cf`.
  The normalized SQLite input stays outside Git; it is not publicly bundled.
- Public snapshot: `global/global-e5b80e4d979e48f0.json`.
- Snapshot SHA-256: `e5b80e4d979e48f09b08c922e04aaccdf7cfd56042bbb39f409c14bd52bfa5ac`.
- Initial export and both frozen-input replays are byte-identical: receipt
  SHA-256 `498dd3040e3be96e2805f06a7f1ce7f2411b72de7eedb4af8fff698841fe877c`.
  See `export-expanded-20261030.json`, `replay-expanded-a-20261030.json` and
  `replay-expanded-b-20261030.json`.
- The separately implemented public graph replay converged in 21 iterations,
  residual `9.745591920268248e-13`, mass `0.9999999999999999`, maximum score
  difference `3.469446951953614e-18`, and identical ordering. This internal replay
  is not third-party review (`public-replay-expanded-20261030.json`).

## Executed checks

Python: 173 tests passed (`python-expanded-20261030.log`). Frontend: 30 tests
passed in the preceding unchanged frontend check. OOD: nine synthetic test
scenarios and 26 invariants passed (`ood-expanded-20261030.json`); these do not
establish real global completeness or fairness.

New-slice local production browser: table ready in 998 ms; actual snapshot hash,
progress counts, filtering/sorting, keyboard detail access, CSV, official award
labels, directional relationships, pagination, 3D, fallback, mobile layout and
hostile input checks passed with no page errors (`browser-expanded-20261030.json`).
The English italic and regular-weight Chinese serif remain. Fresh desktop/mobile
hero screenshots were observed; no visual redesign was introduced.

## Publication and remaining gates

This file records local acceptance. Remote deployment needs its own Actions,
served-file hash and browser readback; those receipts must identify this exact
snapshot. No formal-ranking release tag is issued. Independent third-party
research/governance and software/reproducibility reviews, global enumeration,
independent career corroboration and six ORCID errors remain open. The goal is
active. No old failures, predecessor freezes or unique screenshots were removed.
