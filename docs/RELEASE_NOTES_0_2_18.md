# GAPI 0.2.18 — preserve local capacity and acquisition continuity

This maintenance release keeps the exact 0.2.17 public data, scores, fonts and
layout. Local acquisition had stopped because the retained frozen input reduced
available space below the existing 48 MiB transaction admission requirement.
The 2 GiB limit and fixed 256 MiB reserve were respected throughout.

The local institutional aggregate is now served through the existing bounded
snapshot reader, which verifies packed and decoded hashes. The `rank-global`
CLI writes deterministic gzip; legacy JSON and the Python export default remain
supported. A live local aggregate migrated from 12,630,515 to 549,877 bytes.
HTTP GET before migration, after migration and after retiring the raw aggregate
returned the same decoded SHA-256; HEAD also passed. The retired file contained
only institutional aggregates. Private career inputs and historical recovery
snapshots were retained. The sole collector then resumed and committed new data.

358 Python tests and 42 frontend tests passed locally, including a new
decoded-identity and idempotence test. Clean production build and browser
acceptance remain required in hosted CI before deployment. Those checks and
online settlement are recorded separately after execution.

Public data still contains 148,613 source events, 42,802 observed people and
1,761 scored institutions. This release does not claim complete world coverage,
formal award/composite scoring or completed independent external review.
