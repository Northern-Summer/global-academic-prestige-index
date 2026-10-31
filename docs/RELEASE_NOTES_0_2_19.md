# GAPI 0.2.19 — remove repeated queue scans

This maintenance release keeps the exact 0.2.18 public scores and visual assets.
It changes acquisition queue execution, not scientific eligibility, evidence,
retry ordering or scoring policy.

A real 200-person Wikidata batch spent 76.78 seconds selecting work, followed
by 21.19 seconds acquiring and committing 690 events. SQLite's plan repeatedly
scanned a materialized exact-membership set. A direct indexed-join candidate
only moved the scan to the prefix set and was rejected. The final implementation
builds exact and prefix membership sets once, retaining the legacy predicate
for unusual identifiers and excluding NULL from prefix membership.

Under the sole writer lock, old and new queries selected identical ordered
200-person lists on the same database. Wikidata fresh selection changed from
82.27 to 0.12 seconds; due selection changed from 0.64 to 0.45 seconds. ORCID
fresh selection changed from 1.92 to 0.012 seconds, while its due scan increased
from 0.37 to 1.48 seconds. The combined measured ORCID selection still decreased.
These are bounded host measurements, not universal throughput guarantees.
No persistent index, source-body cache or database copy was added.

360 Python tests passed. New checks compare unusual identifiers, source
isolation, case/prefix behavior and retries against the original membership
predicate. A 3,200-candidate/3,000-member fixture has a machine-speed-independent
two-million SQLite instruction ceiling: the legacy implementation is interrupted
and the new implementation passes. The source-loss and retry-fairness tests
remain in the full regression suite.

A subsequent real HTTP request reached its stage deadline without committing
new events. The queue fix does not remove upstream transport limits; the failed
request is retained as evidence, and the single collector resumes normally.
Hosted CI must pass full tests, production build and browser acceptance before
automatic deployment. Independent external review and global completeness
remain unproven; this is a real-observation prerelease.
