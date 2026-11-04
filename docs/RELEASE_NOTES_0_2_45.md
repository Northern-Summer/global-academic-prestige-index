ORCID collection can now opt into one bounded `/record` XML response per new
member with `collect-global --orcid-record`. The default still requests the two
affiliation sections. Both routes retain the same affiliation fields, resource
limits, request pacing, rolling-day allowance and atomic member transactions.
Committed members are skipped across route changes. Refused, corrupt, oversized
or interrupted responses remain unresolved without an endpoint fallback.

Whole-record evidence binds the actual response URL, decoded response hash and
committed member manifest. Independent corroboration checks that binding before
applying the existing fact and independent-origin rules. It never substitutes a
whole-record hash for two section hashes or promotes self-reported facts alone.

Frozen candidate collection reports its verified denominator at startup and at
settled 15-minute intervals. Storage-root registration can inherit the original
candidate prefix after checking its count and provenance; it cannot change source
policy, resource allowances or candidate scope. Ordinary storage entries avoid an
unnecessary containment check while retaining the existing link rejection rules.

Validation covers equivalent extracted facts, route-switch idempotence, evidence
tampering, corrupt gzip, identity mismatch, capacity failure, transaction rollback,
interrupted parsing and process restart. The complete local Python suite passed
577 tests. Bounded production windows confirmed resumable commits, but used
different candidates and do not establish a stable performance improvement.

The published observation data and scores remain the v0.2.38 snapshot. This code
release does not establish complete annual-archive verification, complete frozen
candidate processing, global coverage or formal ranking eligibility. The annual
archive access failure and unresolved records remain open. Details: [collection,
evidence and resume rules](STREAMING.md).
