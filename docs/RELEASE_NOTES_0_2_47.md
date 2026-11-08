Stop repeatedly requesting candidates that cannot currently yield usable data.
Automatic candidate selection now stops after four failed candidate attempts,
separately from HTTP retries within an attempt. ORCID GET 409 responses,
verified permanent redirects and invalid identifiers are not automatically
retried. Their identities, errors and pending counts remain intact; none become
successful acquisitions. Eligible transient failures retain their backoff and
share batch capacity with fresh candidates.

The offline OOD memory adapter now returns the sampled resident memory as well
as retaining its peak. This repairs its interface with the analyzer without
raising memory limits or changing ranking and evidence rules.

Validation includes 67 relevant local regression tests and 24 tests against an
isolated copy of the published predecessor with only this release's changes.
A read-only comparison on the same frozen 33,801-candidate prefix found 234
blocked candidates in the predecessor's 1,000-item batch and zero in the new
batch; 234 fresh candidates filled those slots. The comparison made no network
requests and changed no database rows. It measures selection behavior, not
end-to-end collection throughput.

The public observation dataset remains the v0.2.46 frozen snapshot. No new
personal records or full local aggregate are bundled. Archive rejection
settlement, independent fact verification and complete global coverage remain
unfinished; this release does not claim formal ranking eligibility.

The retry limit is owned by
[`MAX_CANDIDATE_ATTEMPTS` and `CareerStore.pending`](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.47-observations/src/gapi/career_store.py).
ORCID's [official error documentation](https://github.com/ORCID/ORCID-Source/blob/main/orcid-api-web/tutorial/api_errors.md)
describes locked records and permanent identity redirects. Existing
[methodology](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.47-observations/docs/METHODOLOGY.md)
and evidence requirements continue to apply.
