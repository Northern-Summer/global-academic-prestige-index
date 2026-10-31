# GAPI 0.2.24 — refreshed real observations and smaller retained assets

The current-source observation snapshot grows from 198,183 to 217,958 events
and from 58,140 to 64,033 internally deduplicated people. Its institution
directory contains 38,871 institutions; 1,998 have observed network scores,
with 2,341 aggregated edges and 155 observed country codes. These denominators
are distinct. This is a partial public-source observation release, not a
completed global census or a formal ranking approved by external reviewers.

The separate 2023 historical ORCID derivative is unchanged: 2,473,948 retained
source-person rows, 425,076 contributors to observed relations, 62,103 directory
institutions and 487,234 aggregate edges. Its numbers and scores are not pooled
with the current-source snapshot.

The new private ranking input was frozen as a 34,224,320-byte lossless delta.
Complete reconstruction and a second live input-stream hash matched SHA-256
52626cf3802a7a805cf6f25a063e9cdc51c7879632985fbc83dea6dd22a937d6.
Only institution-level aggregates are published. The public decoded aggregate
has SHA-256 f3b022607fe0096602744f10696b987eae85699dbd283a58e667ea100c11c875.
The standalone graph replay matched the scores to a maximum error of
1.74e-18, with probability mass 1 and consistent ordering.

Historical node JSON is now retained as deterministic gzip and decoded at the
owned build/development boundary. The exact 11,554,732 decoded bytes and their
hash are unchanged; the stored file is 1,610,851 bytes. Build provenance binds
the stored input and verifies the decoded emitted asset separately. Corruption,
oversized decompression and linked/non-file inputs fail closed. Old plain
assets remain supported. Browser data contracts, fonts, styling and score
policy are unchanged.

The collector also includes the tested bounded queue-window selection and
interleaved retry/linked/fresh prefixes. Optional scheduling priority cannot
promote evidence quality. Bounded event maintenance enforces SQLite page caps
and restores the original cap on failure. The Python baseline passed 390 tests;
47 frontend tests passed locally, including compressed-storage bounds and
committed-input/build-output provenance. Hosted tests and production browser
acceptance must also pass before deployment is considered settled.

Inactive aggregate working copies are removed from the latest checkout while
their exact contents remain recoverable in the public parent commit. Private
source evidence and historical ranking inputs are retained. External research
and software red-team reviews remain pending; this prerelease does not claim
third-party approval.
