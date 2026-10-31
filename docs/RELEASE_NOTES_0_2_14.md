# GAPI 0.2.14 — real observations and bounded snapshots

This observation release contains 139,984 education and employment events for
40,177 observed people. Its institution directory contains 29,684 nodes, of
which 1,703 have observed-network scores, with 2,194 deduplicated relations
aggregated into 1,884 edges. These are observed source records, not a global
personnel census or a formal global ranking. Independent external dual-track
review remains pending. The separately labelled complete Easy ORCID 2023.6
source view is retained, with its own population and provenance.

The fixed input was verified against the retained full row baseline and live
logical input under exclusive writer ownership. A real retry exposed and repaired
two scaling defects: row-delta alignment could lose the next table after long
insertions, and Windows directory enumeration could lag open-file growth.
The same logical input now uses a 17,973,232-byte delta instead of the rejected
140,613,280-byte representation. All historical recovery inputs were retained;
only the newly generated, unreferenced failed delta was removed after recording
its packed identity. Another query now reads fallback institution labels through
the existing event-ID index rather than sorting every event payload in RAM.
Its complete fixture output matches the previous query.

348 Python regression tests and 41 frontend tests passed. Independent graph
arithmetic replay converged in 132 iterations, with L1 residual
8.528127748257119e-13 and maximum score difference 8.673617379884035e-19.
This validates the published graph arithmetic, not underlying personal facts,
coverage, source independence or third-party auditor identity.

Inactive public slices were losslessly archived with decoded hashes checked
against their public Git originals. The active slice, private evidence and
source boundaries remain. The 2 GiB total ceiling, 256 MiB reserve, 512 MiB
process ceiling, single writer and zero raw-response landing rules remain.
Scientific scoring policy, fonts and page layout are unchanged. Build, browser,
CI and online settlement outcomes are recorded only after actual execution.
