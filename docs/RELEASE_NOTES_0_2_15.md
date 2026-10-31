# GAPI 0.2.15 — more real observations within the same storage budget

This release contains 144,855 real education and employment events for 41,668
observed people, an increase of 4,871 events and 1,491 people over 0.2.14.
The institution directory contains 30,301 nodes. There are 1,735 institutions
with observed-network scores, 2,244 deduplicated relations and 1,931 aggregated
edges. Source observations, scored institutions and formal ranking eligibility
remain separate. The world's academic-personnel denominator is unknown;
independent external dual-track review remains pending. The separately labelled
complete Easy ORCID 2023.6 source view retains its own population and provenance.

The frozen input is a 22,598,968-byte row delta against the retained full
baseline. Its complete reconstruction and logical hash were verified under the
same exclusive writer ownership as scoring. No second full database or raw
source archive was created. A real maintenance pass preserved every stored event
byte and rowid while making a net 42,156,032 bytes of existing database pages
reusable. This was internal capacity, not filesystem space reclaimed. Historical
recovery inputs remain intact. The inactive preceding public aggregate was
losslessly compressed after checking its decoded identity against public Git.

HTTP body reads now return available bytes from the standard-library buffer,
allowing the existing deadline checks to run between slow body fragments.
Deterministic tests reproduce the previous hidden sequence of socket reads and
verify both connection types, unchanged complete bodies and hashes, corrupt gzip
rejection, and connection retirement after a truncated chunked response. Socket
timeouts and protocol framing still limit responsiveness; no production speedup
is claimed from these tests alone. Concurrency, quotas and source permissions
are unchanged.

355 Python regression tests and 41 frontend tests passed. A separately implemented
public graph replay converged in 131 iterations with L1 residual
9.848206234883139e-13, maximum score difference 2.6020852139652106e-18 and conserved
probability mass. This checks graph arithmetic, not personal facts, source
independence, worldwide coverage or external-auditor identity. Scientific scoring
policy, fonts and layout are unchanged. Production browser acceptance, CI,
deployment and online hash readback are reported only after actual execution.
