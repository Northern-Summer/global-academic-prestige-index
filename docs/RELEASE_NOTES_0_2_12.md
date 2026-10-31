# 0.2.12 — Refreshed real observations within the storage budget

The current-events view now contains 123,077 observed events for 35,160 people,
compared with 101,915 events and 28,739 people in the preceding published slice.
There are 26,096 directory nodes, 1,614 scored institutions, 1,755 aggregate edges
and 2,054 deduplicated relations. Directory size is not career coverage. These
counts do not establish complete world coverage or independently verified careers.
The separate, completely scanned Easy ORCID 2023.6 view retains its own source,
denominator and scores; it is not combined with current-events ranks.

A new optional private row archive streams normalized tables directly into XZ,
without first creating a SQLite staging database. The actual logical input was
233,370,402 bytes, compressed to 122,376,756 bytes. Its entire decoded stream was
hash verified before promotion. The fresh export scored the same unchanged live
tables under the exclusive writer lock. Original responses were not saved and
individual trajectories are not publicly bundled.

Small-fixture restoration, exact event preservation, identical scores, repeat
archive identity, corruption, budget interruption, malicious schema rejection,
temporary-file cleanup and the no-restore fresh CLI path are tested. Full-scale
local restoration was not attempted: a second database requires separate space
admission. Existing SQLite/gzip/XZ historical inputs remain supported and intact.

338 Python tests and 41 frontend tests passed. The first production build
rejected the expanded directory at the previous 25,000-node limit. The catalog
limit is now 32,768, with explicit accept/reject boundary tests. Tables still
render 50 rows per page and 3D node/edge limits remain unchanged. The rerun
production build passed; payload identity and transport byte limits remain.
An independently implemented
public graph replay converged after 132 iterations, with probability mass 1 and
maximum score difference 2.6020852139652106e-18. This verifies arithmetic, not the
truth of personal source statements or global representativeness. The first
real export safely failed when concurrent frontend fixture removal prevented
storage inspection; its traceback is retained locally. The successful rerun
followed completion of those tests without weakening storage checks.

Before the production build, task-accounted usage was 1,795,586,184 bytes, leaving
83,462,008 bytes after the fixed 256 MiB reserve. The 2 GiB cap remains enforced.
There are no typography or visual-design changes in this release. Independent
external research/data and software/security reviews remain pending; this is an
observation release, not a certified global ranking.

See `outputs/ROW_SNAPSHOT_PUBLIC_ACCEPTANCE_033.json` for the frozen aggregate
identity, arithmetic replay and resource measurements. Deployment and browser
results are separately recorded and must not be inferred from this document.
