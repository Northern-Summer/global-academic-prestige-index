# GAPI 0.2.25 — expanded real observations and bounded storage

The current-source dataset grows from 217,958 to 238,433 events and from
64,033 to 69,417 internally deduplicated people. It contains 41,120 directory
institutions, 2,129 institutions with observed network scores, 2,524 aggregate
edges and 156 observed country/region codes. These denominators are distinct;
this remains an incomplete public-source observation dataset, not a global
census, employment probability estimate or externally approved formal ranking.

The private input is frozen under SHA-256
8fb2d855dc259843d4cc036c17bb1fa6bd8354ecaaaf46c82e283c7e07305935.
Its 444,828,462 logical bytes are retained in a 33,486,268-byte lossless delta.
Complete reconstruction and a second live input-stream hash matched. Only
institution aggregates are public. The new decoded aggregate SHA-256 is
8e564048812a16e6e04c48504ee0cc5f8ad229eafa164a1abd2bbfd23de7557a.
An independently implemented graph replay preserved ordering with maximum
score error 1.74e-18 and probability mass 0.9999999999999999. This checks
arithmetic; it does not independently verify personal facts or world coverage.

The collector now bounds linked-candidate selection to the current window,
using the existing person index. SQLite admission accommodates small records
within an enforced page-growth cap instead of rejecting them solely on a
worst-case member estimate. Reserve and total-budget limits remain unchanged.

Historical relationship files are stored as deterministic gzip, just as the
node file already was. Full decoded contents and manifest identity are
unchanged; all 62,103 historical institutions and 487,234 historical edges
remain independently available. Current and historical scores are not pooled.
The shared reader preserves existing routes, fonts, styling and score policy.

Row snapshot creation can explicitly choose XZ preset 6; the default remains
3. This changes storage representation only. Python regression passed 394
tests, including dense full/delta reconstruction and invalid-preset rejection.
All 47 frontend tests passed after relationship-file compression. Hosted
tests, production build and browser acceptance must pass before deployment
is reported as complete. Independent external research and software reviews
remain pending.
