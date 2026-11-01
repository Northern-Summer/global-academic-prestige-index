# GAPI 0.2.28 — refreshed real observations

The published current-source dataset grows from 263,275 to 282,901 events and
from 75,661 to 80,668 internally deduplicated people. It contains 44,821 directory
institutions, 2,286 institutions with observed network scores and 2,806 aggregate
edges, with 156 observed country/region codes. These are distinct populations.
The dataset remains incomplete; directory membership is not ranking eligibility,
and observed flows are not personal employment probabilities.

The public institutional aggregate occupies 1,210,187 compressed bytes and
31,871,202 decoded bytes. Its decoded SHA-256 is
71b392d3e7916ed0273c0d0cacd58561d963123af4ecfdf5d0aaa31e042e9d5a.
The private input SHA-256 is
66aeb01544a80ea8d5bf788644e249fec73537b6c8ad56b736efbb520784c771.
Its 541,308,724 logical bytes are represented by an 11,016,836-byte increment
against the preceding released observation input, at dependency depth 3.
Complete reconstruction and a separate live row-stream hash matched. Personal
trajectories are excluded from public assets; no raw source material was saved.

The independently implemented public graph replay reproduced ordering with
maximum score difference 1.74e-18 and probability mass 0.9999999999999999.
Arithmetic replay does not independently validate personal facts, source
independence, rights or worldwide coverage. Independent external research and
software reviews remain pending.

The completed collection window processed 74,327 of 191,845 discovered Wikidata
candidates and 11,368 of 689,220 ORCID candidates. Neither source enumeration is
complete. These denominators describe committed discovery queues, not all
academic personnel worldwide. A final batch reported anomalous wall-clock
duration; its cause is unresolved and it is not used as a normal throughput
estimate. Pending records and retry evidence are retained.

This release uses the v27 lossless storage implementation without new scoring,
evidence or interface rules. The previously executed 412 Python and 47 frontend
regression tests remain the implementation baseline; hosted tests, production
build, browser acceptance and online hash readback must pass for this new data.
Typography, layout and the separate historical dataset remain unchanged. The
decoded aggregate is approaching its existing 32 MiB limit; future larger
releases need bounded institution-detail shards rather than a larger browser
resource allowance. The 2 GiB storage budget and 256 MiB reserve remain enforced.
