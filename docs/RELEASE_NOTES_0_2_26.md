# GAPI 0.2.26 — more real observations, less repeated storage

The current-source dataset grows from 238,433 to 263,275 events and from
69,417 to 75,661 internally deduplicated people. The institution directory has
43,383 entries; 2,215 institutions have observed network scores, connected by
2,680 aggregate edges. There are 156 observed country/region codes. These
counts describe different populations. The collection remains incomplete and
does not establish employment probabilities or a verified global census.

The public institutional aggregate occupies 1,174,275 compressed bytes.
Decoded SHA-256:
37b4c5e9e2562567a3ad510bddcb090a06ef2f498e2c44e3c5db733fdf700197.
The private input has SHA-256
40fd9d5a8beb93a09802ca7a08b2922ed52cd3588300e61d41281a8c23b9642f.
Its 498,156,379 logical bytes are retained as a 12,154,192-byte increment
against the preceding frozen input. Complete reconstruction and the live
input-stream hash agree. Personal inputs are not included in public assets.

An independently implemented graph-to-score replay reproduced ordering with
probability mass 1.0 and maximum score difference 1.74e-18. This verifies
arithmetic, not the underlying personal facts, source independence or worldwide
coverage. Independent external research and software reviews remain pending.

Storage changes address measured duplication. Explicit opt-in delta format /3
allows at most four dependency layers, with cycle, missing-base, corruption,
descendant-depth and size checks. Earlier formats retain their full-baseline
requirements. Two retained inputs were losslessly rebased, reducing their
physical storage by 29,055,488 bytes while preserving logical identities.
The retained predecessor is replaced only after full reconstruction checks.

Historical career projections can use independently decoded rows with a shared
4 KiB compression dictionary. An accepted 500,000-row migration made 9,584,640
database bytes reusable; the database file did not shrink. A representative
2,000-lookup pilot became slower (median 0.045777 to 0.075241 seconds), within
the declared acceptance bound. This is a disclosed space/time tradeoff.
The slower 8 KiB pilot was rejected and restored. Scientific rules and scores
do not depend on these physical representations.

Python regression: 407 tests passed. Frontend regression: all 47 tests passed.
The current typefaces, layout, interactions and separate historical dataset
remain unchanged. Hosted tests, production build, browser acceptance and online
hash readback are required before deployment is considered settled. The total
2 GiB storage limit and fixed 256 MiB reserve remain enforced; these local
improvements do not prove that the entire remaining collection fits.
