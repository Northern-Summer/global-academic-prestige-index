# GAPI 0.2.17 — refreshed real observations

The frozen observation view now contains 148,613 source events from 42,802
observed people, with country fields spanning 147 country/territory codes.
The network has 1,761 scored institutions and 1,968 directed aggregate edges.
This updates the previous 144,855-event, 41,668-person release. These are
observed records, not a worldwide population denominator or a complete census.
The independent Easy ORCID 2023.6 source view retains its separate enumeration.

The new private normalized row delta was fully reconstructed and checked
against live input under the sole writer lock. Raw source materials were not
saved. The repository stores a 900,491-byte deterministic gzip aggregate;
the decoded SHA-256 is
`2a4a6f2022ed8f9d88baf21ac910445984c43b52d99483e68c60a1f2b791ee18`.
Public output contains institutional aggregates, not individual trajectories.

Two compatibility corrections accompany this refresh: the font maintenance
tool uses the shared bounded compressed-snapshot reader, and SHA256SUMS names
the actual stored gzip file. Typography, layout and scoring policy are unchanged.
27 relevant Python regression tests and standalone graph arithmetic replay
passed locally; replay mass is 0.9999999999999999, maximum score error is
1.74e-18 and convergence took 131 iterations. Existing hosted CI must execute
full tests, the clean production build and production browser acceptance before
automatic deployment. No local production build or browser pass is claimed.

Formal qualification, complete award/composite scoring and independent external
dual-track review remain incomplete. Unknown worldwide missingness is not
covered by parameter sensitivity. This is a real-observation prerelease.
