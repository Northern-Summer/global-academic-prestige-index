# GAPI 0.2.22 — real observations and bounded private storage

This observation prerelease refreshes the public-source talent network. It is
not a complete global personnel census or a formal ranking approved by external
auditors. The fully scanned Easy ORCID 2023.6 historical view retains its
separate source date, scope and denominator.

Scientific rules are unchanged: influence flows from employer to training
institution, uncertain stages and dates are disclosed, and missing award and
composite scores remain missing. New observations do not automatically acquire
independent corroboration. External dual-track review remains pending.

The private derivative store now supports bounded compressed blocks while
retaining original row numbers, pseudonymous person keys and source-line
hashes. A real 150,000-row upgrade retained the exact original tuple hash;
all historical source rows remained. Repeated bounded migration changed zero
rows, and confirmed unreachable predecessor blocks were removed. Normal score
reads verify the decoded block identity directly rather than recompressing
every row. The same 3,000-row person-order check fell from 0.469 to 0.078 seconds;
this is a local comparison, not a claim about full-table scoring throughput.

Three retained historical ranking inputs were also repacked losslessly,
reclaiming 31,466,804 physical bytes without changing their decoded identities.
The row reader avoids repeated suffix copies; the same 50,000-row prefix
averaged 2.994 seconds before and 0.558 seconds after. These maintenance
improvements do not alter the scores or remove necessary evidence.

The 2 GiB task budget, fixed 256 MiB reserve, bounded process memory, single
writer and zero raw-source-file storage remain enforced. Person histories and
private acquisition diagnostics are not bundled publicly. Fonts, layout and
visual encoding are unchanged.

All 383 Python regression tests passed locally. Independent public graph replay,
hosted frontend tests, production build/browser checks, and online source/data
hash readback are separate publication gates. Arithmetic replay does not certify
the truth of underlying personal facts or establish third-party auditor identity.

Frozen public scope: 198,183 observed events, 58,140 people, 36,624 directory
institutions, 1,917 scored institutions and 2,199 directed aggregate edges.
The source coverage lists 155 country/territory codes; this is not a population
coverage denominator. Compared with 0.2.21, this adds 10,947 events and 3,303
people. The institutional snapshot is 1,018,814 compressed bytes.

Independent stdlib graph replay converged in 131 iterations, with probability
mass 0.9999999999999999, maximum stored-score error 2.6020852139652106e-18,
and L1 residual 8.823679092072323e-13. The decoded aggregate SHA-256 is
04da0bd7993f9887db40480042ab58d71e2cee1f6bda925cdc9fba93b90a8662.
The private normalized input independently matched its frozen live stream:
371,243,517 decoded bytes, SHA-256
100b8ab12a07fd48f2cd123575648223e063a1b2c9639d4d3f9c215b623f7e83.
