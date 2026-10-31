# GAPI 0.2.21 — continued real observations and smaller historical inputs

This observation prerelease refreshes the current public-source network. It is
not a complete global census or a formal ranking approved by external auditors.
The separate, completely scanned Easy ORCID 2023.6 historical view retains its
own source, date and denominator.

Due-retry selection now starts from the sparse retry table instead of scanning
all discovered candidates. A fixed-query live ABBA comparison measured 0.9391 s
versus 0.0043 s with identical result hashes. This is local query performance;
network throughput remains governed by upstream service and quota conditions.
Fresh work, retry eligibility, ordering and scientific rules are unchanged.

Retained private row deltas can now be losslessly repacked into the existing
format 2. One historical input shrank from 26,094,336 to 8,385,680 bytes,
reclaiming 17,708,656 physical bytes. Full reconstruction retained its exact
280,862,320-byte logical hash and baseline. Atomic replacement follows source
and successor validation; failed writes retain the predecessor. No historical
version, source fact or evidence was deleted. Original manifest packed-size
fields describe historical storage, while logical input identities remain
unchanged. The 2 GiB budget and fixed 256 MiB reserve remain enforced.

All 374 Python regression tests passed locally, including capacity failures,
corruption, idempotence, legacy recovery and exact restored scores. Independent
public replay, frontend tests, hosted production build/browser checks and online
hash readback are separate gates required before publication is called settled.
Person histories and private acquisition diagnostics are not publicly bundled.

New records do not automatically become independently corroborated facts.
Missing awards and composite values remain missing. Independent external
dual-track review remains pending.

Frozen public scope: 187,236 events, 54,837 people, 35,397 directory institutions,
1,881 scored institutions and 2,140 directed aggregate edges. Compared with
0.2.20 this adds 7,351 events and 2,176 people. These are observed-source counts,
not a world-population denominator. Public gzip size is 993,046 bytes.

The independent stdlib score replay passed: probability mass 1.0, 131 iterations,
maximum score error 1.734723475976807e-18 and L1 residual 9.017302963348905e-13.
All 43 frontend tests passed locally against the new committed-snapshot contract.
No claim of independent third-party review follows from these arithmetic checks.
