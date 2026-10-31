# 0.2.13 — Exact incremental inputs and continued real observations

The current-events view advances from 123,077 to 128,215 events and from 35,160
to 36,710 people. It contains 27,197 directory nodes, 1,647 scored institutions,
1,809 aggregate edges and 2,113 deduplicated relations. The separately frozen
Easy ORCID 2023.6 whole-source view is preserved with its own denominator. These
are observed source records, not complete world personnel statistics or a
formally reviewed university ranking.

The private normalized input is 243,098,053 logical bytes. Instead of writing
another complete snapshot, the new exact row-stream difference adds 7,258,116
packed bytes against the retained full 0.2.12 baseline. The full baseline and
entire reconstructed target passed SHA-256 checks before promotion. The scorer
read unchanged live tables under the same exclusive writer lock. No original
responses, individual trajectories or private archives are publicly bundled.

Differences contain bounded copy, skip and literal-add operations. Synchronization
uses at most 32 lines and approximately 5 MiB per side. Only a retained full row
baseline is accepted; recursive delta chains are rejected. Missing/changed bases,
overrun commands, bad hashes, truncation, trailing records and resource failures
reject the candidate. Repeated identical inputs preserve identity and mtime.
Restoring a full SQLite database still needs separately admitted space; that
full-scale local restore was not attempted. Small-fixture exact restoration and
score equivalence passed, as did the real full-stream reconstruction check.

345 Python regression tests passed. An internal bounded read-only review found a
malformed JSON-header handling defect; it was fixed and regression tested. The
review did not find another confirmed loss, promotion or budget-bypass defect
within its scope. It is not independent external certification. Public graph
arithmetic replay passed separately; see `outputs/ROW_DELTA_PUBLIC_ACCEPTANCE_034.json`.

Two inactive public institution slices were losslessly archived after checking
their decoded hashes and exact original public Git objects. This reclaimed
19,742,001 logical bytes while retaining both compressed originals and public Git
recovery. The active slice and all private historical inputs remain. Binary chunk
deduplication was rejected after an actual read-only estimate showed expansion.
A small grouped-row experiment freed internal SQLite pages only and was not
migrated into production. Measurements are published with this release.

The 2 GiB cap, 256 MiB reserve, 512 MiB process cap, single writer and zero raw
response landing rules remain. Scientific scoring rules, source rights and
typography are unchanged. Independent external dual-track review remains pending.
Browser, build and online settlement results are recorded separately after actual
execution rather than inferred from the implementation.
