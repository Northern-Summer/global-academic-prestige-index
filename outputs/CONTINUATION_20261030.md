# GAPI continuation: candidate completion and real observation release

Date: 2026-10-30. This record describes executed work, not a formal global ranking approval.

## Delivered data boundary

The committed Wikidata candidate selection is processed: **3,484 / 3,484**.
The ORCID selection is **1,057 / 1,062**. Five candidates remain pending with
preserved errors: two HTTP 409 conflicts and three mismatched XML root identities.
They are not counted as received, rejected, independently corroborated or globally
covered. Persisted retry deadlines remain in force. Global personnel denominator
and complete source enumeration remain unknown/incomplete.

| Measure | Predecessor | New frozen observation slice |
|---|---:|---:|
| Internal pseudonymous people | 1,962 | 4,130 |
| Source career events | 7,584 | 15,917 |
| Country/region codes in events | 88 | 90 |
| Deduplicated training/employer relations | 278 | 302 |
| Aggregated directed edges | 239 | 262 |
| Scored network endpoints | 297 | 323 |
| Institution identifier directory | 2,790 | 4,704 |

Directory identifiers are not an institutional census. Events lacking explicit
degree, employment, dates or identity remain unresolved. Collection volume does
not establish independent evidence. Official profile checks still yield **zero**
independent corroborations; award and composite university scores remain missing.

Snapshot: `global-b67e5a129183289f.json`.
SHA-256: `b67e5a129183289fbb929a680e1c0ed7cc673619dd435cfdb361eb020bc793c1`.
Private normalized input SHA-256:
`2a2085e817fa05e2769f1b9dab62dc305a357c57ca75b41f701296358cb42e1d`.
Private input is outside the repository and is never bundled publicly.

## Corrections and executed checks

- The acquisition panel reads frozen per-source counts. Accessible progress bars
  expose received, rejected, pending and retry counts without inventing world coverage.
  Its schema rejects inconsistent totals and fabricated world denominators.
- A real read-only progress query exposed `database is locked` on member commit.
  The fault was reproduced before correction. SQLite now waits at most five seconds
  for brief readers; the existing OS lock still rejects a second writer immediately.
  An uncommitted member rolls back on failure. No WAL or extra writer was introduced.
- Python: **172 tests passed** (`python-reader-fix-20261030.log`).
  Frontend: **30 passed** (`node-progress-20261030.log`).
  OOD: **9 synthetic pressure scenarios / 26 invariants passed**
  (`ood-progress-20261030.json`). These fixtures do not prove real-world accuracy.
- Public graph replay passed without canonical ranking imports: 22 iterations,
  probability mass 1, maximum score difference `3.469446951953614e-18`
  (`public-replay-queue-20261030.json`).
- Two read-only replays from the same private frozen input matched the initial
  export byte for byte, including the manifest. Receipt SHA-256:
  `381391628e10f8d5e4562bcf574eb227297db3e2e78bbc10f36f2253c937a093`.
  See `export-queue-20261030.json`, `replay-queue-a-20261030.json` and
  `replay-queue-b-20261030.json`.
- New-slice local production browser passed: 826 ms table readiness; search,
  filtering, sorting, keyboard details, CSV, 3D, directional list, pagination,
  WebGL fallback, mobile layout, hostile numeric/label input, and frozen counts
  (`browser-queue-20261030.json`). Desktop, mobile and expanded progress screenshots
  were observed. The English italic and regular-weight Chinese serif are preserved.
- A local browser failure was retained: root-base build served under the Pages
  subpath produced asset 404s. Rebuilding with the actual production base passed.
  This did not change scoring or public production routes.
- 799 / 799 observed Ringgold IDs processed, including 37 new decisions and
  21 newly confirmed reciprocal bridges. Current totals: 511 confirmed,
  208 current/reciprocal unconfirmed, 80 unmatched. This is identifier evidence only.
- Source integrity check: SQLite `ok`, zero events without committed member evidence,
  zero provisional archive events, zero raw archive/XML files. Pre-freeze task
  storage was 204,476,865 bytes; normalized freeze adds 34,447,360 bytes.
  The observed collector peak working set was 49,082,368 bytes, below 512 MiB.

## Publication, recovery and remaining work

Normal non-force pushes and automatic Pages deployment are authorized. Each
deployment requires the production build, browser checks and served-file hash
readback. This record alone does not certify settlement; the deployment/browser
receipts identify the exact published source and snapshot.

No new formal ranking tag is authorized by these results. Independent third-party
research/governance and software/reproducibility reviews are still pending. Internal
test suites and graph replay are not independent third-party approval.

Two byte-identical obsolete replay receipts were removed after SHA-256 checks;
6,284 logical bytes reclaimed. Original export and independent replay receipts,
predecessor source/data snapshots, private input freezes and all failure logs remain.
Previously refused raw-cache cleanup targets were untouched. Dependencies and the
user's development server were preserved. See `cleanup-duplicates-20261030.json`.

Resume through the existing API queue and persisted retry deadlines; then expand
versioned discovery selections and official corroboration. Do not read a large raw
archive to disk. Current five ORCID failures, incomplete global enumeration,
limited independent evidence, canonical award attribution and external dual-track
review remain explicit gates. The goal is active, not complete.
