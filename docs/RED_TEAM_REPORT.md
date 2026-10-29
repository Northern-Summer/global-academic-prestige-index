# Independent red-team status

Status: **Not conducted.** This repository has not received independent third-party review. The local tests are developer verification and do not satisfy the planned independent review gate.

## Internal red/blue review, 2026-10-29

Two implementation-team agents independently reviewed methods/statistics and
software/security/UX, followed by scoped blue-team fixes and read-only cross
challenges. This is **internal verification**, not either external track.
The predecessor baseline is `16d92ceeb7f3a491626b6a2ea4784e90db24ab56`.

Confirmed defects and implemented fixes: unfinished multilingual degree labels
cannot become completed doctorates; overflowing PageRank row/prior sums fail
closed; source deletion distinguishes fixed and expanded node universes;
rendered counts, stages, score mass and damping are validated; text labels are
escaped; build provenance binds inputs and served outputs; CI separates code-only
from real-manifest validation. No evidence justified replacing the architecture.

`outputs/REVIEW-20261029.md` and the machine acceptance receipt contain the exact
test/browser/cleanup outcomes and remaining limits. Passing these checks
establishes only those declared cases. External reviewer independence, conflict
disclosures, complete OOD/discipline analysis and global enumeration remain open.
No formal global ranking is approved.

Before a real ranking release, two independent review tracks are required:

1. Research validity and data governance: measurement claims, direction, coverage, entity resolution, attribution, rights, privacy, and OOD behavior.
2. Software security and reproducibility: hostile inputs, public-data leakage, dependency/build risks, frontend injection and resource exhaustion, and clean-room reproduction.

The review must bind a fixed repository revision, input dataset, and config. Reviewers must disclose conflicts. Critical/high findings must be fixed and independently retested; lower-severity findings need a published disposition. Publish reviewer identity or organization, scope, methods, conflicts, findings, and retest status. Do not describe this prototype as red-team approved.
