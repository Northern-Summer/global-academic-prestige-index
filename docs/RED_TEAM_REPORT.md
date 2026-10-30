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

An additional bounded internal read-only pass on 2026-10-30 reproduced three
defects: first-ID duplicate evidence selection, unfinished degree confirmation in
the official adapter, and an unimplemented independent-corroboration path. Fixes
add source-preserving deterministic aggregation, a shared completed-qualification
rule and a derived evidence-decision owner. A cross-challenge then exposed an
unverified page/person connection and overstrict optional-date equality. Official
profile v3 requires a same-ORCID connection; optional dates are conflict checked
without pretending missing fields were confirmed. These findings and their
negative/legitimate cases are covered by executable regression tests. Actual test
counts, collection scope and deployment settlement belong to the dated acceptance
receipt. Internal agents remain members of the implementation team and do not
satisfy independent third-party review.

Continuation adds a runnable offline OOD module (9 scenarios/26 invariants in the
executed report), a stdlib-only public aggregate replay using an independently
implemented PageRank iteration, and conservative Nobel label attribution. Exact
ROR candidates remain unconfirmed. See docs/OOD.md and the continuation receipts.
These are internal executable checks, not external reviewer acceptance.

The institution bridge's bounded internal cross-challenge additionally reproduced
stale reciprocal reuse after a newly detected ROR ambiguity, silent replacement of
an existing conflicting WD alias, and nested JSON type pollution bypassing the
batch failure boundary. Bridge v2 now consumes only the current unambiguous scan,
preserves conflicting originals outside the active projection, rechecks predecessor
decisions and rejects polluted containers through explicit validation. Regression
tests include ambiguous and conflicting intermediates, valid same-target prior
evidence, interrupted/resumed batches and oversized entity isolation. Real-sweep
counts and the source-specific unresolved remainder belong to the dated receipt;
these tests do not prove global identity accuracy.

The automatic deployment increment received a bounded internal read-only review.
That review flagged the GITHUB_TOKEN/legacy-Pages trigger restriction; deployment
now explicitly requests the existing Pages build using a narrow pages:write token.
Source freshness across distinct refs remains best effort, with pre-push checking
and superseded workflow cancellation; no global atomic-release claim is made.
Real Actions and public readback receipts, not the presence of workflow YAML,
determine whether the automatic path has actually succeeded.

Before a real ranking release, two independent review tracks are required:

1. Research validity and data governance: measurement claims, direction, coverage, entity resolution, attribution, rights, privacy, and OOD behavior.
2. Software security and reproducibility: hostile inputs, public-data leakage, dependency/build risks, frontend injection and resource exhaustion, and clean-room reproduction.

The review must bind a fixed repository revision, input dataset, and config. Reviewers must disclose conflicts. Critical/high findings must be fixed and independently retested; lower-severity findings need a published disposition. Publish reviewer identity or organization, scope, methods, conflicts, findings, and retest status. Do not describe this prototype as red-team approved.
