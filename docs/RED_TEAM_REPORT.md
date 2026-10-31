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

The 2026-10-30 bounded batch-transport review reproduced nested item-ID type
pollution interrupting adjacent entities, and socket timeouts at a caller's
deadline being recorded as member failures. Scalar item/time validation and
deadline-aware connection/read exception handling now cover those counterexamples.
The implementation-team reviewer independently reran both original cases and the
early-timeout, access-refusal, maxlag and oversized-response controls. This was a
read-only internal review with mock transport/in-memory SQLite, not a third-party
review or proof of the underlying career facts. The dated batch acceptance record
contains actual test counts, live acquisition measurements and deployment status.

1. Research validity and data governance: measurement claims, direction, coverage, entity resolution, attribution, rights, privacy, and OOD behavior.
2. Software security and reproducibility: hostile inputs, public-data leakage, dependency/build risks, frontend injection and resource exhaustion, and clean-room reproduction.

The review must bind a fixed repository revision, input dataset, and config. Reviewers must disclose conflicts. Critical/high findings must be fixed and independently retested; lower-severity findings need a published disposition. Publish reviewer identity or organization, scope, methods, conflicts, findings, and retest status. Do not describe this prototype as red-team approved.

### 2026-10-31: internal full-source and recovery checks

A bounded read-only implementation-team review checked exact HQ reuse, person
merging across HQ/delta, joint appointments and the full-source checksum gate.
It reproduced a P2 false rejection when HQ contained identical duplicate rows.
Coverage now marks every equivalent HQ projection while retaining a deterministic
representative for the ordered reuse proof. A mismatched HQ identity is rejected
at scoring. Both cases have executable regression fixtures.

A real connection ended before the archive's gzip EOF. Its delta stayed
provisional. Same-process range recovery now reuses the bounded transport reader;
registered Zenodo archives without ETag require the exact pinned URL/length/MD5
and final whole-file verification. The read-only follow-up checked range framing
and normal continuation. End-to-end fixtures additionally reject changed suffixes,
wrong checksums and stalled reconnections, and verify socket cleanup. Initial
Content-Length drift is rejected before decoding. The subsequent real replay read
the complete source and matched the earlier full-census SHA-256; it required zero
range reconnections, so it does not constitute a real-network reconnect trial.

The full regression contains 276 passing tests. The actual 487,234-edge browser
export was checked for hashes, endpoint/chunk indexes, privacy and stage totals;
real deterministic replay evidence is in `outputs/EASY_ORCID_FULL_ACCEPTANCE.json`
and `outputs/EASY_ORCID_FULL_WEB_IDENTITY.json`. These checks do not establish
career-fact accuracy, absence of unknown vulnerabilities, visual/browser
acceptance, or independent third-party approval. The production frontend has not
yet consumed this new browser contract.

## 2026-10-31: full-source browser integration (local acceptance)

The production build now consumes the complete aggregate historical bundle.
The region omission found in the first rendered scene was repaired using fixed
ROR metadata. All 62,103 prior node fields, including scores and ordering, were
compared unchanged; 23,313 nodes have known regions, while 38,790 remain unknown.
The selected institution scene rendered seven actual colors and 436 known-region
nodes. A visible legend explains them. Historical table readiness was 1,738 ms
in the local production-path trial, not an Internet performance guarantee.

Executed checks: 276 Python tests, 38 Node tests, production build, historical
browser acceptance and the existing production browser regression. Checks cover
partial chunk failure without stale results, direct-link loading, filters, CSV,
keyboard, mobile, font preservation and WebGL fallback. The initial browser
attempt used the wrong preview base and failed; the corrected Pages base passed.
An initial Python run also exposed two obsolete minimal entity-table fixtures;
both were aligned with the production metadata schema and the full suite rerun.
Failures remain in the corresponding `outputs/DERIVED_REGION_*` logs.

Evidence: `outputs/DERIVED_REGION_IDENTITY.json`,
`outputs/DERIVED_REGION_FULL_RETEST.txt`, `outputs/DERIVED_REGION_NODE_TESTS.txt`,
`outputs/derived-region-pages.json`, and
`outputs/DERIVED_REGION_LEGACY_BROWSER.json`. Local screenshots were inspected.
This is internal automated and visual acceptance, not deployment, independent
third-party review, complete global coverage or factual verification of careers.

## 2026-10-31: cross-source counting and scheduling correction

The earlier `cross_source_candidates=0` measurement compared raw institution
identifiers and undercounted matches between Wikidata and ROR. A bounded
person-grouped diagnostic now reuses adjudication's fact interpretation and
active institution aliases. On the same 66,904-event input, it found 477 shared
persons and 39 exact eligible fact matches. The old raw count remains available
under an explicit name. Event statuses and official-confirmation rules did not
change; these candidates do not prove independent origins.

A four-minute finite queued collection added 544 events, reaching 67,448 events.
Afterward, shared persons were 535 and exact fact matches 45. This is an observed
batch result, not a controlled estimate of scheduler effectiveness. ORCID fresh
work now uses at most half its slots for explicit stable identities present in
Wikidata, while retaining ordinary queue and due-retry capacity. There are no new
name-based merges, persistent selection indexes, raw downloads or evidence
promotions. Tests cover date conflict, unfinished qualifications, ambiguous
aliases, rejected membership, deterministic selection, retry backoff, ordinary
work progress and aggregate-only diagnostic output.

The last eight-second Wikidata window ended without a completed member. The old
collector labeled that as suspended despite `stage_time_exhausted=true` and no
access error. The collector now distinguishes a finite acquisition deadline from
source failure and checks the deadline after candidate selection before opening
a network window. Tests exercise both adapter-reported exhaustion and a raised
`StageDeadline`. An initial test-fixture duplicate insertion failed; the fixture
was corrected and rerun, with the failure log retained.

Evidence: `outputs/CROSS_SOURCE_OVERLAP_VERIFIED.json`,
`outputs/CROSS_SOURCE_PRIORITY_AFTER.json`,
`outputs/CROSS_SOURCE_PRIORITY_COLLECTION.txt`, and the corresponding test logs.
The independent official-confirmation count remains zero. This internal review
does not satisfy the required external research or software red-team tracks.

The follow-up real 90-second collection preserved both source checkpoints and
added another 229 events. ORCID's exhausted finite window was correctly labeled
without suspending it, and Wikidata then committed 49 members. Final counts were
67,677 events, 554 shared persons and 50 exact normalized fact candidates; official
confirmations remained zero. The final regression passed 282 tests. Database
integrity was `ok`, all events had member evidence, and no provisional archive
events or raw archive files were detected. Final receipts are
`outputs/CROSS_SOURCE_RUNTIME_RETEST.txt`,
`outputs/CROSS_SOURCE_SETTLED_CORROBORATION.json`,
`outputs/CROSS_SOURCE_SETTLED_QUALITY.json`, and
`outputs/CROSS_SOURCE_SETTLEMENT.json`. None of this completes the remaining
source queues, global enumeration or external review.
