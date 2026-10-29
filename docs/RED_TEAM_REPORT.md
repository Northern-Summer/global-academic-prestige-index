# Independent red-team status

Status: **Not conducted.** This repository has not received independent third-party review. The local tests are developer verification and do not satisfy the planned independent review gate.

Before a real ranking release, two independent review tracks are required:

1. Research validity and data governance: measurement claims, direction, coverage, entity resolution, attribution, rights, privacy, and OOD behavior.
2. Software security and reproducibility: hostile inputs, public-data leakage, dependency/build risks, frontend injection and resource exhaustion, and clean-room reproduction.

The review must bind a fixed repository revision, input dataset, and config. Reviewers must disclose conflicts. Critical/high findings must be fixed and independently retested; lower-severity findings need a published disposition. Publish reviewer identity or organization, scope, methods, conflicts, findings, and retest status. Do not describe this prototype as red-team approved.
