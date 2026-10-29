# Build, review and observation publication

The public site is a real-observation research preview. CI, local developer tests
and implementation-team reviews do not establish independent third-party approval
or a formal global ranking.

## CI and code review

The fast test job runs Python, Node and a code-only Vite build. A separate job
builds the explicitly selected committed real release manifest, verifies its
snapshot hash, checks build provenance and uploads a seven-day site artifact.
The normal default is `outputs/global/manifest.json`; a manual workflow run can
select a committed aggregate manifest. These jobs have read-only repository access,
bounded timeouts and no source acquisition or personal database access.

The optional `workflow_dispatch` browser job builds the same selection and runs
the existing production browser acceptance against its preview base. On the hosted
Linux runner only, it installs pinned Playwright 1.56.1 without changing the lockfile,
and reuses hosted Chrome; it never downloads a browser. A missing browser is a
failed check, not a pass. Local execution reuses already installed dependencies.
The preview process is bounded by its shell job and stopped by an exit trap.

Actions are pinned to official repository commit SHAs, checked with `git ls-remote`
on 2026-10-29. A future update must verify provenance, inspect change scope and run
the applicable checks. These pins do not lock the mutable hosted runner image,
Python/Node patch resolution or every transitive browser-tool dependency.

The PR template records source rights, statistical counterexamples, failure modes,
fresh visual evidence and recovery. `CODEOWNERS` routes review to the repository
owner; it does not configure branch protection, require a second person or confer
independent-auditor status. Those remote settings have not been changed here.

## Build identity and local publication preflight

Vite captures the web/config/lockfile/published-document input hashes when loading
the build config, rejects changes during build, and writes `build-provenance.json`
after output emission. Text input hashes normalize CRLF to LF; served output hashes
cover exact bytes. The proof binds all site files, selected aggregate manifest,
snapshot hash, deployment base and build-time Git revision.

```powershell
$env:GAPI_CODE_ONLY='0'
$env:GITHUB_ACTIONS='true'  # selects the Pages project base for a public build
$env:GAPI_RELEASE_MANIFEST='outputs/global/manifest.json'
npm run build
node scripts/build-provenance.mjs verify --publish
python outputs/publish-preview.py --preflight-only
```

`--publish` is a local read-only preflight, not deployment. It rejects changed
inputs, changed/missing/extra output files, a wrong base, local/code-only data,
uncommitted source or release bytes, and mismatched snapshots. A later commit
containing data/receipts can still use a valid build if its actual source and
selected aggregate bytes remain identical. An unrelated dirty evidence artifact
does not invalidate the build; only authoritative build/release inputs matter.

The publisher executes preflight before credential use or remote mutation. It then
captures site bytes and rechecks them against the preflight hash set, avoiding
publication of a different site if files change during capture. The existing
non-force remote ref update, immutable release tag and remote readback remain.
This is content-addressed provenance, not cryptographic signing or proof that an
attacker who controls the source and proof generator cannot forge both.

## CD boundary

The deployed project currently uses legacy GitHub Pages from `gh-pages`. The old
automatic Actions deployment path inherited code-only mode and conflicted with
that route; it is removed. CI produces verified artifacts and never changes Pages
configuration. The existing reviewed publisher remains the sole deployment path:
it validates a selected real observation build, makes a new prerelease, keeps old
tags, and reads back refs. Public HTTP asset hashes and browser acceptance are
separate post-deployment gates. Remote CI jobs and deployment must actually run
before their results can be described as passed.

Local regression fixtures use small temporary repositories under the task's
D-drive budget on Windows. They cover unchanged repeated verification, data-only
commits, source/asset/manifest/snapshot modification, dirty source at build time,
in-flight input changes, line-ending portability and disallowed build modes.
