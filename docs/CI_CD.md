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

The production browser job is required for trusted source pushes before deployment;
it is also available as an optional `workflow_dispatch` check. It builds the same selection and runs
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

## Automatic CD and fallback

The user explicitly authorized automatic deployment on 2026-10-29. Pages retains
the existing legacy `gh-pages` mode. A push to `codex/global-streaming`, after the
test, real-observation build and production-browser jobs succeed, downloads that exact build artifact.
The deploy job alone receives `contents:write`; pull requests and manual browser
runs cannot deploy. `deploy-pages.mjs` uses the same `verifyBuild` policy and site
manifest as the local publisher, captures and rechecks all bytes, confirms the
source revision is current, and performs a non-force sibling commit/push. Source
freshness checks before preparation and push are best effort across distinct refs;
non-force push protects the Pages predecessor, not an atomic source+site snapshot.
Workflow concurrency cancels superseded same-branch runs. The script does
not rebuild, fetch personal sources, change Pages configuration or retarget tags.

An ambiguous push is reconciled by the exact commit, never blindly repeated.
Because a `GITHUB_TOKEN` push does not implicitly trigger Pages, the deploy job
has `pages:write` and explicitly requests the existing legacy build using the
[official Pages builds endpoint](https://docs.github.com/en/rest/pages/pages#request-a-github-pages-build).
It first checks the existing configuration and latest build; an ambiguous POST
is reconciled without a second automatic POST. It never changes the site mode. A
bounded Pages propagation check then verifies every public file hash. Its receipt
is uploaded even on failure. A changed source/asset, oversized or private artifact,
wrong repository/ref/event, stale source, or stale public asset fails the job.
This is automatic observation deployment, never formal-ranking approval. The
existing local publisher is an explicitly run fallback and prerelease creator,
sharing the same proof and public-manifest owner. Neither route bypasses failed
data/source gates. A successful source push alone is not successful deployment.

Local regression fixtures use small temporary repositories under the task's
D-drive budget on Windows. They cover unchanged repeated verification, data-only
commits, source/asset/manifest/snapshot modification, dirty source at build time,
in-flight input changes, line-ending portability and disallowed build modes.
