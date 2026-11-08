# Publishing reviewed code

`scripts/publish-code.py:publish_code` owns the Git object and retry boundary for
an explicit reviewed code path set. It starts from the verified public branch
commit rather than local HEAD, so private local ancestors and unrelated files do
not become public. It uses the existing GitHub HTTPS origin and normal Git/GCM
credential interface; the caller supplies the exact expected repository. Both
the fetch URL and the sole push URL must identify that repository over HTTPS.
Multiple push URLs are refused before any external write.

Before calling it, review the paths, run their applicable tests, confirm the
public base and obtain the publication authority required for that operation.
A path list is an input, not a privacy audit or permission grant. Keep personal
inputs and restricted data outside the reviewed set. Acquisition data permissions
remain owned by `gapi.publication` and the frozen publication policy.

Call `publish_code(root, repository=..., base=..., branch=..., paths=...,
receipt=..., message=...)` from the existing task operation. The receipt must be
inside that checkout and unique to the intended operation. Reuse it after a
transport failure. A prepared receipt fixes the commit before push; an accepted
push with a lost response is reconciled by reading the remote ref. Repeating a
settled operation returns the same commit without another push or receipt write.
The receipt binds both repository and full branch ref. Missing legacy target
fields fail closed and need explicit binding from the original operation; retry
arguments cannot supply defaults. Changed paths, bytes, public base or target
require reconciliation. Unrelated
remote changes are preserved; the publisher never force-pushes.

Do not run two integrators for the same operation. The helper preserves the
existing single-integrator contract; it does not provide a distributed lock or
an atomic remote compare-and-swap API. Git non-force checks and remote readback
remain mandatory. Releases, tags, CI, Pages deployment and public data/hash
readback use their existing settlement steps after this code operation.

For already verified aggregate assets held in memory, `external_bytes` accepts
only content-addressed public gzip paths, with at most 24 MiB retained bytes.
This route requires `prepare_only=True`: it prepares the exact reviewed tree
without writing duplicate local asset blobs or attempting a Git push. The
existing immutable-object transport must settle those blob identities before
publishing the tree, commit and non-force branch update. Preserve the prepared
receipt and confirmed object identities on interruption; inspect remote object
and ref identities before retrying an uncertain write. Private acquisition
records and unqualified aggregates remain outside this interface.

Historical task wrappers retain their original parameters and receipts. New
code operations reuse this owner rather than copying its implementation. Run
`python -B -m unittest discover -s tests -p test_code_publication.py` for the Git
object/transport regressions. These use real temporary Git trees and an explicit
simulated remote, without production credentials, uploads or personal data.
