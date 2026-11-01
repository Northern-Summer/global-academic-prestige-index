This observation release refreshes real institution aggregates and makes the
online institution identifier bridge usable with the completed historical
source directory. The manifest remains the authority for exact data counts,
input identity, configuration and output checksums.

The bridge accepts a bounded public aggregate manifest as an optional candidate
source. It verifies node asset hashes, sizes, schema and content-addressed paths,
including the repository's gzip representation without writing decoded JSON.
Historical identifiers only nominate candidates: identity promotion still needs
revision-pinned Wikidata identifiers and reciprocal agreement from the frozen
ROR directory. A confirmed institution identity does not corroborate a person's
career. Existing scores change only after an explicit ranking/export run.

The acquisition ledger now increments its source member counter in the same
transaction as a genuinely new member, instead of counting the entire source
after each person. Resume reconciles it once against the ledger. Duplicate
imports, rollback, mismatched content and source isolation are regression-tested.
An in-memory 100,000-member benchmark measured a 99.77% reduction in the cost of
that counter operation; it does not establish network or disk throughput gains.

All 436 Python regression tests passed locally. The production browser suite
and deployment must also pass before this prerelease can be described as live.
The configured data ceiling remains 4 GiB after the previously documented
lossless-storage work, with the existing 256 MiB reserve, 512 MiB process limit,
single writer and prohibition on saving raw source archives.

No individual trajectories are published. Fonts and layout are unchanged.
Awards or composite scores remain missing when their required evidence is
missing. Coverage remains incomplete; independent external review is pending.
This is an observation prerelease, not a completed global personnel census or
an independently approved formal ranking.
