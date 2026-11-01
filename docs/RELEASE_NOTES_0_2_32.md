Private row differences now align unchanged records by verified primary keys.
The former 32-row lookahead could encode unchanged records again after a long
run of insertions. The optimized path requires matching archive schemas and
ordinary ascending keys; unsupported orderings retain the existing fallback.
The reconstruction format and ranking rules are unchanged.

Two retained real inputs were repacked with complete reconstruction checks:

- 11,016,836 to 6,043,392 bytes; logical input SHA-256 unchanged.
- 12,154,192 to 7,582,920 bytes; logical input SHA-256 unchanged.
- Actual reduction: 9,544,716 bytes. This is file space, not reusable database
  pages, and does not solve the overall long-term storage limit.
- All 429 Python tests passed, including insertion runs, composite and binary
  keys, unsupported collations, historical reconstruction and repeated repacks.

Freezing the newer input still exceeded the original 2 GiB budget after these
lossless reductions. Under the user's conditional authorization, the configured
ceiling is now 4 GiB. The source registry was migrated by changing only the
budget field under the existing writer lock; its predecessor was preserved.
The 256 MiB write reserve, 512 MiB process limit, single writer and raw-file
prohibition remain in force. Raising the ceiling does not preallocate it.
All 430 Python regression tests passed after the resource-contract change.

Production browser validation exposed a test-fixture size regression as the
directory grew: the compact transport is 15,014,575 bytes, but expanding its
defaults produced a 35,840,439-byte attack fixture, exceeding the unchanged
32 MiB ingress limit. Attack tests now mutate the actual compact transport and
assert that the fixture fits the ingress limit before testing number rejection
and label escaping. All 50 frontend unit tests passed. The failed browser run
blocked deployment; production browser validation must pass before release.

This version refreshes institution aggregates from a newly frozen real input.
Its committed manifest identifies the exact counts, scope and checksums.
Candidate projections, canonical events, evidence and frozen inputs have
different purposes and are reported separately; institution counts are not
person counts or a denominator for acquisition completeness.

No individual career trajectories are published. Fonts and page layout are
unchanged. World coverage is incomplete, and independent external review is
pending. This remains an observation prerelease, not a completed global census.
