# GAPI Methodology

## Current page: U.S. aggregate hiring sample preview

This method applies only to the local real-data view produced by `gapi ingest-us-faculty`. It does not describe the canonical person-event reference implementation, synthetic teaching fixtures, award scoring, or a global university ranking.

### Source and observation unit

The importer reads the `Academia/Academia` aggregate edge list at the pinned LarremoreLab repository commit recorded in the score snapshot. Rows aggregate faculty counts by employing institution and doctoral institution for a 2011–2020 U.S. employer sample. They are source-published aggregate counts, not GAPI-verified individual people, new hires, or unique-person events. The source research distinguishes current faculty rosters and hiring/retention dynamics; see its paper and repository documentation for the original definitions.

The two input CSV artifacts are pinned to expected SHA-256 digests. The local snapshot records each URL, revision, file size, observed hash, scope, and algorithm settings. The upstream data repository does not declare a reuse license, so generated real scores remain local-only pending rights review.

### Institution universe and edge direction

The PageRank node universe is the set of U.S. employing institutions listed in the source `Academia/Academia` institution statistics (368 in the inspected snapshot). An observed relation is represented as `employer Y -> doctoral origin X`, so influence from the hiring institution propagates toward the doctoral institution.

Edges whose doctoral origin is outside the 368-node employer universe are retained in source-level coverage counters but excluded from the PageRank matrix. The snapshot reports both excluded edge count and excluded aggregate faculty count globally and per employer. Source-recorded doctoral-origin and faculty counts include the source rows outside the ranking matrix; separate `ranking_network_*` values include only edges inside the PageRank universe. These counts must not be compared as if they shared the same denominator.

### Score calculation

For each employer `Y` and doctoral origin `X`, source counts are summed across duplicate rows in the selected all-academia taxonomy. The weighted adjacency is row-normalized by employer; PageRank uses `alpha=0.85`, uniform personalization over all employer nodes, L1 tolerance `1e-12`, and at most 200 iterations. Dangling mass is redistributed uniformly by the shared `pagerank` implementation. The reported score is a network centrality value within this sample, not a probability, causal effect, graduate placement rate, or measure of overall university quality.

`algorithm.edges` is the count of unique nonzero source institution-pair rows after duplicate pair aggregation. `algorithm.within_ranked_network_edges` counts unique pairs whose employer and doctoral origin are both in the ranking universe. `algorithm.external_doctoral_origin_edges` counts pairs from in-universe employers to origins outside it. The associated `external_doctoral_origin_count` sums source aggregate counts over those external pairs.

## Separate reference implementation

`src/gapi/ranking.py` also implements a configurable canonical person-event reference model with education stages, evidence references, eligibility, awards, composite scores, and uncertainty-related contracts. Synthetic files under `data/demo/` exercise that model in automated tests. Neither those rules nor their synthetic values contribute to the current U.S. aggregate score page.

## Limits and next validation

- The employer universe and hiring observations are U.S.-bounded and cover 2011–2020.
- Only doctorate-origin relationships are present in this source view; bachelor, master, postdoctoral, award, and complete career-path scores are not available here.
- The aggregate input prevents person-level duplicate review and independent verification of individual degree, job title, dates, or evidence URLs.
- One research dataset is not independent multi-source corroboration; its source structure and coverage may shape the score.
- Sensitivity analysis, OOD subgroup validation, uncertainty intervals, and independent third-party review are outstanding.
- The data repository's reuse rights are unresolved; do not publish or redistribute the derived snapshot without documented permission.

The separate reference-model defaults and their assumptions are documented in `configs/default.json` and its tests. They must not be presented as validated parameters for this aggregated U.S. sample.
# Global observation contract (2026-10-29)

The global streaming view is owned by `global_analysis.py` (schema
`gapi-global-observed/v1`); the earlier reference/demo and US-study contracts below
remain distinct. All qualifying edge endpoints define its network, regardless of
formal eligibility. Only valid in-universe edges enter each PageRank row denominator;
zero edges are dangling. Shared PageRank rejects negative/nonfinite weights.
The global view retains missing awards/composites, observed rank versus unavailable
formal rank, and conditional alpha/stage/source/region/time scenarios. Intensity uses
observed training people as its explicit denominator, never all graduates or jobseekers.
Independent official-profile corroboration and full OOD review are pending. An observed
source statement is not a confirmed personal fact. See [STREAMING.md](STREAMING.md).
