# GAPI Methodology v0.1

## Measurement target

GAPI measures observed links in which a person received education or research training at institution X and later held a qualifying role at institution Y. It does not identify the causal effect of education, teaching quality, employment probability, or the complete global mobility network.

## Event and edge construction

The canonical input stores training and employment as separate events connected by an internal person identifier. The ranking layer emits a directed edge `Y -> X`, so influence from the hiring institution flows to the training institution. Events are deduplicated by person, training stage, training institution, and employer. Duplicate source records should be consolidated before scoring.

The reference implementation counts one event per person/stage/training institution/employer tuple. A role beginning before a known training completion year is excluded. Administrative role categories are excluded; the configured qualifying types are faculty, researcher, research scientist, and postdoc. Postdoctoral training is represented as its own `postdoc` stage; it is not treated as a degree. Joint appointments are included only with an explicit fraction.

## Network score

For employer `y`, training institution `x`, and stage `s`, the weighted edge is:

`W[y,x] = sum(stage_weight[s] * event_fraction)`

Rows are normalized by outgoing edge weight. PageRank uses a uniform personalization vector over the institution directory, damping `alpha = 0.85`, L1 convergence tolerance `1e-12`, and a maximum of 200 iterations. Dangling-node mass is redistributed uniformly. The reported network score sums to 1 across all nodes, including nodes that do not qualify for the displayed ranking. Qualification does not alter the underlying graph calculation.

Default stage weights are bachelor 0.20, master 0.35, doctorate 1.00, and postdoc 0.60. These are declared policy defaults, not empirically established universal values.

## Awards and composite score

Awards are separately counted by explicit institutional attribution. The reference categories are personal (1.0), institutional (0.5), and technical history (0.25). An award contributes to the main award score only when the record explicitly names an awarded or producing institution. It is never propagated through the talent-flow graph. Duplicate award identity/year/recipient records count once.

Network and award scores are converted to empirical percentiles over eligible institutions. The composite score is `0.80 * network_percentile + 0.20 * award_percentile`. This is a presentation choice and should be tested against alternative weights before any real ranking release.

## Eligibility

Production defaults require at least 10 observed events, two education source classes, a traceability ratio of at least 0.80, and an institution with a stable identifier or explicit confirmation. The current implementation verifies event references, source URLs, hashes, and an approved or synthetic-demo license state. It does not yet implement a real-world entity confirmation workflow. Demo thresholds are relaxed only inside the synthetic fixture generator.

Institutions outside eligibility remain visible in the directory. Ranking scores are withheld from ineligible rows to avoid making missing coverage look like low measured influence.

## Reproducibility

Inputs and config are JSON. Output ordering is deterministic. The CLI can write a release manifest containing input labels and SHA-256 hashes of the result and release envelope. For a formal release, the manifest must identify immutable input artifact hashes and the exact source revision; the current CLI input label is not a substitute for that release process.

## Current implementation limits

- Network volume, intensity, coverage-aware uncertainty, sensitivity sweeps, and bootstrap intervals are not yet implemented as separate published measures.
- Geography, discipline, institution-size adjustment, and source-language perturbation are not currently applied to scores.
- No verified real-world event dataset or award catalog is bundled.
- The web view filters the stored edge stage breakdown but currently uses a small synthetic graph.
- The current traceability field validates event-to-evidence linkage; it does not prove the source's factual correctness.
