The annual observation network now includes the fully processed ORCID 2026
archive and five additional API records recovered after archive member-size
limits. The institution aggregate contains 13,929,065 people, 27,129,454 events,
780,389 deduplicated person-level relations, 539,357 directed aggregate edges,
255,860 institution identifiers and 67,086 scored institutions.

The release uses bounded, content-addressed gzip partitions. Compressed and
decoded hashes bind every asset; the independent verifier reconstructs the
original logical input and reproduces the graph scores and stored ordering.
The full aggregate has eleven parameter/source sensitivity scenarios. The
independent replay converged in 110 iterations with maximum score error
1.3010426069826053e-17 and probability mass 1.000000000000001.

The annual source contains 31,152,265 members: 29,992,306 parsed successfully and
1,159,959 were rejected. Of the rejected members, 1,159,821 contain XML error
documents, 136 exceed the member-size limit and two exceed field limits. Failed
inputs remain failures. The frozen API candidate prefix has six successful
records out of 33,801; annual reuse and retained failures do not count as API
success. No complete global personnel denominator has been established.

There are 5,466,990 observed events and 21,662,464 unresolved events. Missing
fields remain missing. Independent official-profile confirmations remain zero;
the observation scores are not formal university rankings, employment rates or
causal estimates. The separate historical 2023 observation bundle retains its
own source and denominator.

Only institution aggregates are public under the existing noncommercial
research publication policy. Individual histories, private incremental inputs,
the source database and local recovery checkpoints are excluded.

Recovery now preserves completed aggregates, processes only explicitly frozen
new people, and retains the preceding generation. The optional native index
observer passes an explicit Python bytes object across its callback boundary;
compressed index storage retains the exact original index identity.

See the version-bound [data transport](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.48-observations/docs/DATA_TRANSPORT.md),
[methodology](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.48-observations/docs/METHODOLOGY.md)
and [source policy](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.48-observations/configs/publication-20261029.json).
