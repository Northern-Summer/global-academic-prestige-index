Annual observation partitions now use bounded concurrent prefetch while retaining
the original field and row order. The previous sequential reader exceeded its
120-second loading deadline on a direct public-site check, despite passing the
hosted local browser tests. A failed or cancelled request aborts the owned
prefetch window; no partial dataset is accepted.

The reader keeps the same compressed and decoded hash checks, HTTP gzip handling,
publication rules, scores and missing values. Queued decoded input is bounded;
large scalar partitions run alone. Tests compare sequential and concurrent
results, exercise real HTTP gzip decoding and reject corrupt inputs and invalid
concurrency settings.

The frozen data is unchanged from v0.2.48: 13,929,065 people, 27,129,454 events,
539,357 directed aggregate edges and 255,860 institution identifiers. The annual
archive has 29,992,306 successfully parsed members and 1,159,959 rejected members.
The separate API prefix remains six successful records out of 33,801. Global
coverage is unknown, 21,662,464 events remain unresolved and independent official
profile confirmations remain zero. These are observation scores, not formal
university rankings.

See the version-bound [data transport](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.49-observations/docs/DATA_TRANSPORT.md)
and [methodology](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.49-observations/docs/METHODOLOGY.md).
