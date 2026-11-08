The affiliation parser now buffers only fields used by normalized events.
Unused department, city and credit-name text no longer rejects an otherwise
processable record. Retained text remains bounded by the existing event budget,
and the completed canonical event must fit 16 KiB. Long role titles are retained
without truncation. The remaining two parser-limit records were recovered with
four API requests; repeated execution made no requests and added no records.
All 138 annual parser-limit members now have alternate API evidence. The
original annual rejection ledger remains unchanged.

Browser prefetch now frees a network slot as soon as a request completes, even
when an earlier response is slow. Completed values still reserve queued bytes.
The maximum remains six active requests and 8 MiB of queued decoded input;
canonical order, compressed and decoded hashes, cancellation and rejection of
partial results are unchanged. The previous direct public-site run exhausted
its 600-second deadline. The scheduling regression reproduces stalled early
responses and verifies output equivalence and resource bounds; it is not an
end-to-end public-network speed guarantee.

The new aggregate adds nine unresolved events and two people with affiliation
events: 13,929,184 people, 27,130,046 events and 21,662,848 unresolved events.
There are no new qualifying talent relations: the 539,379 aggregate edges,
780,421 person-level relations and 67,087 scored institutions are unchanged.
The narrow append path validates committed new people, event status, member
counts and label evidence before reusing graph and sensitivity results. The
complete export still passes independent graph replay and publication checks.

The API selection contains 33,939 candidates, with 139 successful API records.
Another 13 have normal verified annual records; 33,787 correspond to retained
annual error documents. Those outcomes are not API successes. The archive
continues to record 29,992,306 parsed and 1,159,959 rejected members. Global
coverage is unknown and independent official profile confirmations remain zero.
These are observation scores, not formal university rankings.

See the version-bound [data transport](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.51-observations/docs/DATA_TRANSPORT.md)
and [methodology](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.51-observations/docs/METHODOLOGY.md).
