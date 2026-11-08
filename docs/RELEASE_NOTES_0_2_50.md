Recovered 131 additional API records for annual archive members that exceeded
the original parser limits. The release adds 583 events and 32 qualifying
person-level relations to v0.2.49. Across the parser-limit follow-up, 136 of 138
members now have alternate API evidence; two field-limit failures remain.
Original annual rejection records and source errors are preserved.

The complete aggregate contains 13,929,182 people with affiliation events,
27,130,037 events, 539,379 directed aggregate edges and 255,861 institution
identifiers. Successful API records without affiliation events contribute to
member counts and contribute zero people to event coverage. This corrects the
one-person overcount documented in the v0.2.48 and v0.2.49 release notices.

Incremental export preserves the original entity, label-selection, edge and
ranking rules, including new institution identifiers and labels supported by
earlier event identities. The full output was independently reconstructed and
its graph scores reproduced. Compressed assets are generated in bounded memory
and published as immutable Git objects, avoiding another complete local copy.
No annual source download, XML replay or base-graph rebuild was performed.

The API selection contains 33,939 candidates, of which 137 have been fetched
successfully. The annual archive still records 29,992,306 parsed members and
1,159,959 rejected members; alternate API recovery does not rewrite those
historical outcomes. Of all events, 5,467,198 are classified as observed and
21,662,839 remain unresolved. Global coverage is unknown and independent
official profile confirmations remain zero. These are institution observation
scores; formal ranking qualification remains unmet.

See the version-bound [data transport](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.50-observations/docs/DATA_TRANSPORT.md)
and [methodology](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.50-observations/docs/METHODOLOGY.md).
