Productive partition loading now uses a 60-second idle deadline and a 20-minute
absolute page deadline. Only another fully verified partition renews the idle
deadline. A stalled load, external cancellation, or final settlement cancels
outstanding requests and clears the timer. Hash checks, canonical consumption,
six-request concurrency and the 8 MiB decoded queue bound are unchanged.

This corrects a public-browser run that reached at least 120 of 131 partitions
before the former fixed 600-second deadline discarded the unfinished result.
The change does not claim faster network transfer or completed public-browser
acceptance. No source records were fetched again and the published dataset is
unchanged from v0.2.51: 13,929,184 people with affiliation events and 27,130,046
events, including 21,662,848 unresolved events. Global coverage remains unknown;
these are observation scores, not formal university rankings.

See the version-bound [data transport](https://github.com/Northern-Summer/global-academic-prestige-index/blob/v0.2.52-observations/docs/DATA_TRANSPORT.md).
