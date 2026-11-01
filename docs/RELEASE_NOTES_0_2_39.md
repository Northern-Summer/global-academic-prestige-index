Collection can now try the existing explicit-degree-and-employer selection after an academic-occupation query reaches its time limit. The two selections retain separate cursors and share the deduplicated candidate queue, source backoff and single request stream. A timeout does not mark either selection complete.

A bounded event-block codec is included with integrity, size-limit and process-exit recovery tests. It is used by the storage experiment; live database storage has not switched to blocks. Production migration and frozen-version compatibility remain unverified.

This is a code update. Published scores retain the v0.2.38 frozen input: 759,416 events, including 672,382 unresolved events. Collection continues separately. These counts are not verified careers or a complete world census. Public data contains institution aggregates; award and composite scores remain missing where evidence is insufficient.

Validation: 451 Python tests passed locally. Independent external review is still pending. Ranking rules, eligibility thresholds and page typography are unchanged.
