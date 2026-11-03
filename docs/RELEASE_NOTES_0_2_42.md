Acquisition status now separates processing of discovered candidates from annual archive integrity. The new `candidate_processing` section reports a selection hash, pending counts and a processing fraction for each discovered queue. The new `source_integrity` section includes annual archives without a run and checks that a verified run matches the configured source identity. Neither section claims complete world coverage.

The existing `acquisition_progress` fields remain compatible with frozen snapshots. Its legacy `enumeration_complete` flag describes the discovered selection, not full source enumeration. New consumers can use the explicit processing and archive integrity fields.

Validation: 461 Python tests passed locally, including completed-queue/incomplete-archive, failed and missing archive runs, real parser verification, changed source identity and report snapshot compatibility. The test log includes ResourceWarning messages about unclosed fixture connections; those warnings are retained with the local evidence.

This is a code update. Published scores retain the v0.2.38 frozen input of 759,416 events. Live acquisition is separate. Complete source enumeration, full-archive capacity and independent third-party review remain unfinished.
