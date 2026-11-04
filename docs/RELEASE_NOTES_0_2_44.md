Task storage admission now counts the complete checkout, including dependencies
and Git history, alongside the shared acquisition tree. Optional external
delivery directories are part of the frozen configuration and must match across
nested source tasks. Overlapping roots count once. Strict storage paths reject
links and Windows reparse points in their ancestors before overlap removal.

The existing 32 GB total ceiling, 256 MiB reserve and 512 MiB process limit are
unchanged. A running collector retains its loaded code and configuration;
additional delivery roots must be registered at a settled writer boundary.
File sizes are logical measurements, not atomic physical-allocation snapshots.

Reviewed code publication uses one shared helper with an explicit path set,
public parent, HTTPS repository and branch. Its receipt binds the target and
content before push and reconciles uncertain responses by remote ref readback.
It preserves unrelated local changes and refuses force publication. It requires
one integrator; it does not provide distributed exclusion.

Validation: 560 Python tests passed locally in 149.221 seconds, followed by 27
focused resource and monitoring tests. A real Windows junction regression failed
in both alias cases before the fix and passed afterward; a normal duplicate
directory still counts once. The suite also exercises isolated Git publication,
response-loss retry, target drift and unrelated private ancestors. Fixture
ResourceWarnings and interrupted earlier test runs remain in local evidence.

This code release retains the published v0.2.38 observation snapshot and scores.
It does not claim complete 2026 archive verification, complete candidate
processing, independent qualification of personal facts, global coverage or final
personal-database retirement. The current collector is not interrupted to load
these changes. Details: [storage and resume rules](STREAMING.md),
[code publication contract](PUBLISHING.md).
