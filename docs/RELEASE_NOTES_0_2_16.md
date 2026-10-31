# GAPI 0.2.16 — identical real scores, smaller repository storage

This maintenance release retains the exact 0.2.15 public score bytes and their
SHA-256: 144,855 source events, 41,668 observed people and 1,735 scored institutions.
The repository aggregate is now deterministic gzip, reducing its working copy
from 21,726,360 to 888,997 bytes. The manifest binds packed and decoded hashes and
sizes. Production builds verify both identities and emit the same JSON URL and
bytes. Committed packed-object verification, bounded decompression, path checks
and legacy uncompressed input support remain part of publication preflight.

357 Python tests and 42 frontend tests passed. After migrating the real input,
the full 357-test Python suite passed again. Production-build comparison with
0.2.15 found identical score bytes, JavaScript, CSS and fonts; only the data
manifest and streaming documentation changed. Standalone graph arithmetic replay
also passed. Browser and online settlement are recorded separately after actual
execution. A first local browser attempt used a preview server with the wrong
base path and timed out; the preview was restarted with the production base and
the failed attempt was retained.

This change frees local working capacity without deleting private evidence,
historical recovery inputs or source facts. It does not remove decoded space
needed during a static build. Acquisition remains separate from build tests,
which temporarily consume task storage. Worldwide coverage remains unknown and
external independent dual-track review remains pending. No scoring policy,
source qualification, typography or layout change is introduced.
