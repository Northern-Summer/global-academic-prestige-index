# GAPI 0.2.23 — bounded storage readers and source dictionaries

This is a software maintenance prerelease. The published real-observation
dataset remains the frozen v0.2.22 input: 198,183 events and 58,140 people.
Live collection continues separately; this release does not claim a completed
global census or independent third-party approval.

The private event writer can now reuse small source-scoped compression seeds
through the existing verified dictionary codec. Complete normalized evidence
is retained, with a maximum of 16 seeds of at most 8 KiB each per writer.
Dictionaries and events commit atomically. Readers continue accepting older
payloads; ranking snapshots flatten dictionary dependencies into their existing
self-contained encoding. Source versions, rules and frozen acquisition
configuration are unchanged. Seeds are cleared when the writer closes.

`python -m gapi repack-event-pages --share-sources --max-events 20000`
performs a bounded, resumable migration under the existing exclusive writer
lock. Keep the returned `after_rowid` for subsequent calls. No raw source body,
second database or VACUUM is needed. Reusable SQLite pages are not filesystem
bytes reclaimed. A read-only experiment on 20,000 retained events reduced
payload bytes from 11,315,789 to 8,459,536, with less than 1 KiB of compressed
seed overhead. This is a bounded sample, not a global capacity forecast.

The historical block reader now uses both an 8 MiB accounted cache budget and
a 128-entry cap. Keys retain actual encoded bytes so changed or missing stored
blocks cannot be masked by cached values. On the same 10,000-record workload,
fixed-seed shuffled reads improved from 2.031 s to 0.328 s with matching decoded
hashes and about 34 MB sampled process memory. This does not establish parity
with legacy single-row reads or whole-table performance.

Validation: 386 Python tests passed, including legacy/new codec roundtrips,
rollback, bounds, missing/corrupt dependencies, source-seed lifetime, replay,
score preservation and idempotent migration. Frontend code, fonts and ranking
policy are unchanged. Hosted build/browser/deployment checks must settle before
the release is marked deployed. Existing data-license restrictions and pending
external review continue to apply.
