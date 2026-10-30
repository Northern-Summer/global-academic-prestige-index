# ORCID redirect recovery — 2026-10-30

## Observed problem and scoped correction

The preceding real observation release was deployed from
`9fc6ff6b1e5130e4b95282b62c5116530b0ba974`. All four Actions jobs passed in
run `40995592914`. Independent public readback matched 22 served-file hashes;
the production browser loaded the 17,207-event slice in 3,914 ms and passed
the interaction, version, privacy, progress, mobile and fallback checks without
page errors. Fresh public desktop/mobile hero screenshots were observed. See
`deployment-expanded-20261030.json` and `browser-expanded-public-20261030.json`.

A bounded diagnosis of due ORCID errors found two requests that automatically
redirected to different ORCID identifiers, two HTTP 409 responses with error
code 9018, and one HTTP 404 with error code 9016 and a not-found message.
The sixth candidate's later retry deadline was respected. Error bodies and XML
remained in memory; only hashes and normalized diagnostics were retained in the
external task directory. No interpretation of code 9018 beyond the actual
HTTP conflict is asserted.

The shared HTTP boundary now stops ORCID redirects and closes the response before
issuing another request. The ingestion owner accepts only same-host, same-section
permanent 301/308 redirects between checksum-valid identifiers as **candidate
discovery**. It saves URL/status/time/metadata-hash evidence in the existing
discovery ledger and enqueues a separate target. It never merges person keys,
copies old facts onto a new identity, counts the old record as retrieved, or
silently consumes an extra redirected request outside quota accounting.

## Actual recovery and verification

The real queued-only collection exited normally. It recorded two permanent
redirects, independently fetched both destination affiliation pairs and committed
two additional members. Neither destination had education/employment events, so
career-event and score counts did not increase. This is a successful acquisition
of empty public affiliation sections, not new talent-flow evidence. No new broad
discovery pages or raw archives were enabled.

The ORCID candidate set is now 1,293, with 1,287 committed and six originals still
pending. The expanded denominator includes the two new destination candidates;
this is not a claim that two of the six original failures disappeared. Wikidata's
3,865 candidates remain fully processed. The four classes of original error
remain visible: two verified redirect routes with unresolved original identity,
two HTTP conflicts, one not-found response and one previously observed path
mismatch whose backoff has not elapsed. World coverage remains unknown.

The focused suite passed 14 tests, including first-response closure, no automatic
target request, quota separation, temporary/unsafe/wrong-section redirect refusal,
idempotent evidence, partial original transaction rollback, independent target
validation, and unchanged original progress. The full Python suite passed **178**
tests (`python-redirect-recovery-20261030.log`). Live task storage after collection
was 292,306,617 bytes, below the authorized budget; raw files were not saved.

No frontend typography, scoring policy, formal ranking qualification, source
permissions, external accounts or services were changed. Frozen predecessor
inputs and error evidence remain available. Independent third-party dual-track
review is still pending. No formal global ranking or new release tag is issued.

## Frozen-input acceptance after recovery

- Input/code owner: `35a02db0b0efb8270b647962ba3b062524d5270b`.
- Private normalized input: `4461b7b61b49d46f2d6affb527fe8c508441241f1a03b5c27470d473535677f1.sqlite`,
  37,642,240 bytes, outside Git. Its filename hash was checked against file bytes.
- Public snapshot: `global-5dfe5e90a13b4242.json`, SHA-256
  `5dfe5e90a13b42428434ea50ef45ca924f55d3789ca7c993bce0d649f723493c`.
- Initial export and frozen replay receipts are byte-identical, SHA-256
  `d6434865a6737ce6c60a1306693eb255a578411c688e4a02f889f01020c0c073`.
- Every institution node and edge is exactly equal to the preceding expanded
  slice. Separately implemented graph replay again passed with 366 endpoints,
  303 edges, 21 iterations, mass within floating-point rounding of one, and
  maximum score difference `3.469446951953614e-18`.
- Frontend regression: 30/30 passed. SQLite integrity, member evidence, archive
  exclusion, zero raw files, budget and candidate counters passed the executable
  acceptance check. See `redirect-acceptance-20261030.json` and
  `public-replay-redirect-recovery-20261030.json`.

This section is local acceptance. The subsequent source commit and deployment
receipt identify the final public revision; transport alone is not settlement.
