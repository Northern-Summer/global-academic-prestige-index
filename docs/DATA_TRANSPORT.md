# Public institution transport

The logical dataset remains `gapi-global-observed/v1`. The optional physical
representation `gapi-institution-defaults/1` removes repeated institution
fields without changing any observation, score, reason or missing value.

The envelope has exactly four keys:

```json
{
  "schema_version": "gapi-institution-defaults/1",
  "payload": {"schema_version": "gapi-global-observed/v1"},
  "node_defaults": {"formal_rank": null},
  "node_rows": [{"institution_id": "example:A", "name": "Example"}]
}
```

`payload` contains all original top-level fields except `nodes`. Each node is
reconstructed by copying `node_defaults` and overriding it with the corresponding
`node_rows` object. A value is omitted only if the same field exists in every
original node and equals the explicit default, including its JSON type. Absent
fields stay absent; null, false and zero remain distinct. Institution identity
and name are always explicit. These are encoding defaults, not statistical
imputation or assumptions about missing evidence.

The writer verifies canonical byte equality after reconstruction before accepting
the encoding. It retains the legacy format when the envelope saves no bytes.
The manifest's `output_sha256` identifies the uncompressed transport bytes;
`repository_snapshot.sha256` identifies the retained gzip bytes.
`logical_output_sha256`, when present, identifies reconstructed logical JSON
using sorted keys, ASCII escaping, compact separators and no non-finite numbers.
All earlier logical snapshots remain readable.

Readers reject extra envelope keys, a nested `nodes` payload, identity defaults,
prototype-related keys, over 65,536 nodes, over 64 fields per reconstructed node,
and defaults exceeding the 65,536-byte bound. Existing browser byte limits apply
before expansion; the full original schema, score and duplicate-identity checks
apply after expansion. The directory cap bounds row expansion. These guards do
not claim that every possible decoded JavaScript object consumes the same memory
as its JSON representation.

The Python writer/reader, JavaScript reader and standalone public verifier share
a protocol fixture and are tested for exact reconstruction, invalid input and
independent graph-to-score replay. The standalone verifier imports no GAPI
ranking implementation. Its verification concerns arithmetic and byte identity;
it does not establish the truth of career claims or independent audit status.

On the frozen v0.2.28 dataset, 44,821 nodes account for 30,637,716 of 31,871,202
logical bytes. Repeated per-field reason objects account for 7,601,519 bytes
(field totals include their own object overhead and are not additive).
The measured transport is 13,424,433 bytes and its gzip is 1,046,105 bytes.
The original logical SHA-256 remains
`71b392d3e7916ed0273c0d0cacd58561d963123af4ecfdf5d0aaa31e042e9d5a`.
These are measurements of one frozen public dataset, not future capacity claims.
