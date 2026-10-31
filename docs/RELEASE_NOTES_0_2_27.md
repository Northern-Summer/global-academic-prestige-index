# GAPI 0.2.27 — lossless storage maintenance

This code release fixes repeated storage of private frozen ranking inputs.
The published observation data remains the v0.2.26 snapshot: 263,275 events,
75,661 internally deduplicated people, 43,383 directory institutions and 2,215
institutions with observed network scores. Local acquisition has continued;
unfrozen later batches are not represented as newly published scores.

A full retained input previously compressed each event, encoded the compressed
bytes as text, then compressed the complete stream. The new physical envelope
exposes repeated event fields to outer compression and reconstructs every
original logical row, including its original compressed-payload digest.
The actual 233,370,402-byte logical input shrank from 122,376,756 to 35,754,116
stored bytes, saving 86,622,640 bytes. The complete input SHA-256 is unchanged.
An in-memory full-file preview predicted the exact accepted candidate size.

Four additional retained delta inputs were rebased against verified existing
inputs, saving 38,286,888 bytes. Their logical identities remain unchanged.
The full replacement was staged only after those savings supplied sufficient
space beside its predecessor. The 2 GiB total limit and 256 MiB reserve were
not relaxed. No source XML, raw archive or individual trajectory was published.

Readers retain legacy full/delta compatibility. Full packing rejects corrupt
headers, invalid records, missing footers, truncation, filename mismatch and
incorrect reconstructed identities. Replacement also checks source-storage
identity, candidate size and an optional measured decode-time limit. A failed
candidate keeps its predecessor; cleanup touches only an exclusively created
partial. Repeated successful maintenance verifies the stored input and returns
without rewriting it. Existing bounded delta dependency and cycle rules apply.

The actual full sequential decode took 9.610 seconds versus one preceding
5.515-second trial, within the predeclared 18.7875-second acceptance bound.
This trades frozen-input reading time for space; it does not claim a faster
frontend or collector. Cross-zlib-version byte reconstruction has not been
established by these same-runtime measurements. Three representative real
dependency branches, including the published v26 private input, reconstructed
with their original hashes. The public aggregate remained byte-identical.

All 412 Python regression tests and all 47 frontend tests passed. Source
interpretation, evidence eligibility, ranking arithmetic, fonts and layout are
unchanged. Global enumeration remains incomplete. External independent research
and software reviews remain pending; this release is not a formal global ranking.
