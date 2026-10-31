# 0.2.11 — Bounded XML transport compression

ORCID affiliation requests now negotiate HTTP gzip on the existing public XML
routes. The parser still receives bounded chunks of the exact decoded XML. Source
hashes describe decoded XML bytes, as before; no raw responses, archives or XML
trees are saved. Servers returning identity encoding remain supported. JSON,
archive checksums, byte-range continuation, request quotas, identity redirects and
career classification retain their existing owners and contracts.

Both wire and decoded sizes are limited to the configured member bound, each
decoded read is at most 64 KiB, and CRC/trailer validation must finish before a
member commits. Truncated, corrupt, concatenated or oversized gzip streams fail
the existing member transaction. A test corrupting the second affiliation
response verifies that neither the first section's events nor evidence parts are
committed. No retry silently stitches together a partial response.

A live comparison alternated identity/gzip request order for four already
committed candidates. All extracted fields agreed. Three records had empty
affiliation sections and were returned uncompressed. The fourth contained eight
affiliations: 20,181 decoded bytes required 2,679 wire bytes with compression.
This small, nonrandom sample supports compatibility and reduced bytes for that
response, not a population-wide throughput estimate. An initial comparison on a
pending identity stopped at an official redirect; it was not followed or merged,
and its incomplete diagnostic remains preserved. The benchmark explicitly
supports pending or committed selection and writes aggregate measurements only.

The website continues to select the preceding frozen observation release. This
maintenance release does not claim a newer published ranking, complete world
coverage or independent external red-team approval. The existing typography and
visual design are unchanged. The single-writer collector continues within the
2 GiB storage budget, 256 MiB reserve and 512 MiB process budget.
