Reduced repeated institution fields in the public data file. All 44,821 directory
entries, 2,286 scored institutions, scores and explanations are unchanged from
v0.2.28; this release adds no new career observations to the published snapshot.

- JSON transport: 31,871,202 → 13,424,433 bytes (57.9% smaller).
- Retained gzip: 1,210,187 → 1,046,105 bytes (13.6% smaller).
- Python, JavaScript and a standalone verifier reconstruct the same logical
  dataset. Export integration checks repeatability and private-record exclusion.
- Existing logical snapshots remain supported. Fonts, layout and ranking rules
  are unchanged.

The frozen dataset still contains 282,901 events and 80,668 people. These are
partial public observations; independent external review remains pending.
