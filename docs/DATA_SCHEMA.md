# Normalized input contract

## Current streaming observation contract

The real-observation pipeline uses the SQLite definitions in
`src/gapi/career_store.py`, the event producers in `global_sources.py` and
`orcid_stream.py`, and the versioned interpretations in `career_normalization.py`.
The JSON model below is the separate legacy/demo importer; it is not the current
global acquisition schema.

Each private career event has a stable pseudonymous person key, source, immutable
member/statement locator, kind (`education`, `employment`, `training`), nullable
stage, nullable institution identifier, dates and evidence payload. A postdoc is
`training/postdoc`, never a doctoral degree. Missing stage, identity or dates remain
unresolved and cannot become inferred qualifications. The database preserves
acquisition rows; later interpretations and corroboration decisions are derived
tables with explicit rule versions. Neither transport changes nor interpretations
upgrade evidence independence.

ORCID affiliation XML stores exact section URL, retrieval time, byte length and
SHA-256 in `member_parts`; the member hash identifies the canonical section-hash
manifest. For `wikibase-claims-info-batch/v1`, each member's `qid@revision` identifies
the entity, its member hash is `digest(entity)` (canonical JSON SHA-256), and
`member_parts` identifies the **whole batch's exact response hash** and URL.
Events carry the batch response hash as `content_sha256` and canonical entity hash
as `entity_sha256`. Existing single-EntityData members retain their original exact
response hashes. These conventions are transport-labeled and must not be conflated.
Original XML/JSON bodies and individual trajectories are not public export tables.

## Legacy JSON importer

Input is UTF-8 JSON with `institutions`, `education_events`, `employment_events`, `awards`, and `evidence` arrays. The Python dataclasses in `src/gapi/model.py` are the authoritative v0.1 field definitions.

Each education event requires an internal person key, institution, one of `bachelor`, `master`, `doctorate`, or `postdoc`, optional year, evidence key, and source class. Employment events require an institution, role type, optional start year, evidence key, and source class. The ranking layer only accepts an education/employment pair into the primary graph when both years are present and the employment start is not before training. Records with missing temporal anchors remain in the input snapshot but are excluded from the primary graph until reviewed. Awards require a stable award key/year/category, explicit attribution fields where known, and evidence key. The source evidence URL must use HTTP(S), the content hash must be lowercase SHA-256, and accepted events require an `approved` or `synthetic_demo` license status.

This v0.1 contract does not publish raw person identifiers. Future public exports must aggregate to institution-level records and must pass a privacy and source-rights review.
