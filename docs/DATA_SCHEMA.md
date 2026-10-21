# Normalized input contract

Input is UTF-8 JSON with `institutions`, `education_events`, `employment_events`, `awards`, and `evidence` arrays. The Python dataclasses in `src/gapi/model.py` are the authoritative v0.1 field definitions.

Each education event requires an internal person key, institution, one of `bachelor`, `master`, `doctorate`, or `postdoc`, optional year, evidence key, and source class. Employment events require an institution, role type, optional start year, evidence key, and source class. Awards require a stable award key/year/category, explicit attribution fields where known, and evidence key. The source evidence URL must use HTTP(S), the content hash must be lowercase SHA-256, and accepted events require an `approved` or `synthetic_demo` license status.

This v0.1 contract does not publish raw person identifiers. Future public exports must aggregate to institution-level records and must pass a privacy and source-rights review.
