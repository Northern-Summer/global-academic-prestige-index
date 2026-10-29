# Data sources, provenance, and licensing

## Source policy

Candidate source tiers are: official institution and award records; official institutional directories; openly licensed structured scholarly data; and constrained public profile APIs. Each source has its own retrieval, retention, attribution, and redistribution rules. Public visibility is not equivalent to redistribution permission.

OpenAlex states that its metadata is released under CC0 and is available through its API and snapshots: <https://openalex.org/about>. Wikidata structured data is CC0: <https://www.wikidata.org/wiki/Wikidata:Licensing>. ORCID's public API is governed by its own terms and must be reviewed before use or redistribution: <https://info.orcid.org/public-client-terms-of-service/>.

These references describe source-level policies, not blanket permission for every upstream record or downstream derivative. Re-check current terms before a real ingestion or release.

## ROR institution directory snapshot

The repository's real source-data view is populated by `gapi ingest-ror`, which resolves the latest ROR data release through the Zenodo API. It verifies the upstream archive checksum and confirms the release license is CC0 before parsing. The normalized snapshot includes ROR IDs, names and aliases, countries, types, status, external identifiers, update dates, source version, retrieval time, and a SHA-256 manifest.

This directory is **not** the GAPI talent-flow dataset. Registry membership does not prove an individual's degree, training stage, job title, or employment. It cannot enter the PageRank calculation as education or employment evidence.

Run a full import with:

```powershell
python -m gapi ingest-ror
```

The command writes the normalized institution directory to `data/real/institutions.json.gz` and its `.manifest.json` file. The upstream ZIP is processed in a temporary system directory and removed after parsing. Set `GAPI_SCRATCH_DIR` to an external scratch volume when the system temporary drive does not have enough space. If resumability is needed, `--cache-dir` must point outside the project tree; the importer rejects an in-project raw-cache path. Generated `data/real` outputs are ignored by Git by default.

## ORCID Public Data File 2025

ORCID's official [2025 Public Data File](https://orcid.figshare.com/articles/dataset/ORCID_Public_Data_File_2025/30375589) is CC0 and includes public education and employment activity records. The official integration guide documents eleven activity archives; the 2025 Figshare metadata reports approximately 194 GB compressed across those shards and an additional approximately 43 GB summaries archive. ORCID also documents access to the 2023–2025 files through a Google BigQuery collaboration, which requires a Google Cloud account.

This is the strongest identified open global source for researcher-declared career activity, but it is not a census: profile registration and activity completeness are voluntary, institution names/identifiers can be absent, and record visibility is selective. A future importer should stream one shard at a time from a configurable temporary volume, hash and validate the upstream shard, normalize only education/employment events into privacy-minimized institution aggregates, and delete each raw shard after verified processing. It must not turn OpenAlex publication affiliations into degree or employment facts. No ORCID bulk activity data has been ingested into this release yet, so it contributes no current score.

OpenAlex provides an independent open scholarly graph and institution identifiers, while ROR provides an independently curated organization registry. These can cross-check institution identity and research activity, but they are not independent confirmation of a person's education or employment assertion. Event-level corroboration must come from another source that actually asserts the same career event, such as an official institution CV/profile or a separately sourced research dataset.

## Published U.S. faculty hiring network

The real-score view uses the `Academia/Academia` edge list from [LarremoreLab/us-faculty-hiring-networks](https://github.com/LarremoreLab/us-faculty-hiring-networks), pinned to commit `841e9c96f949677aadbffded10a002586ff65cc4`. Its README describes employer-by-doctoral-institution faculty counts and the 2011–2020 U.S. faculty-hiring sample. This GAPI importer selects only the all-academia scope so the same faculty are not counted again in field and domain scopes.

The upstream repository does not declare a license for its data files. The importer downloads to an automatically removed temporary cache by default and writes a local score snapshot. An explicitly requested resumable cache must be outside the project tree. The source and derivative are not bundled with this code release or included in Git. Public redistribution and GitHub Pages publication of generated scores remain on hold until reuse permission is established.

Run locally with:

```powershell
python -m gapi ingest-us-faculty --out data/real/us-academia-network.json
```

The result records the pinned commit, input SHA-256, scope, node/edge counts, parameters, and convergence. By default raw CSV downloads live only in a temporary system directory and are removed after processing; `--cache-dir` opts into resumable caching only outside the project. The result contains institution-level aggregates only, no individual identifiers. The score is PageRank over published aggregate doctoral-hiring counts, not the source paper's SpringRank, a placement probability, or a global ranking.

## Provenance requirements

Every accepted event must refer to an evidence record containing a source URL, retrieval time, source class, evidence level, license status, and SHA-256 content hash. Each dataset release should preserve a source registry with retrieval policy, allowed fields, retention policy, redistribution status, and update date. Unclear rights mean the event cannot be accepted into a public release.

The public output should contain institution-level aggregates and source identifiers only where the applicable terms allow it. The internal person key must be pseudonymous and must not appear in public ranking exports.

## Demo data

`data/demo/dataset.json` and `data/demo/rankings.json` are deterministic synthetic fixtures generated by `python -m gapi demo`. Their URLs use the reserved `.invalid` domain. The fixture records are invented test values, not observations and not citations to source pages. Never use them as real evidence or merge them into a production dataset.

## Acquisition status

ROR, OpenAlex, and the U.S. faculty-hiring research connector are implemented. ROR and OpenAlex institution directories provide broad real global organization metadata; neither is a global academic career-history or hiring-flow census. OpenAlex institution import honors rate limits and supports checkpoints when explicitly requested; its default page cache is temporary and removed after processing. Global person-level career-history acquisition remains unimplemented. The current real score is therefore explicitly a scoped U.S.-employer research sample.
# Streaming career sources (2026-10-29)

Current source, access, license and storage contracts are in [STREAMING.md](STREAMING.md)
and `configs/streaming.json`. The frozen annual target is ORCID **2026**, not 2025.
Summary importer exists; the local annual-download route currently returns 202/empty,
and upstream checksum/bytes remain unknown. Individual ORCID API XML and Wikidata
EntityData are separate, partially scanned sources. Wikidata statements retain revision,
qualifiers and references. Publication affiliations are not career facts. ORCID API
redistribution is disabled by the task registry. Existing directory/US collectors
below are legacy capabilities, not part of the zero-raw-disk streaming flow.
