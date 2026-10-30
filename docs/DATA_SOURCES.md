# Data sources, provenance, and licensing

The separate [official Nobel API](https://www.nobelprize.org/about/developer-zone-2/)
is documented as key-free. Its [service terms](https://www.nobelprize.org/about/terms-of-use-for-api-nobelprize-org-and-data-nobelprize-org/),
checked 2026-10-29, allow CC0 use subject to those terms, request low server burden,
and prohibit misleading claims or implied endorsement. The versioned
`configs/nobel-20261029.json` freezes this source contract. Only award/category and
institution-label aggregates are exported; original responses and recipient
trajectories are excluded. Nobel data is independent of ORCID/Wikidata collection,
but is not an independent confirmation of anyone's education or employment.

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

## Historical ORCID 2025 source discovery

ORCID's official [2025 Public Data File](https://orcid.figshare.com/articles/dataset/ORCID_Public_Data_File_2025/30375589) is CC0 and includes public education and employment activity records. The official integration guide documents eleven activity archives; the 2025 Figshare metadata reports approximately 194 GB compressed across those shards and an additional approximately 43 GB summaries archive. ORCID also documents access to the 2023–2025 files through a Google BigQuery collaboration, which requires a Google Cloud account.

This paragraph preserves an earlier source-discovery checkpoint, not an instruction to download archives. The active frozen target is the 2026 summary release documented below. The streaming importer never writes source archives/XML. ORCID registration, completeness and visibility are voluntary, so neither annual release is a census. No verified annual archive contributes to the current scores.

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

The global streaming career pipeline is implemented and partially populated with real ORCID and Wikidata observations. ROR supplies stable institution crosswalks. OpenAlex and Crossref supply candidate identifiers, never degree or employment facts. The U.S. study remains a separately named local historical sample. Source enumeration, independent official corroboration and formal-global-ranking eligibility are incomplete. Legacy directory importers use temporary caches and are excluded from the zero-raw-disk streaming route.

## Streaming career sources (2026-10-29)

Current source, access, license and storage contracts are in [STREAMING.md](STREAMING.md)
and `configs/streaming.json`. The frozen annual target is ORCID **2026**, not 2025.
Summary importer exists; the local annual-download route currently returns 202/empty,
and upstream checksum/bytes remain unknown. Individual ORCID API XML and Wikidata
EntityData are separate, partially scanned sources. Wikidata statements retain revision,
qualifiers and references. Publication affiliations are not career facts. ORCID API
redistribution defaults to disabled in the frozen acquisition registry; an explicit
reviewed release policy can permit institution aggregates. Existing directory/US collectors
above are legacy capabilities, not part of the zero-raw-disk streaming flow.

Public API acquisition now requests only education and employment XML sections.
Their independent hashes/URLs commit atomically with the normalized member; earlier
whole-record snapshots remain immutable. Failed candidates are retained with
persistent backoff, and source refusals do not terminate other source queues.

[Crossref's public REST API](https://github.com/CrossRef/rest-api-doc) is an additional
unauthenticated candidate-discovery route with a separately persisted live cursor.
It writes only validated ORCID identifiers and page hashes, never paper affiliations,
author names or assertions of academic careers. Index recency, publisher deposits,
duplicates and source disagreements remain coverage limitations. OpenAlex first-page
success followed by an actual 401 on its next cursor is retained as an access
boundary; Crossref is an independent metadata source, not an alternate OpenAlex
authentication channel. See the streaming guide for real trial counts and limits.

## Reviewed noncommercial ORCID aggregate release policy

`configs/publication-20261029.json` freezes the project's reuse decision separately
from the immutable acquisition lock. ORCID's current [Terms of Use](https://info.orcid.org/terms-of-use/),
sections 3 and 4, grant distribution and public display rights for public Record
Data. Its [Public API terms](https://info.orcid.org/public-client-terms-of-service/)
still restrict API acquisition to noncommercial use and require quotas and other
conditions. The release policy therefore permits this noncommercial research
project to publish institution-level derived observations only. It does not label
API data CC0, grant rights to linked works, publish individual histories or assert
independent legal certification. The policy records retrieval time, official URLs
and hashes of the reviewed live Markdown sections; its own hash enters the release
manifest. Without explicit selection of this policy, the default remains restrictive.
# API-only continuation and official Nobel attribution

The active continuation uses bounded public API responses, never a local archive.
`configs/orcid-archive-20261029.json` records only the official 2026 archive metadata
observed from https://api.figshare.com/v2/articles/34697766 : the 57,290,954,546-byte
upstream size and `md5:a55a16fe0b292bda03cc6016546c8984` do not imply a download,
full checksum verification, or source completion. This metadata is not an active
task configuration and does not modify the frozen source registry.

Nobel-only award-time label attributions use the existing official page hashes.
The fixed normalized ROR directory hash is independently checked before exact
name/alias-and-country candidate matching. No fuzzy or LLM identity decisions are
made, and those candidates are not canonical merges or a complete award score.

## Institution identifier bridge (checked 2026-10-30)

Wikidata's [P3500](https://www.wikidata.org/w/index.php?title=Property:P3500&oldid=2559905787)
is the Ringgold organization identifier; [P6782](https://www.wikidata.org/w/index.php?title=Property:P6782&oldid=2537555919)
is the ROR identifier. The property pages were retrieved from the official site
through the registered public research fallback after the primary research channel
returned HTTP 503. This is source verification, not an entity match produced by an
LLM. Actual mapping uses exact structured statements and the pinned reciprocal ROR
directory only. Wikidata structured data and the ROR directory retain their existing
source-specific CC0 declarations; this does not access or republish the proprietary
Ringgold database. The normalized query ledger and acquisition window are evidence;
property distinct-value constraints alone do not prove a particular ID is unique.
