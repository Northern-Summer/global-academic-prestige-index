# Global Academic Prestige Index (GAPI)

GAPI measures observed academic hiring relationships. Its current site view shows a **real, computed doctoral hiring network score** derived from the published U.S. faculty hiring research dataset. It is a scoped research-sample score, not a global census or authoritative measure of university quality.

## Quick start

Requirements: Python 3.11+ and Node.js 22+.

```powershell
python -m pip install -e .
python -m gapi ingest-ror
python -m gapi ingest-us-faculty --out data/real/us-academia-network.json
python -m unittest discover -s tests -v
npm ci
npm test
npm run dev
```

Open the local URL printed by Vite. To rebuild the static site, run `npm run build`. To rank a normalized dataset, run:

```powershell
gapi rank --input path/to/dataset.json --config configs/default.json --out build/rankings.json --manifest build/manifest.json
```

The demo generator remains available for automated algorithm tests only. The website never loads the demo files or uses them as a fallback.

`ingest-ror` downloads the CC0 ROR organization registry. `ingest-openalex-institutions` downloads the CC0 OpenAlex organization directory. `ingest-us-faculty` downloads a commit-pinned faculty-hiring network export, filters it to the all-academia network, and computes a PageRank score from the observed employer-to-doctoral-institution counts. Raw downloads use a temporary system directory and are removed after processing; only normalized directories and institution-level derived scores are written to `data/real`, which is ignored by Git. Set `GAPI_SCRATCH_DIR` to a scratch directory on a drive with sufficient capacity when processing large source files. Optional `--cache-dir` and `--checkpoint-dir` values are for resumability and must point outside the project tree; the importer rejects paths inside the project. Organization directories are not global career-history data. The current score source sample covers U.S. employing institutions over 2011–2020; doctoral institutions can be outside the U.S. The page identifies that scope and never labels it a complete global ranking.

ORCID's 2025 CC0 Public Data File is the identified next global career-activity source. Its eleven official activity shards total about 194 GB compressed. The importer is not yet implemented and no ORCID bulk activities contribute to current scores. This source boundary and the difference between institution identity checks and career-event corroboration are documented in [DATA_SOURCES.md](docs/DATA_SOURCES.md).

The upstream faculty-data repository does not declare a reuse license. The real faculty snapshot and generated score file are ignored by Git and kept local. Generated scores must not be included in a public website or release until reuse rights are clarified. A clean checkout therefore shows a real-data loading notice until the authorized import is run.

## Project status

- Deterministic reference algorithm, evidence-linked input contract, real research-sample PageRank ingestion, and ranking table are implemented.
- The currently computed real score covers a U.S. hiring sample and is not a global hiring census. Award scores and stage comparisons are not available from this aggregated source.
- Public redistribution of the source-derived ranking is on hold because the upstream data repository does not declare a reuse license. Independent third-party red-team review remains outstanding.

## Documentation

- [Methodology](docs/METHODOLOGY.md)
- [Data sources and licensing](docs/DATA_SOURCES.md)
- [Biases and limitations](docs/BIAS_AND_LIMITATIONS.md)
- [Red-team status](docs/RED_TEAM_REPORT.md)
- [Contribution guide](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Citation](CITATION.cff)

Code is Apache-2.0. Documentation is CC BY 4.0. Data rights are source-specific; see [DATA_SOURCES.md](docs/DATA_SOURCES.md).
