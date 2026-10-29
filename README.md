# Global Academic Prestige Index (GAPI)

GAPI measures **observed academic education, training and employment relationships** using traceable public statements and deterministic network calculations. Real observations and formal ranking eligibility are separate. **Global collection and independent corroboration are incomplete; no formal global ranking has been released.**

The site shows a global observation view plus a separately named historical US faculty-hiring study where locally available. It never substitutes synthetic scores. Synthetic fixtures are used only for algorithm tests.

## Quick start

Python 3.11+ and Node.js 22+. The Windows task defaults to external D-drive processing, a 2 GiB total data cap and a 512 MiB parser budget. Source XML/archives are never saved by the new streaming pipeline.

```powershell
python -m pip install -e .
npm ci
$env:PYTHONPATH='src'
python -m gapi sources freeze
python -m gapi ingest-wikidata --discover --resume --max-members 200 --max-seconds 300
python -m gapi ingest-orcid-api --discover --resume --max-members 200 --max-seconds 300
python -m gapi corroborate
python -m gapi quality
python -m gapi rank-global
npm run dev
```

Edit the task's external work location before freezing if needed. Its source lock rejects changes within the same work directory. Repeating `--discover --resume` advances an ordered live discovery page and processes unfinished queue members; it does **not** claim complete global source enumeration. The default Wikidata selection uses university-teacher candidates without converting their occupation into a job fact. The annual 2026 ORCID summary importer is implemented separately, but download bytes and upstream checksum are currently inaccessible from this execution environment.

For strict streaming, failures, snapshot scope, optional DuckDB/Parquet analysis and local/public builds, follow [the streaming guide](docs/STREAMING.md).

```powershell
python -m unittest discover -s tests -v
npm test
# Code/docs only; contains no published score dataset:
$env:GAPI_CODE_ONLY='1'
npm run build
```

A score-bearing production build requires an explicitly selected real release manifest. Hash/path errors, missing inputs and restricted datasets fail closed. `export-release` exports only source-permitted institution aggregates. ORCID API observations and the historical US sample remain local pending reuse review.

## Implemented and remaining

Implemented: source/config freezes; bounded SAX/gzip/tar processing; atomic member transactions, OS writer lock and replay; Wikidata revisions/references; conservative stage and stable-ID rules; unresolved queues; probability-conserving PageRank; observed qualification reasons; conditional parameter/source/stage/region scenarios; hash-bound institution exports; accessible table/details/CSV and on-demand Three.js.

Remaining: complete frozen global source enumeration, independent official-profile corroboration, fuller entity crosswalks, official award data and attribution, discipline/English-coverage OOD analysis, and independent dual-track red-team review. Awards and composite scores stay missing without evidence. Matching source statements does not alone establish independent confirmation. No employment probability or causal training effect is claimed.

The existing ROR/OpenAlex directory collectors and US study importer are preserved legacy commands. They use external temporary raw caches and **do not** satisfy the new zero-raw-disk acquisition contract. Do not run them as part of the strict streaming flow. The preserved normalized ROR directory supplies verified identity crosswalks; its directory size is never presented as career-flow coverage.

## Documentation

- [Streaming, resources and exact source boundaries](docs/STREAMING.md)
- [Methodology](docs/METHODOLOGY.md)
- [Sources and source-specific licensing](docs/DATA_SOURCES.md)
- [Bias and limitations](docs/BIAS_AND_LIMITATIONS.md)
- [Independent review status](docs/RED_TEAM_REPORT.md)
- [External review handoff](outputs/RED_TEAM_HANDOFF.md)
- [Contributing](CONTRIBUTING.md), [security](SECURITY.md), [citation](CITATION.cff)

Code: Apache-2.0. Documentation: CC BY 4.0. Real data rights are source-specific.
