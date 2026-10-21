# Global Academic Prestige Index (GAPI)

GAPI is an open research prototype for measuring evidence-backed academic talent-flow relationships between research institutions. It is not an authoritative measure of university quality, and a person observed at an employer is not an employment-probability denominator.

The current site and bundled ranking use **synthetic demo records only**. They contain no real institutions, people, or measured global results.

## Quick start

Requirements: Python 3.11+ and Node.js 22+.

```powershell
python -m pip install -e .
python -m gapi demo --out-dir data/demo
python -m unittest discover -s tests -v
npm ci
npm test
npm run dev
```

Open the local URL printed by Vite. To rebuild the static site, run `npm run build`. To rank a normalized dataset, run:

```powershell
gapi rank --input path/to/dataset.json --config configs/default.json --out build/rankings.json --manifest build/manifest.json
```

The demo generator is deterministic and writes both `rankings.json` and `scenarios.json`. The site lets you compare a reference mix, stage-weight changes, network-only/award-only composites, and a removed synthetic hiring hub. These are constructed teaching fixtures, not observations about real institutions. The production eligibility defaults leave low-coverage demo institutions unranked; the bundled site uses an explicitly relaxed demo-only event threshold.

## Project status

- Deterministic reference algorithm, evidence-linked input contract, synthetic fixture, and static Three.js frontend are implemented.
- Live source acquisition, a validated global dataset, formal uncertainty intervals, and independent third-party red-team review are not yet available.
- No real-world or global ranking is published by this repository version.

## Documentation

- [Methodology](docs/METHODOLOGY.md)
- [Data sources and licensing](docs/DATA_SOURCES.md)
- [Biases and limitations](docs/BIAS_AND_LIMITATIONS.md)
- [Red-team status](docs/RED_TEAM_REPORT.md)
- [Contribution guide](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Citation](CITATION.cff)

Code is Apache-2.0. Documentation is CC BY 4.0. Data rights are source-specific; see [DATA_SOURCES.md](docs/DATA_SOURCES.md).
