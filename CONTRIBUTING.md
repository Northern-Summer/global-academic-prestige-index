# Contributing

Contributions should preserve the distinction between observed evidence, derived scores, and interpretation. Do not add factual records without provenance or rights status. Do not add real person-level records to the public fixtures.

Before opening a pull request, run:

```powershell
python -m pip install --no-deps -e .
python -m gapi demo --out-dir data/demo
python -m unittest discover -s tests -v
npm ci
npm test
npm run build
```

Algorithm changes should include a small synthetic counterexample that would fail if the intended direction, deduplication, or attribution rule were broken.
