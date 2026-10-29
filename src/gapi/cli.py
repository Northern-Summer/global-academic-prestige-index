from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from .model import parse_dataset
from .ranking import DEFAULT_CONFIG, calculate_rankings, release_manifest
from .demo import generate
from .openalex import ingest_institutions
from .ror import ingest_ror
from .us_faculty import ingest_and_rank
from .stream_cli import register, execute


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(prog="gapi", description="Reproducible academic talent-flow ranking")
    sub = parser.add_subparsers(dest="command", required=True)
    register(sub)
    rank = sub.add_parser("rank", help="calculate rankings from a dataset")
    rank.add_argument("--input", type=Path, required=True)
    rank.add_argument("--config", type=Path)
    rank.add_argument("--out", type=Path, required=True)
    rank.add_argument("--manifest", type=Path)
    demo = sub.add_parser("demo", help="generate deterministic synthetic demo data and ranking")
    demo.add_argument("--out-dir", type=Path, default=Path("data/demo"))
    openalex = sub.add_parser("ingest-openalex-institutions", help="download the real OpenAlex global institution directory")
    openalex.add_argument("--out", type=Path, default=Path("data/real/openalex-institutions.json.gz"))
    openalex.add_argument("--checkpoint-dir", type=Path, help="optional resumable raw cache; must be outside the project")
    openalex.add_argument("--per-page", type=int, default=200)
    openalex.add_argument("--max-pages", type=int, help="bounded partial import for tests or previews")
    ror = sub.add_parser("ingest-ror", help="download the latest real ROR global organization registry dump")
    ror.add_argument("--out", type=Path, default=Path("data/real/institutions.json.gz"))
    ror.add_argument("--cache-dir", type=Path, help="optional raw archive cache; must be outside the project")
    us_faculty = sub.add_parser(
        "ingest-us-faculty",
        help="compute a local real PageRank from the published US faculty-hiring research dataset",
    )
    us_faculty.add_argument("--cache-dir", type=Path, help="optional raw-source cache; must be outside the project")
    us_faculty.add_argument("--out", type=Path, default=Path("data/real/us-academia-network.json"))
    args = parser.parse_args()
    if execute(args):
        return
    if args.command == "demo":
        generate(args.out_dir)
        return
    if args.command == "rank":
        payload = _read_json(args.input)
        config = _read_json(args.config) if args.config else DEFAULT_CONFIG
        institutions, education, employment, awards, evidence = parse_dataset(payload)
        result = calculate_rankings(institutions, education, employment, awards, config, evidence)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        if args.manifest:
            args.manifest.parent.mkdir(parents=True, exist_ok=True)
            inputs = {"dataset": hashlib.sha256(args.input.read_bytes()).hexdigest()}
            if args.config:
                inputs["config"] = hashlib.sha256(args.config.read_bytes()).hexdigest()
            manifest = release_manifest(inputs, config, result)
            args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return
    if args.command == "ingest-openalex-institutions":
        result = ingest_institutions(
            args.out,
            checkpoint_dir=args.checkpoint_dir,
            per_page=args.per_page,
            max_pages=args.max_pages,
            on_page=lambda page, total: print(f"Retrieved page {page}; source records reported: {total}", flush=True),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    if args.command == "ingest-ror":
        result = ingest_ror(args.out, cache_dir=args.cache_dir)
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    if args.command == "ingest-us-faculty":
        result = ingest_and_rank(args.cache_dir, args.out)
        print(json.dumps(result, indent=2, sort_keys=True))
