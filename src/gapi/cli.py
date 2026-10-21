from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from .model import parse_dataset
from .ranking import DEFAULT_CONFIG, calculate_rankings, release_manifest
from .demo import generate


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(prog="gapi", description="Reproducible academic talent-flow ranking")
    sub = parser.add_subparsers(dest="command", required=True)
    rank = sub.add_parser("rank", help="calculate rankings from a dataset")
    rank.add_argument("--input", type=Path, required=True)
    rank.add_argument("--config", type=Path)
    rank.add_argument("--out", type=Path, required=True)
    rank.add_argument("--manifest", type=Path)
    demo = sub.add_parser("demo", help="generate deterministic synthetic demo data and ranking")
    demo.add_argument("--out-dir", type=Path, default=Path("data/demo"))
    args = parser.parse_args()
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
