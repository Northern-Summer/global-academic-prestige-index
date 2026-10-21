"""Generate deterministic synthetic records for the public demo."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .model import parse_dataset
from .ranking import DEFAULT_CONFIG, calculate_rankings, release_manifest


def build_demo() -> dict:
    institutions = [
        {"id": "gapi:alpha", "name": "Northstar Institute (DEMO)", "country": "Synthetic", "region": "Demo Region", "institution_type": "research_university", "aliases": [], "stable_id": "DEMO-001"},
        {"id": "gapi:bravo", "name": "Meridian University (DEMO)", "country": "Synthetic", "region": "Demo Region", "institution_type": "research_university", "aliases": [], "stable_id": "DEMO-002"},
        {"id": "gapi:charlie", "name": "Tidewater Research University (DEMO)", "country": "Synthetic", "region": "Demo Region", "institution_type": "research_university", "aliases": [], "stable_id": "DEMO-003"},
        {"id": "gapi:low", "name": "Unobserved Example Institute (DEMO)", "country": "Synthetic", "region": "Demo Region", "institution_type": "research_institute", "aliases": [], "stable_id": "DEMO-004"},
    ]
    evidence = []
    education = []
    employment = []
    for person_index in range(24):
        origin_index = person_index % 3
        destination_index = (origin_index + 1 + (person_index // 3) % 2) % 3
        person_id = f"demo-person-{person_index:03d}"
        source_class = "official_profile" if person_index % 2 == 0 else "official_cv"
        for suffix, source in (("edu", source_class), ("job", "official_profile" if source_class == "official_cv" else "official_cv")):
            evidence_id = f"demo-{person_index:03d}-{suffix}"
            evidence.append({
                "id": evidence_id,
                "url": f"https://example.invalid/gapi-synthetic-fixture/{evidence_id}",
                "retrieved_at": "2026-01-01T00:00:00Z",
                "source_class": source,
                "evidence_level": "synthetic_fixture",
                "license_status": "synthetic_demo",
                "content_sha256": hashlib.sha256(evidence_id.encode()).hexdigest(),
            })
        education.append({"person_id": person_id, "institution_id": institutions[origin_index]["id"], "stage": "doctorate", "year": 2000 + person_index % 12, "evidence_id": f"demo-{person_index:03d}-edu", "source_class": source_class, "accepted": True})
        employment.append({"person_id": person_id, "institution_id": institutions[destination_index]["id"], "role_type": "faculty", "year_start": 2015 + person_index % 8, "evidence_id": f"demo-{person_index:03d}-job", "source_class": "official_profile" if source_class == "official_cv" else "official_cv", "accepted": True, "joint_fraction": None})
    awards = [
        {"award_id": "synthetic-award-personal-001", "year": 2020, "recipient_id": "demo-person-001", "category": "personal", "institution_id": "gapi:alpha", "producing_institution_id": None, "training_institution_id": None, "evidence_id": "demo-000-job", "accepted": True},
        {"award_id": "synthetic-milestone-001", "year": 2021, "recipient_id": None, "category": "technical_history", "institution_id": "gapi:bravo", "producing_institution_id": None, "training_institution_id": None, "evidence_id": "demo-001-job", "accepted": True},
    ]
    return {"dataset_kind": "synthetic/demo", "institutions": institutions, "education_events": education, "employment_events": employment, "awards": awards, "evidence": evidence}


def generate(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset = build_demo()
    (output_dir / "dataset.json").write_text(json.dumps(dataset, indent=2, sort_keys=True), encoding="utf-8")
    institutions, education, employment, awards, evidence = parse_dataset(dataset)
    config = {
        "eligibility": {"min_events": 5, "min_source_classes": 2, "min_traceability": 0.80},
        "alpha": 0.85,
    }
    result = calculate_rankings(institutions, education, employment, awards, config, evidence)
    result["dataset_kind"] = "synthetic/demo"
    result["demo_notice"] = "Synthetic records only. No real institution or person data; not a global ranking."
    (output_dir / "rankings.json").write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    dataset_hash = hashlib.sha256(json.dumps(dataset, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
    manifest = release_manifest({"dataset": dataset_hash}, config, result)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
