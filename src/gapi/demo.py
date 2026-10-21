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
    flows = [
        (0, 1, {"doctorate": 6, "master": 2, "bachelor": 1, "postdoc": 1}),
        (0, 2, {"doctorate": 3, "master": 1}),
        (1, 0, {"doctorate": 4, "master": 1, "postdoc": 1}),
        (1, 2, {"doctorate": 2, "bachelor": 1}),
        (2, 0, {"doctorate": 1, "master": 1}),
        (2, 1, {"doctorate": 1}),
    ]
    person_index = 0
    for employer_index, origin_index, stage_counts in flows:
      for stage, count in stage_counts.items():
       for _ in range(count):
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
        education_year = 2000 + person_index % 15
        education.append({"person_id": person_id, "institution_id": institutions[origin_index]["id"], "stage": stage, "year": education_year, "evidence_id": f"demo-{person_index:03d}-edu", "source_class": source_class, "accepted": True})
        employment.append({"person_id": person_id, "institution_id": institutions[employer_index]["id"], "role_type": "postdoc" if stage == "postdoc" else ("faculty" if person_index % 2 == 0 else "researcher"), "year_start": max(education_year, 2015 + person_index % 8), "evidence_id": f"demo-{person_index:03d}-job", "source_class": "official_profile" if source_class == "official_cv" else "official_cv", "accepted": True, "joint_fraction": None})
        person_index += 1
    awards = [
        {"award_id": "synthetic-award-personal-001", "year": 2020, "recipient_id": "demo-person-023", "category": "personal", "institution_id": "gapi:charlie", "producing_institution_id": None, "training_institution_id": None, "evidence_id": "demo-023-job", "accepted": True},
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

    scenario_definitions = [
        ("baseline", "Reference mix", "All training stages and separate award signal; fixed synthetic inputs.", dataset, config),
        ("doctorate_only", "Doctorate only", "Recalculate after setting bachelor, master, and postdoc weights to zero.", dataset, {**config, "stage_weights": {"bachelor": 0, "master": 0, "doctorate": 1, "postdoc": 0}}),
        ("network_only", "Talent network only", "Set the composite award contribution to zero; network input is unchanged.", dataset, {**config, "composite_network_weight": 1.0}),
        ("awards_only", "Award signal only", "Set the composite network contribution to zero; awards remain a separate synthetic signal.", dataset, {**config, "composite_network_weight": 0.0}),
        ("without_alpha_hiring", "Remove one hiring hub", "Remove all synthetic employment events at Northstar Institute and recalculate.", {**dataset, "employment_events": [event for event in dataset["employment_events"] if event["institution_id"] != "gapi:alpha"]}, config),
    ]
    scenarios = []
    for scenario_id, label, description, scenario_dataset, scenario_config in scenario_definitions:
        scenario_institutions, scenario_education, scenario_employment, scenario_awards, scenario_evidence = parse_dataset(scenario_dataset)
        scenario_result = calculate_rankings(scenario_institutions, scenario_education, scenario_employment, scenario_awards, scenario_config, scenario_evidence)
        scenarios.append({"id": scenario_id, "label": label, "description": description, "result": {"nodes": scenario_result["nodes"], "edges": scenario_result["edges"], "convergence": scenario_result["convergence"], "algorithm": scenario_result["algorithm"]}})
    (output_dir / "scenarios.json").write_text(json.dumps({"dataset_kind": "synthetic/demo", "notice": "Every scenario is a constructed teaching fixture, not an observed institutional comparison.", "scenarios": scenarios}, indent=2, sort_keys=True), encoding="utf-8")
    dataset_hash = hashlib.sha256(json.dumps(dataset, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
    manifest = release_manifest({"dataset": dataset_hash}, config, result)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
