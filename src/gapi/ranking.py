from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from typing import Any, Iterable

from .model import AwardEvent, EducationEvent, EmploymentEvent, Institution


DEFAULT_CONFIG: dict[str, Any] = {
    "stage_weights": {"bachelor": 0.20, "master": 0.35, "doctorate": 1.0, "postdoc": 0.60},
    "alpha": 0.85,
    "tolerance": 1e-12,
    "max_iterations": 200,
    "qualifying_role_types": ["faculty", "researcher", "research_scientist", "postdoc"],
    "eligibility": {"min_events": 10, "min_source_classes": 2, "min_traceability": 0.80},
    "award_weights": {"personal": 1.0, "institutional": 0.5, "technical_history": 0.25},
    "composite_network_weight": 0.80,
}


def merge_config(config: dict[str, Any] | None) -> dict[str, Any]:
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in (config or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    alpha = merged["alpha"]
    if not isinstance(alpha, (int, float)) or not 0 <= alpha < 1:
        raise ValueError("alpha must be in [0, 1)")
    if merged["max_iterations"] < 1 or merged["tolerance"] <= 0:
        raise ValueError("max_iterations and tolerance must be positive")
    if not 0 <= merged["composite_network_weight"] <= 1:
        raise ValueError("composite_network_weight must be in [0, 1]")
    if any(not isinstance(weight, (int, float)) or weight < 0 for weight in merged["stage_weights"].values()):
        raise ValueError("stage weights must be non-negative numbers")
    return merged


def _accepted_events(
    education: Iterable[EducationEvent], employment: Iterable[EmploymentEvent], config: dict[str, Any]
) -> list[tuple[EducationEvent, EmploymentEvent]]:
    education_by_person: dict[str, list[EducationEvent]] = defaultdict(list)
    for event in education:
        if event.accepted:
            education_by_person[event.person_id].append(event)
    employment_by_person: dict[str, list[EmploymentEvent]] = defaultdict(list)
    for event in employment:
        valid_fraction = event.joint_fraction is None or 0 < event.joint_fraction <= 1
        resolved_joint = not event.is_joint_appointment or event.joint_fraction is not None
        if event.accepted and event.role_type in config["qualifying_role_types"] and valid_fraction and resolved_joint:
            employment_by_person[event.person_id].append(event)

    accepted: list[tuple[EducationEvent, EmploymentEvent]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for person_id, trainings in education_by_person.items():
        for training in trainings:
            for job in employment_by_person.get(person_id, []):
                if training.institution_id == job.institution_id:
                    continue
                # Missing dates cannot establish that training preceded employment.
                if training.year is None or job.year_start is None:
                    continue
                if job.year_start < training.year:
                    continue
                key = (person_id, training.stage, training.institution_id, job.institution_id)
                if key in seen:
                    continue
                seen.add(key)
                accepted.append((training, job))
    return accepted


def pagerank(nodes: list[str], edges: dict[tuple[str, str], float], alpha: float, tolerance: float, max_iterations: int, personalization: dict[str, float] | None = None) -> tuple[dict[str, float], dict[str, Any]]:
    if len(set(nodes)) != len(nodes):
        raise ValueError("PageRank nodes must be unique")
    if not 0 <= alpha < 1 or tolerance <= 0 or max_iterations < 1:
        raise ValueError("invalid PageRank parameters")
    if any(not math.isfinite(value) or value < 0 for value in edges.values()):
        raise ValueError("edge weights must be finite and non-negative")
    if not nodes:
        return {}, {"iterations": 0, "error": 0.0, "converged": True}
    n = len(nodes)
    index = {node: i for i, node in enumerate(nodes)}
    if personalization is not None:
        if set(personalization) - set(nodes) or any(not math.isfinite(v) or v < 0 for v in personalization.values()) or sum(personalization.values()) <= 0:
            raise ValueError("invalid personalization vector")
        total = sum(personalization.values())
        prior = [personalization.get(node, 0.0) / total for node in nodes]
    else:
        prior = [1.0 / n] * n
    outgoing = [0.0] * n
    valid_edges = {(source, target): value for (source, target), value in edges.items()
                   if source in index and target in index and value > 0}
    for (source, _target), value in valid_edges.items():
        if source in index:
            outgoing[index[source]] += value
    rank = prior[:]
    error = math.inf
    iterations = 0
    for iterations in range(1, max_iterations + 1):
        next_rank = [(1.0-alpha)*value for value in prior]
        dangling = sum(rank[i] for i, total in enumerate(outgoing) if total == 0)
        next_rank = [value + alpha*dangling*prior[i] for i,value in enumerate(next_rank)]
        for (source, target), weight in valid_edges.items():
            next_rank[index[target]] += alpha * rank[index[source]] * weight / outgoing[index[source]]
        error = sum(abs(a - b) for a, b in zip(next_rank, rank))
        rank = next_rank
        if error < tolerance:
            break
    return dict(zip(nodes, rank)), {"iterations": iterations, "error": error, "converged": error < tolerance}


def _percentiles(scores: dict[str, float], eligible: set[str]) -> dict[str, float]:
    ordered = sorted(value for node, value in scores.items() if node in eligible)
    if not ordered:
        return {node: 0.0 for node in scores}
    if len(ordered) == 1:
        return {node: (1.0 if node in eligible else 0.0) for node in scores}
    value_percentiles: dict[float, float] = {}
    position = 0
    while position < len(ordered):
        end = position + 1
        while end < len(ordered) and ordered[end] == ordered[position]:
            end += 1
        average_position = (position + end - 1) / 2
        value_percentiles[ordered[position]] = average_position / (len(ordered) - 1)
        position = end
    result = {node: value_percentiles.get(value, 0.0) if node in eligible else 0.0 for node, value in scores.items()}
    return result


def calculate_rankings(
    institutions: list[Institution],
    education: list[EducationEvent],
    employment: list[EmploymentEvent],
    awards: list[AwardEvent],
    config: dict[str, Any] | None = None,
    evidence: list[Any] | None = None,
) -> dict[str, Any]:
    cfg = merge_config(config)
    nodes = sorted({institution.id for institution in institutions})
    evidence_by_id = {item.id: item for item in (evidence or [])}
    accepted_events = _accepted_events(education, employment, cfg)
    stage_weights = cfg["stage_weights"]
    edges: dict[tuple[str, str], float] = defaultdict(float)
    edge_stages: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    sources_by_origin: dict[str, set[str]] = defaultdict(set)
    event_counts: dict[str, int] = defaultdict(int)
    traced_counts: dict[str, int] = defaultdict(int)
    for training, job in accepted_events:
        # Employer institution -> training institution.
        fraction = job.joint_fraction if job.joint_fraction is not None else 1.0
        sources_by_origin[training.institution_id].add(training.source_class)
        event_counts[training.institution_id] += 1
        if training.evidence_id in evidence_by_id and job.evidence_id in evidence_by_id:
            traced_counts[training.institution_id] += 1
        if stage_weights[training.stage] <= 0:
            continue
        edge = (job.institution_id, training.institution_id)
        weight = stage_weights[training.stage] * fraction
        edges[edge] += weight
        edge_stages[edge][training.stage] += weight
    traceability = {
        node: traced_counts.get(node, 0) / event_counts[node] if event_counts.get(node, 0) else 0.0
        for node in nodes
    }
    eligibility_cfg = cfg["eligibility"]
    stable_ids = {institution.id for institution in institutions if institution.stable_id}
    eligibility_reasons: dict[str, list[str]] = {}
    for node in nodes:
        reasons = []
        if event_counts.get(node, 0) < eligibility_cfg["min_events"]:
            reasons.append("insufficient_observed_events")
        if len(sources_by_origin.get(node, set())) < eligibility_cfg["min_source_classes"]:
            reasons.append("insufficient_source_classes")
        if traceability.get(node, 0.0) < eligibility_cfg["min_traceability"]:
            reasons.append("traceability_below_threshold")
        if node not in stable_ids:
            reasons.append("missing_stable_institution_identifier")
        eligibility_reasons[node] = reasons
    eligible = {node for node, reasons in eligibility_reasons.items() if not reasons}
    network, convergence = pagerank(nodes, edges, cfg["alpha"], cfg["tolerance"], cfg["max_iterations"])
    award_scores = {node: 0.0 for node in nodes}
    award_category_scores: dict[str, dict[str, float]] = {node: defaultdict(float) for node in nodes}
    award_contributions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_awards: set[tuple[str, int, str | None]] = set()
    for award in awards:
        if not award.accepted:
            continue
        award_key = (award.award_id, award.year, award.recipient_id)
        if award_key in seen_awards:
            continue
        seen_awards.add(award_key)
        category_weight = cfg["award_weights"].get(award.category, 0.0)
        # Only explicit institutional attribution enters the main award score.
        target = award.institution_id or award.producing_institution_id
        if target in award_scores and category_weight:
            award_scores[target] += category_weight
            award_category_scores[target][award.category] += category_weight
            award_contributions[target].append({"award_id": award.award_id, "year": award.year, "category": award.category, "evidence_id": award.evidence_id})
    network_pct = _percentiles(network, eligible)
    award_pct = _percentiles(award_scores, eligible)
    gamma = cfg["composite_network_weight"]
    rows = []
    by_id = {institution.id: institution for institution in institutions}
    for node in nodes:
        institution = by_id[node]
        status = "ranked" if node in eligible else "unranked_low_coverage"
        rows.append({
            "rank": None,
            "institution_id": node,
            "institution_name": institution.name,
            "country": institution.country,
            "region": institution.region,
            "institution_type": institution.institution_type,
            "network_score": network[node] if status == "ranked" else None,
            "award_score": award_scores[node] if status == "ranked" else None,
            "award_categories": dict(award_category_scores[node]) if status == "ranked" else None,
            "composite_score": gamma * network_pct[node] + (1 - gamma) * award_pct[node] if status == "ranked" else None,
            "network_percentile": network_pct[node] if status == "ranked" else None,
            "award_percentile": award_pct[node] if status == "ranked" else None,
            "coverage": {"event_count": event_counts.get(node, 0), "source_class_count": len(sources_by_origin.get(node, set())), "traceability": traceability.get(node, 0.0)},
            "status": status,
            "unranked_reasons": eligibility_reasons[node],
            "award_contributions": award_contributions.get(node, []),
        })
    ranked = sorted((row for row in rows if row["status"] == "ranked"), key=lambda row: (-row["composite_score"], row["institution_name"]))
    rank = 0
    previous_score: float | None = None
    for position, row in enumerate(ranked, 1):
        if previous_score is None or not math.isclose(row["composite_score"], previous_score, rel_tol=0.0, abs_tol=1e-12):
            rank = position
        previous_score = row["composite_score"]
        row["rank"] = rank
    return {
        "schema_version": "gapi-ranking/v1",
        "algorithm": {"alpha": cfg["alpha"], "tolerance": cfg["tolerance"], "max_iterations": cfg["max_iterations"], "stage_weights": cfg["stage_weights"], "composite_network_weight": gamma},
        "convergence": convergence,
        "nodes": rows,
        "edges": [{"source": source, "target": target, "weight": weight, "stages": dict(edge_stages[(source, target)])} for (source, target), weight in sorted(edges.items())],
        "limitations": ["Scores measure observed, evidence-backed mobility relationships; they are not employment probabilities or causal estimates."],
    }


def release_manifest(inputs: dict[str, Any], config: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    canonical = json.dumps({"inputs": inputs, "config": config, "result": result}, ensure_ascii=True, sort_keys=True).encode()
    return {
        "schema_version": "gapi-release-manifest/v1",
        "input_sha256": inputs,
        "config_sha256": hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
        "result_sha256": hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest(),
        "release_sha256": hashlib.sha256(canonical).hexdigest(),
    }
