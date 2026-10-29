"""Offline, bounded OOD probes of production rules using synthetic fixtures.

This module never opens a task directory or a personal-data database. The
fixture store is SQLite :memory:; acquisition, normalization and graph rules
remain owned by their production modules. No result is a global accuracy or
fairness estimate, or an independent third-party review.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sqlite3
import time
from collections import Counter
from pathlib import Path

from .career_normalization import (
    DDL as INTERPRETATION_DDL, POLICY_SHA256, VERSION, interpretation,
)
from .career_store import DDL
from .global_analysis import rank_global, relation_rows
from .ranking import DEFAULT_CONFIG, pagerank
from .task import BudgetStop, Task, canonical, digest


VERSION_OOD = "gapi-ood/1"
MEMORY_BYTES = 128 * 1024**2
SECONDS = 10
COMPLETED = (
    ("PhD", "doctorate"), ("博士", "doctorate"), ("Doctorat", "doctorate"),
    ("Doctorado", "doctorate"), ("Doutorado", "doctorate"), ("박사", "doctorate"),
    ("Maestría", "master"), ("Mestrado", "master"), ("석사", "master"),
    ("B.S.", "bachelor"), ("M.S.", "master"), ("학사", "bachelor"),
)
UNFINISHED = (
    "PhD student", "博士未完成", "Doctorado no finalizado", "Doctorado abandonado",
    "Doutorado interrompido", "Doctorat non terminé", "PhD candidate",
    "honorary PhD", "在读博士", "unknown degree",
)
FIXTURE_SPEC = {
    "version": VERSION_OOD, "completed": COMPLETED, "unfinished": UNFINISHED,
    "duplicate_copies": 64, "low_evidence_people": 128,
    "postdoc_copies": 128, "isolated_nodes": 64,
    "same_name": "Alex Example", "only_synthetic_identifiers": True,
}


class _Budget:
    """Reuse the existing RSS sampler; no task/config/disk initialization."""

    def __init__(self):
        self.config = {"memory_bytes": MEMORY_BYTES, "sources": {}}
        self.started = time.monotonic()
        self.max_sampled_rss = 0

    def check(self, anticipated=0):
        # This store is memory-only, so anticipated disk writes never occur.
        if time.monotonic() - self.started >= SECONDS:
            raise BudgetStop("offline OOD time budget exhausted")
        self.check_memory()

    def check_memory(self):
        self.max_sampled_rss = max(self.max_sampled_rss, Task.check_memory(self))


class _Fixture:
    """Only the production analyzer's small public store protocol, in memory."""

    def __init__(self, budget):
        self.task = budget
        self.db = sqlite3.connect(":memory:")
        self.db.execute("PRAGMA temp_store=MEMORY")
        self.db.executescript(DDL + INTERPRETATION_DDL)
        self.input_rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.db.close()

    def event(self, person, institution, kind, stage, *, source="wikidata",
              start=None, end=None, role=None, status="observed", region="Asia",
              name=None, member_status="observed"):
        event_id = f"fixture-{len(self.input_rows):06d}"
        locator = "fixture/" + event_id
        row = dict(id=event_id, person=person, institution=institution, kind=kind,
                   stage=stage, year_start=start, year_end=end, role=role,
                   country="ZZ", source=source, locator=locator, status=status,
                   display_name=name or person)
        payload = canonical(row).decode()
        self.db.execute("INSERT OR IGNORE INTO runs(source,identity,status) VALUES(?,?,?)",
                        (source, "synthetic-fixture/v1", "verified"))
        self.db.execute("INSERT INTO members VALUES(?,?,?,?,NULL)",
                        (source, locator, hashlib.sha256(payload.encode()).hexdigest(), member_status))
        self.db.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (event_id, person, institution, kind, stage, start, end,
                         role, "ZZ", source, locator, status, payload))
        self.db.execute("INSERT OR IGNORE INTO entities VALUES(?,?,?,?,?)",
                        (institution, institution, "ZZ", 1,
                         canonical({"region": region, "type": "synthetic-fixture"}).decode()))
        inferred = interpretation(row)
        if inferred:
            self.db.execute("INSERT INTO event_interpretations VALUES(?,?,?,?,?,?,?)",
                            (event_id, VERSION, POLICY_SHA256, *inferred))
        self.input_rows.append({**row, "member_status": member_status, "region": region})
        if len(self.input_rows) % 64 == 0:
            self.task.check()

    def pair(self, person, *, source="wikidata", training="X", employer="Y",
             stage="doctorate", **kwargs):
        self.event(person, training, "education", stage, source=source,
                   start=2000, end=2005, role="PhD", **kwargs)
        self.event(person, employer, "employment", "academic", source=source,
                   start=2010, role="Professor", **kwargs)

    def report(self):
        return {
            "people": self.db.execute("SELECT count(DISTINCT person) FROM events").fetchone()[0],
            "events": len(self.input_rows), "countries": ["ZZ"],
            "event_status": dict(self.db.execute("SELECT status,count(*) FROM events GROUP BY status")),
            "member_status": dict(self.db.execute("SELECT status,count(*) FROM members GROUP BY status")),
        }

    @property
    def input_sha256(self):
        return digest(self.input_rows)


def _scores(edges, nodes=None):
    nodes = nodes or sorted({n for edge in edges for n in edge})
    return pagerank(nodes, edges, .85, 1e-12, 1000)


def _graph(store):
    edges = Counter()
    stages = Counter()
    for (employer, training, stage), _train, _job in relation_rows(store):
        edges[employer, training] += DEFAULT_CONFIG["stage_weights"][stage]
        stages[stage] += 1
    return dict(edges), dict(stages)


def _case(metrics, checks):
    return {"metrics": metrics, "checks": checks, "passed": all(checks.values())}


def run_ood(*, measurements=False):
    """Return a deterministic offline report, optionally with runtime telemetry.

    CLI stdout is canonical JSON; a failed invariant returns exit code 1.
    Budget/sampling failures raise BudgetStop/RuntimeError and are not passes.
    The 128 MiB guard samples whole-process resident memory, including imports;
    it is a cooperative guard rather than an OS hard memory or CPU limit.
    """
    budget = _Budget()
    budget.check()
    hashes = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
              for name in ("ood.py", "career_normalization.py", "career_rules.py",
                           "career_store.py", "global_analysis.py", "ranking.py", "task.py")}
    seed_basis = digest({"fixtures": FIXTURE_SPEC, "rules": hashes, "policy": POLICY_SHA256})
    seed = int(seed_basis[:16], 16)
    rng = random.Random(seed)
    cases, input_hashes = {}, {}

    with _Fixture(budget) as store:
        expected = Counter(stage for _, stage in COMPLETED)
        for index, (role, stage) in enumerate(COMPLETED):
            person = f"completed-{index}"
            store.event(person, "X", "education", None, source="orcid-public-api",
                        end=2005, role=role)
            store.event(person, "Y", "employment", None, source="orcid-public-api",
                        start=2010, role="Professor")
        for index, role in enumerate(UNFINISHED):
            person = f"unfinished-{index}"
            store.event(person, "X", "education", None, source="orcid-public-api",
                        end=2005, role=role)
            store.event(person, "Y", "employment", None, source="orcid-public-api",
                        start=2010, role="Professor")
        _, stages = _graph(store)
        cases["multilingual_degree_completion"] = _case(
            {"completed_labels": len(COMPLETED), "unfinished_or_unknown_labels": len(UNFINISHED),
             "accepted_relations_by_stage": stages, "accepted_relations": sum(stages.values())},
            {"completed_stage_counts_match": stages == dict(expected),
             "unfinished_or_unknown_not_promoted": all(not train["person"].startswith("unfinished-")
                                                       for _, train, _ in relation_rows(store))})
        input_hashes["multilingual_degree_completion"] = store.input_sha256

    with _Fixture(budget) as store:
        store.pair("stable-1")
        baseline, _ = _graph(store)
        injections = [("wikidata" if i % 2 else "orcid-public-api")
                      for i in range(FIXTURE_SPEC["duplicate_copies"])]
        rng.shuffle(injections)
        for source in injections:
            store.pair("stable-1", source=source)
        duplicate_edges, _ = _graph(store)
        cases["duplicate_and_alternative_evidence"] = _case(
            {"injected_copies": len(injections), "before_weight": baseline.get(("Y", "X")),
             "after_weight": duplicate_edges.get(("Y", "X")),
             "relations_after_dedup": len(list(relation_rows(store)))},
            {"no_duplicate_amplification": duplicate_edges == baseline,
             "surviving_source_retains_relation": len(list(relation_rows(store, allowed_sources={"orcid-public-api"}))) == 1})
        input_hashes["duplicate_and_alternative_evidence"] = store.input_sha256

    with _Fixture(budget) as store:
        store.event("stable-A", "X", "education", "doctorate", end=2005, name=FIXTURE_SPEC["same_name"])
        store.event("stable-B", "Y", "employment", "academic", start=2010, name=FIXTURE_SPEC["same_name"])
        store.pair("stable-C", name=FIXTURE_SPEC["same_name"])
        relations = list(relation_rows(store))
        false_merges = sum(train["person"] != job["person"] for _, train, job in relations)
        cases["same_name_distinct_ids"] = _case(
            {"stable_ids": 3, "same_display_name": True, "relations": len(relations),
             "fixture_false_person_joins": false_merges},
            {"stable_keys_separate_people": len(relations) == 1 and relations[0][1]["person"] == "stable-C",
             "zero_false_fixture_joins": false_merges == 0})
        input_hashes["same_name_distinct_ids"] = store.input_sha256

    with _Fixture(budget) as store:
        store.pair("baseline")
        baseline, _ = _graph(store)
        for i in range(FIXTURE_SPEC["low_evidence_people"]):
            store.pair(f"unresolved-{i}", training="LOW", employer="FAKE", status="unresolved")
        store.pair("uncommitted", training="UNCOMMITTED", employer="FAKE", member_status="pending")
        report = rank_global(store)
        rows = {n["institution_id"]: n for n in report["nodes"]}
        after, _ = _graph(store)
        cases["single_source_low_evidence_injection"] = _case(
            {"injected_unresolved_events": 2 * FIXTURE_SPEC["low_evidence_people"],
             "pending_member_events": 2, "qualifying_edges": len(after),
             "formal_ranks": sum(n["formal_rank"] is not None for n in report["nodes"]),
             "injection_institution_score": rows["LOW"]["network_score"]},
            {"low_evidence_no_score_amplification": after == baseline,
             "missing_is_not_zero": rows["LOW"]["network_score"] is None,
             "single_source_not_formal": rows["X"]["formal_rank"] is None and
                 "insufficient_independent_source_classes" in rows["X"]["unranked_reasons"]})
        input_hashes["single_source_low_evidence_injection"] = store.input_sha256

    with _Fixture(budget) as store:
        for _ in range(FIXTURE_SPEC["postdoc_copies"]):
            store.event("postdoc", "X", "employment", None, source="orcid-public-api",
                        start=2006, end=2009, role="Postdoctoral researcher")
        store.event("postdoc", "Y", "employment", None, source="orcid-public-api",
                    start=2010, role="Professor")
        edges, stages = _graph(store)
        cases["postdoc_density"] = _case(
            {"raw_postdoc_events": FIXTURE_SPEC["postdoc_copies"], "relations_by_stage": stages,
             "edge_weight": edges.get(("Y", "X")), "doctorate_relations": stages.get("doctorate", 0)},
            {"postdoc_separate_and_capped": stages == {"postdoc": 1},
             "uses_configured_postdoc_weight": edges.get(("Y", "X")) == DEFAULT_CONFIG["stage_weights"]["postdoc"]})
        input_hashes["postdoc_density"] = store.input_sha256

    with _Fixture(budget) as store:
        for index, (degree_end, job_start) in enumerate(((None, 2010), (2005, None), (2010, 2005))):
            store.event(str(index), "X", "education", "doctorate", end=degree_end)
            store.event(str(index), "Y", "employment", "academic", start=job_start)
        count = len(list(relation_rows(store)))
        cases["missing_and_reversed_years"] = _case(
            {"invalid_date_pairs": 3, "accepted_relations": count}, {"dates_not_invented": count == 0})
        input_hashes["missing_and_reversed_years"] = store.input_sha256

    with _Fixture(budget) as store:
        store.event("conflicted", "X", "education", "doctorate", source="orcid-public-api", end=1995, role="PhD")
        store.event("conflicted", "Y", "employment", "academic", start=2000, role="Professor")
        store.event("conflicted", "Z", "employment", "academic", source="orcid-public-api", start=2000, role="Professor")
        store.pair("reference", source="orcid-public-api", training="V", employer="W")
        report = rank_global(store)
        fixed = report["sensitivity"]["delete_source_wikidata"]
        expanded = report["expanded_universe_sensitivity"]["delete_source_wikidata"]
        cases["source_loss_network_boundary"] = _case(
            {"reference_nodes": report["algorithm"]["network_universe"],
             "fixed": fixed, "expanded": expanded},
            {"fixed_reports_lost_boundary": (fixed["available_edges"], fixed["edges"], fixed["filtered_boundary_edges"]) == (2, 1, 1),
             "expanded_includes_unlocked_edge": expanded["edges"] == 2 and expanded["new_endpoints"] == ["X", "Z"],
             "both_universes_conserve_mass": all(math.isclose(s["probability_mass"], 1, abs_tol=1e-12) for s in (fixed, expanded)),
             "overlapping_jobs_excluded_at_baseline": report["algorithm"]["network_universe"] == ["V", "W"]})
        input_hashes["source_loss_network_boundary"] = store.input_sha256

    # Region labels are fixture groups, not a claim about actual countries.
    with _Fixture(budget) as store:
        for person, source, training, employer, region in (
            ("large-a", "wikidata", "A", "B", "Europe"),
            ("large-b", "wikidata", "C", "B", "Asia"),
            ("small-a", "orcid-public-api", "A", "C", "Asia"),
        ):
            store.pair(person, source=source, training=training, employer=employer, region=region)
        report = rank_global(store)
        edges, _ = _graph(store)
        base, _ = _scores(edges)
        scaled, _ = _scores({edge: weight * (1000 if edge[0] == "B" else 1) for edge, weight in edges.items()})
        max_drift = max(abs(base[n] - scaled[n]) for n in base)
        scenario = report["sensitivity"]["delete_source_orcid-public-api"]
        base_order = {row["institution_id"]: row["observed_rank"] for row in report["nodes"]}
        groups = dict(Counter(row["region"] for row in report["nodes"]))
        displacement = max(abs(base_order[n] - rank) for n, rank in scenario["ranks"].items())
        cases["region_and_observed_scale"] = _case(
            {"row_scale_multiplier": 1000, "max_score_drift_after_row_scaling": max_drift,
             "source_loss_max_rank_displacement": displacement,
             "fixture_region_group_sizes": groups,
             "region_balanced_prior": report["sensitivity"]["region_balanced_prior"],
             "intensity_denominator": report["uncertainty"]["intensity_denominator"]},
            {"outgoing_row_scale_invariant": max_drift < 1e-12,
             "distinct_region_groups_exercised": groups == {"Asia": 1, "Europe": 2},
             "source_loss_mass_conserved": math.isclose(scenario["probability_mass"], 1, abs_tol=1e-12),
             "balanced_prior_mass_conserved": math.isclose(report["sensitivity"]["region_balanced_prior"]["probability_mass"], 1, abs_tol=1e-12)})
        input_hashes["region_and_observed_scale"] = store.input_sha256

    forward, _ = _scores({("Y", "X"): 1})
    reverse, _ = _scores({("X", "Y"): 1})
    both, _ = _scores({("Y", "X"): 1, ("X", "Y"): 1})
    nodes = ["X", "Y"] + [f"isolated-{i}" for i in range(FIXTURE_SPEC["isolated_nodes"])]
    isolated, convergence = _scores({("Y", "X"): 1, ("Y", "outside-universe"): 999}, nodes)
    rejected = []
    for label, weights in (("overflow", {("X", "X"): 1e308, ("X", "Y"): 1e308}),
                           ("nan", {("X", "Y"): math.nan}),
                           ("infinity", {("X", "Y"): math.inf}),
                           ("negative", {("X", "Y"): -1})):
        try:
            _scores(weights, ["X", "Y"])
        except ValueError:
            rejected.append(label)
    cases["direction_isolation_and_extreme_weights"] = _case(
        {"forward_training_score": forward["X"], "reverse_training_score": reverse["X"],
         "bidirectional_training_score": both["X"], "isolated_nodes": len(nodes)-2,
         "probability_mass": math.fsum(isolated.values()), "invalid_weight_cases_rejected": rejected},
        {"employer_votes_for_training": forward["X"] > forward["Y"] and reverse["X"] < reverse["Y"],
         "asymmetry_changes_scores": forward["X"] != both["X"],
         "boundary_and_dangling_conserve_mass": math.isclose(math.fsum(isolated.values()), 1, abs_tol=1e-12),
         "isolates_preserve_direction": isolated["X"] > isolated["Y"],
         "converged": convergence["converged"],
         "invalid_and_overflow_weights_fail_closed": rejected == ["overflow", "nan", "infinity", "negative"]})
    budget.check()
    report = {
        "schema_version": VERSION_OOD, "dataset_kind": "synthetic/deidentified-test-fixtures",
        "seed": seed, "seed_basis_sha256": seed_basis, "fixture_spec_sha256": digest(FIXTURE_SPEC),
        "fixture_input_sha256": input_hashes, "rule_code_sha256": hashes,
        "normalization_policy_sha256": POLICY_SHA256,
        "budget": {"seconds": SECONDS, "process_resident_bytes": MEMORY_BYTES,
                   "enforcement": "cooperative checks; no OS hard limit; SQLite memory only"},
        "case_count": len(cases), "check_count": sum(len(c["checks"]) for c in cases.values()),
        "passed": all(case["passed"] for case in cases.values()), "cases": cases,
        "limitations": [
            "Synthetic fixtures only; no real personal data, network access or filesystem database reads.",
            "Fixture join and classification counts are not measured global entity-resolution accuracy.",
            "Region labels and size multipliers do not establish fairness or correct unknown global missingness.",
            "Source-loss stress tests report fixed and expanded universes separately, without treating missing scores as zero.",
            "No external-rank validation, discipline/OOD ground truth, bootstrap coverage or independent third-party review.",
        ],
    }
    if measurements:
        report["execution"] = {"elapsed_seconds": time.monotonic()-budget.started,
                               "max_sampled_rss_bytes": budget.max_sampled_rss,
                               "telemetry_in_deterministic_hash": False}
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measure", action="store_true", help="include nondeterministic runtime telemetry")
    args = parser.parse_args(argv)
    report = run_ood(measurements=args.measure)
    print(canonical(report).decode())
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
