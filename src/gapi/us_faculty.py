"""Import the published U.S. faculty hiring edge list and compute a local GAPI score.

The upstream repository does not declare a data-reuse license. Data and scores are
therefore written only to caller-selected local paths and must not be published
without resolving reuse rights.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from urllib.request import Request, urlopen

from .ranking import pagerank
from .storage import require_external_cache, temporary_raw_cache


SOURCE_REPOSITORY = "https://github.com/LarremoreLab/us-faculty-hiring-networks"
SOURCE_COMMIT = "841e9c96f949677aadbffded10a002586ff65cc4"
SOURCE_URL = (
    "https://raw.githubusercontent.com/LarremoreLab/us-faculty-hiring-networks/"
    f"{SOURCE_COMMIT}/data/edge-lists.csv"
)
INSTITUTION_STATS_URL = (
    "https://raw.githubusercontent.com/LarremoreLab/us-faculty-hiring-networks/"
    f"{SOURCE_COMMIT}/data/institution-stats.csv"
)
SOURCE_LICENSE = "not_declared_by_upstream_repository"
EXPECTED_EDGE_LIST_SHA256 = "615fd8587d26c2bdb365e857d4c6c89675644a680fcc4b3bbf31e1f2ff09e164"
EXPECTED_INSTITUTION_STATS_SHA256 = "927f82b828c2c5b817ba57d910371d06d039d7ac7b93a4dc526b0eb00843c3a9"
MAX_EDGE_LIST_BYTES = 40 * 1024 * 1024
MAX_INSTITUTION_STATS_BYTES = 5 * 1024 * 1024
SAMPLE_START = 2011
SAMPLE_END = 2020


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _download_pinned_csv(url: str, destination: Path, expected_sha256: str, max_bytes: int) -> Path:
    request = Request(url, headers={"User-Agent": "GAPI-reproducible-research/0.1"})
    temporary: Path | None = None
    try:
        with urlopen(request, timeout=90) as response:
            if response.status != 200:
                raise RuntimeError(f"upstream returned HTTP {response.status}")
            declared_size = response.headers.get("Content-Length")
            if declared_size and int(declared_size) > max_bytes:
                raise ValueError("upstream CSV exceeds the configured size limit")
            fd, temporary_name = tempfile.mkstemp(prefix="gapi-fhn-", suffix=".part", dir=destination.parent)
            temporary = Path(temporary_name)
            received = 0
            with os.fdopen(fd, "wb") as output:
                while block := response.read(1024 * 1024):
                    received += len(block)
                    if received > max_bytes:
                        raise ValueError("upstream CSV exceeds the configured size limit")
                    output.write(block)
                output.flush()
                os.fsync(output.fileno())
        if _sha256(temporary) != expected_sha256:
            raise ValueError("downloaded CSV SHA-256 does not match the pinned source artifact")
        os.replace(temporary, destination)
        return destination
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _rank_summary(nodes: list[str], scores: dict[str, float], names: dict[str, str]) -> dict[str, int]:
    ordered = sorted(nodes, key=lambda node: (-scores[node], names.get(node, node).casefold(), node))
    ranks: dict[str, int] = {}
    prior_score: float | None = None
    prior_rank = 0
    for position, node in enumerate(ordered, 1):
        if prior_score is None or abs(scores[node] - prior_score) > 1e-12:
            prior_rank = position
        ranks[node] = prior_rank
        prior_score = scores[node]
    return ranks


def _boundary_sensitivity(
    employer_nodes: list[str],
    all_nodes: list[str],
    all_edges: dict[tuple[str, str], int],
    names: dict[str, str],
    *,
    alpha: float,
    tolerance: float,
    max_iterations: int,
) -> dict[str, Any]:
    internal_edges = {edge: count for edge, count in all_edges.items() if edge[0] in employer_nodes and edge[1] in employer_nodes}
    induced_scores, induced_convergence = pagerank(employer_nodes, internal_edges, alpha, tolerance, max_iterations)
    expanded_scores, expanded_convergence = pagerank(all_nodes, all_edges, alpha, tolerance, max_iterations)
    induced_ranks = _rank_summary(employer_nodes, induced_scores, names)
    expanded_ranks = _rank_summary(employer_nodes, {node: expanded_scores[node] for node in employer_nodes}, names)

    def comparison(k: int) -> dict[str, Any]:
        effective_k = min(k, len(employer_nodes))
        first = set(sorted(employer_nodes, key=lambda node: (induced_ranks[node], names.get(node, node).casefold()))[:effective_k])
        second = set(sorted(employer_nodes, key=lambda node: (expanded_ranks[node], names.get(node, node).casefold()))[:effective_k])
        return {
            "k": k,
            "effective_k": effective_k,
            "overlap_count": len(first & second),
            "overlap_fraction": len(first & second) / effective_k if effective_k else 1.0,
            "induced_only": sorted(names.get(node, node) for node in first - second),
            "expanded_only": sorted(names.get(node, node) for node in second - first),
        }

    shifts = [abs(induced_ranks[node] - expanded_ranks[node]) for node in employer_nodes]
    return {
        "interpretation": "Scores use different node universes and normalization; compare within-employer ordinal positions and overlaps, not raw PageRank magnitudes.",
        "induced_employer_subgraph": {
            "nodes": len(employer_nodes),
            "edges": len(internal_edges),
            "convergence": induced_convergence,
            "score_sum_over_employer_nodes": sum(induced_scores.values()),
        },
        "all_observed_origins_as_nodes": {
            "nodes": len(all_nodes),
            "edges": len(all_edges),
            "external_origin_nodes": len(set(all_nodes) - set(employer_nodes)),
            "convergence": expanded_convergence,
            "score_sum_over_employer_nodes": sum(expanded_scores[node] for node in employer_nodes),
            "score_sum_over_external_origin_nodes": sum(expanded_scores[node] for node in all_nodes if node not in set(employer_nodes)),
        },
        "external_origins_as_boundary_sinks": {
            "status": "computed",
            "definition": "External doctoral-origin nodes receive observed inbound edges and have no outgoing edges; dangling rank is redistributed uniformly over the expanded node universe.",
            "reference": "all_observed_origins_as_nodes",
        },
        "rank_comparison_among_employer_nodes": {
            "top_10": comparison(10),
            "top_25": comparison(25),
            "top_50": comparison(50),
            "rank_shift_abs_median": sorted(shifts)[len(shifts) // 2],
            "rank_shift_abs_p95": sorted(shifts)[int((len(shifts) - 1) * 0.95)],
            "rank_shift_abs_max": max(shifts),
        },
        "employer_ranks": {
            node: {"induced_subgraph": induced_ranks[node], "expanded_origin_nodes": expanded_ranks[node]}
            for node in employer_nodes
        },
    }


def fetch_source(cache_dir: Path, timeout: int = 90) -> tuple[Path, str]:
    """Fetch the pinned source into a local cache, reusing a complete prior copy."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    destination = cache_dir / f"edge-lists-{SOURCE_COMMIT}.csv"
    if destination.is_file():
        digest = _sha256(destination)
        if digest != EXPECTED_EDGE_LIST_SHA256:
            raise ValueError("cached edge list SHA-256 does not match the pinned source artifact")
        return destination, digest

    request = Request(SOURCE_URL, headers={"User-Agent": "GAPI-reproducible-research/0.1"})
    temporary: Path | None = None
    try:
        with urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise RuntimeError(f"upstream returned HTTP {response.status}")
            declared_size = response.headers.get("Content-Length")
            if declared_size and int(declared_size) > MAX_EDGE_LIST_BYTES:
                raise ValueError("upstream edge list exceeds the configured size limit")
            fd, temporary_name = tempfile.mkstemp(prefix="gapi-fhn-", suffix=".part", dir=cache_dir)
            temporary = Path(temporary_name)
            with os.fdopen(fd, "wb") as output:
                received = 0
                while block := response.read(1024 * 1024):
                    received += len(block)
                    if received > MAX_EDGE_LIST_BYTES:
                        raise ValueError("upstream edge list exceeds the configured size limit")
                    output.write(block)
                output.flush()
                os.fsync(output.fileno())
        if temporary.stat().st_size < 1000:
            raise ValueError("upstream edge list is unexpectedly small")
        if _sha256(temporary) != EXPECTED_EDGE_LIST_SHA256:
            raise ValueError("downloaded edge list SHA-256 does not match the pinned source artifact")
        os.replace(temporary, destination)
        return destination, _sha256(destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def calculate_us_academia_network(
    source_path: Path,
    institution_stats_path: Path,
    *,
    alpha: float = 0.85,
    tolerance: float = 1e-12,
    max_iterations: int = 200,
) -> dict[str, Any]:
    """Calculate PageRank from the dataset's all-academia doctoral hiring network.

    Upstream rows aggregate faculty by employing institution and doctoral institution.
    Edges point from employer to doctorate-granting institution, matching GAPI's
    influence propagation direction. One all-academia scope is used to avoid counting
    the same faculty again across domain and field networks.
    """
    required = {
        "TaxonomyLevel", "TaxonomyValue", "InstitutionId", "InstitutionName",
        "DegreeInstitutionId", "DegreeInstitutionName", "Total",
    }
    institutions: dict[str, dict[str, Any]] = {}
    all_institutions: dict[str, str] = {}
    edge_weights: dict[tuple[str, str], int] = {}
    source_rows = 0
    employer_institutions: set[str] = set()
    with institution_stats_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required_stats = {"InstitutionId", "InstitutionName", "TaxonomyLevel", "TaxonomyValue"}
        if reader.fieldnames is None or not required_stats.issubset(reader.fieldnames):
            raise ValueError("institution stats CSV is missing required ranking columns")
        for row in reader:
            if row["TaxonomyLevel"] != "Academia" or row["TaxonomyValue"] != "Academia":
                continue
            institution_id = f"fhnet:{int(row['InstitutionId'])}"
            name = row["InstitutionName"].strip()
            prior = all_institutions.setdefault(institution_id, name)
            if not name or prior != name:
                raise ValueError(f"invalid or conflicting name for ranked institution {institution_id}")
            employer_institutions.add(institution_id)
            institutions[institution_id] = {"institution_id": institution_id, "name": name, "roles": {"employer"}}
    if not employer_institutions:
        raise ValueError("institution stats contain no academia ranking institutions")
    with source_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError("source CSV is missing required faculty hiring columns")
        for row in reader:
            if row["TaxonomyLevel"] != "Academia" or row["TaxonomyValue"] != "Academia":
                continue
            source_rows += 1
            try:
                count = int(row["Total"])
                employer_id = f"fhnet:{int(row['InstitutionId'])}"
                degree_id = f"fhnet:{int(row['DegreeInstitutionId'])}"
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid institution identifier or faculty count in academia row") from exc
            if count < 0:
                raise ValueError("faculty counts must be non-negative")
            for institution_id, name, role in (
                (employer_id, row["InstitutionName"], "employer"),
                (degree_id, row["DegreeInstitutionName"], "doctoral_origin"),
            ):
                if not name.strip():
                    raise ValueError(f"empty name for institution {institution_id}")
                prior = all_institutions.setdefault(institution_id, name.strip())
                if prior != name.strip():
                    raise ValueError(f"conflicting names for upstream institution ID {institution_id}")
                if institution_id in employer_institutions:
                    item = institutions.setdefault(institution_id, {"institution_id": institution_id, "name": name.strip(), "roles": set()})
                    item["roles"].add(role)
            if count:
                edge = (employer_id, degree_id)
                edge_weights[edge] = edge_weights.get(edge, 0) + count

    if source_rows == 0 or not institutions or not edge_weights:
        raise ValueError("source contains no all-academia faculty hiring edges")
    nodes = sorted(employer_institutions)
    network_edges = {
        (employer, degree_origin): count
        for (employer, degree_origin), count in edge_weights.items()
        if employer in employer_institutions and degree_origin in employer_institutions
    }
    if not network_edges:
        raise ValueError("no observed hiring relations connect the ranking institutions")
    scores, convergence = pagerank(nodes, network_edges, alpha, tolerance, max_iterations)
    names = {node: institution["name"] for node, institution in institutions.items()}
    expanded_node_set = employer_institutions | {node for edge in edge_weights for node in edge}
    boundary_sensitivity = _boundary_sensitivity(
        nodes,
        sorted(expanded_node_set),
        edge_weights,
        names,
        alpha=alpha,
        tolerance=tolerance,
        max_iterations=max_iterations,
    )
    placements: dict[str, int] = {}
    hired: dict[str, int] = {}
    internal_placements: dict[str, int] = {}
    internal_hires: dict[str, int] = {}
    external_source_edges: dict[str, int] = {}
    external_source_placements: dict[str, int] = {}
    for (employer, degree_origin), count in edge_weights.items():
        placements[degree_origin] = placements.get(degree_origin, 0) + count
        hired[employer] = hired.get(employer, 0) + count
        if employer in employer_institutions and degree_origin in employer_institutions:
            internal_placements[degree_origin] = internal_placements.get(degree_origin, 0) + count
            internal_hires[employer] = internal_hires.get(employer, 0) + count
        elif employer in employer_institutions:
            external_source_edges[employer] = external_source_edges.get(employer, 0) + 1
            external_source_placements[employer] = external_source_placements.get(employer, 0) + count

    ranked = sorted(nodes, key=lambda node: (-scores[node], institutions[node]["name"].casefold(), node))
    output_nodes = []
    previous: float | None = None
    current_rank = 0
    for position, node in enumerate(ranked, start=1):
        score = scores[node]
        if previous is None or abs(score - previous) > 1e-12:
            current_rank = position
        previous = score
        institution = institutions[node]
        output_nodes.append({
            "rank": current_rank,
            "institution_id": node,
            "name": institution["name"],
            "roles": sorted(institution["roles"]),
            "gapi_network_score": score,
            "source_recorded_doctoral_origin_count": placements.get(node, 0),
            "source_recorded_faculty_count": hired.get(node, 0),
            "ranking_network_doctoral_origin_count": internal_placements.get(node, 0),
            "ranking_network_employer_count": internal_hires.get(node, 0),
            "excluded_external_doctoral_origin_edges": external_source_edges.get(node, 0),
            "excluded_external_doctoral_origin_count": external_source_placements.get(node, 0),
        })

    return {
        "dataset_kind": "real/derived-research-sample",
        "score_name": "gapi_doctoral_hiring_pagerank",
        "scope": {
            "employing_region": "United States",
            "employment_sample_start": SAMPLE_START,
            "employment_sample_end": SAMPLE_END,
            "education_stage": "doctorate",
            "network_view": "Academia/Academia",
            "institution_universe": "US employing institutions listed in the source Academia/Academia stats; doctoral-origin institutions outside this employer set are excluded from PageRank",
        },
        "source": {
            "repository": SOURCE_REPOSITORY,
            "url": SOURCE_URL,
            "commit": SOURCE_COMMIT,
            "license_status": SOURCE_LICENSE,
            "source_rows_in_scope": source_rows,
        },
        "input_sha256": _sha256(source_path),
        "algorithm": {
            "name": "PageRank",
            "edge_direction": "employing_institution_to_doctoral_institution",
            "edge_weight": "source-published aggregate faculty count",
            "alpha": alpha,
            "tolerance": tolerance,
            "max_iterations": max_iterations,
            "nodes": len(nodes),
            "edges": len(edge_weights),
            "rankable_employers": len(employer_institutions),
            "within_ranked_network_edges": len(network_edges),
            "external_doctoral_origin_edges": len(edge_weights) - len(network_edges),
            "external_doctoral_origin_count": sum(
                count for (employer, origin), count in edge_weights.items()
                if employer in employer_institutions and origin not in employer_institutions
            ),
            **convergence,
        },
        "boundary_sensitivity": boundary_sensitivity,
        "publication_status": "local_only_license_review_required",
        "notice": (
            "Real published aggregate faculty-hiring data; this is a US-employer sample, "
            "not a global census or an employment probability. The source repository "
            "does not declare a reuse license. Do not redistribute this derived output "
            "until rights are clarified."
        ),
        "nodes": output_nodes,
    }


def _ingest_and_rank_cached(cache_dir: Path, output_path: Path, *, alpha: float = 0.85) -> dict[str, Any]:
    source_path, digest = fetch_source(cache_dir)
    stats_path = cache_dir / f"institution-stats-{SOURCE_COMMIT}.csv"
    if stats_path.is_file():
        if _sha256(stats_path) != EXPECTED_INSTITUTION_STATS_SHA256:
            raise ValueError("cached institution stats SHA-256 does not match the pinned source artifact")
    else:
        _download_pinned_csv(INSTITUTION_STATS_URL, stats_path, EXPECTED_INSTITUTION_STATS_SHA256, MAX_INSTITUTION_STATS_BYTES)
    result = calculate_us_academia_network(source_path, stats_path, alpha=alpha)
    result["input_sha256"] = {
        "edge_list": digest,
        "institution_stats": _sha256(stats_path),
    }
    result["input_artifacts"] = {
        "edge_list": {"url": SOURCE_URL, "commit": SOURCE_COMMIT, "size_bytes": source_path.stat().st_size, "sha256": digest},
        "institution_stats": {
            "url": INSTITUTION_STATS_URL,
            "commit": SOURCE_COMMIT,
            "size_bytes": stats_path.stat().st_size,
            "sha256": _sha256(stats_path),
        },
    }
    result["schema_version"] = "gapi-us-faculty-score/v2"
    result["snapshot_revision"] = (
        f"us-fhn-{SOURCE_COMMIT[:12]}-"
        f"{digest[:12]}-{result['input_sha256']['institution_stats'][:12]}"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    temporary_output = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary_output.write_bytes(encoded)
    os.replace(temporary_output, output_path)
    manifest = {
        "schema_version": "gapi-score-manifest/v1",
        "snapshot_revision": result["snapshot_revision"],
        "dataset_kind": result["dataset_kind"],
        "source_commit": SOURCE_COMMIT,
        "output": output_path.name,
        "output_size_bytes": len(encoded),
        "output_sha256": hashlib.sha256(encoded).hexdigest(),
        "input_artifacts": result["input_artifacts"],
        "publication_status": result["publication_status"],
    }
    manifest_path = output_path.with_suffix(output_path.suffix + ".manifest.json")
    temporary_manifest = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
    temporary_manifest.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    os.replace(temporary_manifest, manifest_path)
    summary = {
        "output_path": str(output_path),
        "snapshot_revision": result["snapshot_revision"],
        "manifest_path": str(manifest_path),
        "input_sha256": digest,
        "nodes": result["algorithm"]["nodes"],
        "edges": result["algorithm"]["edges"],
        "within_ranked_network_edges": result["algorithm"]["within_ranked_network_edges"],
        "source_rows_in_scope": result["source"]["source_rows_in_scope"],
        "converged": result["algorithm"]["converged"],
        "top_institutions": [
            {"rank": row["rank"], "name": row["name"], "score": row["gapi_network_score"]}
            for row in result["nodes"][:10]
        ],
        "publication_status": result["publication_status"],
    }
    return summary


def ingest_and_rank(cache_dir: Path | None, output_path: Path, *, alpha: float = 0.85) -> dict[str, Any]:
    """Ingest into a temporary source cache unless persistent caching is requested."""
    if cache_dir is not None:
        return _ingest_and_rank_cached(require_external_cache(cache_dir), output_path, alpha=alpha)
    with temporary_raw_cache("gapi-us-faculty-") as temporary_cache:
        summary = _ingest_and_rank_cached(Path(temporary_cache), output_path, alpha=alpha)
    summary["raw_source_cache"] = "temporary_removed_after_processing"
    return summary
