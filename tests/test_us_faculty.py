from __future__ import annotations

import csv
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from gapi.us_faculty import calculate_us_academia_network, fetch_source, ingest_and_rank


FIELDS = [
    "TaxonomyLevel", "TaxonomyValue", "InstitutionId", "InstitutionName",
    "DegreeInstitutionId", "DegreeInstitutionName", "Total", "Men", "Women",
]


class PublishedFacultyHiringImportTests(unittest.TestCase):
    def write_csv(self, rows: list[list[str]]) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        path = Path(temp_dir.name) / "edges.csv"
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(FIELDS)
            writer.writerows(rows)
        return path

    def write_stats_csv(self, extra: list[tuple[str, str]] | None = None) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        path = Path(temp_dir.name) / "stats.csv"
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["InstitutionId", "InstitutionName", "TaxonomyLevel", "TaxonomyValue"])
            for institution_id, name in (extra or [("1", "Hiring University"), ("2", "Doctoral University"), ("3", "Unrelated Institution")]):
                writer.writerow([institution_id, name, "Academia", "Academia"])
            writer.writerow(["4", "Field only", "Field", "Physics"])
        return path

    def test_uses_only_all_academia_rows_and_employer_to_degree_direction(self) -> None:
        source = self.write_csv([
            ["Academia", "Academia", "1", "Hiring University", "2", "Doctoral University", "10", "0", "0"],
            ["Field", "Physics", "1", "Hiring University", "2", "Doctoral University", "500", "0", "0"],
            ["Academia", "Academia", "1", "Hiring University", "2", "Doctoral University", "3", "0", "0"],
            ["Academia", "Academia", "1", "Hiring University", "9", "External Doctoral University", "7", "0", "0"],
        ])
        result = calculate_us_academia_network(source, self.write_stats_csv())
        by_id = {row["institution_id"]: row for row in result["nodes"]}

        self.assertEqual(result["source"]["source_rows_in_scope"], 3)
        self.assertEqual(result["algorithm"]["edges"], 2)
        self.assertEqual(result["algorithm"]["nodes"], 3)
        self.assertEqual(result["algorithm"]["within_ranked_network_edges"], 1)
        self.assertEqual(result["algorithm"]["external_doctoral_origin_edges"], 1)
        self.assertEqual(result["algorithm"]["external_doctoral_origin_count"], 7)
        self.assertEqual(result["scope"]["institution_universe"].split(";")[0], "US employing institutions listed in the source Academia/Academia stats")
        self.assertEqual(by_id["fhnet:2"]["source_recorded_doctoral_origin_count"], 13)
        self.assertEqual(by_id["fhnet:2"]["ranking_network_doctoral_origin_count"], 13)
        self.assertEqual(by_id["fhnet:2"]["rank"], 1)
        self.assertEqual(by_id["fhnet:1"]["source_recorded_faculty_count"], 20)
        self.assertEqual(by_id["fhnet:1"]["ranking_network_employer_count"], 13)
        self.assertEqual(by_id["fhnet:1"]["excluded_external_doctoral_origin_edges"], 1)
        self.assertEqual(by_id["fhnet:1"]["excluded_external_doctoral_origin_count"], 7)
        sensitivity = result["boundary_sensitivity"]
        self.assertEqual(sensitivity["induced_employer_subgraph"]["nodes"], 3)
        self.assertEqual(sensitivity["all_observed_origins_as_nodes"]["nodes"], 4)
        self.assertTrue(sensitivity["all_observed_origins_as_nodes"]["convergence"]["converged"])
        self.assertEqual(sensitivity["rank_comparison_among_employer_nodes"]["top_10"]["overlap_count"], 3)
        self.assertEqual(result["dataset_kind"], "real/derived-research-sample")
        self.assertEqual(result["publication_status"], "local_only_license_review_required")

    def test_rejects_negative_counts_and_conflicting_source_names(self) -> None:
        negative = self.write_csv([
            ["Academia", "Academia", "1", "Hiring University", "2", "Doctoral University", "-1", "0", "0"],
        ])
        with self.assertRaisesRegex(ValueError, "non-negative"):
            calculate_us_academia_network(negative, self.write_stats_csv())

        conflict = self.write_csv([
            ["Academia", "Academia", "1", "Hiring University", "2", "Doctoral University", "1", "0", "0"],
            ["Academia", "Academia", "1", "Different Name", "2", "Doctoral University", "1", "0", "0"],
        ])
        with self.assertRaisesRegex(ValueError, "conflicting names"):
            calculate_us_academia_network(conflict, self.write_stats_csv())

    def test_same_pinned_input_produces_identical_scores(self) -> None:
        source = self.write_csv([
            ["Academia", "Academia", "1", "甲大学", "2", "乙大学", "8", "0", "0"],
            ["Academia", "Academia", "2", "乙大学", "1", "甲大学", "2", "0", "0"],
        ])
        stats = self.write_stats_csv([("1", "甲大学"), ("2", "乙大学")])
        first = calculate_us_academia_network(source, stats)
        second = calculate_us_academia_network(source, stats)
        self.assertEqual(first["nodes"], second["nodes"])
        self.assertTrue(first["algorithm"]["converged"])

    def test_default_ingest_removes_temporary_raw_cache_after_success(self) -> None:
        observed = {}

        def fake_import(cache_dir: Path, output_path: Path, *, alpha: float = 0.85):
            observed["cache"] = cache_dir
            self.assertTrue(cache_dir.exists())
            return {"output_path": str(output_path)}

        with patch("gapi.us_faculty._ingest_and_rank_cached", side_effect=fake_import):
            result = ingest_and_rank(None, Path("rankings.json"))

        self.assertEqual(result["output_path"], "rankings.json")
        self.assertTrue(str(observed["cache"]).startswith(tempfile.gettempdir()))
        self.assertFalse(observed["cache"].exists())

    def test_persistent_raw_cache_inside_project_is_rejected(self) -> None:
        from gapi.storage import project_root

        with self.assertRaisesRegex(ValueError, "outside the project"):
            ingest_and_rank(project_root() / "data" / "real" / ".us-faculty-cache", Path("rankings.json"))

    def test_corrupt_pinned_edge_cache_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cache = Path(temp)
            from gapi.us_faculty import SOURCE_COMMIT
            (cache / f"edge-lists-{SOURCE_COMMIT}.csv").write_text("not the pinned source", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                fetch_source(cache)


if __name__ == "__main__":
    unittest.main()
