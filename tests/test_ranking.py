import unittest

from gapi.demo import build_demo
from gapi.model import parse_dataset
from gapi.ranking import DEFAULT_CONFIG, _percentiles, calculate_rankings, pagerank, release_manifest


class RankingTests(unittest.TestCase):
    def setUp(self):
        payload = build_demo()
        self.institutions, self.education, self.employment, self.awards, self.evidence = parse_dataset(payload)

    def test_edge_direction_rewards_training_institution(self):
        nodes = ["employer", "alma"]
        correct, _ = pagerank(nodes, {("employer", "alma"): 1.0}, 0.85, 1e-12, 200)
        reversed_scores, _ = pagerank(nodes, {("alma", "employer"): 1.0}, 0.85, 1e-12, 200)
        self.assertGreater(correct["alma"], correct["employer"])
        self.assertGreater(reversed_scores["employer"], reversed_scores["alma"])

    def test_demo_is_synthetic_and_low_coverage_is_unranked(self):
        payload = build_demo()
        result = calculate_rankings(self.institutions, self.education, self.employment, self.awards,
                                    {"eligibility": {"min_events": 5}}, self.evidence)
        self.assertEqual(payload["dataset_kind"], "synthetic/demo")
        ranked = [row for row in result["nodes"] if row["status"] == "ranked"]
        unranked = [row for row in result["nodes"] if row["status"] != "ranked"]
        self.assertEqual(len(ranked), 3)
        self.assertEqual(len(unranked), 1)
        self.assertIsNone(unranked[0]["composite_score"])
        self.assertEqual(len({row["rank"] for row in ranked}), 3)

    def test_duplicate_sources_do_not_duplicate_edges(self):
        education = self.education + [self.education[0], self.education[0]]
        employment = self.employment + [self.employment[0]]
        a = calculate_rankings(self.institutions, self.education, self.employment, [], {"eligibility": {"min_events": 1}}, self.evidence)
        b = calculate_rankings(self.institutions, education, employment, [], {"eligibility": {"min_events": 1}}, self.evidence)
        self.assertEqual(a["edges"], b["edges"])

    def test_award_duplicate_is_counted_once(self):
        result = calculate_rankings(self.institutions, self.education, self.employment,
                                    self.awards + [self.awards[0]], {"eligibility": {"min_events": 5}}, self.evidence)
        alpha = next(row for row in result["nodes"] if row["institution_id"] == "gapi:alpha")
        self.assertEqual(alpha["award_score"], 1.0)

    def test_config_rejects_invalid_alpha(self):
        with self.assertRaises(ValueError):
            calculate_rankings(self.institutions, self.education, self.employment, self.awards,
                               {"alpha": 1.0}, self.evidence)

    def test_tied_scores_receive_tied_percentiles(self):
        result = _percentiles({"a": 0.5, "b": 0.5, "c": 0.5}, {"a", "b", "c"})
        self.assertEqual(set(result.values()), {0.5})

    def test_administrative_roles_and_unresolved_joint_appointments_are_excluded(self):
        employment = [
            {"person_id": "test-person", "institution_id": "gapi:bravo", "role_type": "administrator", "year_start": 2020, "evidence_id": "demo-000-job", "source_class": "official_cv", "accepted": True},
            {"person_id": "test-person", "institution_id": "gapi:charlie", "role_type": "faculty", "year_start": 2020, "evidence_id": "demo-000-job", "source_class": "official_cv", "accepted": True, "is_joint_appointment": True},
        ]
        from gapi.model import EmploymentEvent
        parsed = [EmploymentEvent(**item) for item in employment]
        result = calculate_rankings(self.institutions, self.education[:1], parsed, [], {"eligibility": {"min_events": 1}}, self.evidence)
        self.assertEqual(result["edges"], [])

    def test_event_without_evidence_is_rejected(self):
        payload = build_demo()
        payload["education_events"][0]["evidence_id"] = "missing"
        with self.assertRaisesRegex(ValueError, "missing evidence"):
            parse_dataset(payload)

    def test_unlicensed_accepted_event_is_rejected(self):
        payload = build_demo()
        payload["evidence"][0]["license_status"] = "unknown"
        with self.assertRaisesRegex(ValueError, "non-approved evidence license"):
            parse_dataset(payload)

    def test_manifest_binds_hashes_not_paths(self):
        result = calculate_rankings(self.institutions, self.education, self.employment, [], {"eligibility": {"min_events": 5}}, self.evidence)
        manifest = release_manifest({"dataset": "a" * 64}, DEFAULT_CONFIG, result)
        self.assertEqual(manifest["input_sha256"]["dataset"], "a" * 64)
        self.assertRegex(manifest["config_sha256"], r"^[0-9a-f]{64}$")

    def test_default_policy_is_stricter_than_demo(self):
        self.assertEqual(DEFAULT_CONFIG["eligibility"]["min_events"], 10)
        result = calculate_rankings(self.institutions, self.education, self.employment, self.awards,
                                    None, self.evidence)
        self.assertTrue(all(row["status"] != "ranked" for row in result["nodes"]))


if __name__ == "__main__":
    unittest.main()
