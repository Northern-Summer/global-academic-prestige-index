import unittest
from pathlib import Path
from tempfile import gettempdir
from unittest.mock import patch

from gapi.ror import ingest_ror, normalize_record


class RorNormalizationTests(unittest.TestCase):
    def test_normalizes_real_registry_contract_without_inventing_a_score(self):
        result = normalize_record({
            "id": "https://ror.org/03yrm5c26",
            "names": [
                {"value": "Test Research University", "types": ["ror_display", "label"]},
                {"value": "TRU", "types": ["acronym"]},
                {"value": "University of Test", "types": ["alias"]},
            ],
            "types": ["Education", "Facility"],
            "locations": [{"geonames_details": {"city": "Test City", "country_code": "GB", "country_name": "United Kingdom", "continent_name": "Europe"}}],
            "status": "active",
            "admin": {"last_modified": "2025-01-02"},
            "established": 1900,
            "external_ids": [{"type": "Wikidata", "all": ["Q12345"]}],
        })

        self.assertEqual(result["id"], "03yrm5c26")
        self.assertEqual(result["name"], "Test Research University")
        self.assertEqual(result["country_codes"], ["GB"])
        self.assertEqual(result["types"], ["Education", "Facility"])
        self.assertEqual(result["aliases"], ["University of Test"])
        self.assertEqual(result["external_ids"], {"Wikidata": ["Q12345"]})
        self.assertFalse(any("score" in key or "rank" in key for key in result))

    def test_rejects_invalid_registry_ids(self):
        with self.assertRaises(ValueError):
            normalize_record({"id": "https://example.com/not-a-ror", "names": []})

    def test_default_import_uses_and_removes_temporary_raw_cache(self):
        observed = {}

        def fake_import(output, cache, *, opener, sleep):
            observed["cache"] = cache
            self.assertTrue(cache.exists())
            return {"output": output.name}

        with patch("gapi.ror._ingest_ror_cached", side_effect=fake_import):
            result = ingest_ror(Path("institutions.json.gz"))

        self.assertEqual(result["output"], "institutions.json.gz")
        self.assertTrue(str(observed["cache"]).startswith(gettempdir()))
        self.assertFalse(observed["cache"].exists())

    def test_persistent_raw_cache_inside_project_is_rejected(self):
        from gapi.storage import project_root

        with self.assertRaisesRegex(ValueError, "outside the project"):
            ingest_ror(Path("institutions.json.gz"), cache_dir=project_root() / "data" / "real" / ".ror-cache")


if __name__ == "__main__":
    unittest.main()
