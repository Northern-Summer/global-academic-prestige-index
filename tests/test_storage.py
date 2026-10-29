import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from gapi.storage import project_root, require_external_cache
from gapi.openalex import ingest_institutions
from gapi.ror import ingest_ror
from gapi.us_faculty import ingest_and_rank


class RawCacheBoundaryTests(unittest.TestCase):
    def test_project_tree_is_rejected_as_persistent_raw_cache(self):
        with self.assertRaisesRegex(ValueError, "outside the project"):
            require_external_cache(project_root() / "data" / "real" / ".raw-cache")

    def test_external_directory_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            external = Path(temporary) / "raw-cache"
            self.assertEqual(require_external_cache(external), external.resolve())

    def test_configured_scratch_directory_is_used_and_cleaned(self):
        with tempfile.TemporaryDirectory() as temporary:
            scratch = Path(temporary) / "scratch"
            with patch.dict("os.environ", {"GAPI_SCRATCH_DIR": str(scratch)}):
                from gapi.storage import temporary_raw_cache

                with temporary_raw_cache("gapi-test-") as cache:
                    cache_path = Path(cache)
                    self.assertEqual(cache_path.parent, scratch.resolve())
                    self.assertTrue(cache_path.exists())
                self.assertFalse(cache_path.exists())

    def test_configured_project_scratch_is_rejected(self):
        with patch.dict("os.environ", {"GAPI_SCRATCH_DIR": str(project_root() / "work" / "raw")}):
            from gapi.storage import temporary_raw_cache

            with self.assertRaisesRegex(ValueError, "outside the project"):
                temporary_raw_cache("gapi-test-")

    def test_each_importer_cleans_partial_raw_files_on_failure(self):
        cases = [
            ("gapi.ror._ingest_ror_cached", 1, lambda out: ingest_ror(out)),
            ("gapi.openalex._ingest_institutions_cached", 1, lambda out: ingest_institutions(out)),
            ("gapi.us_faculty._ingest_and_rank_cached", 0, lambda out: ingest_and_rank(None, out)),
        ]
        for target, cache_index, run in cases:
            with self.subTest(importer=target), tempfile.TemporaryDirectory() as temporary:
                scratch = Path(temporary) / "scratch"
                output = Path(temporary) / "derived.json"
                output.write_bytes(b"prior verified snapshot")
                observed = []

                def fail_after_partial_download(*args, **kwargs):
                    cache = args[cache_index]
                    observed.append(cache)
                    (cache / "partial-source.part").write_bytes(b"partial source")
                    raise OSError("source interrupted")

                with patch.dict("os.environ", {"GAPI_SCRATCH_DIR": str(scratch)}):
                    with patch(target, side_effect=fail_after_partial_download):
                        with self.assertRaisesRegex(OSError, "source interrupted"):
                            run(output)
                self.assertEqual(len(observed), 1)
                self.assertFalse(observed[0].exists())
                self.assertEqual(list(scratch.iterdir()), [])
                self.assertEqual(output.read_bytes(), b"prior verified snapshot")


if __name__ == "__main__":
    unittest.main()
