import gzip
import json
import tempfile
import unittest
from unittest.mock import patch
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from email.message import Message
from urllib.parse import parse_qs, urlparse

from gapi.openalex import _request_page, ingest_institutions


def institution(openalex_id, name, country="US"):
    return {
        "id": f"https://openalex.org/{openalex_id}",
        "ror": None,
        "display_name": name,
        "display_name_acronyms": [],
        "alternate_names": [],
        "country_code": country,
        "type": "education",
        "geo": None,
        "works_count": 12,
        "cited_by_count": 34,
        "created_date": "2020-01-01",
        "updated_date": "2025-01-01",
    }


class OpenAlexIngestionTests(unittest.TestCase):
    def test_default_page_cache_is_temporary_and_removed(self):
        observed = {}

        def fake_import(output, checkpoint_dir, **kwargs):
            observed["cache"] = checkpoint_dir
            self.assertTrue(checkpoint_dir.exists())
            return {"record_count": 0}

        with patch("gapi.openalex._ingest_institutions_cached", side_effect=fake_import):
            result = ingest_institutions(Path("institutions.json.gz"))

        self.assertEqual(result["record_count"], 0)
        self.assertTrue(str(observed["cache"]).startswith(tempfile.gettempdir()))
        self.assertFalse(observed["cache"].exists())

    def test_persistent_raw_cache_inside_project_is_rejected(self):
        from gapi.storage import project_root

        with self.assertRaisesRegex(ValueError, "outside the project"):
            ingest_institutions(
                Path("institutions.json.gz"),
                checkpoint_dir=project_root() / "data" / "real" / ".openalex-cache",
            )

    def test_import_is_resumable_and_emits_a_verifiable_compressed_snapshot(self):
        pages = {
            "*": {"meta": {"count": 2, "next_cursor": "cursor-2"}, "results": [institution("I2", "Zulu University")]},
            "cursor-2": {"meta": {"count": 2, "next_cursor": None}, "results": [institution("I1", "Alpha University", "GB")]},
        }

        def opener(request, timeout):
            cursor = parse_qs(urlparse(request.full_url).query)["cursor"][0]
            return BytesIO(json.dumps(pages[cursor]).encode())

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / "institutions.json.gz"
            cache = root / "cache"
            partial = ingest_institutions(output, checkpoint_dir=cache, max_pages=1, opener=opener, sleep=lambda _seconds: None)
            self.assertFalse(partial["complete"])
            self.assertEqual(partial["record_count"], 1)

            complete = ingest_institutions(output, checkpoint_dir=cache, opener=opener, sleep=lambda _seconds: None)
            with gzip.open(output, "rt", encoding="utf-8") as stream:
                data = json.load(stream)
            self.assertTrue(complete["complete"])
            self.assertEqual(complete["record_count"], 2)
            self.assertEqual([row["name"] for row in data["records"]], ["Alpha University", "Zulu University"])
            self.assertEqual(len({row["id"] for row in data["records"]}), 2)
            self.assertEqual(complete["output_sha256"], __import__("hashlib").sha256(output.read_bytes()).hexdigest())

    def test_retries_rate_limit_using_retry_after(self):
        attempts = []
        delays = []
        headers = Message()
        headers["Retry-After"] = "3"

        def opener(request, timeout):
            attempts.append(request)
            if len(attempts) == 1:
                raise HTTPError(request.full_url, 429, "rate limited", headers, BytesIO())
            return BytesIO(b'{"ok":true}')

        self.assertEqual(_request_page("https://example.org", opener=opener, sleep=delays.append), {"ok": True})
        self.assertEqual(len(attempts), 2)
        self.assertEqual(delays, [3.0])


if __name__ == "__main__":
    unittest.main()
