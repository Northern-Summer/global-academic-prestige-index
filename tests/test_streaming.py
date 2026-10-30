import gzip
import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from gapi.career_store import CareerStore
from gapi.global_sources import orcid_api_events, wikidata_events
from gapi.orcid_stream import SummaryParser, stream_archive
from gapi.ranking import pagerank
from gapi.task import BudgetStop, Task

PERSON = "0000-0002-1825-0097"
XML = b'''<record path="/0000-0002-1825-0097" xmlns:c="http://www.orcid.org/ns/common"><education-summary put-code="1"><role-title>PhD</role-title><end-date><c:year>2000</c:year></end-date><c:organization><c:name>University A</c:name><c:address><c:country>CN</c:country></c:address><c:disambiguated-organization><c:disambiguated-organization-identifier>https://ror.org/03vek6s52</c:disambiguated-organization-identifier><c:disambiguation-source>ROR</c:disambiguation-source></c:disambiguated-organization></c:organization></education-summary></record>'''


def archive(members):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as t:
        for name, content in members:
            header = tarfile.TarInfo(name)
            header.size = len(content)
            t.addfile(header, io.BytesIO(content))
    return gzip.compress(data.getvalue(), mtime=0)


class StreamingTests(unittest.TestCase):
    def setUp(self):
        import os
        self.tmp = tempfile.TemporaryDirectory(prefix="gapi-stream-test-", dir="D:/GAPI/global-academic-prestige-index" if os.name == "nt" else None)
        self.root = Path(self.tmp.name)
        self.config = json.loads(Path("configs/streaming.json").read_text())
        self.config["work_dir"] = str(self.root / "store")
        # Unit fixtures may run on any host; use D on this Windows environment.
        self.path = self.root / "task.json"

    def tearDown(self):
        self.tmp.cleanup()

    def store(self, data, *, checksum=None, **changes):
        self.config["sources"]["orcid-2026-summary"].update(checksum=checksum or "sha256:" + hashlib.sha256(data).hexdigest(), size_bytes=len(data))
        self.config.update(changes)
        self.path.write_text(json.dumps(self.config))
        task = Task(self.path)
        task.freeze()
        return CareerStore(task)

    def test_bounded_parser_and_stage(self):
        rows = []
        p = SummaryParser("p", "source", "member.xml", rows.append)
        for i in range(0, len(XML), 7):
            p.feed(XML[i:i+7])
        p.feed(b"", True)
        self.assertEqual(rows[0]["stage"], "doctorate")
        self.assertEqual(rows[0]["year_end"], 2000)
        self.assertEqual(rows[0]["institution"], "ror:03vek6s52")

    def test_brief_readonly_monitor_does_not_interrupt_member_commit(self):
        import sqlite3
        import threading
        s=self.store(archive([]))
        s.start('wikidata')
        reader=sqlite3.connect((s.task.root/'careers.sqlite').as_uri()+'?mode=ro',uri=True,check_same_thread=False)
        reader.execute('BEGIN')
        reader.execute('SELECT count(*) FROM members').fetchone()
        release=threading.Timer(.2,reader.rollback)
        release.start()
        try:
            with s.member('wikidata','Q1@1'):
                s.finish_member('wikidata','Q1@1','a'*64)
            self.assertIsNotNone(s.seen('wikidata','Q1@1'))
        finally:
            release.join()
            reader.close()
            s.close()

    def test_reader_wait_does_not_admit_a_second_writer(self):
        s=self.store(archive([]))
        try:
            with self.assertRaisesRegex(RuntimeError,'another task writer'):
                CareerStore(s.task)
        finally:
            s.close()

    def test_realistic_stream_complete_and_idempotent_replay(self):
        data = archive([(f"summaries/097/{PERSON}.xml", XML)])
        class Bounded(io.BytesIO):
            def read(self, n=-1):
                if n < 0 or n > 65536:
                    raise AssertionError("unbounded read")
                return super().read(n)
        s = self.store(data)
        try:
            first = stream_archive(s, "orcid-2026-summary", Bounded(data))
            second = stream_archive(s, "orcid-2026-summary", Bounded(data))
            self.assertEqual(first["events"], second["events"])
            self.assertEqual(second["sources"][0]["status"], "verified")
            self.assertEqual(second["events"], 1)
            self.assertFalse(any(p.suffix in (".gz", ".xml", ".tar") for p in s.task.root.rglob("*")))
        finally: s.close()

    def test_partial_resume_does_not_promote(self):
        data = archive([(f"summaries/097/{PERSON}.xml", XML)])
        s = self.store(data)
        try:
            a = stream_archive(s, "orcid-2026-summary", io.BytesIO(data), max_members=1)
            self.assertEqual(a["sources"][0]["status"], "provisional")
            b = stream_archive(s, "orcid-2026-summary", io.BytesIO(data))
            self.assertEqual(b["sources"][0]["status"], "verified")
            self.assertEqual(b["events"], 1)
        finally: s.close()

    def test_checksum_and_gzip_trailer_fail_closed(self):
        data = archive([(f"summaries/097/{PERSON}.xml", XML)])
        s = self.store(data)
        try:
            for broken in (data[:-5], data[:-8] + b"x"*8):
                with self.assertRaises((EOFError, OSError)):
                    stream_archive(s, "orcid-2026-summary", io.BytesIO(broken))
                self.assertEqual(s.report()["sources"][0]["status"], "failed")
        finally: s.close()

    def test_malicious_paths_and_entities_are_rejected_without_events(self):
        data = archive([("../../escape.xml", XML), (f"summaries/097/{PERSON}.xml", b'<!DOCTYPE r [<!ENTITY x "boom">]><r>&x;</r>')])
        s = self.store(data)
        try:
            result = stream_archive(s, "orcid-2026-summary", io.BytesIO(data))
            self.assertEqual(result["events"], 0)
            self.assertEqual(result["member_status"]["rejected"], 2)
        finally: s.close()

    def test_budget_and_version_changes_rejected(self):
        data = archive([])
        s = self.store(data)
        try:
            with self.assertRaises(BudgetStop): s.task.check(2*1024**3)
            s.task.config["rules_version"] = "changed"
            with self.assertRaises(ValueError): s.task.require_frozen()
        finally: s.close()

    def test_pagerank_boundary_conserves_probability_and_zero_edges(self):
        result, convergence = pagerank(["a", "b", "isolated"], {("a", "b"): 1, ("a", "outside"): 100, ("b", "a"): 0}, .85, 1e-12, 200)
        self.assertAlmostEqual(sum(result.values()), 1, places=12)
        self.assertGreater(result["b"], result["a"])
        self.assertTrue(convergence["converged"])

    def test_nonfinite_and_negative_weights_rejected(self):
        for weight in (float("nan"), float("inf"), -1):
            with self.assertRaises(ValueError): pagerank(["a"], {("a", "a"): weight}, .85, 1e-12, 200)

    def test_educated_at_does_not_infer_doctorate(self):
        entity = {"id":"Q1", "lastrevid":1, "claims":{"P69":[{"id":"Q1$test", "mainsnak":{"datavalue":{"value":{"id":"Q2"}}}}]}}
        row = list(wikidata_events(entity, "a"*64))[0]
        self.assertIsNone(row["stage"])
        self.assertEqual(row["status"], "unresolved")

    def test_oversized_member_is_consumed_and_rejected_without_raw_disk(self):
        data = archive([(f"summaries/097/{PERSON}.xml", XML)])
        s=self.store(data,max_member_bytes=64)
        try:
            result=stream_archive(s,'orcid-2026-summary',io.BytesIO(data))
            self.assertEqual(result['events'],0)
            self.assertEqual(result['member_status']['rejected'],1)
        finally:s.close()

    def test_wrong_upstream_hash_never_promotes(self):
        data=archive([(f"summaries/097/{PERSON}.xml",XML)])
        s=self.store(data,checksum='sha256:'+'0'*64)
        try:
            with self.assertRaisesRegex(ValueError,'upstream checksum mismatch'):
                stream_archive(s,'orcid-2026-summary',io.BytesIO(data))
            self.assertEqual(s.report()['sources'][0]['status'],'failed')
        finally:s.close()

    def test_process_member_change_rolls_back_without_duplication(self):
        data=archive([(f"summaries/097/{PERSON}.xml",XML)])
        s=self.store(data)
        try:
            stream_archive(s,'orcid-2026-summary',io.BytesIO(data),max_members=1)
            changed=archive([(f"summaries/097/{PERSON}.xml",XML.replace(b'PhD',b'MSc'))])
            with self.assertRaisesRegex(ValueError,'member changed'):
                stream_archive(s,'orcid-2026-summary',io.BytesIO(changed))
            self.assertEqual(s.report()['events'],1)
        finally:s.close()
