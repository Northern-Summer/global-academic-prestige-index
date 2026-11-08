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
        # Resource fixtures account for their own checkout, never a concurrent
        # production checkout with dependencies, logs and unrelated artifacts.
        from unittest.mock import patch
        project = self.root / 'project'
        project.mkdir()
        self.enterContext(patch('gapi.task.project_root', return_value=project))
        self.config = json.loads(Path("configs/streaming.json").read_text())
        self.config["work_dir"] = str(self.root / "store")
        # Unit fixtures may run on any host; use D on this Windows environment.
        self.path = self.root / "task.json"

    def tearDown(self):
        self.tmp.cleanup()

    def store(self, data, *, checksum=None, source_compression=False, **changes):
        self.config["sources"]["orcid-2026-summary"].update(checksum=checksum or "sha256:" + hashlib.sha256(data).hexdigest(), size_bytes=len(data))
        self.config.update(changes)
        self.path.write_text(json.dumps(self.config))
        task = Task(self.path)
        task.freeze()
        # Existing fixtures explicitly exercise the legacy writer. Source-codec
        # tests enable the new physical writer on the same source contract.
        return CareerStore(task,source_compression=source_compression)

    def test_bounded_parser_and_stage(self):
        rows = []
        p = SummaryParser("p", "source", "member.xml", rows.append)
        for i in range(0, len(XML), 7):
            p.feed(XML[i:i+7])
        p.feed(b"", True)
        self.assertEqual(rows[0]["stage"], "doctorate")
        self.assertEqual(rows[0]["year_end"], 2000)
        self.assertEqual(rows[0]["institution"], "ror:03vek6s52")

    def test_declared_archive_root_and_x_checksum_directory_parse(self):
        person='0000-0002-7869-831X'
        data=archive([(f'./ORCID_2026_10_summaries/31X/{person}.xml',
                       XML.replace(PERSON.encode(),person.encode()))])
        s=self.store(data)
        try:
            result=stream_archive(s,'orcid-2026-summary',io.BytesIO(data))
            self.assertEqual(result['events'],1)
            self.assertEqual(result['member_status'],{'observed':1})
            self.assertEqual(result['sources'][0]['status'],'verified')
        finally:s.close()

    def test_other_vintage_and_malicious_member_roots_remain_rejected(self):
        from gapi.orcid_stream import summary_member_pattern
        spec=self.config['sources']['orcid-2026-summary']
        pattern=summary_member_pattern(spec)
        for name in (f'ORCID_2025_10_summaries/097/{PERSON}.xml',
                     f'../ORCID_2026_10_summaries/097/{PERSON}.xml',
                     f'ORCID_2026_10_summaries/../097/{PERSON}.xml',
                     f'ORCID_2026_10_summaries/3XX/{PERSON}.xml',
                     f'ORCID_2026_10_summaries/31x/{PERSON}.xml'):
            with self.subTest(name=name):self.assertIsNone(pattern.fullmatch(name))
        data=archive([(f'ORCID_2025_10_summaries/097/{PERSON}.xml',XML)])
        s=self.store(data)
        try:
            result=stream_archive(s,'orcid-2026-summary',io.BytesIO(data))
            self.assertEqual(result['events'],0)
            self.assertEqual(result['member_status'],{'rejected':1})
        finally:s.close()

    def test_wrong_root_diagnostic_does_not_disclose_unexpected_person_identifier(self):
        rows=[]
        parser=SummaryParser('p','source',PERSON,rows.append,section='educations')
        unexpected='/private-other-person/educations'
        with self.assertRaisesRegex(ValueError,"element='educations'; identity_path=different") as raised:
            parser.feed(f'<educations path="{unexpected}"/>'.encode(),True)
        self.assertNotIn(unexpected,str(raised.exception))
        self.assertEqual(rows,[])
        parser=SummaryParser('p','source',PERSON,rows.append,section='educations')
        with self.assertRaisesRegex(ValueError,'identity_path=missing'):
            parser.feed(b'<educations/>',True)

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

    def test_new_member_limit_advances_across_reopened_prefix_replay(self):
        from unittest.mock import patch
        people=[PERSON,'0000-0002-7869-831X','0000-0001-5109-3700']
        data=archive([(f'summaries/097/{p}.xml',XML.replace(PERSON.encode(),p.encode())) for p in people])
        s=self.store(data)
        try:
            for expected in (1,2,3):
                with patch.object(s,'member',wraps=s.member) as writes:
                    report=stream_archive(s,'orcid-2026-summary',io.BytesIO(data),max_new_members=1)
                self.assertEqual(writes.call_count,1)
                self.assertEqual(report['events'],expected)
                self.assertEqual(report['sources'][0]['members'],expected)
                self.assertEqual(report['sources'][0]['status'],'provisional')
                task=s.task
                s.close()
                s=CareerStore(task,source_compression=False)
            with patch.object(s,'member',side_effect=AssertionError('replay must not open a write transaction')):
                complete=stream_archive(s,'orcid-2026-summary',io.BytesIO(data),max_new_members=1)
            self.assertEqual(complete['events'],3)
            self.assertEqual(complete['sources'][0]['status'],'verified')
            self.assertEqual(s.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
        finally:s.close()

    def test_new_limit_counts_rejected_members_without_weakening_scan_limit(self):
        data=archive([('../../invalid.xml',XML),(f'summaries/097/{PERSON}.xml',XML)])
        s=self.store(data)
        try:
            first=stream_archive(s,'orcid-2026-summary',io.BytesIO(data),max_new_members=1)
            self.assertEqual(first['member_status'],{'rejected':1})
            limited=stream_archive(s,'orcid-2026-summary',io.BytesIO(data),max_new_members=1,max_members=1)
            self.assertEqual(limited['events'],0)
            advanced=stream_archive(s,'orcid-2026-summary',io.BytesIO(data),max_new_members=1)
            self.assertEqual(advanced['events'],1)
            self.assertEqual(advanced['sources'][0]['status'],'provisional')
        finally:s.close()

    def test_invalid_new_member_limit_does_not_consume_or_start_source(self):
        from unittest.mock import patch
        data=archive([])
        s=self.store(data)
        try:
            for limit in (0,-1,True,1.5):
                with patch.object(s,'start',side_effect=AssertionError('must validate before starting')):
                    with self.assertRaisesRegex(ValueError,'positive integer'):
                        stream_archive(s,'orcid-2026-summary',io.BytesIO(data),max_new_members=limit)
        finally:s.close()

    def test_cli_selects_frozen_archive_and_reports_selected_source_failure(self):
        from contextlib import redirect_stdout
        from types import SimpleNamespace
        from unittest.mock import patch
        from gapi.stream_cli import execute_task
        source='orcid-2025-summary'
        data=archive([(f'summaries/097/{PERSON}.xml',XML)])
        self.config['sources'][source]={**self.config['sources']['orcid-2026-summary'],
            'filename':'ORCID_2025_10_summaries.tar.gz','url':'https://example.org/frozen-2025.tar.gz',
            'checksum':'sha256:'+hashlib.sha256(data).hexdigest(),'size_bytes':len(data)}
        s=self.store(data)
        task=s.task
        s.close()
        args=SimpleNamespace(command='ingest-orcid',source=source,max_members=None,
            max_new_members=1,max_seconds=10)
        urls=[]
        def fresh_fixture_source():
            # Each transport/report case starts independently. The production
            # annual CLI intentionally refuses a second scan of committed data.
            fixture=CareerStore(task)
            try:
                for table in ('events','members','runs'):
                    fixture.db.execute('DELETE FROM '+table+' WHERE source=?',(source,))
                fixture.db.commit()
            finally:fixture.close()
        class HTTP:
            def within(self,seconds):
                return __import__('contextlib').nullcontext()
            def open_archive(self,source):
                return self.open(task.config['sources'][source]['url'])
            def open(self,url):
                urls.append(url)
                return io.BytesIO(data)
            def close(self):pass
        with patch('gapi.stream_cli.PublicHTTP',return_value=HTTP()), \
             patch.object(CareerStore,'report',side_effect=MemoryError('whole-store report forbidden')):
            for state in ('provisional','verified'):
                fresh_fixture_source()
                args.max_new_members=1 if state=='provisional' else None
                output=io.StringIO()
                with redirect_stdout(output):self.assertTrue(execute_task(task,args))
                report=json.loads(output.getvalue())
                self.assertEqual(report['sources'][0]['source'],source)
                self.assertEqual(report['sources'][0]['status'],state)
                self.assertEqual(report['sources'][0]['members'],1)
                self.assertEqual(report['report_scope'],'committed-archive-run-only')
                self.assertNotIn('events',report)
        self.assertEqual(urls,['https://example.org/frozen-2025.tar.gz']*2)
        fresh_fixture_source()
        with patch.object(HTTP,'open',side_effect=OSError('bounded injected access failure')):
            with patch('gapi.stream_cli.PublicHTTP',return_value=HTTP()), \
                 patch.object(CareerStore,'report',side_effect=MemoryError('whole-store report forbidden')):
                with redirect_stdout(io.StringIO()),self.assertRaises(SystemExit) as raised:
                    execute_task(task,args)
                self.assertEqual(raised.exception.code,2)
        from urllib.error import HTTPError
        for url,kind in (
            ('https://example.org/robots.txt','robots'),
            ('https://example.org/frozen-2025.tar.gz','frozen-archive'),
            ('https://storage.example/object?signature=private-value','other-endpoint'),
        ):
            with self.subTest(request_kind=kind):
                output=io.StringIO()
                with patch.object(HTTP,'open',side_effect=HTTPError(url,403,'denied',{},None)), \
                     patch('gapi.stream_cli.PublicHTTP',return_value=HTTP()), \
                     patch.object(CareerStore,'report',side_effect=MemoryError('whole-store report forbidden')):
                    with redirect_stdout(output),self.assertRaises(SystemExit) as raised:
                        execute_task(task,args)
                self.assertEqual(raised.exception.code,2)
                result=json.loads(output.getvalue())
                self.assertEqual(result['archive_http_failure'],
                    {'status':403,'stage':'download','request_kind':kind})
                self.assertNotIn('private-value',output.getvalue())
                self.assertEqual(next(r for r in result['sources'] if r['source']==source)['status'],
                    'access_failed')
        class DeniedStream(io.BytesIO):
            def read(self, size=-1):
                raise HTTPError(task.config['sources'][source]['url'],403,'denied',{},None)
        output=io.StringIO()
        with patch.object(HTTP,'open',return_value=DeniedStream(data)), \
             patch('gapi.stream_cli.PublicHTTP',return_value=HTTP()), \
             patch.object(CareerStore,'report',side_effect=MemoryError('whole-store report forbidden')):
            with redirect_stdout(output),self.assertRaises(SystemExit) as raised:
                execute_task(task,args)
        self.assertEqual(raised.exception.code,2)
        result=json.loads(output.getvalue())
        self.assertEqual(result['archive_http_failure'],
            {'status':403,'stage':'archive-scan','request_kind':'frozen-archive'})
        self.assertEqual(next(r for r in result['sources'] if r['source']==source)['status'],'failed')
        args.source='wikidata'
        with patch.object(HTTP,'open',side_effect=AssertionError('invalid source must not open HTTP')):
            with patch('gapi.stream_cli.PublicHTTP',return_value=HTTP()):
                with self.assertRaisesRegex(ValueError,'annual ORCID archive'):
                    execute_task(task,args)

    def test_archive_receipt_reads_only_selected_run_and_no_career_tables(self):
        import sqlite3
        data=archive([(f'summaries/097/{PERSON}.xml',XML)])
        s=self.store(data)
        try:
            from unittest.mock import patch
            with patch.object(s,'report',side_effect=MemoryError('forbidden full report')):
                receipt=stream_archive(s,'orcid-2026-summary',io.BytesIO(data),full_report=False)
            self.assertEqual(receipt['sources'][0]['members'],1)
            self.assertEqual(receipt['sources'][0]['status'],'verified')
            s.db.execute("INSERT INTO runs(source,identity,status) VALUES('other','unrelated','failed')")
            s.db.commit()
            def allow_run_only(action,table,column,database,trigger):
                if action==sqlite3.SQLITE_READ and table!='runs':return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK
            s.db.set_authorizer(allow_run_only)
            bounded=s.archive_report('orcid-2026-summary')
            self.assertEqual(bounded['sources'],receipt['sources'])
            self.assertFalse(bounded['complete_global_census'])
            self.assertNotIn('member_status',bounded)
            with self.assertRaisesRegex(ValueError,'annual source'):
                s.archive_report('wikidata')
        finally:s.close()

    def test_invalid_report_mode_cannot_start_or_read_archive(self):
        from unittest.mock import patch
        data=archive([])
        s=self.store(data)
        try:
            with patch.object(s,'start',side_effect=AssertionError('must not start')):
                with self.assertRaisesRegex(ValueError,'full_report must be boolean'):
                    stream_archive(s,'orcid-2026-summary',io.BytesIO(data),full_report='false')
        finally:s.close()

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
            with self.assertRaises(BudgetStop): s.task.check(s.task.config['budget_bytes'])
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
