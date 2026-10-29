import io
import json
import subprocess
import sys
import unittest

from gapi.career_rules import classify
from gapi.global_analysis import export_local, rank_global
from gapi.global_sources import ingest_orcid_api
from gapi.orcid_stream import private_person
import test_streaming as fixtures
from test_streaming import XML, PERSON, archive


class AnalysisTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store
    def seed(self,s):
        for source in ('wikidata','orcid-public-api'):
            s.start(source)
        def event(source,person,inst,kind,stage,start,end):
            return dict(source=source,person=person,institution=inst,kind=kind,stage=stage,year_start=start,year_end=end,locator='statement',status='observed',country='CN')
        with s.member('wikidata','statement'):
            s.event(event('wikidata','private-person','wd:Q1','education','doctorate',2000,2005))
            s.event(event('wikidata','private-person','wd:Q2','employment','academic',2010,None))
            s.finish_member('wikidata','statement','a'*64)
        with s.member('orcid-public-api','statement'):
            s.event(event('orcid-public-api','restricted-person','ringgold:123','education','master',2000,2001))
            s.event(event('orcid-public-api','restricted-person','ringgold:456','employment','academic',2015,None))
            s.finish_member('orcid-public-api','statement','b'*64)

    def test_public_filter_excludes_unapproved_source_in_all_fields(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            result=rank_global(s,public=True)
            text=json.dumps(result)
            self.assertNotIn('ringgold:',text)
            self.assertNotIn('private-person',text)
            self.assertNotIn('restricted-person',text)
            self.assertEqual(result['coverage']['events'],2)
            self.assertEqual(len(result['edges']),1)
            for node in result['nodes']:
                self.assertIsNone(node['award_score'])
                self.assertIsNone(node['composite_score'])
                self.assertIsNone(node['formal_rank'])
        finally:s.close()

    def test_repeat_output_is_byte_identical_and_probabilities_conserve(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            a=export_local(s)
            first=(s.task.root/'global-local.json').read_bytes()
            b=export_local(s)
            self.assertEqual(first,(s.task.root/'global-local.json').read_bytes())
            self.assertEqual(a['manifest'],b['manifest'])
            result=json.loads(first)
            self.assertAlmostEqual(sum(n['network_score'] or 0 for n in result['nodes']),1)
            self.assertTrue(result['sensitivity']['delete_source_wikidata']['converged'])
        finally:s.close()

    def test_process_exit_releases_writer_and_rolls_back_current_member(self):
        s=self.store(archive([]));s.close()
        code='''import os,sys
from pathlib import Path
from gapi.task import Task
from gapi.career_store import CareerStore
s=CareerStore(Task(Path(sys.argv[1])))
s.start('wikidata')
s.db.execute('BEGIN IMMEDIATE')
s.db.execute("INSERT INTO members VALUES('wikidata','uncommitted','dead','pending',NULL)")
os._exit(19)
'''
        import os
        env=dict(os.environ,PYTHONPATH=str(__import__('pathlib').Path('src').resolve()))
        child=subprocess.run([sys.executable,'-c',code,str(self.path)],env=env,timeout=10,capture_output=True)
        self.assertEqual(child.returncode,19,child.stderr)
        from gapi.career_store import CareerStore
        from gapi.task import Task
        s=CareerStore(Task(self.path))
        try:
            self.assertIsNone(s.seen('wikidata','uncommitted'))
            self.assertEqual(s.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
        finally:s.close()

    def test_strict_api_uses_xml_incremental_reads_without_person_tree(self):
        class Response(io.BytesIO):
            headers={'Content-Type':'application/vnd.orcid+xml'}
            def read(self,n=-1):
                self_limit=65536
                if n<0 or n>self_limit: raise AssertionError('unbounded read')
                return super().read(n)
        class HTTP:
            def open(self,url,accept):
                if accept!='application/vnd.orcid+xml': raise AssertionError('must use XML')
                return Response(XML)
        s=self.store(archive([]))
        try:
            result=ingest_orcid_api(s,HTTP(),[PERSON])
            self.assertEqual(result['events'],1)
            payload=json.loads(s.db.execute('SELECT payload FROM events').fetchone()[0])
            self.assertEqual(payload['transport'],'incremental-xml-no-tree')
        finally:s.close()

    def test_candidate_and_honorary_degrees_are_not_promoted(self):
        for value in ('PhD candidate','honorary PhD','PhD student','在读博士'):
            self.assertIsNone(classify('education',value)[1])

    def test_intensity_denominator_resolves_aliases_and_deduplicates_people(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:
                s.db.executemany('INSERT INTO institution_aliases VALUES(?,?,?)',[
                    ('wd:Q1','ror:example','test'),('ringgold:123','ror:example','test')])
            result=rank_global(s)
            node=next(n for n in result['nodes'] if n['institution_id']=='ror:example')
            self.assertEqual(node['observed_training_denominator'],2)
            public=rank_global(s,public=True)
            node=next(n for n in public['nodes'] if n['institution_id']=='ror:example')
            self.assertEqual(node['observed_training_denominator'],1)
        finally:s.close()


