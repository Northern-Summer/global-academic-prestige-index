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

    def test_versioned_noncommercial_publication_policy_allows_only_aggregate_fields(self):
        from pathlib import Path
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:
                s.db.execute('''UPDATE events SET role=CASE WHEN kind='education' THEN 'Master' ELSE 'Professor' END,
                    payload=json_set(gapi_event_text(payload),'$.role',CASE WHEN kind='education' THEN 'Master' ELSE 'Professor' END)
                    WHERE source='orcid-public-api' ''')
            policy=json.loads(Path('configs/publication-20261029.json').read_text())
            result=rank_global(s,public=True,publication_policy=policy)
            self.assertEqual(len(result['edges']),2)
            text=json.dumps(result)
            self.assertNotIn('restricted-person',text)
            self.assertNotIn('private-person',text)
            self.assertNotIn('put_code',text)
            self.assertIsNone(result['nodes'][0]['formal_rank'])
            policy['purpose']='revenue-generating'
            with self.assertRaises(ValueError):rank_global(s,public=True,publication_policy=policy)
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

    def test_source_deletion_rejoins_surviving_alternative_evidence(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            # The same person's doctorate has a second source, which must survive
            # removal of the first source before the person-level join/dedup.
            with s.member('orcid-public-api','alternative'):
                s.event(dict(source='orcid-public-api',person='private-person',institution='wd:Q1',
                    kind='education',stage='doctorate',role='PhD',year_start=2000,year_end=2005,
                    locator='alternative',status='observed',country='CN'))
                s.event(dict(source='orcid-public-api',person='private-person',institution='wd:Q2',
                    kind='employment',stage='academic',role='Professor',year_start=2010,year_end=None,
                    locator='alternative',status='observed',country='CN'))
                s.finish_member('orcid-public-api','alternative','c'*64)
            result=rank_global(s)
            remaining=result['sensitivity']['delete_source_wikidata']
            self.assertEqual(remaining['edges'],1)
            self.assertFalse(remaining['prior_only'])
            self.assertEqual(result['sensitivity']['delete_source_orcid-public-api']['edges'],1)
        finally:s.close()

    def seed_conflicting_jobs(self, store, *, include_reference=True):
        for source in ('wikidata','orcid-public-api'):
            store.start(source)
        facts = [
            ('orcid-public-api','conflicted-person','X','education','doctorate',1990,1995,'PhD'),
            ('wikidata','conflicted-person','Y','employment','academic',2000,None,'Professor'),
            ('orcid-public-api','conflicted-person','Z','employment','academic',2000,None,'Professor'),
        ]
        if include_reference:
            facts += [
                ('orcid-public-api','reference-person','V','education','doctorate',1990,1995,'PhD'),
                ('orcid-public-api','reference-person','W','employment','academic',2000,None,'Professor'),
            ]
        for source in ('wikidata','orcid-public-api'):
            with store.member(source,'conflict-fixture'):
                for fact_source,person,institution,kind,stage,start,end,role in facts:
                    if fact_source != source:continue
                    store.event(dict(source=source,person=person,institution=institution,kind=kind,
                        stage=stage,year_start=start,year_end=end,role=role,locator='conflict-fixture',
                        status='observed',country='CN'))
                store.finish_member(source,'conflict-fixture','d'*64)

    def test_relation_evidence_is_independent_of_event_id_order(self):
        s=self.store(archive([]))
        try:
            for source in ('wikidata','orcid-public-api'):
                s.start(source)
                with s.member(source,'same-facts'):
                    for inst,kind,stage,start,end,role in (
                        ('X','education','doctorate',2000,2005,'PhD'),
                        ('Y','employment','academic',2010,None,'Professor')):
                        s.event(dict(source=source,person='same-person',institution=inst,kind=kind,
                            stage=stage,year_start=start,year_end=end,role=role,locator='same-facts',
                            status='corroborated' if source=='orcid-public-api' else 'observed'))
                    s.finish_member(source,'same-facts','a'*64)
            original=rank_global(s)
            node=next(n for n in original['nodes'] if n['institution_id']=='X')
            self.assertEqual(node['relations'],1)
            self.assertEqual(node['corroborated_relations'],1)
            self.assertEqual(node['source_classes'],['orcid-public-api','wikidata'])
            # Mutate only ordering keys, never facts or source records.
            with s.db:
                s.db.execute("UPDATE events SET id='z-'||id WHERE source='wikidata'")
            self.assertEqual(original,rank_global(s))
        finally:s.close()

    def test_source_deletion_reports_filtered_edges_and_expanded_universe(self):
        s=self.store(archive([]))
        try:
            self.seed_conflicting_jobs(s)
            result=rank_global(s)
            self.assertEqual(result['algorithm']['network_universe'],['V','W'])
            self.assertEqual(result['algorithm']['method_version'],'gapi-observed-network/3')
            fixed=result['sensitivity']['delete_source_wikidata']
            self.assertEqual((fixed['available_edges'],fixed['edges'],fixed['filtered_boundary_edges']),(2,1,1))
            self.assertEqual(fixed['new_endpoints'],['X','Z'])
            self.assertEqual(set(fixed['ranks']),{'V','W'})
            self.assertAlmostEqual(fixed['probability_mass'],1,places=12)
            expanded=result['expanded_universe_sensitivity']['delete_source_wikidata']
            self.assertEqual((expanded['available_edges'],expanded['edges'],expanded['filtered_boundary_edges']),(2,2,0))
            self.assertEqual(set(expanded['ranks']),{'V','W','X','Z'})
            self.assertAlmostEqual(expanded['probability_mass'],1,places=12)
            self.assertEqual(expanded['universe_kind'],'reference-plus-new-endpoints')
            self.assertIn('fixed-reference-universe',result['uncertainty']['rank_range_scope'])
            by_id={node['institution_id']:node for node in result['nodes']}
            self.assertIsNone(by_id['X']['rank_range'])
            for node in ('V','W'):
                fixed_ranks=[scenario['ranks'][node] for scenario in result['sensitivity'].values()
                             if not scenario['prior_only']]
                self.assertEqual(by_id[node]['rank_range'],[min(fixed_ranks),max(fixed_ranks)])
            self.assertEqual(result,rank_global(s))
        finally:s.close()

    def test_boundary_only_scenario_does_not_claim_fixed_universe_evidence(self):
        s=self.store(archive([]))
        try:
            self.seed_conflicting_jobs(s,include_reference=False)
            result=rank_global(s)
            self.assertEqual(result['algorithm']['network_universe'],[])
            fixed=result['sensitivity']['delete_source_wikidata']
            self.assertEqual((fixed['available_edges'],fixed['edges'],fixed['filtered_boundary_edges']),(1,0,1))
            self.assertTrue(fixed['prior_only'])
            self.assertEqual(fixed['probability_mass'],0)
            expanded=result['expanded_universe_sensitivity']['delete_source_wikidata']
            self.assertFalse(expanded['prior_only'])
            self.assertEqual(expanded['new_endpoints'],['X','Z'])
            self.assertAlmostEqual(expanded['probability_mass'],1,places=12)
            self.assertGreater(expanded['ranks']['Z'],expanded['ranks']['X'])
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
                section=url.rsplit('/',1)[-1]
                if section not in ('educations','employments'):raise AssertionError('irrelevant person/works data requested')
                body=XML.replace(b'<record path="/'+PERSON.encode()+b'"',
                                 b'<educations path="/'+PERSON.encode()+b'/educations"').replace(b'</record>',b'</educations>')
                return Response(body if section=='educations' else b'<employments path="/'+PERSON.encode()+b'/employments"/>')
        s=self.store(archive([]))
        try:
            result=ingest_orcid_api(s,HTTP(),[PERSON])
            self.assertEqual(result['events'],1)
            payload=json.loads(s.db.execute('SELECT payload FROM events').fetchone()[0])
            self.assertEqual(payload['transport'],'incremental-affiliation-xml/v1')
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],2)
        finally:s.close()

    def test_candidate_and_honorary_degrees_are_not_promoted(self):
        for value in ('PhD candidate','honorary PhD','PhD student','在读博士'):
            self.assertIsNone(classify('education',value)[1])

    def test_second_affiliation_failure_rolls_back_all_events_and_parts(self):
        class Response(io.BytesIO):
            headers={'Content-Type':'application/vnd.orcid+xml'}
        class HTTP:
            def open(self,url,accept):
                if url.endswith('/employments'):raise OSError('truncated second section')
                return Response(XML.replace(b'<record path="/'+PERSON.encode()+b'"',
                    b'<educations path="/'+PERSON.encode()+b'/educations"').replace(b'</record>',b'</educations>'))
        s=self.store(archive([]))
        try:
            ingest_orcid_api(s,HTTP(),[PERSON])
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],0)
            self.assertIsNone(s.seen('orcid-public-api',PERSON))
            self.assertEqual(s.db.execute('SELECT attempts FROM discovery_retries').fetchone()[0],1)
        finally:s.close()

    def test_affiliation_identity_and_wrong_section_fail_closed(self):
        from gapi.orcid_stream import SummaryParser
        for body in (b'<educations path="/different/educations"/>',
                     b'<educations path="/'+PERSON.encode()+b'/educations"><employment-summary/></educations>'):
            parser=SummaryParser('private','source',PERSON,lambda event:None,section='educations')
            with self.subTest(body=body),self.assertRaises(ValueError):
                parser.feed(body,True)

    def test_access_denial_pauses_one_source_without_requesting_other_people(self):
        from urllib.error import HTTPError
        class HTTP:
            calls=0
            def open(self,url,accept):
                self.calls+=1
                raise HTTPError(url,403,'Forbidden',{},None)
        s=self.store(archive([]))
        try:
            http=HTTP()
            ingest_orcid_api(s,http,[PERSON,'0000-0001-5109-3700'])
            self.assertEqual(http.calls,1)
            self.assertEqual(s.db.execute("SELECT status FROM runs WHERE source='orcid-public-api'").fetchone()[0],'access_failed')
        finally:s.close()

    def test_server_pause_survives_reopen_and_prevents_all_source_requests(self):
        from gapi.career_store import CareerStore
        from gapi.task import Task
        from gapi.source_http import SourcePause
        import time
        class HTTP:
            calls=0
            def open(self,url,accept):
                self.calls+=1
                raise SourcePause(429,time.time()+120,'server requested two minutes')
        s=self.store(archive([]))
        http=HTTP()
        ingest_orcid_api(s,http,[PERSON,'0000-0001-5109-3700'])
        self.assertEqual(http.calls,1)
        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_retries').fetchone()[0],0)
        self.assertEqual(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
        s.close()
        s=CareerStore(Task(self.path))
        try:
            ingest_orcid_api(s,http,[PERSON])
            self.assertEqual(http.calls,1)
            self.assertFalse(s.source_ready('orcid-public-api'))
        finally:s.close()

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


