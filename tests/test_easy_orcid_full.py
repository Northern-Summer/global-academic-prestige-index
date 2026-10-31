import json
import unittest
from unittest.mock import patch
import test_easy_orcid_landing as fixtures
from gapi.easy_orcid_landing import ingest, rank_projection
from gapi.easy_orcid_full import ingest_full
from gapi.event_storage import configure_reader
from gapi.task import BudgetStop


class FullDeltaTests(unittest.TestCase):
    store=fixtures.LandingTests.store
    source=fixtures.LandingTests.source
    record=fixtures.LandingTests.record

    def setup_sources(self):
        s=self.store();record=self.record()
        spec,http=self.source([record]);spec['derived_from']='fixture'
        ingest(s,http,spec,max_seconds=5)
        configure_reader(s.db)
        return s,record

    def full_source(self,rows):
        spec,http=self.source(rows)
        spec.update(id='easy-orcid-2023.6-full',derived_from='fixture')
        return spec,http

    def test_exact_match_reuses_hq_and_changed_payload_is_separate(self):
        s,record=self.setup_sources()
        changed=json.loads(json.dumps(record));changed['educations'][0]['end']['year']=1999
        spec,http=self.full_source([record,changed])
        r=ingest_full(s,http,spec,max_seconds=5)
        self.assertTrue(r['source_complete']);self.assertEqual(r['reused_hq_source_rows'],1)
        self.assertEqual(r['committed_delta_rows'],1)
        self.assertTrue(r['all_hq_projections_present'])
        self.assertEqual(r['distinct_hq_rows_reused'],1)
        before=s.db.execute('SELECT row_number,person,row_sha256,payload AS projected_payload FROM easy_full_delta').fetchall()
        again=ingest_full(s,http,spec,max_seconds=5)
        self.assertEqual(r['exact_reuse_sha256'],again['exact_reuse_sha256'])
        self.assertEqual(before,s.db.execute('SELECT row_number,person,row_sha256,payload AS projected_payload FROM easy_full_delta').fetchall())
        self.assertEqual(s.db.execute('SELECT status FROM easy_career_state').fetchone()[0],'verified')

    def test_prefix_and_wrong_checksum_cannot_verify_delta(self):
        s,record=self.setup_sources();spec,http=self.full_source([record,record])
        self.assertFalse(ingest_full(s,http,spec,max_records=1,max_seconds=5)['source_complete'])
        with s.db:s.db.execute('DELETE FROM easy_full_state')
        spec['md5']='0'*32
        self.assertFalse(ingest_full(s,http,spec,max_seconds=5)['source_complete'])
        self.assertEqual(s.db.execute('SELECT status FROM easy_career_state').fetchone()[0],'verified')

    def test_hq_and_full_identities_cannot_be_mixed(self):
        s,record=self.setup_sources();spec,http=self.full_source([record])
        with self.assertRaisesRegex(ValueError,'lineage'):
            ingest_full(s,http,dict(spec,derived_from='other'),max_seconds=5)
        ingest_full(s,http,spec,max_seconds=5)
        with self.assertRaisesRegex(ValueError,'identity'):
            ingest_full(s,http,dict(spec,version='other'),max_seconds=5)
        with s.db:s.db.execute("UPDATE easy_career_state SET status='provisional'")
        with self.assertRaisesRegex(ValueError,'verified HQ'):
            ingest_full(s,http,spec,max_seconds=5)

    def test_full_score_stream_merges_persons_and_counts_new_relations_once(self):
        s,record=self.setup_sources()
        changed=json.loads(json.dumps(record));changed['employments'][0]['xrefs']['ror']='03yrm5c26'
        spec,http=self.full_source([record,record,changed])
        result=ingest_full(s,http,spec,max_seconds=5)
        self.assertEqual(result['reused_hq_source_rows'],2)
        self.assertEqual(result['distinct_hq_rows_reused'],1)
        s.db.executescript('CREATE TABLE active_institution_aliases(alias,institution); CREATE TABLE entities(id,name,country,payload);')
        # Same person has overlapping employers: the original joint-role rule
        # must exclude the relationship, including across reused and new rows.
        with patch('gapi.global_analysis.resolve_institutions'):
            output=rank_projection(s,include_full=True)
        self.assertEqual(output['distinct_retained_people'],1)
        self.assertEqual(output['deduplicated_relations'],0)
        self.assertEqual(output['source'],'easy-orcid-2023.6-full')

    def test_full_score_refuses_missing_hq_reuse_coverage(self):
        s,record=self.setup_sources();spec,http=self.full_source([])
        result=ingest_full(s,http,spec,max_seconds=5)
        self.assertTrue(result['source_complete']);self.assertFalse(result['all_hq_projections_present'])
        with self.assertRaisesRegex(ValueError,'every reused HQ'):
            rank_projection(s,include_full=True)

    def test_duplicate_hq_rows_use_projection_content_coverage(self):
        for repeats in (1,2):
            with self.subTest(full_repeats=repeats):
                s=self.store();record=self.record()
                spec,http=self.source([record,record]);spec['derived_from']='fixture'
                ingest(s,http,spec,max_seconds=5);configure_reader(s.db)
                spec,http=self.full_source([record]*repeats)
                result=ingest_full(s,http,spec,max_seconds=5)
                self.assertTrue(result['source_complete'])
                self.assertTrue(result['all_hq_projections_present'])
                self.assertEqual(result['distinct_hq_rows_reused'],2)
                self.assertEqual(result['committed_delta_rows'],0)

    def test_full_score_refuses_mismatched_hq_identity(self):
        s,record=self.setup_sources();spec,http=self.full_source([record])
        ingest_full(s,http,spec,max_seconds=5)
        with s.db:s.db.execute("UPDATE easy_career_state SET identity='other'")
        with self.assertRaisesRegex(ValueError,'HQ identity'):
            rank_projection(s,include_full=True)

    def test_capacity_stop_retains_hq_and_prior_delta_then_resume_is_idempotent(self):
        s,record=self.setup_sources();changed=json.loads(json.dumps(record))
        changed['educations'][0]['end']['year']=1999
        other=json.loads(json.dumps(record));other['educations'][0]['end']['year']=1998
        spec,http=self.full_source([record,changed,other]);calls=0
        def check(*args):
            nonlocal calls
            calls+=1
            if calls==4:raise BudgetStop('delta capacity')
        original=s.task.check;s.task.check=check
        r=ingest_full(s,http,spec,max_seconds=5)
        self.assertFalse(r['source_complete']);self.assertIn('delta capacity',r['error'])
        self.assertEqual(r['committed_delta_rows'],1)
        self.assertEqual(s.db.execute('SELECT status FROM easy_career_state').fetchone()[0],'verified')
        s.task.check=original
        r=ingest_full(s,http,spec,max_seconds=5)
        self.assertTrue(r['source_complete']);self.assertEqual(r['committed_delta_rows'],2)
