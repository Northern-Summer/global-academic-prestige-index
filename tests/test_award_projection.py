import gzip
import hashlib
import json
import unittest
from unittest.mock import patch

from gapi.award_projection import label_attributions,exact_candidates,project_awards,public_projection
from gapi.task import canonical,digest
from gapi.nobel import ingest_nobel
from test_nobel import HTTP,page,laureate
import test_streaming as fixtures
from test_streaming import archive


def label(name='Official University',country='USA'):
    return {'name':name,'country':country,'city':'City'}


def row(key,labels,kind='personal',portion='1/2'):
    return (key,'PRIVATE-PERSON',2000,'Physics',kind,portion,json.dumps(labels),0)


class LabelAttributionTests(unittest.TestCase):
    def test_single_multiple_and_institutional_prizes_stay_distinct(self):
        one,two=label(),label('Other University')
        result,_=label_attributions([row('1',[one]),row('2',[one,two]),row('3',[one],'institutional')])
        self.assertEqual(result[digest(one)]['nobel_share_single_affiliation'],{'numerator':'1','denominator':'2'})
        self.assertEqual(result[digest(one)]['nobel_share_equal_split_sensitivity'],{'numerator':'3','denominator':'4'})
        self.assertEqual(result[digest(two)]['nobel_share_single_affiliation'],{'numerator':'0','denominator':'1'})
        self.assertEqual(result[digest(one)]['institutional_events'],1)
        self.assertNotIn('PRIVATE-PERSON',json.dumps(result))
        with self.assertRaises(ValueError):label_attributions([row('4',[one,one])])

    def test_exact_country_match_does_not_guess_similar_names_or_historical_identity(self):
        labels={digest(v):{**v,'first_award_year':1901} for v in [label(),label('Similar University'),label('Official University','Unknown')]}
        record={'id':'123','name':'Official University','aliases':['Official Uni'],
                'status':'active','country_code':'US','geo':{'country_name':'United States'},'established':1950}
        result=exact_candidates(labels,[record])
        self.assertEqual(len(result[digest(label())]),1)
        self.assertTrue(result[digest(label())][0]['historical_identity_requires_review'])
        self.assertEqual(result[digest(label('Similar University'))],[])
        self.assertEqual(result[digest(label('Official University','Unknown'))],[])
        result=exact_candidates(labels,[record,{**record,'id':'456'}])
        self.assertEqual(len(result[digest(label())]),2)  # ambiguity preserved


class ProjectionStoreTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_projection_is_idempotent_candidates_are_not_merges_and_stale_input_fails(self):
        from pathlib import Path
        from gapi.nobel import load_source
        s=self.store(archive([]))
        try:
            ingest_nobel(s,HTTP([page(0,[laureate(affiliations=[{'name':{'en':'Official University'},'country':{'en':'USA'}}])],1)]),load_source(Path('configs/nobel-20261029.json')))
            directory=self.root/'data/real';directory.mkdir(parents=True)
            content=gzip.compress(canonical({'records':[{'id':'123','name':'Official University','aliases':[],
                'status':'active','country_code':'US','geo':{'country_name':'United States'},'established':1800}]}),mtime=0)
            (directory/'institutions.json.gz').write_bytes(content)
            with patch('gapi.storage.project_root',return_value=self.root),patch('gapi.award_projection.ROR_SHA256',hashlib.sha256(content).hexdigest()):
                a=canonical(project_awards(s));self.assertEqual(canonical(project_awards(s)),a)
                self.assertEqual(json.loads(a)['unique_candidate_labels'],1)
                self.assertEqual(s.db.execute('SELECT count(*) FROM institution_aliases').fetchone()[0],0)
                self.assertEqual(s.db.execute('SELECT count(*) FROM entities').fetchone()[0],0)
                with s.db:s.db.execute("UPDATE official_awards SET portion='1/3'")
                with self.assertRaisesRegex(ValueError,'stale'):public_projection(s)
            self.assertEqual(s.db.execute('SELECT count(*) FROM award_label_projection').fetchone()[0],1)
        finally:s.close()
