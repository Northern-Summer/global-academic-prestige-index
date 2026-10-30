"""Batch transport must preserve the single-entity evidence/privacy contract."""
import json
import unittest
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from gapi.global_sources import ingest_wikidata, ingest_orcid_api
from gapi.source_http import StageDeadline, entity_batch_route
from gapi.task import digest
import test_streaming as fixtures
from test_streaming import archive, PERSON, XML
from test_source_http import Response


def entity(qid):
    def value(v):return {'datavalue':{'value':v}}
    return {'id':qid,'lastrevid':2,'claims':{
        'P496':[{'mainsnak':value(PERSON)}],
        'P69':[{'id':qid+'$degree','mainsnak':value({'id':'Q100'}),
                'qualifiers':{'P512':[value({'id':'Q752297'})],
                              'P582':[value({'time':'+2000-01-01T00:00:00Z','precision':9})]},
                'references':[]}]}}


class BatchHTTP:
    def __init__(self, rows=None):
        self.rows=rows or {}; self.calls=[]; self.bytes_received=0
    def json(self,url):
        self.calls.append(url)
        ids=parse_qs(urlsplit(url).query).get('ids')
        ids=ids[0].split('|') if ids else [url.rsplit('/',1)[-1].removesuffix('.json')]
        body={'entities':{qid:self.rows.get(qid,entity(qid)) for qid in ids}}
        self.bytes_received+=100
        return body,digest(body)


class BatchTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_ten_entities_one_request_and_idempotent_replay(self):
        s=self.store(archive([]));http=BatchHTTP();ids=['Q'+str(i) for i in range(1,11)]
        try:
            ingest_wikidata(s,http,ids,batch_size=10)
            self.assertEqual(len(http.calls),1)
            self.assertTrue(entity_batch_route(urlsplit(http.calls[0])))
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],10)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],10)
            self.assertEqual(s.pending('ingest-orcid-api'),[PERSON])
            raw=s.db.execute('SELECT payload FROM events LIMIT 1').fetchone()[0]
            self.assertEqual(json.loads(raw)['transport'],'wikibase-claims-info-batch/v1')
            self.assertEqual(s.db.execute('SELECT sha256 FROM members WHERE locator=?',('Q1@2',)).fetchone()[0],digest(entity('Q1')))
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],10)
            ingest_wikidata(s,http,ids,batch_size=10)
            self.assertEqual(len(http.calls),1)
            self.assertFalse(list(s.task.root.rglob('*.xml')))
        finally:s.close()

    def test_one_bad_entity_rolls_back_without_losing_its_neighbors(self):
        bad=entity('Q2');bad['claims']['P69'].append({'mainsnak':[]})
        s=self.store(archive([]))
        try:
            ingest_wikidata(s,BatchHTTP({'Q2':bad}),['Q1','Q2','Q3'],batch_size=10)
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT identifier FROM discovery_retries').fetchall(),[('Q2',)])
        finally:s.close()

    def test_unexpected_entity_keys_fail_closed(self):
        s=self.store(archive([]));http=BatchHTTP()
        try:
            with patch.object(http,'json',return_value=({'entities':{'Q99':entity('Q99')}},'a'*64)):
                ingest_wikidata(s,http,['Q1','Q2'],batch_size=10)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],0)
        finally:s.close()

    def test_nested_item_type_pollution_is_isolated_to_one_person(self):
        s=self.store(archive([]));bad=entity('Q2')
        bad['claims']['P69'][0]['qualifiers']['P512'][0]['datavalue']['value']['id']=[]
        try:
            ingest_wikidata(s,BatchHTTP({'Q2':bad}),['Q1','Q2','Q3'],batch_size=10)
            self.assertEqual(s.db.execute('SELECT locator FROM members ORDER BY locator').fetchall(),[('Q1@2',),('Q3@2',)])
            self.assertEqual(s.db.execute('SELECT identifier FROM discovery_retries').fetchall(),[('Q2',)])
        finally:s.close()

    def test_time_scalars_are_validated_without_truncating_extended_years(self):
        from gapi.global_sources import qualifier_year
        def claim(value):return {'qualifiers':{'P582':[{'datavalue':{'value':value}}]}}
        for value in ({'time':[],'precision':9},{'time':'+2000-00-00T00:00:00Z','precision':[]},
                      {'time':'not-a-date','precision':9}):
            with self.subTest(value=value),self.assertRaises(ValueError):qualifier_year(claim(value),'P582')
        self.assertIsNone(qualifier_year(claim({'time':'-1990-00-00T00:00:00Z','precision':9}),'P582'))
        self.assertIsNone(qualifier_year(claim({'time':'+10000-00-00T00:00:00Z','precision':9}),'P582'))
        self.assertEqual(qualifier_year(claim({'time':'+2000-00-00T00:00:00Z','precision':9}),'P582'),2000)

    def test_oversized_batch_uses_bounded_individual_reads(self):
        s=self.store(archive([]));http=BatchHTTP();normal=http.json
        def fetch(url):
            if '/w/api.php?' in url:
                http.calls.append(url)
                raise ValueError('structured response exceeds bound')
            return normal(url)
        try:
            with patch.object(http,'json',side_effect=fetch):
                ingest_wikidata(s,http,['Q1','Q2'],batch_size=10)
            self.assertEqual(len(http.calls),3)
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],2)
        finally:s.close()

    def test_access_denial_never_falls_back_or_requests_another_batch(self):
        s=self.store(archive([]));http=BatchHTTP()
        try:
            with patch.object(http,'json',side_effect=HTTPError('url',403,'denied',{},None)) as fetch:
                ingest_wikidata(s,http,['Q'+str(i) for i in range(1,12)],batch_size=10)
                self.assertEqual(fetch.call_count,1)
            self.assertEqual(s.db.execute("SELECT status FROM runs WHERE source='wikidata'").fetchone()[0],'access_failed')
        finally:s.close()

    def test_maxlag_pause_is_persisted_and_not_downgraded(self):
        s=self.store(archive([]));http=BatchHTTP()
        try:
            with patch.object(http,'json',return_value=({'error':{'code':'maxlag'}},'a'*64)):
                ingest_wikidata(s,http,['Q1','Q2'],batch_size=10)
            self.assertFalse(s.source_ready('wikidata'))
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],0)
        finally:s.close()

    def test_stage_budget_does_not_create_member_failure_or_retry_penalty(self):
        s=self.store(archive([]));http=BatchHTTP()
        try:
            with patch.object(http,'json',side_effect=StageDeadline('work window')):
                ingest_wikidata(s,http,['Q1'],batch_size=10)
            self.assertEqual(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_retries').fetchone()[0],0)
            detail=json.loads(s.db.execute("SELECT detail FROM runs WHERE source='wikidata'").fetchone()[0])
            self.assertTrue(detail['stage_time_exhausted'])
        finally:s.close()

    def test_orcid_time_boundary_rolls_back_first_section_without_defer(self):
        s=self.store(archive([]))
        class HTTP:
            def open(self,url,*args):
                if url.endswith('employments'):raise StageDeadline('work window')
                body=XML.replace(b'<record path="/0000-0002-1825-0097"',b'<educations path="/0000-0002-1825-0097/educations"').replace(b'</record>',b'</educations>')
                return Response(body,url,'application/xml')
        try:
            ingest_orcid_api(s,HTTP(),[PERSON])
            for table in ('events','members','member_parts','failures','discovery_retries'):
                self.assertEqual(s.db.execute('SELECT count(*) FROM '+table).fetchone()[0],0,table)
        finally:s.close()

    def test_batch_route_cannot_authorize_search_write_or_extra_parameters(self):
        base='https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q1&props=claims%7Cinfo&format=json&maxlag=5'
        self.assertTrue(entity_batch_route(urlsplit(base)))
        for url in (base+'&token=secret',base+'&ids=Q2',base.replace('wbgetentities','wbeditentity'),
                    base.replace('wbgetentities','query'),base.replace('Q1','Q1%7Cbad'),
                    base.replace('claims%7Cinfo','claims%7Clabels'),base.replace('maxlag=5','maxlag=0')):
            self.assertFalse(entity_batch_route(urlsplit(url)),url)
        s=self.store(archive([]))
        try:
            for size in (0,11,True):
                with self.assertRaises(ValueError):ingest_wikidata(s,None,['Q1'],batch_size=size)
        finally:s.close()


if __name__=='__main__':unittest.main()
