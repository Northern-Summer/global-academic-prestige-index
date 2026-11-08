"""Optional one-response XML ingress and truthful source-evidence equivalence."""
import hashlib
import gzip
import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import test_streaming as fixtures
import test_corroboration as corroboration_fixtures
from gapi.corroboration import decisions
from gapi.global_sources import ingest_orcid_api
from gapi.global_analysis import rank_global
from gapi.orcid_stream import API_RECORD_TRANSPORT, private_person
from gapi.source_http import StageDeadline
from gapi.task import BudgetStop, digest


def bodies():
    person=fixtures.PERSON.encode()
    education=fixtures.XML.replace(b'<record path="/'+person+b'"',
        b'<educations path="/'+person+b'/educations"').replace(b'</record>',b'</educations>')
    employment=education.replace(b'educations',b'employments').replace(b'education-summary',b'employment-summary')
    employment=employment.replace(b'put-code="1"',b'put-code="2"').replace(b'PhD',b'Professor')
    employment=employment.replace(b'end-date',b'start-date')
    inner=employment.split(b'>',1)[1].rsplit(b'</employments>',1)[0]
    record=fixtures.XML.replace(b'</record>',inner+b'</record>')
    return {'educations':education,'employments':employment,'record':record}


class Response(io.BytesIO):
    headers={'Content-Type':'application/vnd.orcid+xml'}
    def read(self,n=-1):
        if not 0<=n<=65536:raise AssertionError('unbounded read')
        return super().read(n)


class HTTP:
    def __init__(self,values=None):
        self.values=values or bodies();self.calls=[];self.requests_count=0
    def open(self,url,accept):
        assert accept=='application/vnd.orcid+xml'
        self.calls.append(url);self.requests_count+=1
        body=self.values[url.rsplit('/',1)[-1]]
        if isinstance(body,Exception):raise body
        return body if hasattr(body,'read') else Response(body)


class RecordRouteTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_unused_long_fields_do_not_reject_identical_affiliation(self):
        from gapi.orcid_stream import SummaryParser
        def parse(body,width):
            rows=[];parser=SummaryParser('p','source','fixture.xml',rows.append)
            for i in range(0,len(body),width):parser.feed(body[i:i+width])
            parser.feed(b'',final=True)
            return rows
        expected=parse(fixtures.XML,7)
        extra=(b'<department-name>'+b'x'*32000+b'</department-name>'
               b'<city>'+b'y'*32000+b'</city><credit-name>'+b'z'*32000+b'</credit-name>')
        body=fixtures.XML.replace(b'<role-title>',extra+b'<role-title>')
        for width in (1,113,65536):self.assertEqual(parse(body,width),expected)

    def test_long_used_field_is_lossless_and_normalized_byte_cap_remains(self):
        from gapi.orcid_stream import SummaryParser
        from gapi.task import canonical
        from gapi.event_storage import MAX_BYTES
        role='PhD '+('x'*7654)
        for width in (1,97,65536):
            rows=[];parser=SummaryParser('p','source','fixture.xml',rows.append)
            raw=fixtures.XML.replace(b'PhD',role.encode())
            for i in range(0,len(raw),width):parser.feed(raw[i:i+width])
            parser.feed(b'',final=True)
            self.assertEqual(rows[0]['role'],role)
            self.assertEqual(rows[0]['stage'],'doctorate')
            self.assertLessEqual(len(canonical(rows[0])),MAX_BYTES)
        # UTF-8 input fits the text buffer, but canonical JSON escaping would
        # exceed the existing stored-event cap. No partial record may escape.
        rows=[];parser=SummaryParser('p','source','fixture.xml',rows.append)
        with self.assertRaisesRegex(ValueError,'normalized event exceeds 16 KiB'):
            parser.feed(fixtures.XML.replace(b'PhD',('中'*3000).encode()),final=True)
        self.assertEqual(rows,[])
        rows=[];parser=SummaryParser('p','source','fixture.xml',rows.append)
        with self.assertRaisesRegex(ValueError,'affiliation field exceeds bound'):
            parser.feed(fixtures.XML.replace(b'PhD',b'x'*(MAX_BYTES+1)),final=True)
        self.assertLessEqual(parser.current['text_bytes'],MAX_BYTES)
        self.assertEqual(rows,[])

    def test_oversized_second_section_rolls_back_first_and_preserves_retry(self):
        store=self.store(fixtures.archive([]))
        try:
            values=bodies();values['employments']=values['employments'].replace(b'Professor',('中'*3000).encode())
            http=HTTP(values);ingest_orcid_api(store,http,[fixtures.PERSON],report=False)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM members').fetchone()[0],0)
            reason=store.db.execute('SELECT reason FROM discovery_retries').fetchone()[0]
            self.assertIn('normalized event exceeds 16 KiB',reason)
            self.assertEqual(len(http.calls),2)
            self.assertEqual(store.db.execute('SELECT sum(requests) FROM request_reservations').fetchone()[0],2)
        finally:store.close()

    def test_collector_opt_in_reaches_adapter_and_cli_default_stays_sections(self):
        import argparse
        from gapi.stream_cli import register
        from gapi.collection import collect
        from gapi.candidate_selection import freeze
        parser=argparse.ArgumentParser();register(parser.add_subparsers(dest='command'))
        self.assertFalse(parser.parse_args(['collect-global']).orcid_record)
        self.assertTrue(parser.parse_args(['collect-global','--orcid-record']).orcid_record)
        store=self.store(fixtures.archive([]))
        try:
            with store.db:store.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',
                ('ingest-orcid-api',fixtures.PERSON,'q','r'))
            identity=freeze(store)['selection_id'];http=HTTP()
            result=collect(store,http,max_seconds=10,discovery=False,
                candidate_selection=identity,orcid_record=True)
            self.assertEqual(result['orcid_api_route'],'record')
            self.assertEqual(len(http.calls),1)
            self.assertTrue(result['frozen_candidate_processing']['processing_complete'])
            self.assertFalse(result['global_complete'])
        finally:store.close()

    def test_same_facts_one_request_truthful_digest_and_idempotent_route_switch(self):
        facts=[]
        for record in (False,True):
            store=self.store(fixtures.archive([]),work_dir=str(self.root/str(record)))
            http=HTTP()
            try:
                ingest_orcid_api(store,http,[fixtures.PERSON],whole_record=record,report=False)
                rows=[json.loads(row[0]) for row in store.db.execute('SELECT payload FROM events')]
                self.assertEqual(len(rows),2)
                facts.append(sorted(json.dumps({k:v for k,v in row.items()
                    if k not in ('retrieved_at','transport','locator','url')},sort_keys=True) for row in rows))
                self.assertEqual(len(http.calls),1 if record else 2)
                self.assertEqual(store.db.execute('SELECT sum(requests) FROM request_reservations').fetchone()[0],len(http.calls))
                if record:
                    part=store.db.execute('SELECT part,url,sha256,bytes FROM member_parts').fetchone()
                    expected=hashlib.sha256(bodies()['record']).hexdigest()
                    self.assertEqual(part,('record',http.calls[0],expected,len(bodies()['record'])))
                    self.assertEqual(store.seen('orcid-public-api',fixtures.PERSON)[0],
                        digest({'transport':API_RECORD_TRANSPORT,'parts':{'record':expected}}))
                    self.assertTrue(all(row['locator']==fixtures.PERSON+'#record' for row in rows))
                before=list(store.db.execute('SELECT * FROM events ORDER BY id'))
                ingest_orcid_api(store,http,[fixtures.PERSON],whole_record=not record,report=False)
                self.assertEqual(before,list(store.db.execute('SELECT * FROM events ORDER BY id')))
                self.assertEqual(len(http.calls),1 if record else 2)
            finally:store.close()
        self.assertEqual(facts[0],facts[1])

    def test_identity_truncation_capacity_and_access_failure_do_not_fallback(self):
        record=bodies()['record']
        cases=[('wrong-person',record.replace(fixtures.PERSON.encode(),b'0000-0002-7869-831X')),
               ('truncated',record[:-9]),
               ('oversized',record),
               ('denied',HTTPError('https://pub.orcid.org/v3.0/'+fixtures.PERSON+'/record',403,'denied',{},None))]
        for name,body in cases:
            with self.subTest(name=name):
                store=self.store(fixtures.archive([]),work_dir=str(self.root/name),
                    max_member_bytes=128 if name=='oversized' else 16*1024**2)
                http=HTTP({'record':body})
                try:
                    ingest_orcid_api(store,http,[fixtures.PERSON],whole_record=True,report=False)
                    self.assertIsNone(store.seen('orcid-public-api',fixtures.PERSON))
                    self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
                    self.assertEqual(store.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],0)
                    self.assertEqual(len(http.calls),1)
                finally:store.close()

    def test_deadline_after_parsing_rolls_back_without_penalizing_candidate(self):
        class Interrupted(Response):
            def read(self,n=-1):
                if self.tell():raise StageDeadline('injected after parsed body')
                return super().read(n)
        store=self.store(fixtures.archive([]))
        try:
            ingest_orcid_api(store,HTTP({'record':Interrupted(bodies()['record'])}),
                [fixtures.PERSON],whole_record=True,report=False)
            self.assertIsNone(store.seen('orcid-public-api',fixtures.PERSON))
            for table in ('events','member_parts','discovery_retries','failures'):
                self.assertEqual(store.db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)
            ingest_orcid_api(store,HTTP(),[fixtures.PERSON],whole_record=True,report=False)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],2)
        finally:store.close()

    def test_transaction_capacity_failure_leaves_no_record_and_can_resume(self):
        store=self.store(fixtures.archive([]))
        try:
            with patch.object(store,'finish_member',side_effect=BudgetStop('injected capacity')):
                ingest_orcid_api(store,HTTP(),[fixtures.PERSON],whole_record=True,report=False)
            self.assertIsNone(store.seen('orcid-public-api',fixtures.PERSON))
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],0)
            ingest_orcid_api(store,HTTP(),[fixtures.PERSON],whole_record=True,report=False)
            self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],2)
        finally:store.close()

    def test_corrupt_record_gzip_cannot_commit_parsed_affiliations(self):
        from test_xml_transport import Response as WireResponse
        from gapi.source_http import PublicHTTP
        store=self.store(fixtures.archive([]))
        packed=bytearray(gzip.compress(bodies()['record'],mtime=0));packed[-8]^=1
        def opener(request,**kwargs):
            response=WireResponse(bytes(packed));response.geturl=lambda:request.full_url
            return response
        http=PublicHTTP(store.task,opener=opener,sleep=lambda _:None)
        try:
            ingest_orcid_api(store,http,[fixtures.PERSON],whole_record=True,report=False)
            self.assertEqual(http.requests_count,1)
            self.assertIsNone(store.seen('orcid-public-api',fixtures.PERSON))
            self.assertEqual(store.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:http.close();store.close()

    def test_record_evidence_requires_real_route_and_member_manifest(self):
        store=self.store(fixtures.archive([]))
        try:
            corroboration_fixtures.CorroborationTests.seed(self,store)
            legacy=decisions(store);self.assertEqual(len(legacy),2)
            person=fixtures.PERSON
            # A real training -> employment relation makes graph equality
            # observable; an education-only fixture has no ranking edge.
            with store.db:
                store.event(dict(source='orcid-public-api',person=private_person(person),
                    institution='ror:01mrfdz82',kind='employment',stage='academic',
                    year_start=2010,year_end=None,role='Professor',country='US',
                    locator=person+'#employments',status='observed',assertion_origin=person,
                    url=f'https://pub.orcid.org/v3.0/{person}/employments'))
            original_rank=rank_global(store)
            self.assertTrue(original_rank['edges'])
            row=store.db.execute("SELECT id,payload FROM events WHERE source='orcid-public-api'").fetchone()
            event=json.loads(row[1]);event.update(transport=API_RECORD_TRANSPORT,
                locator=fixtures.PERSON+'#record',url='https://pub.orcid.org/v3.0/'+fixtures.PERSON+'/record')
            manifest=digest({'transport':API_RECORD_TRANSPORT,'parts':{'record':'b'*64}})
            with store.db:
                store.db.execute('UPDATE events SET payload=? WHERE id=?',(json.dumps(event),row[0]))
                store.db.execute("UPDATE member_parts SET part='record',url=? WHERE source='orcid-public-api'",(event['url'],))
                store.db.execute("UPDATE members SET sha256=? WHERE source='orcid-public-api'",(manifest,))
            current=decisions(store);self.assertEqual(len(current),2)
            record_rank=rank_global(store)
            self.assertEqual(record_rank['edges'],original_rank['edges'])
            self.assertEqual([(n['institution_id'],n['network_score'],n['formal_rank']) for n in record_rank['nodes']],
                             [(n['institution_id'],n['network_score'],n['formal_rank']) for n in original_rank['nodes']])
            for key in legacy:
                self.assertEqual(current[key]['origins'],legacy[key]['origins'])
                self.assertEqual(current[key]['supported_fields'],legacy[key]['supported_fields'])
                self.assertEqual(current[key]['policy_sha256'],legacy[key]['policy_sha256'])
            for changes in ({'url':event['url']+'?other=1'},{'locator':fixtures.PERSON+'#educations'},
                            {'assertion_origin':'copied-client'},{'year_end':1968}):
                with store.db:store.db.execute('UPDATE events SET payload=? WHERE id=?',(json.dumps(dict(event,**changes)),row[0]))
                self.assertEqual(decisions(store),{})
            with store.db:
                store.db.execute('UPDATE events SET payload=? WHERE id=?',(json.dumps(event),row[0]))
                store.db.execute("UPDATE members SET sha256=? WHERE source='orcid-public-api'",('f'*64,))
            self.assertEqual(decisions(store),{})
        finally:store.close()


if __name__=='__main__':unittest.main()
