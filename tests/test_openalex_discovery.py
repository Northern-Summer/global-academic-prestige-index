import unittest
from urllib.parse import parse_qs,urlsplit

from gapi.openalex_discovery import discover_openalex
from gapi.career_rules import valid_orcid
import test_streaming as fixtures
from test_streaming import archive


class HTTP:
    def __init__(self,results):self.responses=iter(results);self.urls=[]
    def json(self,url):
        self.urls.append(url)
        result=next(self.responses)
        if isinstance(result,Exception):raise result
        return result,'a'*64


def page(cursor,orcid='0000-0002-1825-0097'):
    return {'meta':{'next_cursor':cursor,'count':1000},'results':[
        {'id':'https://openalex.org/A123','orcid':'https://orcid.org/'+orcid}]}


class OpenAlexDiscoveryTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_cursor_resume_minimal_fields_dedup_and_no_career_inference(self):
        s=self.store(archive([]))
        try:
            http=HTTP([page('next-1'),page('next-2')])
            discover_openalex(s,http)
            discover_openalex(s,http)
            params=parse_qs(urlsplit(http.urls[1]).query)
            self.assertEqual(params['cursor'],['next-1'])
            self.assertEqual(params['select'],['id,orcid'])
            self.assertNotIn('api_key',params)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],1)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:s.close()

    def test_access_failure_does_not_advance_cursor_or_erase_queue(self):
        s=self.store(archive([]))
        try:
            discover_openalex(s,HTTP([page('first')]))
            with self.assertRaises(OSError):discover_openalex(s,HTTP([OSError('offline')]))
            self.assertEqual(s.db.execute('SELECT cursor_after FROM discovery_pages').fetchone()[0],'first')
            self.assertEqual(len(s.pending('ingest-orcid-api')),1)
        finally:s.close()

    def test_nonadvancing_cursor_and_bad_orcid_are_not_accepted(self):
        s=self.store(archive([]))
        try:
            with self.assertRaises(ValueError):discover_openalex(s,HTTP([page('*')]))
            discover_openalex(s,HTTP([page('next','0000-0002-1825-0098')]))
            self.assertFalse(s.pending('ingest-orcid-api'))
        finally:s.close()

    def test_orcid_check_digit_and_ascii_format(self):
        self.assertTrue(valid_orcid('0000-0002-1825-0097'))
        for value in ('0000-0002-1825-0098','００００-0002-1825-0097','0000-0001-1111-1111',None):
            self.assertFalse(valid_orcid(value))
