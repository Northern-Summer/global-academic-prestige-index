import unittest
from gapi.global_sources import ingest_wikidata
import test_streaming as fixtures
from test_streaming import archive,PERSON


class HTTP:
    def __init__(self,ids):self.ids=ids;self.calls=0
    def json(self,url):
        self.calls+=1
        return {'entities':{'Q1':{'id':'Q1','lastrevid':2,'claims':{'P496':[
            {'mainsnak':{'datavalue':{'value':value}}} for value in self.ids]}}}},'a'*64


class CrossSourceFollowupTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_explicit_id_is_queued_atomically_without_facts_or_promotion(self):
        s=self.store(archive([]));http=HTTP([PERSON])
        try:
            ingest_wikidata(s,http,['Q1']);ingest_wikidata(s,http,['Q1'])
            self.assertEqual(http.calls,1)
            self.assertEqual(s.pending('ingest-orcid-api'),[PERSON])
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
        finally:s.close()

    def test_conflicting_stable_ids_are_not_followed(self):
        s=self.store(archive([]))
        try:
            ingest_wikidata(s,HTTP([PERSON,'0000-0001-5109-3700']),['Q1'])
            self.assertEqual(s.pending('ingest-orcid-api'),[])
        finally:s.close()
