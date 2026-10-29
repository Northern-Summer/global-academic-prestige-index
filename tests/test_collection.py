import unittest
from unittest.mock import patch

from gapi.collection import collect
from gapi.task import BudgetStop
import test_streaming as fixtures
from test_streaming import archive


class CollectionTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_one_source_failure_does_not_stop_another_or_erase_queue(self):
        s=self.store(archive([]))
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-orcid-api','0000-0002-1825-0097','a','b'),('ingest-wikidata','Q1','a','b')])
            def ingest(store,http,ids,**kwargs):
                store.start('wikidata')
                with store.member('wikidata','Q1@1'):
                    store.finish_member('wikidata','Q1@1','a'*64)
            with patch('gapi.collection.ingest_orcid_api',side_effect=OSError('source offline')) as orcid, \
                 patch('gapi.collection.ingest_wikidata',side_effect=ingest) as wikidata, \
                 patch('gapi.collection.discover_dump',side_effect=OSError('dump offline')):
                result=collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10)
            self.assertEqual(orcid.call_count,1)
            self.assertEqual(wikidata.call_count,1)
            self.assertIn('ingest-orcid-api',result['suspended_sources'])
            self.assertEqual(len(s.pending('ingest-orcid-api')),1)
            self.assertFalse(s.pending('ingest-wikidata'))
            self.assertFalse(result['global_complete'])
        finally:s.close()

    def test_budget_refusal_propagates_and_preserves_pending_work(self):
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
            with patch('gapi.collection.ingest_wikidata',side_effect=BudgetStop('disk limit')), self.assertRaises(BudgetStop):
                collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10)
            self.assertEqual(s.pending('ingest-wikidata'),['Q1'])
        finally:s.close()

    def test_empty_sources_try_dump_once_and_do_not_loop_forever(self):
        s=self.store(archive([]))
        try:
            with patch('gapi.collection.discover_dump',return_value={'status':'provisional'}) as dump:
                result=collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10)
            dump.assert_called_once()
            self.assertEqual(result['status'],'resumable-source-boundary')
        finally:s.close()
