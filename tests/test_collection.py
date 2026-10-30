import unittest
from unittest.mock import patch

from gapi.collection import collect
from gapi.task import BudgetStop
import test_streaming as fixtures
from test_streaming import archive


class CollectionTests(unittest.TestCase):
    def setUp(self):
        fixtures.StreamingTests.setUp(self)
        discovery=patch('gapi.collection.enqueue_discovery',side_effect=OSError('discovery offline'))
        discovery.start()
        self.addCleanup(discovery.stop)
        crossref=patch('gapi.collection.discover_crossref',side_effect=OSError('Crossref offline'))
        crossref.start()
        self.addCleanup(crossref.stop)
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_candidate_progress_keeps_failures_and_missing_world_denominator(self):
        s=self.store(archive([]))
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-wikidata','Q1','a','b'),('ingest-wikidata','Q2','a','b'),
                    ('ingest-wikidata','Q3','a','b'),('ingest-orcid-api','private-id','c','d')])
                s.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)',[
                    ('wikidata','Q1@1','a'*64,'observed',None),
                    ('wikidata','Q2@1','b'*64,'rejected','explicit invalid source member')])
            s.defer('ingest-wikidata','Q3','temporarily unavailable',now=1000)
            r=s.candidate_progress()
            wd=next(x for x in r['sources'] if x['source']=='wikidata')
            self.assertEqual((wd['candidates'],wd['processed'],wd['observed_members'],wd['rejected_members'],
                wd['pending'],wd['pending_with_errors']),(3,2,1,1,1,1))
            self.assertFalse(wd['enumeration_complete'])
            self.assertIsNone(r['world_personnel_denominator'])
            self.assertFalse(r['complete_global_census'])
            self.assertNotIn('private-id',str(r))
            self.assertEqual(r,s.candidate_progress())
            self.assertEqual(len(s.candidate_progress(allowed_sources={'wikidata'})['sources']),1)
        finally:s.close()

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

    def test_queued_only_never_refills_or_discovers_archive(self):
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
            def ingest(store,http,ids,**kwargs):
                with store.db:store.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',('wikidata','Q1@1','a'*64,'observed',None))
            with patch('gapi.collection.ingest_wikidata',side_effect=ingest), \
                 patch('gapi.collection.discover_openalex') as oa, \
                 patch('gapi.collection.discover_crossref') as cr, \
                 patch('gapi.collection.enqueue_discovery') as wd, \
                 patch('gapi.collection.discover_dump') as dump:
                r=collect(s,None,discovery=False,max_seconds=10)
            self.assertEqual(sum(t.call_count for t in (oa,cr,wd,dump)),0)
            self.assertFalse(r['discovery_enabled'])
            self.assertFalse(s.pending('ingest-wikidata'))
            with self.assertRaisesRegex(ValueError,'queued-only'):
                collect(s,None,{'id':'feed'},discovery=False)
        finally:s.close()

    def test_budget_refusal_propagates_and_preserves_pending_work(self):
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
            with patch('gapi.collection.discover_openalex',return_value={}), patch('gapi.collection.ingest_wikidata',side_effect=BudgetStop('disk limit')), self.assertRaises(BudgetStop):
                collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10)
            self.assertEqual(s.pending('ingest-wikidata'),['Q1'])
        finally:s.close()

    def test_empty_sources_try_dump_once_and_do_not_loop_forever(self):
        s=self.store(archive([]))
        try:
            with patch('gapi.collection.discover_openalex',side_effect=OSError('independent source offline')) as openalex, patch('gapi.collection.discover_dump',return_value={'status':'provisional'}) as dump:
                result=collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10)
            dump.assert_called_once()
            openalex.assert_called_once()
            self.assertEqual(result['status'],'resumable-source-boundary')
        finally:s.close()

    def test_default_api_only_never_opens_dump_when_all_apis_fail(self):
        s=self.store(archive([]))
        try:
            with patch('gapi.collection.discover_openalex',side_effect=OSError('offline')), \
                 patch('gapi.collection.discover_dump',side_effect=AssertionError('archive route forbidden')) as dump:
                result=collect(s,None,max_seconds=10)
            dump.assert_not_called()
            self.assertFalse(result['archive_discovery_enabled'])
            self.assertEqual(result['status'],'resumable-source-boundary')
        finally:s.close()

    def test_queue_refills_more_than_one_page_in_one_invocation(self):
        s=self.store(archive([]))
        pages=[]
        def discover(store,http,**kwargs):
            index=len(pages)
            if index==3:return {'pages':0,'status':'exhausted'}
            pages.append(index)
            with store.db:
                store.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-orcid-api',str(index),'a','b'))
                store.db.execute('INSERT INTO discovery_pages VALUES(?,?,?,?,?,?,?)',
                    ('feed',str(index),str(index+1),'q','r','now',1))
            return {'pages':1}
        def ingest(store,http,ids,**kwargs):
            store.start('orcid-public-api')
            for identifier in ids:
                with store.member('orcid-public-api',identifier):
                    store.finish_member('orcid-public-api',identifier,'a'*64)
        try:
            with patch('gapi.collection.discover_openalex',side_effect=discover), \
                 patch('gapi.collection.ingest_orcid_api',side_effect=ingest), \
                 patch('gapi.collection.discover_dump',return_value={}):
                result=collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10,batch_members=1)
            self.assertEqual(pages,[0,1,2])
            self.assertEqual(result['discovery_pages_committed'],3)
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],3)
            self.assertFalse(result['global_complete'])
        finally:s.close()

    def test_failed_first_candidate_cannot_starve_later_candidates(self):
        s=self.store(archive([]))
        with s.db:
            s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                ('ingest-wikidata',qid,'a','b') for qid in ('Q1','Q2')])
        tried=[]
        def ingest(store,http,ids,**kwargs):
            store.start('wikidata')
            tried.extend(ids)
            if ids==['Q1']:
                store.defer('ingest-wikidata','Q1','truncated response')
            else:
                with store.member('wikidata','Q2@1'):
                    store.finish_member('wikidata','Q2@1','a'*64)
        try:
            with patch('gapi.collection.discover_openalex',return_value={}), \
                 patch('gapi.collection.ingest_wikidata',side_effect=ingest), \
                 patch('gapi.collection.discover_dump',return_value={}):
                collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10,batch_members=1)
            self.assertEqual(tried,['Q1','Q2'])
            self.assertEqual(s.pending('ingest-wikidata',now=10**12),['Q1'])
        finally:s.close()

    def test_backoff_survives_reopen_and_new_work_has_priority(self):
        from gapi.career_store import CareerStore
        from gapi.task import Task
        s=self.store(archive([]))
        with s.db:
            s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                ('ingest-wikidata',qid,'a','b') for qid in ('Q1','Q2')])
        s.defer('ingest-wikidata','Q1','offline',now=100)
        s.close()
        s=CareerStore(Task(self.path))
        try:
            self.assertEqual(s.pending('ingest-wikidata',now=159),['Q2'])
            self.assertEqual(s.pending('ingest-wikidata',now=160),['Q1','Q2'])
            s.defer('ingest-wikidata','Q1','offline',now=160)
            self.assertEqual(s.db.execute('SELECT attempts,next_attempt_at FROM discovery_retries').fetchone(),(2,280))
        finally:s.close()

    def test_due_retry_gets_reserved_capacity_with_many_new_candidates(self):
        s=self.store(archive([]))
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-wikidata','Q'+str(i),'a','b') for i in range(1,101)])
            s.defer('ingest-wikidata','Q99','was offline',now=100)
            pending=s.pending('ingest-wikidata',8,now=160)
            self.assertIn('Q99',pending)
            self.assertEqual(len(pending),8)
            self.assertEqual(len(set(pending)),8)
        finally:s.close()

    def test_malformed_discovery_response_does_not_stop_other_source(self):
        class HTTP:
            def json(self,url):return [],'a'*64
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
            def ingest(store,http,ids,**kwargs):
                store.start('wikidata')
                with store.member('wikidata','Q1@1'):store.finish_member('wikidata','Q1@1','a'*64)
            with patch('gapi.collection.ingest_wikidata',side_effect=ingest),patch('gapi.collection.discover_dump',return_value={}):
                result=collect(s,HTTP(),{'id':'feed','size_bytes':100},max_seconds=10)
            self.assertIsNotNone(s.seen('wikidata','Q1@1'))
            self.assertIn('openalex-discovery',result['suspended_discovery'])
        finally:s.close()
