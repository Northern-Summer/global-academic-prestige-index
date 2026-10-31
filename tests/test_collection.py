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

    def test_source_time_fairness_does_not_make_fast_transport_wait_for_equal_batch_counts(self):
        s=self.store(archive([]));clock=[0.0];calls=[]
        identifiers={'ingest-orcid-api':'0000-0002-1825-0097','ingest-wikidata':'Q1'}
        def ingest(source,seconds):
            def run(store,http,ids,**options):
                self.assertLessEqual(len(ids),200)
                self.assertLessEqual(options['max_seconds'],60)
                duration=min(seconds,options['max_seconds'])
                clock[0]+=duration
                calls.append(source)
                with store.member(source,str(len(calls))):
                    store.finish_member(source,str(len(calls)),'a'*64)
            return run
        try:
            with patch('gapi.collection.time.monotonic',side_effect=lambda:clock[0]), \
                 patch.object(s,'pending',side_effect=lambda command,*a,**kw:[identifiers[command]]), \
                 patch('gapi.collection.ingest_orcid_api',side_effect=ingest('orcid-public-api',60)), \
                 patch('gapi.collection.ingest_wikidata',side_effect=ingest('wikidata',20)):
                result=collect(s,None,max_seconds=240,discovery=False,batch_members=200)
            self.assertEqual(calls,['orcid-public-api','wikidata','wikidata','wikidata']*2)
            self.assertEqual(result['source_service_seconds'],{'ingest-orcid-api':120.0,'ingest-wikidata':120.0})
            self.assertEqual(result['status'],'resumable-time-boundary')
            self.assertFalse(result['global_complete'])
        finally:s.close()

    def test_real_reader_contention_rolls_back_member_and_keeps_candidate_resumable(self):
        import sqlite3
        s=self.store(archive([]))
        try:
            s.start('wikidata')
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
            s.db.execute('PRAGMA busy_timeout=10')
            def ingest(store,http,ids,**kwargs):
                reader=sqlite3.connect(store.task.root/'careers.sqlite')
                try:
                    reader.execute('BEGIN')
                    reader.execute('SELECT count(*) FROM members').fetchone()
                    with store.member('wikidata','Q1@1'):
                        store.finish_member('wikidata','Q1@1','a'*64)
                finally:reader.close()
            with patch('gapi.collection.ingest_wikidata',side_effect=ingest) as adapter:
                result=collect(s,None,max_seconds=10,discovery=False)
            self.assertEqual(adapter.call_count,1)
            self.assertEqual(result['status'],'resumable-storage-contention')
            self.assertTrue(result['stages'][0]['storage_contended'])
            self.assertEqual(result['suspended_sources'],[])
            self.assertEqual(s.pending('ingest-wikidata'),['Q1'])
            self.assertIsNone(s.seen('wikidata','Q1@1'))
            self.assertFalse(s.db.in_transaction)
            self.assertEqual(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
            def recovered(store,http,ids,**kwargs):
                with store.member('wikidata','Q1@1'):store.finish_member('wikidata','Q1@1','a'*64)
            with patch('gapi.collection.ingest_wikidata',side_effect=recovered):
                collect(s,None,max_seconds=10,discovery=False)
            self.assertFalse(s.pending('ingest-wikidata'))
        finally:s.close()

    def test_database_errors_other_than_contention_are_not_hidden(self):
        import sqlite3
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
            with patch('gapi.collection.ingest_wikidata',side_effect=sqlite3.OperationalError('unknown database defect')):
                with self.assertRaisesRegex(sqlite3.OperationalError,'unknown database defect'):
                    collect(s,None,max_seconds=10,discovery=False)
        finally:s.close()

    def test_queue_selection_deadline_never_starts_a_negative_network_window(self):
        s=self.store(archive([]));clock=[0]
        try:
            def pending(command,*args,**kwargs):
                clock[0]=2
                return ['candidate']
            with patch('gapi.collection.time.monotonic',side_effect=lambda:clock[0]), \
                 patch.object(s,'pending',side_effect=pending), \
                 patch('gapi.collection.ingest_orcid_api') as adapter:
                result=collect(s,None,max_seconds=1,discovery=False)
            adapter.assert_not_called()
            self.assertEqual(result['stages'],[])
            self.assertEqual(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
        finally:s.close()

    def test_large_failure_summary_keeps_deadline_and_request_controls_parseable(self):
        import json
        s=self.store(archive([]))
        try:
            s.start('wikidata')
            failures=[{'entity':f'Q{i}','reason':'unavailable '*40} for i in range(20)]
            details={'requested':200,'failures':failures,'stage_time_exhausted':True}
            s.status('wikidata','partial',detail=json.dumps(details))
            stored=s.db.execute("SELECT detail FROM runs WHERE source='wikidata'").fetchone()[0]
            self.assertLessEqual(len(stored),1024)
            parsed=json.loads(stored)
            self.assertTrue(parsed['stage_time_exhausted'])
            self.assertEqual(parsed['requested'],200)
            self.assertEqual(parsed['failure_summary_count'],20)
            self.assertTrue(parsed['failure_summary_truncated'])
            self.assertEqual(parsed['failures'],failures[:len(parsed['failures'])])
            s.status('wikidata','partial',detail=json.dumps(details))
            self.assertEqual(stored,s.db.execute("SELECT detail FROM runs WHERE source='wikidata'").fetchone()[0])
            small=json.dumps({'requested':1,'failures':[],'stage_time_exhausted':False})
            s.status('wikidata','partial',detail=small)
            self.assertEqual(small,s.db.execute("SELECT detail FROM runs WHERE source='wikidata'").fetchone()[0])
            with self.assertRaises(ValueError):s.status('wikidata','failed',detail=json.dumps({'control':'x'*2000}))
            self.assertEqual(('partial',small),s.db.execute("SELECT status,detail FROM runs WHERE source='wikidata'").fetchone())
        finally:s.close()

    def test_acquisition_window_exhaustion_does_not_suspend_or_fail_the_source(self):
        import json
        from gapi.source_http import StageDeadline
        for raised in (False,True):
            with self.subTest(raised=raised):
                s=self.store(archive([]))
                try:
                    with s.db:s.db.execute('INSERT OR IGNORE INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
                    def ingest(store,http,ids,**kwargs):
                        store.start('wikidata')
                        if raised:raise StageDeadline('finite window ended')
                        store.status('wikidata','partial',detail=json.dumps({'requested':200,
                            'failures':[{'reason':'source timeout '*40} for _ in range(20)],
                            'stage_time_exhausted':True}))
                    with patch('gapi.collection.ingest_wikidata',side_effect=ingest):
                        result=collect(s,None,max_seconds=10,discovery=False)
                    self.assertNotIn('ingest-wikidata',result['suspended_sources'])
                    self.assertTrue(result['stages'][0]['stage_time_exhausted'])
                    self.assertEqual(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
                    self.assertEqual(s.pending('ingest-wikidata'),['Q1'])
                finally:s.close()

    def test_linked_orcid_priority_keeps_due_retries_fresh_work_and_evidence_unchanged(self):
        from gapi.orcid_stream import private_person
        from gapi.career_rules import valid_orcid
        def identifier(n):
            digits=f'{n:015d}';total=0
            for digit in digits:total=(total+int(digit))*2
            check=(12-total%11)%11;value=digits+('X' if check==10 else str(check))
            return '-'.join(value[i:i+4] for i in range(0,16,4))
        ids=[identifier(n) for n in range(1,11)]
        self.assertTrue(all(map(valid_orcid,ids)))
        s=self.store(archive([]))
        try:
            s.start('wikidata')
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-orcid-api',value,'a','b') for value in ids])
            with s.member('wikidata','Q1@1'):
                s.event({'source':'wikidata','person':private_person(ids[-1]),'kind':'education',
                    'stage':None,'locator':'Q1@1#statement','status':'unresolved'})
                s.finish_member('wikidata','Q1@1','a'*64)
            s.defer('ingest-orcid-api',ids[-2],'retry',now=100)
            before=list(s.db.execute('SELECT * FROM events'))
            chosen=s.pending('ingest-orcid-api',4,now=160,priority_source='wikidata')
            self.assertIn(ids[-1],chosen);self.assertIn(ids[-2],chosen);self.assertIn(ids[0],chosen)
            self.assertEqual(len(set(chosen)),4)
            self.assertEqual(chosen,s.pending('ingest-orcid-api',4,now=160,priority_source='wikidata'))
            self.assertEqual(before,list(s.db.execute('SELECT * FROM events')))
            with s.db:
                s.db.execute('INSERT INTO official_profile_subjects VALUES(?,?,?,?)',
                    ('https://experts.colorado.edu/display/fisid_1234','a'*64,'fixture',private_person(ids[-3])))
            preferred=s.pending('ingest-orcid-api',4,now=160,priority_source='wikidata')
            self.assertIn(ids[-3],preferred)
            self.assertIn(ids[-2],preferred)
            self.assertIn(ids[0],preferred)
            self.assertNotIn(ids[-1],preferred)
            self.assertEqual(s.pending('ingest-orcid-api',1,now=100,priority_source='wikidata'),[ids[0]])
            s.defer('ingest-orcid-api',ids[-1],'linked temporarily unavailable',now=160)
            self.assertNotIn(ids[-1],s.pending('ingest-orcid-api',4,now=170,priority_source='wikidata'))
            with self.assertRaises(ValueError):s.pending('ingest-wikidata',priority_source='wikidata')
        finally:s.close()

    def test_linked_priority_validates_only_matches_but_rejects_invalid_ids(self):
        from gapi.orcid_stream import private_person
        from gapi.career_rules import valid_orcid
        s=self.store(archive([]))
        valid='0000-0002-1825-0097';invalid='0000-0002-1825-0098'
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-orcid-api',v,'a','b') for v in [valid,invalid,'unrelated',b'invalid-blob']])
                s.db.executemany('INSERT INTO official_profile_subjects VALUES(?,?,?,?)',[
                    ('https://experts.colorado.edu/'+str(n),'a'*64,'fixture',private_person(v))
                    for n,v in enumerate([valid,invalid])])
            with patch('gapi.career_rules.valid_orcid',wraps=valid_orcid) as validation:
                result=s.pending('ingest-orcid-api',6,priority_source='wikidata')
            # Invalid linked IDs remain ordinary candidates, never priority.
            self.assertEqual(result[0],valid)
            self.assertNotIn('unrelated',[call.args[0] for call in validation.call_args_list])
            self.assertIn(invalid,[call.args[0] for call in validation.call_args_list])
            query=('SELECT d.identifier FROM discovery d LEFT JOIN discovery_retries r '
                   'ON r.source=d.source AND r.identifier=d.identifier WHERE d.source=?')
            self.assertEqual(s._linked_priority(query,('ingest-orcid-api',),'wikidata',10),[valid])
        finally:s.close()

    def test_priority_window_rotates_without_starving_fresh_or_due_work(self):
        from gapi.orcid_stream import private_person
        s=self.store(archive([]))
        target='0000-0002-1825-0097'
        try:
            # Valid target sorts after a full window of unrelated candidates.
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',
                    [('ingest-orcid-api',f'0000-0000-0000-{n:04d}','a','b') for n in range(8192)])
                s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-orcid-api',target,'a','b'))
                s.db.execute('INSERT INTO official_profile_subjects VALUES(?,?,?,?)',
                    ('https://experts.colorado.edu/fixture','a'*64,'fixture',private_person(target)))
            first=s.pending('ingest-orcid-api',4,priority_source='wikidata')
            self.assertNotIn(target,first)
            self.assertNotEqual(s._priority_scan_after,'')
            second=s.pending('ingest-orcid-api',4,priority_source='wikidata')
            self.assertIn(target,second)
            self.assertIn(first[0],second)
            self.assertEqual(s._priority_scan_after,'')
            s.defer('ingest-orcid-api',target,'fixture pause',now=100)
            s.pending('ingest-orcid-api',4,now=110,priority_source='wikidata')
            self.assertNotIn(target,s.pending('ingest-orcid-api',4,now=110,priority_source='wikidata'))
        finally:s.close()

    def test_time_limited_prefix_serves_retries_linked_and_ordinary_candidates(self):
        s=self.store(archive([]))
        due=[f'r{n:04d}' for n in range(100)]
        ordinary=[f'f{n:04d}' for n in range(100)]
        linked=[f'z{n:04d}' for n in range(100)]
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',
                    [('ingest-orcid-api',value,'a','b') for value in due+ordinary+linked])
                s.db.executemany('INSERT INTO discovery_retries VALUES(?,?,?,?,?)',
                    [('ingest-orcid-api',value,1,100,'fixture') for value in due])
            with patch.object(s,'_linked_priority',return_value=linked[:75]):
                selected=s.pending('ingest-orcid-api',200,now=160,priority_source='wikidata')
            self.assertEqual(len(selected),200)
            self.assertEqual(len(set(selected)),200)
            for group in [due,ordinary,linked]:
                self.assertTrue(set(selected[:3])&set(group),selected[:3])
            self.assertEqual(sum(value in due for value in selected),50)
            self.assertEqual(sum(value in ordinary for value in selected),75)
            self.assertEqual(sum(value in linked for value in selected),75)
        finally:s.close()

    def test_priority_deadline_preserves_fresh_retry_queue_and_releases_handler(self):
        from unittest.mock import patch
        s=self.store(archive([]))
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-orcid-api',str(n),'a','b') for n in range(10)])
            s.defer('ingest-orcid-api','9','retry',now=100)
            expected=s.pending('ingest-orcid-api',4,now=160)
            before=list(s.db.execute('SELECT * FROM discovery_retries'))
            def expensive(*args):
                return list(s.db.execute('''WITH RECURSIVE n(x) AS
                    (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<100000)
                    SELECT sum(x) FROM n'''))
            with patch.object(s,'_linked_priority',side_effect=expensive), \
                 patch('gapi.career_store.time.monotonic',side_effect=[0]+[3]*100):
                self.assertEqual(s.pending('ingest-orcid-api',4,now=160,priority_source='wikidata'),expected)
            self.assertTrue(s.last_priority_deferred)
            self.assertEqual(before,list(s.db.execute('SELECT * FROM discovery_retries')))
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],10)
            self.assertEqual(s.pending('ingest-orcid-api',4,now=160),expected)
            self.assertFalse(s.last_priority_deferred)
            with patch.object(s,'_linked_priority',side_effect=__import__('sqlite3').OperationalError('other error')):
                with self.assertRaisesRegex(__import__('sqlite3').OperationalError,'other error'):
                    s.pending('ingest-orcid-api',4,now=160,priority_source='wikidata')
        finally:s.close()

    def test_retry_selection_starts_from_retries_and_preserves_legacy_eligibility(self):
        s=self.store(archive([]))
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-wikidata',f'Q{n}','a','b') for n in range(1,2001)])
                s.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','odd_id','a','b')")
                s.db.executemany('INSERT INTO discovery_retries VALUES(?,?,?,?,?)',[
                    ('ingest-wikidata',value,1,stamp,'fixture') for value,stamp in
                    [('Q1999',100),('Q2000',100),('Q1998',300),('Q1997',100),
                     ('Q1996',100),('Q99999',100),('odd_id',100)]])
                s.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)',[
                    ('wikidata','Q1997@12','a'*64,'observed',None),
                    ('wikidata','Q1996','a'*64,'observed',None),
                    ('wikidata','odd_id@5','a'*64,'observed',None)])
            statements=[]
            s.db.set_trace_callback(statements.append)
            selected=s.pending('ingest-wikidata',8,now=160)
            s.db.set_trace_callback(None)
            self.assertEqual(selected,['Q1999','Q2000','Q1','Q10','Q100','Q1000','Q1001','Q1002'])
            retry=next(sql for sql in statements if 'CROSS JOIN discovery d' in sql)
            legacy=retry.replace('FROM discovery_retries r CROSS JOIN discovery d ON',
                                 'FROM discovery d LEFT JOIN discovery_retries r ON')
            self.assertEqual(list(s.db.execute(retry)),list(s.db.execute(legacy)))
            plan=[row[3] for row in s.db.execute('EXPLAIN QUERY PLAN '+retry)]
            outer=next(line for line in plan if line.startswith(('SEARCH r ','SCAN r ','SEARCH d ','SCAN d ')))
            self.assertTrue(outer.startswith(('SEARCH r ','SCAN r ')),plan)
            self.assertEqual(selected,s.pending('ingest-wikidata',8,now=160))
        finally:
            s.db.set_trace_callback(None)
            s.close()

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
            with patch('gapi.collection.ingest_wikidata',side_effect=ingest) as adapter, \
                 patch.object(s,'report',wraps=s.report) as audit, \
                 patch('gapi.collection.discover_openalex') as oa, \
                 patch('gapi.collection.discover_crossref') as cr, \
                 patch('gapi.collection.enqueue_discovery') as wd, \
                 patch('gapi.collection.discover_dump') as dump:
                r=collect(s,None,discovery=False,max_seconds=10)
            self.assertEqual(sum(t.call_count for t in (oa,cr,wd,dump)),0)
            self.assertFalse(r['discovery_enabled'])
            self.assertFalse(s.pending('ingest-wikidata'))
            self.assertEqual(audit.call_count,1)
            self.assertFalse(adapter.call_args.kwargs['report'])
            self.assertEqual(r['stages'][0]['new_events'],0)
            self.assertGreaterEqual(r['stages'][0]['elapsed_seconds'],0)
            # A mocked, sub-tick batch has no measurable rate (Windows clock).
            if r['stages'][0]['members_per_second'] is None:
                self.assertEqual(r['stages'][0]['elapsed_seconds'],0)
            else:
                self.assertGreater(r['stages'][0]['members_per_second'],0)
            with self.assertRaisesRegex(ValueError,'queued-only'):
                collect(s,None,{'id':'feed'},discovery=False)
        finally:s.close()

    def test_discovery_window_exhaustion_is_pending_not_a_source_failure(self):
        from gapi.source_http import StageDeadline
        s=self.store(archive([]));progress=[]
        try:
            with patch('gapi.collection.enqueue_discovery',side_effect=StageDeadline('window')) as wd, \
                 patch('gapi.collection.discover_openalex',return_value={}), \
                 patch('gapi.collection.discover_crossref',return_value={}):
                result=collect(s,None,max_seconds=10,progress=progress.append)
            self.assertEqual(wd.call_count,1)
            self.assertEqual(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],0)
            self.assertIn('wikidata-discovery',result['suspended_discovery'])
            self.assertIn({'stage':'wikidata-discovery','status':'resumable-stage-time-boundary'},progress)
        finally:s.close()

    def test_budget_refusal_propagates_and_preserves_pending_work(self):
        s=self.store(archive([]))
        try:
            with s.db:s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-wikidata','Q1','a','b'))
            with patch('gapi.collection.discover_openalex',return_value={}), patch('gapi.collection.ingest_wikidata',side_effect=BudgetStop('disk limit')), self.assertRaises(BudgetStop):
                collect(s,None,{'id':'feed','size_bytes':100},max_seconds=10)
            self.assertEqual(s.pending('ingest-wikidata'),['Q1'])
        finally:s.close()

    def test_temporary_source_pause_does_not_disable_source_after_expiry(self):
        s=self.store(archive([]));calls=[]
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-orcid-api','one','a','b'),('ingest-orcid-api','two','a','b'),
                    ('ingest-wikidata','Q1','a','b')])
            def orcid(store,http,ids,**kwargs):
                store.start('orcid-public-api')
                with store.member('orcid-public-api',ids[0]):
                    store.finish_member('orcid-public-api',ids[0],'a'*64)
                # Simulate expiry during independent useful work, without sleep.
                if ids==['two']:
                    with store.db:store.db.execute("DELETE FROM source_pauses WHERE source='wikidata'")
            def wikidata(store,http,ids,**kwargs):
                calls.append(ids)
                store.start('wikidata')
                if len(calls)==1:
                    store.pause_source('wikidata',10**12,'maxlag')
                    store.status('wikidata','access_failed')
                    store.defer('ingest-wikidata','Q1','maxlag',now=0)
                else:
                    with store.member('wikidata','Q1@1'):
                        store.finish_member('wikidata','Q1@1','a'*64)
                    store.status('wikidata','partial')
            with patch('gapi.collection.ingest_orcid_api',side_effect=orcid), \
                 patch('gapi.collection.ingest_wikidata',side_effect=wikidata):
                result=collect(s,None,discovery=False,batch_members=1,max_seconds=10)
            self.assertEqual(len(calls),2)
            self.assertNotIn('ingest-wikidata',result['suspended_sources'])
            self.assertFalse(s.pending('ingest-wikidata'))
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

    def test_pending_matches_original_membership_for_unusual_identifiers_and_retries(self):
        s=self.store(archive([]))
        try:
            identifiers=['Q1','Q2','Q3','Q4','q5','Q6','Q7','Q8','Q%','Q_','',
                'Q9\0tail','学校','A-B','due','future','orcid-only']
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-wikidata',value,'a','b') for value in identifiers])
                s.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)',[
                    ('wikidata',value,'a'*64,status,None) for value,status in [
                        ('Q1','observed'),('Q2@1','observed'),('Q2@2','rejected'),
                        ('Q3@1','rejected'),('Q5@1','observed'),('Q9\0tail','observed'),
                        ('学校@1','observed'),('A-B@1','observed')]])
                s.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',
                    ('orcid-public-api','orcid-only','b'*64,'observed',None))
            for value,when in [('due',100),('future',200)]:s.defer('ingest-wikidata',value,'fixture',now=when)
            for limit in [1,2,4,8,100]:
                original='''SELECT d.identifier FROM discovery d LEFT JOIN discovery_retries r
                    ON r.source=d.source AND r.identifier=d.identifier
                    WHERE d.source='ingest-wikidata' AND COALESCE(r.next_attempt_at,0)<=160
                    AND NOT EXISTS(SELECT 1 FROM members m WHERE m.source='wikidata'
                        AND (m.locator=d.identifier OR m.locator LIKE d.identifier||'@%'))'''
                due=[r[0] for r in s.db.execute(original+' AND r.identifier IS NOT NULL ORDER BY r.next_attempt_at,d.identifier LIMIT ?',(limit,))]
                reserved=min(len(due),max(1,limit//4))
                fresh=[r[0] for r in s.db.execute(original+' AND r.identifier IS NULL ORDER BY d.identifier LIMIT ?',(limit-reserved,))]
                expected=due[:reserved]+fresh+due[reserved:limit-len(fresh)]
                self.assertEqual(s.pending('ingest-wikidata',limit,now=160),expected)
        finally:s.close()

    def test_pending_avoids_quadratic_processed_prefix_scan(self):
        # A VM-instruction ceiling is independent of machine speed. Repeating
        # either 3,000-member join for every candidate exhausts this budget.
        s=self.store(archive([]));ticks=[0]
        def ceiling():
            ticks[0]+=1
            return int(ticks[0]>=2000)
        try:
            with s.db:
                s.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                    ('ingest-wikidata',f'Q{i:06d}','a','b') for i in range(3200)])
                s.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)',[
                    ('wikidata',f'Q{i:06d}@1','a'*64,'observed',None) for i in range(3000)])
            s.db.set_progress_handler(ceiling,1000)
            self.assertEqual(s.pending('ingest-wikidata',200,now=160),
                [f'Q{i:06d}' for i in range(3000,3200)])
            self.assertLess(ticks[0],2000)
        finally:s.db.set_progress_handler(None,0);s.close()

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
