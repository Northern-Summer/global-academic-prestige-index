"""Finite candidate selection, provenance drift and actual SQLite recovery."""
import io
import json
import subprocess
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import test_streaming as fixtures
from gapi.candidate_selection import freeze, verify, processing
from gapi.career_store import CareerStore
from gapi.collection import collect
from gapi.queue_storage import compact_queue
from gapi.task import BudgetStop, Task, canonical, digest


class CandidateSelectionTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_annual_reuse_keeps_api_accounting_and_unknown_failures(self):
        from gapi.orcid_stream import error_document_reasons, summary_roots
        store=self.store(fixtures.archive([]));source='orcid-2026-summary'
        try:
            ids=[f'0000-0002-1825-00{i:02}' for i in range(8)]
            roots=summary_roots(store.task.config['sources'][source]);root=roots[-1]
            store.start(source);store.status(source,'verified')
            with store.db:
                store.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',
                    [('ingest-orcid-api',i,'query','response') for i in ids])
                rows=[]
                for n,i in enumerate(ids[:7]):
                    status='observed' if n<2 else 'rejected'
                    reason=(None if n<2 else error_document_reasons()[n-2] if n<5 else
                            'member_exceeds_size_limit' if n==5 else 'unknown parser failure')
                    prefix=('./'+roots[0]) if n==1 else root
                    rows.append((source,prefix+'/'+i[-3:]+'/'+i+'.xml','a'*64,status,reason))
                store.db.executemany('INSERT INTO members VALUES(?,?,?,?,?)',rows)
                store.db.execute("INSERT INTO discovery_retries VALUES('ingest-orcid-api',?,1,0,'transient')",(ids[5],))
            frozen=freeze(store);upper=frozen['selection']['max_candidate_rowid']
            before=processing(store,frozen['selection_id']);changes=store.db.total_changes
            self.assertEqual(set(store.pending('ingest-orcid-api',now=1)),set(ids))
            for _ in range(2):
                self.assertEqual(set(store.pending('ingest-orcid-api',now=1,max_candidate_rowid=upper,reuse_annual=True)),set(ids[5:]))
            self.assertEqual(processing(store,frozen['selection_id']),before)
            self.assertEqual(store.db.total_changes,changes)
            self.assertEqual(before['sources'][0]['processed'],0)
            self.assertEqual(store.db.execute("SELECT count(*) FROM members WHERE source='orcid-public-api'").fetchone()[0],0)
            with store.db:store.db.execute("UPDATE runs SET identity='different' WHERE source=?",(source,))
            self.assertEqual(set(store.pending('ingest-orcid-api',now=1,max_candidate_rowid=upper,reuse_annual=True)),set(ids))
        finally:store.close()

    def test_annual_reuse_requires_finite_explicit_orcid_scope(self):
        store=self.store(fixtures.archive([]))
        try:
            for command,options in [('ingest-orcid-api',{}),('ingest-wikidata',{'max_candidate_rowid':1}),
                                    ('ingest-orcid-api',{'max_candidate_rowid':None})]:
                with self.assertRaises(ValueError):store.pending(command,reuse_annual=True,**options)
            with self.assertRaises(ValueError):store.pending('ingest-orcid-api',max_candidate_rowid=1,reuse_annual=1)
        finally:store.close()

    def test_collection_opt_in_does_not_request_error_documents_or_claim_success(self):
        from gapi.orcid_stream import error_document_reasons
        store=self.store(fixtures.archive([]));source='orcid-2026-summary'
        try:
            store.start(source);store.status(source,'verified')
            identifier=fixtures.PERSON
            with store.db:
                store.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',('ingest-orcid-api',identifier,'q','r'))
                store.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',
                    (source,'summaries/'+identifier[-3:]+'/'+identifier+'.xml','a'*64,'rejected',error_document_reasons()[0]))
            identity=freeze(store)['selection_id']
            with patch('gapi.collection.ingest_orcid_api') as request:
                result=collect(store,None,max_seconds=10,discovery=False,candidate_selection=identity,reuse_annual=True,full_report=False)
                request.assert_not_called()
            self.assertFalse(result['global_complete'])
            self.assertFalse(result['frozen_candidate_processing']['processing_complete'])
            self.assertEqual(result['frozen_candidate_processing']['sources'][0]['observed_members'],0)
            self.assertEqual(result['frozen_candidate_processing']['sources'][0]['pending'],1)
            self.assertEqual(result['candidate_disposition_policy'],'verified-annual-reuse-and-retained-error-documents/1')
            with self.assertRaises(ValueError):collect(store,None,reuse_annual=True)
        finally:store.close()

    def seed(self,store):
        with store.db:
            store.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',[
                ('ingest-wikidata','Q1','a'*64,'b'*64),
                ('ingest-wikidata','Q2','c'*64,'d'*64),
                ('ingest-orcid-api',fixtures.PERSON,'e'*64,'f'*64)])

    def test_repeated_freeze_and_digest_compaction_do_not_change_selection(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store)
            original=store.db.execute('SELECT rowid,* FROM discovery ORDER BY rowid').fetchall()
            one=freeze(store); two=freeze(store)
            self.assertTrue(one['created']); self.assertFalse(two['created'])
            self.assertEqual(one['selection_id'],two['selection_id'])
            compact_queue(store)
            value,progress=verify(store,one['selection_id'])
            self.assertEqual(value,one['selection'])
            self.assertEqual(sum(row['candidates'] for row in progress['sources']),3)
            self.assertEqual(store.db.execute('SELECT rowid,* FROM discovery ORDER BY rowid').fetchall(),original)
            self.assertEqual(store.db.execute('SELECT count(*) FROM candidate_selections').fetchone()[0],1)
            self.assertEqual(one['candidate_ids_copied'],0)
            self.assertFalse(processing(store,one['selection_id'])['processing_complete'])
        finally:store.close()

    def test_later_discovery_does_not_expand_boundary_or_due_retries(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store); one=freeze(store); identity=one['selection_id']; upper=one['selection']['max_candidate_rowid']
            with store.db:
                store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q0','new-query','new-response')")
                store.db.execute("INSERT INTO discovery_retries VALUES('ingest-wikidata','Q0',1,0,'due')")
                store.db.execute("INSERT INTO discovery_retries VALUES('ingest-wikidata','Q2',1,0,'due')")
            self.assertIn('Q0',store.pending('ingest-wikidata',now=1))
            self.assertEqual(set(store.pending('ingest-wikidata',now=1,max_candidate_rowid=upper)),{'Q1','Q2'})
            _,result=verify(store,identity)
            self.assertEqual(sum(row['candidates'] for row in result['sources']),3)
            self.assertEqual(sum(row['candidates'] for row in store.candidate_progress()['sources']),4)
            newer=freeze(store)
            self.assertNotEqual(newer['selection_id'],identity)
            self.assertFalse(freeze(store)['created'])
            self.assertEqual(store.db.execute('SELECT count(*) FROM candidate_selections').fetchone()[0],2)
        finally:store.close()

    def test_content_deletion_or_provenance_drift_cannot_settle_selection(self):
        for number,mutation in enumerate(("UPDATE discovery SET query_sha256='changed' WHERE identifier='Q1'",
                         "UPDATE discovery SET response_sha256='changed' WHERE identifier='Q1'",
                         "DELETE FROM discovery WHERE identifier='Q1'",
                         "UPDATE discovery SET source='unsupported' WHERE identifier='Q1'")):
            self.config['work_dir']=str(self.root/('drift-'+str(number)))
            self.path=self.root/('drift-'+str(number)+'.json')
            store=self.store(fixtures.archive([]))
            try:
                self.seed(store); identity=freeze(store)['selection_id']
                with store.db:store.db.execute(mutation)
                with self.assertRaises(ValueError):verify(store,identity)
            finally:store.close()

    def test_invalid_boundaries_fail_for_both_read_owners(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store)
            for upper in (-1,True,1.5,'1',2**63):
                with self.subTest(upper=upper):
                    with self.assertRaises(ValueError):store.pending('ingest-wikidata',max_candidate_rowid=upper)
                    with self.assertRaises(ValueError):store.candidate_progress(max_candidate_rowid=upper)
            with self.assertRaises(ValueError):verify(store,'../wrong')
            with self.assertRaises(ValueError):verify(store,'f'*64)
            self.assertEqual(store.db.execute('SELECT count(*) FROM discovery').fetchone()[0],3)
        finally:store.close()

    def test_unknown_source_or_unsettled_transaction_cannot_freeze(self):
        store=self.store(fixtures.archive([]))
        try:
            with store.db:store.db.execute("INSERT INTO discovery VALUES('unknown','x','q','r')")
            with self.assertRaisesRegex(ValueError,'unsupported'):freeze(store)
            store.db.execute('BEGIN')
            with self.assertRaisesRegex(ValueError,'settled'):freeze(store)
            store.db.rollback()
        finally:store.close()

    def test_manifest_tampering_configuration_change_and_budget_failure(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store)
            original_check=store.task.check
            def fail(*a,**kw):
                if store.db.in_transaction:raise BudgetStop('injected selection admission failure')
                return original_check(*a,**kw)
            with patch.object(store.task,'check',side_effect=fail),self.assertRaises(BudgetStop):freeze(store)
            identity=freeze(store)['selection_id']
            with store.db:store.db.execute("UPDATE candidate_selections SET payload='{}'")
            with self.assertRaises(ValueError):verify(store,identity)
            store.task.config['rules_version']='changed'
            with self.assertRaisesRegex(ValueError,'freeze|frozen'):freeze(store)
        finally:store.close()

    def test_collection_uses_frozen_prefix_and_never_claims_global_completion(self):
        store=self.store(fixtures.archive([])); requested=[]
        try:
            with store.db:store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
            identity=freeze(store)['selection_id']
            with store.db:store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q2','c','d')")
            def ingest(store,http,ids,**options):
                requested.extend(ids)
                store.start('wikidata')
                with store.member('wikidata','fixture'):
                    for identifier in ids:store.finish_member('wikidata',identifier+'@1','a'*64)
            with patch('gapi.collection.ingest_wikidata',side_effect=ingest):
                result=collect(store,None,max_seconds=10,discovery=False,candidate_selection=identity)
            self.assertEqual(requested,['Q1'])
            self.assertTrue(result['frozen_candidate_processing']['processing_complete'])
            self.assertEqual(result['frozen_candidate_processing']['sources'][0]['observed_fraction'],1)
            self.assertFalse(result['global_complete'])
            self.assertEqual(store.pending('ingest-wikidata'),['Q2'])
            with self.assertRaisesRegex(ValueError,'queued-only'):
                collect(store,None,candidate_selection=identity)
            with store.db:store.db.execute("UPDATE members SET status='rejected'")
            self.assertFalse(processing(store,identity)['processing_complete'])
        finally:store.close()

    def test_collection_reports_verified_progress_only_at_settled_batch_boundaries(self):
        store=self.store(fixtures.archive([]));clock=[0.0];messages=[]
        try:
            with store.db:
                store.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',
                    [('ingest-wikidata','Q'+str(i),'a','b') for i in (1,2,3)])
            identity=freeze(store)['selection_id']
            def ingest(store,http,ids,**options):
                store.start('wikidata')
                with store.member('wikidata','fixture'):
                    for identifier in ids:store.finish_member('wikidata',identifier+'@1','a'*64)
                clock[0]+=1
            def progress(value):
                self.assertFalse(store.db.in_transaction)
                messages.append(value)
            with patch('gapi.collection.time.monotonic',side_effect=lambda:clock[0]), \
                 patch('gapi.collection.CANDIDATE_PROGRESS_INTERVAL_SECONDS',2,create=True), \
                 patch('gapi.collection.ingest_wikidata',side_effect=ingest):
                result=collect(store,None,max_seconds=5,batch_members=1,discovery=False,
                               candidate_selection=identity,progress=progress)
            updates=[m for m in messages if m['stage']=='frozen-candidate-processing']
            self.assertEqual([m['phase'] for m in updates],['start','interval'])
            self.assertEqual([m['processing']['sources'][0]['processed'] for m in updates],[0,2])
            self.assertTrue(all(m['processing']['selection_id']==identity for m in updates))
            self.assertTrue(all(m['processing']['sources'][0]['candidates']==3 for m in updates))
            self.assertTrue(result['frozen_candidate_processing']['processing_complete'])
            self.assertFalse(result['global_complete'])
        finally:store.close()

    def test_periodic_progress_reverifies_provenance_before_next_batch(self):
        store=self.store(fixtures.archive([]));clock=[0.0];requested=[]
        try:
            with store.db:
                store.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',
                    [('ingest-wikidata','Q'+str(i),'a','b') for i in (1,2,3)])
            identity=freeze(store)['selection_id']
            def ingest(store,http,ids,**options):
                requested.extend(ids);store.start('wikidata')
                with store.member('wikidata','fixture'):
                    for identifier in ids:store.finish_member('wikidata',identifier+'@1','a'*64)
                clock[0]+=1
                if len(requested)==2:
                    with store.db:store.db.execute("UPDATE discovery SET response_sha256='drift' WHERE identifier='Q1'")
            with patch('gapi.collection.time.monotonic',side_effect=lambda:clock[0]), \
                 patch('gapi.collection.CANDIDATE_PROGRESS_INTERVAL_SECONDS',2,create=True), \
                 patch('gapi.collection.ingest_wikidata',side_effect=ingest), \
                 self.assertRaisesRegex(ValueError,'provenance changed'):
                collect(store,None,max_seconds=5,batch_members=1,discovery=False,
                        candidate_selection=identity,progress=lambda value:None)
            self.assertEqual(requested,['Q1','Q2'])
            self.assertFalse(store.db.in_transaction)
        finally:store.close()

    def test_progress_budget_failure_preserves_commits_and_resumes_remaining_candidates(self):
        store=self.store(fixtures.archive([]));clock=[0.0];requested=[]
        try:
            with store.db:
                store.db.executemany('INSERT INTO discovery VALUES(?,?,?,?)',
                    [('ingest-wikidata','Q'+str(i),'a','b') for i in (1,2,3)])
            identity=freeze(store)['selection_id'];calls=[0]
            def checked(*args):
                calls[0]+=1
                if calls[0]==2:raise BudgetStop('injected progress memory admission')
                return processing(*args)
            def ingest(store,http,ids,**options):
                requested.extend(ids);store.start('wikidata')
                with store.member('wikidata','fixture'):
                    for identifier in ids:store.finish_member('wikidata',identifier+'@1','a'*64)
                clock[0]+=1
            with patch('gapi.collection.time.monotonic',side_effect=lambda:clock[0]), \
                 patch('gapi.collection.CANDIDATE_PROGRESS_INTERVAL_SECONDS',2), \
                 patch('gapi.collection.ingest_wikidata',side_effect=ingest), \
                 patch('gapi.candidate_selection.processing',side_effect=checked), \
                 self.assertRaisesRegex(BudgetStop,'progress memory'):
                collect(store,None,max_seconds=5,batch_members=1,discovery=False,
                        candidate_selection=identity,progress=lambda value:None)
            self.assertEqual(requested,['Q1','Q2'])
            self.assertFalse(store.db.in_transaction)
            store.close();store=CareerStore(Task(self.path),source_compression=False)
            self.assertEqual(store.pending('ingest-wikidata'),['Q3'])
            with patch('gapi.collection.ingest_wikidata',side_effect=ingest), \
                 patch('gapi.candidate_selection.processing',wraps=processing) as reports:
                result=collect(store,None,max_seconds=5,batch_members=1,discovery=False,
                               candidate_selection=identity)
            self.assertEqual(reports.call_count,2)  # Start and settlement, no callback work.
            self.assertEqual(requested,['Q1','Q2','Q3'])
            self.assertTrue(result['frozen_candidate_processing']['processing_complete'])
        finally:store.close()

    def test_collection_deadline_skips_due_interval_report(self):
        store=self.store(fixtures.archive([]));clock=[0.0];messages=[]
        try:
            with store.db:store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','a','b')")
            identity=freeze(store)['selection_id']
            def ingest(store,http,ids,**options):
                store.start('wikidata')
                with store.member('wikidata','fixture'):
                    store.finish_member('wikidata','Q1@1','a'*64)
                clock[0]=1
            with patch('gapi.collection.time.monotonic',side_effect=lambda:clock[0]), \
                 patch('gapi.collection.CANDIDATE_PROGRESS_INTERVAL_SECONDS',0.5), \
                 patch('gapi.collection.ingest_wikidata',side_effect=ingest), \
                 patch('gapi.candidate_selection.processing',wraps=processing) as reports:
                result=collect(store,None,max_seconds=1,batch_members=1,discovery=False,
                               candidate_selection=identity,progress=messages.append)
            self.assertEqual(reports.call_count,2)
            self.assertEqual([m['phase'] for m in messages if m['stage']=='frozen-candidate-processing'],['start'])
            self.assertEqual(result['status'],'resumable-time-boundary')
            self.assertTrue(result['frozen_candidate_processing']['processing_complete'])
            self.assertFalse(result['global_complete'])
        finally:store.close()

    def test_actual_process_exit_before_or_after_commit_has_atomic_selection(self):
        code='''
import os,sys
from pathlib import Path
from gapi.candidate_selection import freeze
from gapi.career_store import CareerStore
from gapi.task import Task
store=CareerStore(Task(Path(sys.argv[1])))
if sys.argv[2]=='before':
    store.db.set_trace_callback(lambda sql:os._exit(37) if sql=='COMMIT' else None)
freeze(store)
os._exit(38)
'''
        for point,exit_code in (('before',37),('after',38)):
            self.config['work_dir']=str(self.root/point);self.path=self.root/(point+'.json')
            store=self.store(fixtures.archive([]));self.seed(store)
            expected=digest({'schema_version':'gapi-candidate-selection/1','config_sha256':digest(store.task.config),
                'max_candidate_rowid':3,'sources':[{key:row[key] for key in ('source','candidates','selection_sha256')}
                    for row in store.candidate_progress()['sources']]})
            store.close()
            run=subprocess.run([sys.executable,'-B','-c',code,str(self.path),point],capture_output=True,text=True,timeout=20)
            self.assertEqual(run.returncode,exit_code,run.stderr)
            store=CareerStore(Task(self.path))
            try:
                result=freeze(store)
                self.assertEqual(result['selection_id'],expected)
                self.assertEqual(result['created'],point=='before')
                self.assertEqual(store.db.execute('SELECT count(*) FROM candidate_selections').fetchone()[0],1)
                self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
            finally:store.close()

    def test_cli_freeze_uses_existing_lock_and_zero_network(self):
        from gapi.stream_cli import execute_task
        store=self.store(fixtures.archive([]));self.seed(store)
        command=[sys.executable,'-B','-m','gapi','freeze-candidates','--task',str(self.path)]
        try:
            run=subprocess.run(command,capture_output=True,text=True,timeout=15)
            self.assertNotEqual(run.returncode,0)
        finally:store.close()
        with patch('gapi.source_http.PublicHTTP.open',side_effect=AssertionError('HTTP')) as http, \
             patch('socket.socket.connect',side_effect=AssertionError('socket')) as connect, \
             patch('sys.stdout',new_callable=io.StringIO) as output:
            self.assertTrue(execute_task(Task(self.path),SimpleNamespace(command='freeze-candidates')))
            self.assertTrue(json.loads(output.getvalue())['created'])
        http.assert_not_called();connect.assert_not_called()

    def test_read_only_status_preserves_gapped_rowids_on_backup_and_projection(self):
        from gapi.monitoring import snapshot_report
        store=self.store(fixtures.archive([]))
        try:
            with store.db:
                store.db.execute("INSERT INTO discovery(rowid,source,identifier,query_sha256,response_sha256) VALUES(17,'ingest-wikidata','Q1','a','b')")
            identity=freeze(store)['selection_id']
            with store.db:
                store.db.execute("INSERT INTO discovery(rowid,source,identifier,query_sha256,response_sha256) VALUES(29,'ingest-wikidata','Q2','c','d')")
            expected=processing(store,identity)
            before={p.name for p in store.task.root.iterdir()}
            self.assertEqual(snapshot_report(store.task,candidate_selection=identity)['frozen_candidate_processing'],expected)
            original_stat=type(store.task.root).stat
            class LargeFile:
                st_size=store.task.config['memory_bytes']
            def size(path,*args,**kw):
                if path==store.task.root/'careers.sqlite':return LargeFile()
                return original_stat(path,*args,**kw)
            with patch.object(type(store.task.root),'stat',size):
                actual=snapshot_report(store.task,candidate_selection=identity)
            self.assertEqual(actual['frozen_candidate_processing'],expected)
            self.assertEqual(actual['frozen_candidate_processing']['sources'][0]['candidates'],1)
            self.assertEqual(actual['candidate_processing']['sources'][0]['candidates'],2)
            self.assertEqual({p.name for p in store.task.root.iterdir()},before)
        finally:store.close()
