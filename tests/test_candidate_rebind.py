"""Real SQLite inheritance across resource-only source-freeze transitions."""
import json
import subprocess
import sys
import unittest
from unittest.mock import patch

import test_streaming as fixtures
from gapi.candidate_selection import freeze, processing, rebind_storage_roots
from gapi.career_store import CareerStore
from gapi.task import BudgetStop, Task, canonical, digest


class CandidateRebindTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def prepare(self):
        store=self.store(fixtures.archive([]))
        with store.db:
            store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q1','q','r')")
        previous=freeze(store)
        with store.db:
            store.db.execute("INSERT INTO discovery VALUES('ingest-wikidata','Q2','later','later')")
        old=store.task.require_frozen()
        store.close()
        delivery=self.root/'delivery';delivery.mkdir()
        self.config['additional_storage_roots']=[
            *self.config.get('additional_storage_roots',[]),str(delivery)]
        self.save_config()
        return CareerStore(Task(self.path)),previous,old

    def save_config(self):
        self.path.write_bytes(canonical(self.config))
        freeze_path=self.root/'store'/'sources.lock.json'
        freeze_path.write_bytes(canonical({'schema_version':'gapi-source-freeze/v1',
            'config':self.config,'config_sha256':digest(self.config)}))

    def test_inherits_original_prefix_and_retry_without_rewriting_predecessor(self):
        store,previous,old=self.prepare()
        try:
            before=store.db.execute('SELECT payload FROM candidate_selections').fetchone()[0]
            result=rebind_storage_roots(store,previous['selection_id'],old)
            self.assertNotEqual(result['selection_id'],previous['selection_id'])
            self.assertEqual(result['selection']['sources'],previous['selection']['sources'])
            self.assertEqual(result['selection']['max_candidate_rowid'],1)
            self.assertEqual(processing(store,result['selection_id'])['sources'][0]['candidates'],1)
            self.assertEqual(store.candidate_progress()['sources'][0]['candidates'],2)
            self.assertTrue(result['created'])
            self.assertFalse(rebind_storage_roots(store,previous['selection_id'],old)['created'])
            self.assertEqual(store.db.execute('SELECT count(*) FROM candidate_selections').fetchone()[0],2)
            self.assertEqual(store.db.execute('SELECT payload FROM candidate_selections WHERE id=?',
                (previous['selection_id'],)).fetchone()[0],before)
            self.assertEqual(result['candidate_ids_copied'],0)
        finally:store.close()

    def test_source_rule_budget_or_root_change_is_not_a_storage_registration(self):
        store,previous,old=self.prepare()
        try:
            baseline=json.loads(canonical(store.task.config))
            for key,value in [('rules_version','changed'),('budget_bytes',31_000_000_000),
                              ('work_dir',str(self.root/'different')),
                              ('sources',{})]:
                store.task.config={**baseline,key:value}
                with patch.object(store.task,'require_frozen'),self.assertRaisesRegex(ValueError,'only storage'):
                    rebind_storage_roots(store,previous['selection_id'],old)
            store.task.config=baseline
            self.assertEqual(store.db.execute('SELECT count(*) FROM candidate_selections').fetchone()[0],1)
        finally:store.close()

    def test_invalid_predecessor_and_provenance_drift_are_retained_as_failures(self):
        store,previous,old=self.prepare()
        try:
            for invalid in ({},dict(old,config_sha256='0'*64)):
                with self.assertRaisesRegex(ValueError,'previous source freeze'):
                    rebind_storage_roots(store,previous['selection_id'],invalid)
            with store.db:store.db.execute("UPDATE discovery SET response_sha256='drift' WHERE identifier='Q1'")
            with self.assertRaisesRegex(ValueError,'provenance'):
                rebind_storage_roots(store,previous['selection_id'],old)
            self.assertEqual(store.db.execute('SELECT count(*) FROM candidate_selections').fetchone()[0],1)
        finally:store.close()

    def test_budget_failure_rolls_back_successor_and_reopening_can_retry(self):
        store,previous,old=self.prepare()
        original=store.task.check
        def fail(*args,**kwargs):
            if store.db.in_transaction:raise BudgetStop('injected rebind capacity failure')
            return original(*args,**kwargs)
        try:
            with patch.object(store.task,'check',side_effect=fail),self.assertRaises(BudgetStop):
                rebind_storage_roots(store,previous['selection_id'],old)
            self.assertEqual(store.db.execute('SELECT count(*) FROM candidate_selections').fetchone()[0],1)
        finally:store.close()
        store=CareerStore(Task(self.path))
        try:
            self.assertTrue(rebind_storage_roots(store,previous['selection_id'],old)['created'])
            self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
        finally:store.close()

    def test_unsettled_transaction_and_accounting_root_removal_are_rejected(self):
        store,previous,old=self.prepare()
        try:
            store.db.execute('BEGIN')
            with self.assertRaisesRegex(ValueError,'settled'):
                rebind_storage_roots(store,previous['selection_id'],old)
            store.db.rollback()
            prior=json.loads(canonical(old));prior['config']['additional_storage_roots']=[str(self.root/'retained')]
            prior['config_sha256']=digest(prior['config'])
            with self.assertRaisesRegex(ValueError,'remove accounted'):
                rebind_storage_roots(store,previous['selection_id'],prior)
        finally:store.close()

    def test_real_process_exit_before_or_after_commit_resumes_same_successor(self):
        store,previous,old=self.prepare();store.close()
        old_path=self.root/'previous-freeze.json';old_path.write_bytes(canonical(old))
        code='''
import json,os,sys
from pathlib import Path
from gapi.candidate_selection import rebind_storage_roots
from gapi.career_store import CareerStore
from gapi.task import Task
s=CareerStore(Task(Path(sys.argv[1])))
if sys.argv[4]=='before':
    s.db.set_trace_callback(lambda sql:os._exit(37) if sql=='COMMIT' else None)
rebind_storage_roots(s,sys.argv[2],json.loads(Path(sys.argv[3]).read_bytes()))
os._exit(38)
'''
        for point,exit_code in [('before',37),('after',38)]:
            run=subprocess.run([sys.executable,'-B','-c',code,str(self.path),
                previous['selection_id'],str(old_path),point],capture_output=True,text=True,timeout=20)
            self.assertEqual(run.returncode,exit_code,run.stderr)
            store=CareerStore(Task(self.path))
            try:
                result=rebind_storage_roots(store,previous['selection_id'],old)
                self.assertEqual(result['created'],point=='before')
                self.assertEqual(processing(store,result['selection_id'])['max_candidate_rowid'],1)
                self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
                if point=='before':
                    # The next subprocess must crash after a new insertion,
                    # rather than merely replaying an already present successor.
                    with store.db:store.db.execute('DELETE FROM candidate_selections WHERE id=?',
                                                  (result['selection_id'],))
            finally:store.close()


if __name__=='__main__':unittest.main()
