"""Annual rejection follow-up reuses candidate ownership and exclusion."""
import hashlib
import io
import json
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import test_streaming as fixtures
from test_archive_commit_batches import person
from gapi.archive_recovery import queue_archive_rejections
from gapi.career_store import CareerStore
from gapi.task import BudgetStop, Task

SOURCE='orcid-2026-summary'


class ArchiveRecoveryTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def seed(self,store,count=5):
        store.start(SOURCE)
        with store.member('fixture','rejections'):
            for number in range(1,count+1):
                identifier=person(number)
                store.finish_member(SOURCE,f'summaries/{identifier[-3:]}/{identifier}.xml',
                    hashlib.sha256(identifier.encode()).hexdigest(),'rejected',
                    "ORCID XML root identity does not match frozen member locator; element='error'; identity_path=missing")

    def test_queue_replay_preserves_member_ledger_and_existing_candidate_provenance(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store)
            with store.db:
                store.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',
                                 ('ingest-orcid-api',person(1),'original-query','original-response'))
            before=store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall()
            run=store.db.execute('SELECT * FROM runs').fetchall()
            first=queue_archive_rejections(store,SOURCE,max_members=2)
            self.assertEqual((first['new_candidates'],first['existing_candidates']),(1,1))
            second=queue_archive_rejections(store,SOURCE,after_rowid=first['after_rowid'])
            self.assertEqual(second['new_candidates'],3)
            self.assertEqual(queue_archive_rejections(store,SOURCE)['new_candidates'],0)
            self.assertEqual(store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall(),before)
            self.assertEqual(store.db.execute('SELECT * FROM runs').fetchall(),run)
            self.assertEqual(store.db.execute('SELECT query_sha256,response_sha256 FROM discovery WHERE identifier=?',
                                             (person(1),)).fetchone(),('original-query','original-response'))
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM discovery_retries').fetchone()[0],0)
            self.assertFalse(store.source_integrity()['all_archive_members_processed'])
            self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
        finally:store.close()

    def test_invalid_identity_path_or_digest_is_retained_without_queueing(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store,1)
            rows=[('../escape.xml','a'*64),
                  ('summaries/000/0000-0000-0000-0000.xml','a'*64),
                  (f'summaries/000/{person(2)}.xml','not-a-digest')]
            with store.member('fixture','invalid'):
                for locator,checksum in rows:
                    store.finish_member(SOURCE,locator,checksum,'rejected','retained invalid fixture')
                store.finish_member(SOURCE,f'summaries/000/{person(3)}.xml','b'*64,'observed')
            result=queue_archive_rejections(store,SOURCE)
            self.assertEqual(result['new_candidates'],1)
            self.assertEqual(result['invalid_members_retained'],3)
            self.assertEqual(store.db.execute('SELECT count(*) FROM members').fetchone()[0],5)
        finally:store.close()

    def test_oversized_member_rejection_gets_followup_without_becoming_success(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store,1)
            with store.db:
                store.db.execute("UPDATE members SET reason='member_exceeds_size_limit'")
            result=queue_archive_rejections(store,SOURCE)
            self.assertEqual(result['new_candidates'],1)
            self.assertEqual(store.db.execute('SELECT status,reason FROM members').fetchone(),
                             ('rejected','member_exceeds_size_limit'))
        finally:store.close()

    def test_injected_commit_admission_failure_rolls_back_candidates(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store)
            original=store.task.check
            def fail(*args,**kwargs):
                if store.db.in_transaction:
                    raise BudgetStop('injected follow-up admission failure')
                return original(*args,**kwargs)
            with patch.object(store.task,'check',side_effect=fail),self.assertRaises(BudgetStop):
                queue_archive_rejections(store,SOURCE)
            self.assertEqual(store.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
            self.assertFalse(store.db.in_transaction)
            self.assertEqual(queue_archive_rejections(store,SOURCE)['new_candidates'],5)
        finally:store.close()

    def test_changed_unregistered_or_missing_source_cannot_queue(self):
        store=self.store(fixtures.archive([]))
        try:
            with self.assertRaisesRegex(ValueError,'missing or changed'):
                queue_archive_rejections(store,SOURCE)
            self.seed(store)
            for source in ('unknown','wikidata'):
                with self.assertRaisesRegex(ValueError,'annual ORCID'):
                    queue_archive_rejections(store,source)
            store.task.config['sources'][SOURCE]['version']='changed'
            with self.assertRaisesRegex(ValueError,'missing or changed'):
                queue_archive_rejections(store,SOURCE)
        finally:store.close()

    def test_actual_process_exit_does_not_duplicate_followup(self):
        code='''
import os,sys
from pathlib import Path
from gapi.task import Task
from gapi.career_store import CareerStore
from gapi.archive_recovery import queue_archive_rejections
store=CareerStore(Task(Path(sys.argv[1])))
if sys.argv[2]=='before':
    store.db.set_trace_callback(lambda sql:os._exit(37) if sql=='COMMIT' else None)
queue_archive_rejections(store,'orcid-2026-summary')
os._exit(38)
'''
        for point,expected,exit_code in (('before',0,37),('after',5,38)):
            self.config['work_dir']=str(self.root/point)
            self.path=self.root/(point+'.json')
            store=self.store(fixtures.archive([]))
            self.seed(store)
            store.close()
            result=subprocess.run([sys.executable,'-B','-c',code,str(self.path),point],
                                  capture_output=True,text=True,timeout=20)
            self.assertEqual(result.returncode,exit_code,result.stderr)
            store=CareerStore(Task(self.path))
            try:
                self.assertEqual(store.db.execute('SELECT count(*) FROM discovery').fetchone()[0],expected)
                queue_archive_rejections(store,SOURCE)
                self.assertEqual(store.db.execute('SELECT count(*) FROM discovery').fetchone()[0],5)
                self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
            finally:store.close()

    def test_cli_respects_live_writer_and_does_not_make_network_requests(self):
        store=self.store(fixtures.archive([]))
        self.seed(store)
        command=[sys.executable,'-B','-m','gapi','queue-archive-rejections','--task',str(self.path)]
        try:
            denied=subprocess.run(command,capture_output=True,text=True,timeout=15)
            self.assertNotEqual(denied.returncode,0)
        finally:store.close()
        for expected in (5,0):
            result=subprocess.run(command,capture_output=True,text=True,timeout=15)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(json.loads(result.stdout)['new_candidates'],expected)

    def test_invalid_limits_do_not_mutate_candidate_or_member_ledgers(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store)
            before=store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall()
            cases=[{'after_rowid':value} for value in (-1,True,1.5,'0')]
            cases += [{'max_members':value} for value in (0,100001,True,1.5)]
            cases += [{'max_seconds':value} for value in (0,-1,True,None,'1',float('nan'),float('inf'))]
            for options in cases:
                with self.subTest(options=options),self.assertRaises(ValueError):
                    queue_archive_rejections(store,SOURCE,**options)
            self.assertEqual(store.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall(),before)
        finally:store.close()

    def test_missing_or_wrong_api_kind_and_unsettled_transaction_fail_closed(self):
        store=self.store(fixtures.archive([]))
        try:
            self.seed(store)
            original=store.task.config['sources'].pop('orcid-public-api')
            with self.assertRaisesRegex(ValueError,'not registered'):
                queue_archive_rejections(store,SOURCE)
            store.task.config['sources']['orcid-public-api']={**original,'kind':'unrelated'}
            with self.assertRaisesRegex(ValueError,'not registered'):
                queue_archive_rejections(store,SOURCE)
            store.task.config['sources']['orcid-public-api']=original
            store.db.execute('BEGIN')
            with self.assertRaisesRegex(ValueError,'settled transaction'):
                queue_archive_rejections(store,SOURCE)
            store.db.rollback()
            self.assertEqual(store.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
        finally:store.close()

    def test_cli_entry_makes_zero_http_or_socket_calls(self):
        from gapi.stream_cli import execute_task
        store=self.store(fixtures.archive([]))
        self.seed(store)
        store.close()
        args=SimpleNamespace(command='queue-archive-rejections',source=SOURCE,
            after_rowid=0,max_members=5,max_seconds=60)
        with patch('gapi.source_http.PublicHTTP.open',side_effect=AssertionError('unexpected HTTP')) as http, \
             patch('socket.socket.connect',side_effect=AssertionError('unexpected socket')) as connect, \
             patch('sys.stdout',new_callable=io.StringIO) as output:
            self.assertTrue(execute_task(Task(self.path),args))
            self.assertEqual(json.loads(output.getvalue())['new_candidates'],5)
        http.assert_not_called()
        connect.assert_not_called()
