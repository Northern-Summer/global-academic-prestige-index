"""Bounded archive commits preserve facts, checksums and crash replay."""
import io
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import test_streaming as fixtures
from gapi.career_store import CareerStore, verified_archive_sql
from gapi.event_storage import logical_event_hash
from gapi.orcid_stream import stream_archive
from gapi.task import BudgetStop

SOURCE = 'orcid-2026-summary'


def person(number):
    first = f'{number:015d}'
    total = 0
    for digit in first:
        total = (total+int(digit))*2
    check = (12-total%11)%11
    digits = first+('X' if check == 10 else str(check))
    return '-'.join(digits[i:i+4] for i in range(0,16,4))


def members(count):
    return [(f'summaries/{identifier[-3:]}/{identifier}.xml',
             fixtures.XML.replace(fixtures.PERSON.encode(), identifier.encode()))
            for identifier in map(person, range(1,count+1))]


class ArchiveCommitBatchTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_batch_advances_counter_once_and_preserves_duplicate_evidence(self):
        store = self.store(fixtures.archive([]))
        try:
            store.start(SOURCE)
            rows = [('a', 'a'*64, 'observed', None),
                    ('b', 'b'*64, 'rejected', 'malformed XML'),
                    ('a', 'a'*64, 'rejected', 'duplicate must retain first state')]
            statements = []
            store.db.set_trace_callback(statements.append)
            with store.member(SOURCE, 'ledger-batch'):
                store.finish_members(SOURCE, rows)
            store.db.set_trace_callback(None)
            self.assertEqual(store.db.execute('SELECT locator,sha256,status,reason FROM members ORDER BY rowid').fetchall(), rows[:2])
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?', (SOURCE,)).fetchone()[0], 2)
            self.assertEqual(sum(sql.startswith('UPDATE runs SET members=members+') for sql in statements), 1)
            with store.member(SOURCE, 'same-ledger-batch'):
                store.finish_members(SOURCE, rows)
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?', (SOURCE,)).fetchone()[0], 2)
        finally:
            store.db.set_trace_callback(None)
            store.close()

    def test_batch_digest_conflict_rolls_back_rows_and_counter(self):
        store = self.store(fixtures.archive([]))
        try:
            store.start(SOURCE)
            with store.member(SOURCE, 'original'):
                store.finish_member(SOURCE, 'existing', 'a'*64)
            with self.assertRaisesRegex(ValueError, 'member changed'):
                with store.member(SOURCE, 'conflicting-batch'):
                    store.finish_members(SOURCE, [('new', 'b'*64, 'observed', None),
                                                 ('existing', 'c'*64, 'observed', None)])
            self.assertIsNone(store.seen(SOURCE, 'new'))
            self.assertEqual(store.seen(SOURCE, 'existing'), ('a'*64, 'observed'))
            self.assertEqual(store.db.execute('SELECT members FROM runs WHERE source=?', (SOURCE,)).fetchone()[0], 1)
        finally:
            store.close()

    def test_cli_refuses_committed_prefix_with_or_without_resume(self):
        from types import SimpleNamespace
        from gapi.stream_cli import execute_task
        data=fixtures.archive(members(2))
        store=self.store(data)
        stream_archive(store,SOURCE,io.BytesIO(data),max_members=1,full_report=False)
        task=store.task
        before=store.db.execute('SELECT * FROM runs WHERE source=?',(SOURCE,)).fetchone()
        store.close()
        args=SimpleNamespace(command='ingest-orcid',source=SOURCE,resume=True,
            commit_members=1,max_members=None,max_new_members=None,max_seconds=1)
        for resume in (True,False):
            args.resume=resume
            with self.subTest(resume=resume),patch('gapi.stream_cli.PublicHTTP') as http:
                with self.assertRaisesRegex(ValueError,'would replay committed input'):
                    execute_task(task,args)
                http.return_value.open_archive.assert_not_called()
                http.return_value.open.assert_not_called()
        store=CareerStore(task)
        try:
            self.assertEqual(store.db.execute('SELECT * FROM runs WHERE source=?',(SOURCE,)).fetchone(),before)
            self.assertEqual(store.db.execute('SELECT count(*) FROM members WHERE source=?',(SOURCE,)).fetchone()[0],1)
        finally:store.close()

    def test_logical_rows_match_single_member_commits(self):
        data = fixtures.archive(members(13))
        first = self.store(data, source_compression=True)
        try:
            stream_archive(first,SOURCE,io.BytesIO(data))
            expected = first.db.execute('SELECT rowid,* FROM events ORDER BY rowid').fetchall()
            expected_members = first.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall()
            expected_hash = logical_event_hash(first.db)
        finally:
            first.close()
        self.config['work_dir'] = str(self.root/'second-store')
        self.path = self.root/'second-task.json'
        second = self.store(data, source_compression=True)
        try:
            with patch.object(second,'member',wraps=second.member) as commits:
                result = stream_archive(second,SOURCE,io.BytesIO(data),commit_members=5)
            self.assertEqual(commits.call_count,3)
            self.assertEqual(second.db.execute('SELECT rowid,* FROM events ORDER BY rowid').fetchall(),expected)
            self.assertEqual(second.db.execute('SELECT rowid,* FROM members ORDER BY rowid').fetchall(),expected_members)
            self.assertEqual(logical_event_hash(second.db),expected_hash)
            self.assertEqual(result['sources'][0]['status'],'verified')
            with patch.object(second,'member',side_effect=AssertionError('replay writes')):
                stream_archive(second,SOURCE,io.BytesIO(data),commit_members=5)
        finally:
            second.close()

    def test_existing_event_limit_bounds_each_transaction(self):
        data = fixtures.archive(members(5))
        store = self.store(data,batch_events=2)
        try:
            with patch.object(store,'member',wraps=store.member) as commits:
                result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=128)
            self.assertEqual(commits.call_count,3)
            self.assertEqual(result['events'],5)
            self.assertEqual(result['sources'][0]['members'],5)
        finally:
            store.close()

    def test_member_exceeding_event_limit_keeps_no_partial_events(self):
        rows = members(2)
        affiliation = rows[0][1].split(b'<education-summary',1)[1].split(b'</education-summary>',1)[0]
        rows[0] = (rows[0][0],rows[0][1].replace(b'</record>',
                   b'<education-summary'+affiliation.replace(b'put-code="1"',b'put-code="2"')+
                   b'</education-summary></record>'))
        data = fixtures.archive(rows)
        store = self.store(data,batch_events=1)
        try:
            result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=32)
            self.assertEqual(result['events'],1)
            self.assertEqual(result['member_status'],{'observed':1,'rejected':1})
            self.assertEqual(store.db.execute('SELECT count(*) FROM events WHERE locator=?',(rows[0][0],)).fetchone()[0],0)
        finally:
            store.close()

    def test_interruption_drops_only_uncommitted_members_and_replays(self):
        data = fixtures.archive(members(5))
        store = self.store(data)
        original = store.seen
        final_locator = members(5)[-1][0]
        def interrupt(source,locator):
            if locator == final_locator:
                raise KeyboardInterrupt()
            return original(source,locator)
        try:
            with patch.object(store,'seen',side_effect=interrupt),self.assertRaises(KeyboardInterrupt):
                stream_archive(store,SOURCE,io.BytesIO(data),commit_members=3)
            report = store.report()
            self.assertEqual(report['events'],3)
            self.assertEqual(report['sources'][0]['members'],3)
            self.assertEqual(report['sources'][0]['status'],'interrupted')
            report = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=3)
            self.assertEqual(report['events'],5)
            self.assertEqual(report['sources'][0]['members'],5)
            self.assertEqual(report['sources'][0]['status'],'verified')
        finally:
            store.close()

    def test_rejected_member_discards_partial_events_without_harming_others(self):
        rows = members(4)
        rows[1] = (rows[1][0],rows[1][1]+b'<malformed>')
        data = fixtures.archive(rows)
        store = self.store(data)
        try:
            result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=4)
            self.assertEqual(result['events'],3)
            self.assertEqual(result['member_status'],{'observed':3,'rejected':1})
            self.assertEqual(store.db.execute('SELECT count(*) FROM events WHERE locator=?',(rows[1][0],)).fetchone()[0],0)
            self.assertEqual(result['sources'][0]['status'],'verified')
        finally:
            store.close()

    def test_pending_duplicate_is_hashed_and_not_inserted_twice(self):
        rows = members(3)
        data = fixtures.archive([rows[0],rows[0],*rows[1:]])
        store = self.store(data)
        try:
            result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=4)
            self.assertEqual(result['events'],3)
            self.assertEqual(result['sources'][0]['members'],3)
        finally:
            store.close()

    def test_changed_pending_duplicate_is_not_verified(self):
        row = members(1)[0]
        data = fixtures.archive([row,(row[0],row[1]+b' ')])
        store = self.store(data)
        try:
            with self.assertRaisesRegex(ValueError,'member changed'):
                stream_archive(store,SOURCE,io.BytesIO(data),commit_members=4)
            self.assertEqual(store.report()['sources'][0]['status'],'failed')
            self.assertEqual(store.report()['events'],0)
        finally:
            store.close()

    def test_new_member_bound_settles_buffer_and_replay_is_idempotent(self):
        data = fixtures.archive(members(3))
        store = self.store(data)
        try:
            for expected in (1,2,3):
                result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=4,max_new_members=1)
                self.assertEqual(result['events'],expected)
                self.assertEqual(result['sources'][0]['members'],expected)
                self.assertEqual(result['sources'][0]['status'],'provisional')
            result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=4)
            self.assertEqual(result['events'],3)
            self.assertEqual(result['sources'][0]['status'],'verified')
        finally:
            store.close()

    def test_budget_failure_rolls_back_entire_batch_and_can_replay(self):
        data = fixtures.archive(members(6))
        store = self.store(data)
        original = store.task.check
        def fail_before_commit(*args,**kwargs):
            if store.db.in_transaction:
                raise BudgetStop('injected commit admission failure')
            return original(*args,**kwargs)
        try:
            with patch.object(store.task,'check',side_effect=fail_before_commit):
                result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=3)
            self.assertEqual(result['events'],0)
            self.assertEqual(result['sources'][0]['members'],0)
            self.assertEqual(result['sources'][0]['status'],'budget_stopped')
            self.assertFalse(store.db.in_transaction)
            result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=3)
            self.assertEqual(result['events'],6)
            self.assertEqual(result['sources'][0]['status'],'verified')
        finally:
            store.close()

    def test_progress_only_exposes_committed_members(self):
        data = fixtures.archive(members(101))
        store = self.store(data)
        observations = []
        def progress(value):
            committed = store.db.execute('SELECT count(*) FROM members').fetchone()[0]
            self.assertFalse(store.db.in_transaction)
            self.assertEqual(value['new_members_this_pass'],committed)
            observations.append(committed)
        try:
            result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=32,progress=progress)
            self.assertEqual(observations,[100])
            self.assertEqual(result['sources'][0]['members'],101)
        finally:
            store.close()

    def test_failure_in_final_batch_cannot_promote_source(self):
        data = fixtures.archive(members(3))
        store = self.store(data)
        try:
            with patch.object(store,'event',side_effect=BudgetStop('injected final batch failure')):
                result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=128)
            self.assertEqual(result['events'],0)
            self.assertEqual(result['sources'][0]['members'],0)
            self.assertEqual(result['sources'][0]['status'],'budget_stopped')
        finally:
            store.close()

    def test_cli_passes_explicit_commit_batch_without_another_request(self):
        import json
        from contextlib import redirect_stdout
        from types import SimpleNamespace
        from gapi.stream_cli import execute_task
        data = fixtures.archive(members(3))
        store = self.store(data)
        task = store.task
        store.close()
        args = SimpleNamespace(command='ingest-orcid',source=SOURCE,max_members=None,
                               max_new_members=None,max_seconds=10,commit_members=32)
        class HTTP:
            def within(self,seconds):
                return __import__('contextlib').nullcontext()
            def open_archive(self,source):
                return self.open(task.config['sources'][source]['url'])
            def open(self,url):
                return io.BytesIO(data)
            def close(self):
                pass
        with patch('gapi.stream_cli.PublicHTTP',return_value=HTTP()) as transport:
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertTrue(execute_task(task,args))
            self.assertEqual(transport.call_count,1)
            report = json.loads(output.getvalue())
            self.assertEqual(report['sources'][0]['members'],3)
            self.assertEqual(report['report_scope'],'committed-archive-run-only')
            self.assertNotIn('events',report)
            self.assertEqual(report['sources'][0]['status'],'verified')

    def test_corrupt_trailer_cannot_promote_committed_batches(self):
        data = fixtures.archive(members(6))
        store = self.store(data)
        try:
            with self.assertRaises((EOFError,OSError)):
                stream_archive(store,SOURCE,io.BytesIO(data[:-5]),commit_members=3)
            self.assertEqual(store.report()['sources'][0]['status'],'failed')
            predicate,parameters = verified_archive_sql(store.task)
            self.assertEqual(store.db.execute('SELECT count(*) FROM events e WHERE '+predicate,parameters).fetchone()[0],0)
        finally:
            store.close()

    def test_invalid_batch_limit_is_rejected_before_read_or_source_start(self):
        data = fixtures.archive([])
        store = self.store(data)
        try:
            with patch.object(store,'start',side_effect=AssertionError('started')):
                for value in (0,129,True,1.5,'32'):
                    with self.subTest(value=value),self.assertRaises(ValueError):
                        stream_archive(store,SOURCE,io.BytesIO(data),commit_members=value)
        finally:
            store.close()

    def test_real_process_exit_before_and_after_batch_commit(self):
        data = fixtures.archive(members(6))
        code = '''
import io,os,sys
from pathlib import Path
from contextlib import contextmanager
from gapi.task import Task
from gapi.career_store import CareerStore
from gapi.orcid_stream import stream_archive
store=CareerStore(Task(Path(sys.argv[1])))
original=store.member
@contextmanager
def boundary(*args,**kwargs):
    with original(*args,**kwargs):
        yield
        if sys.argv[2]=='before':os._exit(23)
    os._exit(24)
store.member=boundary
stream_archive(store,'orcid-2026-summary',io.BytesIO(sys.stdin.buffer.read(1024*1024)),commit_members=3)
'''
        for mode,expected,exit_code in (('before',0,23),('after',3,24)):
            self.config['work_dir'] = str(self.root/mode)
            self.path = self.root/(mode+'-task.json')
            store = self.store(data)
            task = store.task
            store.close()
            child = subprocess.run([sys.executable,'-B','-c',code,str(self.path),mode],
                                   input=data,capture_output=True,timeout=30)
            self.assertEqual(child.returncode,exit_code,child.stderr.decode(errors='replace'))
            store = CareerStore(task)
            try:
                self.assertEqual(store.report()['events'],expected)
                self.assertEqual(store.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
                result = stream_archive(store,SOURCE,io.BytesIO(data),commit_members=3)
                self.assertEqual(result['events'],6)
                self.assertEqual(result['sources'][0]['members'],6)
                self.assertEqual(result['sources'][0]['status'],'verified')
            finally:
                store.close()
