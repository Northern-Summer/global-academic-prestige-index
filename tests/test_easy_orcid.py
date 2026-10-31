import gzip
import io
import json
import sqlite3
import unittest
import time
from types import SimpleNamespace

from gapi.easy_orcid import discover, _lines
from gapi.career_store import CareerStore
from gapi.task import BudgetStop


class Response(io.BytesIO):
    headers = {"Content-Encoding": "identity"}
    def __enter__(self): return self
    def __exit__(self, *args): self.close()


class EasyOrcidTests(unittest.TestCase):
    def store(self):
        db=sqlite3.connect(':memory:')
        db.executescript('''CREATE TABLE discovery_pages(source,cursor_before,cursor_after,query_sha256,response_sha256,retrieved_at,row_count,PRIMARY KEY(source,cursor_before));
            CREATE TABLE discovery(source,identifier,query_sha256,response_sha256,PRIMARY KEY(source,identifier));''')
        task=SimpleNamespace(check_memory=lambda:None,check=lambda *a:None)
        store=SimpleNamespace(db=db,task=task)
        store.queue_page=lambda *args:CareerStore.queue_page(store,*args)
        return store

    def http(self, rows):
        raw=gzip.compress(b''.join(json.dumps(x).encode()+b'\n' for x in rows),mtime=0)
        class H:
            def open(self,*args,**kwargs):
                r=Response(raw); return r
        return H()

    def test_streams_gzip_and_queues_only_valid_orcids_idempotently(self):
        rows=[{'orcid':'0000-0002-1825-0097','educations':[]}, {'orcid':'bad'}, {'orcid':'0000-0001-5109-3700'}]
        s=self.store(); result=discover(s,self.http(rows),max_records=3,max_seconds=5)
        self.assertEqual(result['records'],3); self.assertEqual(result['queued'],2)
        self.assertEqual([r[0] for r in s.db.execute('SELECT identifier FROM discovery ORDER BY identifier')],['0000-0001-5109-3700','0000-0002-1825-0097'])

    def test_cursor_page_does_not_duplicate_previous_records(self):
        rows=[{'orcid':'0000-0002-1825-0097'},{'orcid':'0000-0001-5109-3700'},{'orcid':'0000-0003-5000-0001'}]
        s=self.store(); discover(s,self.http(rows),max_records=2,max_seconds=5)
        result=discover(s,self.http(rows),max_records=2,max_seconds=5)
        self.assertEqual(result['cursor'],3); self.assertEqual(result['queued'],1)
        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],3)

    def test_existing_candidates_are_not_reported_as_newly_queued(self):
        s=self.store()
        s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',
                     ('ingest-orcid-api','0000-0002-1825-0097','a','b'))
        s.db.commit()
        result=discover(s,self.http([{'orcid':'0000-0002-1825-0097'}]),max_seconds=5)
        self.assertEqual(result['valid_unique_candidates'],1)
        self.assertEqual(result['queued'],0)

    def test_bound_rejects_oversized_record_and_continues(self):
        s=self.store(); raw=gzip.compress((json.dumps({'orcid':'0000-0002-1825-0097','x':'a'*(2*1024*1024)})+'\n'+json.dumps({'orcid':'0000-0001-5109-3700'})+'\n').encode(),mtime=0)
        class H:
            def open(self,*args,**kwargs): return Response(raw)
        result=discover(s,H(),max_records=2,max_seconds=5)
        self.assertEqual(result['rejected_oversized'],1)
        self.assertEqual(result['queued'],1)

    def test_oversized_eof_is_counted_once_and_memory_stays_bounded(self):
        s=self.store()
        raw=gzip.compress(b'x'*(8*1024**2),mtime=0)
        values=list(_lines(Response(raw),limit=128,deadline=time.monotonic()+5,
                           task=s.task,gzip_stream=True))
        self.assertEqual(values,[None])

    def test_truncated_and_corrupt_stream_do_not_commit_page(self):
        raw=gzip.compress(b'{"orcid":"0000-0002-1825-0097"}\n',mtime=0)
        for broken in (raw[:-1],raw[:-8]+b'broken!!',raw+b'extra'):
            with self.subTest(broken=broken[-8:]):
                s=self.store()
                http=SimpleNamespace(open=lambda *a,**k:Response(broken))
                with self.assertRaises(ValueError):discover(s,http,max_records=100,max_seconds=5)
                self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],0)
                self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)

    def test_capacity_stop_prevents_candidate_or_cursor_mutation(self):
        s=self.store()
        def stop(*args):raise BudgetStop('capacity')
        s.task.check=stop
        with self.assertRaises(BudgetStop):
            discover(s,self.http([{'orcid':'0000-0002-1825-0097'}]),max_seconds=5)
        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],0)

    def test_queue_failure_rolls_back_candidates_and_cursor(self):
        s=self.store()
        s.db.execute("CREATE TRIGGER deny_cursor BEFORE INSERT ON discovery_pages BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            discover(s,self.http([{'orcid':'0000-0002-1825-0097'}]),max_seconds=5)
        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],0)
        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_pages').fetchone()[0],0)

    def test_invalid_time_bounds_make_no_request(self):
        for seconds in (float('nan'),float('inf'),True,0,-1):
            with self.subTest(seconds=seconds):
                http=SimpleNamespace(open=lambda *a,**k:self.fail('network not allowed'))
                with self.assertRaises(ValueError):discover(self.store(),http,max_seconds=seconds)


if __name__ == '__main__': unittest.main()
