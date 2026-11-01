import unittest
from unittest.mock import patch
import test_streaming as fixtures
from test_streaming import archive


class MemberCounterTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_counter_tracks_inserts_retries_rejections_and_rollback(self):
        s=self.store(archive([]))
        def counts():
            return (s.db.execute("SELECT members FROM runs WHERE source='wikidata'").fetchone()[0],
                    s.db.execute("SELECT count(*) FROM members WHERE source='wikidata'").fetchone()[0])
        try:
            s.start('wikidata')
            with s.db:
                s.finish_member('wikidata','Q1@1','a'*64)
                s.finish_member('wikidata','Q1@1','a'*64)
                s.finish_member('wikidata','Q2@1','b'*64,'rejected','fixture')
            self.assertEqual(counts(),(2,2))
            with self.assertRaisesRegex(ValueError,'rollback'),s.db:
                s.finish_member('wikidata','Q3@1','c'*64)
                self.assertEqual(counts(),(3,3))
                raise ValueError('rollback')
            self.assertEqual(counts(),(2,2))
            with self.assertRaisesRegex(ValueError,'member changed'),s.db:
                s.finish_member('wikidata','Q1@1','d'*64)
            self.assertEqual(counts(),(2,2))
            s.start('orcid-public-api')
            with s.db:s.finish_member('orcid-public-api','fixture','a'*64)
            self.assertEqual(counts(),(2,2))
            with s.db:s.db.execute("UPDATE runs SET members=99 WHERE source='wikidata'")
            s.start('wikidata')
            self.assertEqual(counts(),(2,2))
        finally:s.close()

    def test_finishing_members_does_not_recount_source_ledger(self):
        s=self.store(archive([]));queries=[]
        try:
            s.start('wikidata')
            s.db.set_trace_callback(queries.append)
            with s.db:
                for i in range(100):s.finish_member('wikidata',f'Q{i+1}@1','a'*64)
            self.assertFalse(any('count(*)' in q.lower() for q in queries))
            self.assertEqual(s.db.execute("SELECT members FROM runs WHERE source='wikidata'").fetchone()[0],100)
        finally:s.db.set_trace_callback(None);s.close()
