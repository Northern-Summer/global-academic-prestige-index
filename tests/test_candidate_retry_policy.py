"""Candidate retry limits retain failures without repeating inaccessible reads."""
import unittest
import test_streaming as fixtures
from test_streaming import archive


class CandidateRetryPolicyTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_terminal_orcid_failures_remain_pending_without_automatic_requests(self):
        s=self.store(archive([]))
        command='ingest-orcid-api'
        reasons=('HTTP Error 409: Conflict',
                 'permanent official redirect; target queued separately; original identity unresolved',
                 'invalid ORCID format/checksum; not requested')
        try:
            with s.db:
                for i,reason in enumerate(reasons):
                    identifier='candidate-'+str(i)
                    s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',(command,identifier,'query','response'))
                    s.db.execute('INSERT INTO discovery_retries VALUES(?,?,?,?,?)',(command,identifier,1,0,reason))
                s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',(command,'fresh','query','response'))
            before=list(s.db.execute('SELECT * FROM discovery_retries ORDER BY identifier'))
            for clock in (1,10**12):
                self.assertEqual(s.pending(command,10,now=clock),['fresh'])
                self.assertEqual(s.pending(command,10,now=clock,priority_source='wikidata'),['fresh'])
            self.assertEqual(list(s.db.execute('SELECT * FROM discovery_retries ORDER BY identifier')),before)
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],0)
            row=s.candidate_progress(allowed_sources={'orcid-public-api'})['sources'][0]
            self.assertEqual((row['candidates'],row['processed'],row['pending'],row['pending_with_errors']),(4,0,4,3))
        finally:s.close()

    def test_candidate_attempt_cap_preserves_transient_backoff_and_fresh_work(self):
        from gapi.career_store import MAX_CANDIDATE_ATTEMPTS
        s=self.store(archive([]))
        try:
            with s.db:
                for command in ('ingest-orcid-api','ingest-wikidata'):
                    for identifier in ('exhausted','recoverable','waiting','fresh'):
                        s.db.execute('INSERT INTO discovery VALUES(?,?,?,?)',(command,identifier,'query','response'))
                    for identifier,attempts,due in (('exhausted',MAX_CANDIDATE_ATTEMPTS,0),
                                                   ('recoverable',MAX_CANDIDATE_ATTEMPTS-1,0),('waiting',1,1000)):
                        s.db.execute('INSERT INTO discovery_retries VALUES(?,?,?,?,?)',(command,identifier,attempts,due,'temporary network failure'))
            for command in ('ingest-orcid-api','ingest-wikidata'):
                self.assertEqual(set(s.pending(command,10,now=1)),{'recoverable','fresh'})
                self.assertEqual(set(s.pending(command,10,now=1000)),{'recoverable','waiting','fresh'})
                s.defer(command,'recoverable','temporary network failure',now=1000)
                self.assertNotIn('recoverable',s.pending(command,10,now=10**12))
            self.assertEqual(s.db.execute('SELECT count(*) FROM discovery').fetchone()[0],8)
            self.assertEqual(s.db.execute('SELECT count(*) FROM members').fetchone()[0],0)
        finally:s.close()

