"""Evidence query optimization must conserve historical acceptance boundaries."""
import sqlite3
import unittest

from gapi.career_store import member_evidence_sql


class MemberEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript('''
            CREATE TABLE members(source TEXT,locator TEXT,status TEXT,
                PRIMARY KEY(source,locator));
            CREATE TABLE events(id INTEGER PRIMARY KEY,source TEXT,locator TEXT);
        ''')
        self.db.executemany('INSERT INTO members VALUES(?,?,?)', [
            ('a', 'Q1@1', 'observed'), ('a', 'Q2@1#nested', 'observed'),
            ('a', 'Q3@1', 'rejected'), ('b', 'Q4@1', 'observed'),
            ('a', 'UPPER', 'observed'), ('a', 'legacy_%', 'observed')])
        self.locators = [
            ('a', 'Q1@1'), ('a', 'Q1@1#degree'), ('a', 'Q2@1#nested#job'),
            ('a', 'Q3@1#job'), ('a', 'Q4@1#job'), ('a', 'orphan#job'),
            ('a', 'Q1@10#degree'), ('a', 'upper#job'),
            ('a', 'legacy-other#job'), ('b', 'Q4@1#job'),
            ('a', 'Q2@1#other'), ('a', 'Q1@1#nested#job')]
        self.db.executemany('INSERT INTO events(source,locator) VALUES(?,?)', self.locators)

    def results(self, predicate):
        return [r[0] for r in self.db.execute(
            'SELECT ' + predicate + ' FROM events e ORDER BY e.id')]

    def test_expected_observed_members_and_rejected_source_boundaries(self):
        self.assertEqual(self.results(member_evidence_sql()),
            [1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0, 1])

    def test_quality_audit_keeps_rejected_evidence_without_promoting_it(self):
        observed = self.results(member_evidence_sql())
        audit = self.results(member_evidence_sql(observed_only=False))
        self.assertEqual(audit[3], 1)
        self.assertEqual(observed[3], 0)
        self.assertEqual(audit[:3]+audit[4:], observed[:3]+observed[4:])

    def test_differential_nested_and_historical_like_semantics(self):
        for observed in (True, False):
            with self.subTest(observed_only=observed):
                state = " AND m.status='observed'" if observed else ''
                legacy = ("EXISTS(SELECT 1 FROM members m WHERE m.source=e.source" + state +
                    " AND (m.locator=e.locator OR e.locator LIKE m.locator||'#%'))")
                self.assertEqual(self.results(member_evidence_sql(observed_only=observed)),
                    self.results(legacy))

    def test_repeated_queries_do_not_mutate_evidence(self):
        before = list(self.db.iterdump())
        self.assertEqual(self.results(member_evidence_sql()), self.results(member_evidence_sql()))
        self.assertEqual(before, list(self.db.iterdump()))
