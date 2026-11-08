"""Regressions from the independent-context November red team."""
import gzip
import io
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import test_streaming as fixtures
from gapi.career_store import verified_archive_sql
from gapi.orcid_stream import stream_archive
from gapi.task import BudgetStop, digest


class RedteamBoundaries(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_ood_memory_adapter_returns_current_rss_and_preserves_failure(self):
        from gapi.ood import _Budget
        budget = _Budget()
        with patch('gapi.task.Task.check_memory', side_effect=[100, 90, BudgetStop('memory boundary')]):
            self.assertEqual(budget.check_memory(), 100)
            self.assertEqual(budget.check_memory(), 90)
            self.assertEqual(budget.max_sampled_rss, 100)
            with self.assertRaisesRegex(BudgetStop, 'memory boundary'):
                budget.check_memory()

    def test_public_coverage_is_exact_without_python_population_retention(self):
        import tracemalloc
        from gapi.ood import _Budget, _Fixture
        from gapi.global_analysis import rank_global
        # This runs after the full suite in CI; use the authorized task cap.
        # The isolated OOD runner retains its own 128 MiB contract. Python
        # population growth below is still measured independently of this cap.
        budget = _Budget()
        budget.config['memory_bytes'] = 512*1024**2
        with _Fixture(budget) as f:
            f.task.config['sources'] = {'wikidata': {'allow_derivatives': True}}
            # Measure population growth, separately from first-use module imports.
            rank_global(f, public=True)
            f.db.executescript("""WITH RECURSIVE n(x) AS (
                VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<50000)
                INSERT INTO events SELECT printf('id%d',x), printf('%064d',(x-1)/2),
                NULL,'education',NULL,NULL,NULL,NULL,'ZZ','wikidata','synthetic',
                CASE WHEN x%5=0 THEN 'unresolved' ELSE 'observed' END,'{}' FROM n;""")
            tracemalloc.start()
            try:
                start = tracemalloc.get_traced_memory()[0]
                tracemalloc.reset_peak()
                result = rank_global(f, public=True)
                peak = tracemalloc.get_traced_memory()[1]-start
            finally:
                tracemalloc.stop()
            self.assertEqual(result['coverage'], {'people': 25000, 'events': 50000,
                'countries': ['ZZ'], 'event_status': {'observed': 40000, 'unresolved': 10000},
                'member_status': {}})
            self.assertLess(peak, 2*1024**2)

    def test_robots_trickle_obeys_deadline_and_never_caches_partial_rules(self):
        from test_http_trickle import response
        from gapi.source_http import PublicHTTP, StageDeadline
        from gapi.persistent_http import ManagedResponse
        for managed in (False, True):
            with self.subTest(managed=managed):
                clock = [100]
                stream, raw = response(clock, body=b'User-agent: *\nAllow: /\n')
                if managed:
                    stream = ManagedResponse(stream, SimpleNamespace(close=lambda: None))
                task = SimpleNamespace(config={'http_timeout': 30, 'max_retries': 0, 'request_interval': 1})
                http = PublicHTTP(task, opener=lambda *args, **kwargs: stream)
                with patch('gapi.source_http.time.monotonic', side_effect=lambda: clock[0]):
                    with http.within(2), self.assertRaises(StageDeadline):
                        http.allowed('https://www.tamucc.edu/example')
                self.assertTrue(raw.closed)
                self.assertNotIn('https://www.tamucc.edu', http.robots)
                self.assertEqual(raw.calls, 2)

    def test_reserve_cannot_drop_below_authorized_floor(self):
        import json
        from gapi.task import Task
        for invalid in (1, 256*1024**2-1, True, float(256*1024**2)):
            with self.subTest(reserve=invalid):
                self.config['work_dir'] = str(self.root/'reserve-test')
                self.config['reserve_bytes'] = invalid
                self.path.write_text(json.dumps(self.config), encoding='utf-8')
                with self.assertRaisesRegex(ValueError, 'reserve'):
                    Task(self.path)
        for valid in (256*1024**2, 256*1024**2+1):
            self.config['work_dir'] = str(self.root/str(valid))
            store = self.store(fixtures.archive([]), reserve_bytes=valid)
            store.close()

    def test_unfinished_training_cannot_change_intensity_population(self):
        from gapi.ood import _Budget, _Fixture
        from gapi.global_analysis import rank_global
        for source in ('orcid-2026-summary', 'orcid-public-api', 'wikidata'):
            for enriched in (False, True):
                budget = _Budget()
                budget.config['memory_bytes'] = 512*1024**2
                with self.subTest(source=source, enriched=enriched), _Fixture(budget) as f:
                    f.task.config.update(rules_version='career-rules/1', sources={source: {
                        'kind': 'orcid-summary-tar-gzip' if source.endswith('summary') else source,
                        'version': 'synthetic-fixed'}})
                    for person, origin in (('one', 'A'), ('two', 'C')):
                        f.event(person, origin, 'education', 'doctorate', source=source,
                                start=2000, end=2005, role='PhD')
                        f.event(person, 'B', 'employment', 'academic', source=source,
                                start=2010, role='Professor')
                    f.db.execute('UPDATE runs SET identity=?', (digest({
                        'source': f.task.config['sources'][source], 'rules': 'career-rules/1'}),))
                    if not enriched:
                        f.db.execute('DELETE FROM event_interpretations')
                    before = rank_global(f)
                    f.event('unfinished', 'A', 'education', 'doctorate', source=source,
                            start=2000, end=2005, role='PhD not completed')
                    if not enriched:
                        f.db.execute('DELETE FROM event_interpretations')
                    after = rank_global(f)
                    self.assertEqual(before['edges'], after['edges'])
                    for old, new in zip(before['nodes'], after['nodes']):
                        self.assertEqual(old['observed_training_denominator'], new['observed_training_denominator'])
                        self.assertEqual(old['network_intensity_score'], new['network_intensity_score'])
                    # Unknown labels/dates remain observations, not invented degrees.
                    f.event('unknown', 'A', 'education', None, source=source, role='unclassified')
                    f.event('graduate-without-job', 'A', 'education', 'doctorate', source=source,
                            start=2000, end=2005, role='PhD')
                    if not enriched:
                        f.db.execute('DELETE FROM event_interpretations')
                    result = rank_global(f)
                    node = next(n for n in result['nodes'] if n['institution_id'] == 'A')
                    self.assertEqual(node['observed_training_denominator'], 3)
                    self.assertEqual(result['edges'], before['edges'])

    def test_reversed_event_intervals_do_not_form_relations(self):
        from gapi.global_analysis import observed_relations
        training = dict(source='synthetic', locator='training', status='observed', institution='A',
                        kind='education', stage='doctorate', year_start=2000, year_end=2005)
        job = dict(training, institution='B', kind='employment', stage='academic',
                   year_start=2006, year_end=2020, locator='job')
        self.assertEqual(len(list(observed_relations([training, job]))), 1)
        self.assertEqual(list(observed_relations([dict(training, year_start=2010), job])), [])
        self.assertEqual(list(observed_relations([training, dict(job, year_end=2000)])), [])
        self.assertEqual(len(list(observed_relations([dict(training, year_start=2005), job]))), 1)
        self.assertEqual(len(list(observed_relations([dict(training, year_start=None), dict(job, year_end=None)]))), 1)
        self.assertEqual(list(observed_relations([dict(training, year_end=None), job])), [])

    def test_verified_archive_binds_current_source_and_rules(self):
        task = SimpleNamespace(config={'rules_version': 'current', 'sources': {
            'annual': {'kind': 'orcid-summary-tar-gzip', 'version': 'current'},
            'api': {'kind': 'orcid-public-api'}}})
        db = sqlite3.connect(':memory:')
        try:
            db.executescript('CREATE TABLE events(source TEXT); CREATE TABLE runs(source TEXT,identity TEXT,status TEXT);')
            db.executemany('INSERT INTO events VALUES(?)', [('annual',), ('api',)])
            correct = digest({'source': task.config['sources']['annual'], 'rules': 'current'})
            for identity, status, expected in ((correct, 'verified', 2), ('stale', 'verified', 1),
                                               (correct, 'provisional', 1), (correct, 'failed', 1)):
                db.execute('DELETE FROM runs')
                db.execute('INSERT INTO runs VALUES(?,?,?)', ('annual', identity, status))
                predicate, params = verified_archive_sql(task)
                self.assertEqual(db.execute('SELECT count(*) FROM events e WHERE '+predicate, params).fetchone()[0], expected)
            db.execute("UPDATE runs SET identity=?,status='verified'", (correct,))
            task.config['rules_version'] = 'next'
            predicate, params = verified_archive_sql(task)
            self.assertEqual(db.execute('SELECT count(*) FROM events e WHERE '+predicate, params).fetchone()[0], 1)
        finally:
            db.close()

    def test_zero_padding_drain_enforces_deadline_memory_and_replay(self):
        data = gzip.compress(b'\0'*65536, mtime=0)
        store = self.store(data)
        try:
            with patch('gapi.orcid_stream.time.monotonic', side_effect=[0, 0, 100]):
                result = stream_archive(store, 'orcid-2026-summary', io.BytesIO(data), max_seconds=1)
            self.assertEqual(result['sources'][0]['status'], 'budget_stopped')
            self.assertFalse(store.source_integrity()['all_archives_verified'])
            with patch.object(store.task, 'check_memory', side_effect=BudgetStop('memory limit')):
                result = stream_archive(store, 'orcid-2026-summary', io.BytesIO(data))
            self.assertEqual(result['sources'][0]['status'], 'budget_stopped')
            self.assertFalse(store.source_integrity()['all_archives_verified'])
            stream_archive(store, 'orcid-2026-summary', io.BytesIO(data))
            self.assertTrue(store.source_integrity()['all_archives_verified'])
            self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)
        finally:
            store.close()

    def test_zero_padding_expansion_and_frozen_wire_length_are_bounded(self):
        from gapi.orcid_stream import MAX_TAR_PADDING_BYTES, HashReader
        data = gzip.compress(b'\0'*(MAX_TAR_PADDING_BYTES+65536), mtime=0)
        store = self.store(data)
        try:
            with self.assertRaisesRegex(ValueError, 'padding.*expansion limit'):
                stream_archive(store, 'orcid-2026-summary', io.BytesIO(data))
            self.assertFalse(store.source_integrity()['all_archives_verified'])
        finally:
            store.close()
        raw = io.BytesIO(b'x'*1000)
        reader = HashReader(raw, max_bytes=10)
        with self.assertRaisesRegex(ValueError, 'exceeds frozen length'):
            reader.read(65536)
        self.assertEqual(raw.tell(), 11)
