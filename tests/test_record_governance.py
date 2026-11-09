import copy
import unittest
import zlib
import json
from unittest.mock import patch

from gapi.record_governance import effective_record, disposition, RecordGovernance, HistoricalConcordance
from gapi.unified_analysis import UnifiedScan
import test_global_analysis as fixtures


def event(**values):
    return dict(dict(source='orcid-2026-summary',kind='education',stage=None,
        institution='ror:012345678',role='BS',year_start=2000,year_end=2004,status='unresolved'),**values)


class GovernanceRulesTests(unittest.TestCase):
    def test_real_lexical_shapes_and_ambiguous_controls(self):
        for role,stage in [('BS','bachelor'),('B.S.','bachelor'),('MS','master'),('Mestrado','master')]:
            raw=event(role=role);before=copy.deepcopy(raw)
            self.assertEqual(effective_record(raw)['stage'],stage)
            self.assertEqual(raw,before)
        for role in ['MD','Doctor','MBBS','','Doctorate candidate','PhD student','BS student']:
            self.assertNotEqual(disposition(effective_record(event(role=role)))[0],'identified',role)
        self.assertEqual(effective_record(event(kind='employment',role='Research Associate'))['stage'],'academic')
        self.assertIsNone(effective_record(event(kind='employment',role='Research Assistant'))['stage'])

    def test_primary_precedence_and_overlapping_missingness(self):
        self.assertEqual(disposition(event(year_start=2010,year_end=2000,institution=None))[0],'conflict')
        self.assertEqual(disposition(effective_record(event(role='BS student',institution=None)))[0],'excluded')
        self.assertEqual(disposition(event(institution=None))[0],'missing_institution')
        self.assertEqual(disposition(event(role='',year_end=None))[0],'missing_classification')
        self.assertEqual(disposition(event(stage='bachelor',year_end=None))[0],'missing_date')
        self.assertEqual(disposition(event(stage='bachelor'))[0],'identified')
        self.assertIn('missing_date',disposition(event(institution=None,year_end=None))[1])

    def test_history_never_restores_current_or_promotes_evidence(self):
        raw=event(role='',institution=None)
        historical=[['education','doctorate','ror:012345678',2000,2004,'PhD']]
        g=RecordGovernance();g.consume([raw],historical,{},False)
        result=g.public(1)
        self.assertEqual(result['primary'],{'missing_institution':1})
        self.assertEqual(result['historical_records_restored_into_current'],0)
        self.assertEqual(result['independent_evidence_promotions'],0)
        self.assertEqual(result['history']['historical_affiliations_absent_from_current'],1)

    def test_resume_accounting_and_sensitive_oracle(self):
        g=RecordGovernance();g.consume([event()],[],{},False);v=g.snapshot()
        other=RecordGovernance();other.restore(v,1);self.assertEqual(other.snapshot(),v)
        for bad in [dict(v,primary={'identified':2}),dict(v,primary={'nonsense':1}),dict(v,flags={'missing_date':-1})]:
            with self.assertRaises(ValueError):other.restore(bad,1)
        self.assertEqual(g.public(1)['changes']['newly_identified_events'],1)


    def test_all_categories_remain_exhaustive_for_generated_missing_fields(self):
        from itertools import product
        from gapi.career_rules import EXCLUSIONS
        for institution,stage,date,unfinished,reversed_dates in product([False,True],repeat=5):
            row=event(institution='ror:012345678' if institution else None,
                stage='bachelor' if stage else None,role='BS student' if unfinished else '',
                year_start=2010 if reversed_dates else 2000,year_end=(2004 if date else None))
            expected=('conflict' if reversed_dates and date else 'excluded' if unfinished
                else 'missing_institution' if not institution else 'missing_classification' if not stage
                else 'missing_date' if not date else 'identified')
            self.assertEqual(disposition(row)[0],expected,row)
        # Sensitivity: removing the exclusion guard must be caught independently
        # of a changed counter or a parser/import crash.
        import gapi.record_governance as owner
        row=event(role='BS student',stage='bachelor')
        self.assertEqual(disposition(row)[0],'excluded')
        with patch.object(owner,'excluded_qualification',return_value=False):
            self.assertNotEqual(disposition(row)[0],'excluded')
        self.assertEqual(disposition(event(role='BS',stage='bachelor'))[0],'identified')

class GovernanceScanTests(unittest.TestCase):
    setUp=fixtures.AnalysisTests.setUp
    tearDown=fixtures.AnalysisTests.tearDown
    store=fixtures.AnalysisTests.store
    _snapshot_pair=fixtures.AnalysisTests._snapshot_pair
    _snapshot=fixtures.AnalysisTests._snapshot
    _snapshot_events=staticmethod(fixtures.AnalysisTests._snapshot_events)

    def test_audit_resume_equivalence_preserves_source_counters(self):
        left,right=self._snapshot_pair()
        try:
            for store in (left,right):self._snapshot(store,'same',self._snapshot_events())
            scan=UnifiedScan((left,right),governance=True);scan.run(seconds=10,max_people=1)
            resumed=UnifiedScan((left,right),governance=True);resumed.load();resumed.run(seconds=10)
            before=resumed.governance.snapshot();result=resumed.result()
            self.assertEqual(sum(result['coverage']['processing']['primary'].values()),result['coverage']['events'])
            self.assertEqual(result['coverage']['events'],2)
            again=UnifiedScan((left,right),governance=True);again.load();again.run(seconds=10)
            self.assertEqual(again.governance.snapshot(),before)
            # A source/code identity change cannot silently reuse the generation.
            with patch('gapi.unified_analysis.input_binding',return_value='drift'):
                with self.assertRaisesRegex(ValueError,'drift'):UnifiedScan((left,right),governance=True).load()
        finally:
            left.close();right.close()

    def test_governance_atomic_write_failure_preserves_prior_generation(self):
        left,right=self._snapshot_pair()
        try:
            self._snapshot(left,'same',self._snapshot_events())
            scan=UnifiedScan((left,right),governance=True);scan.run(seconds=10,max_people=1)
            original=scan.path.read_bytes()
            scan.governance.flags['missing_date']+=1
            with patch('gapi.unified_analysis._write_aggregate',side_effect=OSError('injected atomic replacement failure')):
                with self.assertRaises(OSError):scan.save()
            self.assertEqual(scan.path.read_bytes(),original)
            restored=UnifiedScan((left,right),governance=True);restored.load()
            self.assertNotEqual(restored.governance.snapshot(),scan.governance.snapshot())
        finally:
            left.close();right.close()

    def test_real_indexed_history_adapter_retains_alias_matching(self):
        left,right=self._snapshot_pair()
        try:
            db=right.db
            db.executescript('CREATE TABLE easy_careers(row_number INTEGER PRIMARY KEY,person BLOB,payload BLOB); CREATE INDEX easy_careers_person ON easy_careers(person);')
            db.executescript("CREATE TABLE easy_career_state(singleton INTEGER,identity TEXT,spec TEXT,status TEXT,scanned INTEGER,report TEXT); INSERT INTO easy_career_state VALUES(1,'frozen','{}','verified',1,'{}');")
            pid='ab'*32;rows=[['education','bachelor','grid:123.a',2000,2004,'BS']]
            db.execute('INSERT INTO easy_careers VALUES(?,?,?)',(1,bytes.fromhex(pid),zlib.compress(json.dumps(rows).encode())));db.commit()
            history=HistoricalConcordance((left,right));history.prepare([pid,'wd:Q1'])
            g=RecordGovernance();g.consume([event()],history.cache[pid],{'grid:123.a':'ror:012345678'},False)
            self.assertEqual(g.public(1)['history']['exact_projection_matches'],1)
            history.prepare([]);self.assertEqual(history.cache,{})
        finally:
            left.close();right.close()
