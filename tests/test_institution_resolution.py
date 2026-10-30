import io
import json
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import test_streaming as fixtures
from test_streaming import archive
from test_institution_bridge import HTTP, TARGET
from gapi.global_analysis import resolve_institutions
from gapi.institution_bridge import resolve_online, ROR_SHA256, discover, VERSION, revalidate_predecessor, PREDECESSOR, DDL


class ResolutionTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def ror(self,records):
        context=ExitStack()
        root=context.enter_context(patch('gapi.global_analysis.project_root'))
        directory=root.return_value.__truediv__.return_value
        directory.exists.return_value=True
        directory.open.return_value.__enter__.return_value=io.BytesIO()
        checksum=context.enter_context(patch('gapi.global_analysis.hashlib.file_digest'))
        checksum.return_value.hexdigest.return_value=ROR_SHA256
        context.enter_context(patch('gapi.global_analysis.ror_records',side_effect=lambda p:iter(records)))
        return context

    def record(self,target):
        return {'id':target.removeprefix('ror:'),'name':'Directory institution','country_code':'US',
            'ror':'https://ror.org/'+target.removeprefix('ror:'),'external_ids':{'wikidata':['Q1']}}

    def seed(self,s,target=TARGET,provenance=ROR_SHA256):
        s.start('wikidata')
        with s.member('wikidata','statement'):
            s.event(dict(source='wikidata',person='p',institution='ringgold:1234',kind='education',
                stage='doctorate',year_end=2000,locator='statement',status='observed'))
            s.finish_member('wikidata','statement','a'*64)
            s.db.execute('INSERT INTO institution_aliases VALUES(?,?,?)',('wd:Q1',target,provenance))

    def test_new_ambiguity_excludes_old_reciprocal_but_preserves_history(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            with self.ror([self.record(TARGET),self.record('ror:05ect4e57')]):
                report=resolve_institutions(s,extra_identifiers=['wd:Q1'])
                self.assertEqual(report['ambiguous_identifiers'],['wd:Q1'])
                self.assertNotIn('wd:Q1',report['verified_reciprocal'])
                result=resolve_online(s,HTTP())
            self.assertEqual(result['confirmed_identifier_bridges'],0)
            self.assertEqual(s.db.execute("SELECT institution FROM institution_aliases WHERE alias='wd:Q1'").fetchone()[0],TARGET)
            self.assertIsNone(s.db.execute("SELECT institution FROM active_institution_aliases WHERE alias='wd:Q1'").fetchone())
            self.assertIsNone(s.db.execute("SELECT institution FROM institution_aliases WHERE alias='ringgold:1234'").fetchone())
        finally:s.close()

    def test_conflicting_reviewed_wd_alias_is_not_overwritten_or_used(self):
        s=self.store(archive([]))
        try:
            self.seed(s,target='ror:05ect4e57',provenance='reviewed-prior')
            with self.ror([self.record(TARGET)]):
                report=resolve_institutions(s,extra_identifiers=['wd:Q1'])
                self.assertEqual(report['conflicting_identifiers'],['wd:Q1'])
                self.assertNotIn('wd:Q1',report['verified_reciprocal'])
                result=resolve_online(s,HTTP())
            self.assertEqual(result['confirmed_identifier_bridges'],0)
            self.assertEqual(s.db.execute("SELECT institution,provenance FROM institution_aliases WHERE alias='wd:Q1'").fetchone(),
                ('ror:05ect4e57','reviewed-prior'))
        finally:s.close()

    def test_same_target_preserves_prior_provenance_and_legitimate_bridge(self):
        s=self.store(archive([]))
        try:
            self.seed(s,provenance='reviewed-prior')
            with self.ror([self.record(TARGET)]):result=resolve_online(s,HTTP())
            self.assertEqual(result['confirmed_identifier_bridges'],1)
            self.assertEqual(s.db.execute("SELECT provenance FROM institution_aliases WHERE alias='wd:Q1'").fetchone()[0],'reviewed-prior')
        finally:s.close()

    def test_nested_response_pollution_uses_failure_boundary(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            for payload in ({'results':[]},{'results':{'bindings':[{'id':[],'entity':{'value':'http://www.wikidata.org/entity/Q1'}}]}},
                            {'results':{'bindings':[{'id':{'value':'1234'},'entity':[]}]}}):
                with self.subTest(payload=payload):
                    http=HTTP();http.json=lambda url:(payload,'a'*64)
                    result=resolve_online(s,http)
                    self.assertEqual(result['failed_batches'],1)
                    self.assertEqual(result['processed_aliases'],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM institution_bridge').fetchone()[0],0)
            self.assertGreater(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
        finally:s.close()

    def test_predecessor_ambiguous_bridge_is_disabled_without_erasing_evidence(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            s.db.executescript(DDL)
            with s.db:
                s.db.execute('INSERT INTO institution_aliases VALUES(?,?,?)',('ringgold:1234',TARGET,'predecessor'))
                s.db.execute('INSERT INTO institution_bridge VALUES(?,?,?,?,?)',
                    ('ringgold:1234',PREDECESSOR,'prior-spec','confirmed_identifier_bridge',json.dumps({'qid':'Q1','target':TARGET})))
            with self.ror([self.record(TARGET),self.record('ror:05ect4e57')]):
                self.assertEqual(revalidate_predecessor(s),1)
                self.assertEqual(revalidate_predecessor(s),0)
            self.assertEqual(s.db.execute('SELECT status FROM institution_bridge WHERE version=?',(VERSION,)).fetchone()[0],
                'predecessor_reciprocal_conflict')
            self.assertIsNone(s.db.execute("SELECT institution FROM active_institution_aliases WHERE alias='ringgold:1234'").fetchone())
            self.assertEqual(s.db.execute('SELECT count(*) FROM institution_bridge').fetchone()[0],2)
            self.assertEqual(s.db.execute('SELECT count(*) FROM institution_aliases').fetchone()[0],2)
        finally:s.close()
