import copy
import json
import unittest
from contextlib import nullcontext
from unittest.mock import patch

import test_streaming as fixtures
from test_streaming import archive
from gapi.institution_bridge import resolve_online, entity_target, fetch_entities, ROR_SHA256

TARGET='ror:03vek6s52'


def entity():
    return {'id':'Q1','lastrevid':123,'claims':{
        'P3500':[{'id':'Q1$ringgold','mainsnak':{'datavalue':{'value':'1234'}}}],
        'P6782':[{'id':'Q1$ror','mainsnak':{'datavalue':{'value':'03vek6s52'}}}]}}


class HTTP:
    def __init__(self):self.calls=[];self.qids=['Q1'];self.entity=entity();self.fail=False
    def within(self,seconds):return nullcontext()
    def json(self,url):
        self.calls.append(url)
        if self.fail:raise OSError('bounded connection loss')
        if 'sparql?' in url:
            return {'results':{'bindings':[{'id':{'value':'1234'},'entity':{'value':'http://www.wikidata.org/entity/'+q}} for q in self.qids]}},'a'*64
        return {'entities':{'Q1':self.entity}},'b'*64


class InstitutionBridgeTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def seed(self,s):
        s.start('orcid-public-api')
        with s.member('orcid-public-api','private-member'):
            s.event(dict(source='orcid-public-api',person='private-person',institution='ringgold:1234',
                kind='education',stage='doctorate',role='PhD',year_end=2005,locator='private-member',status='observed'))
            s.finish_member('orcid-public-api','private-member','c'*64)
            s.db.execute('INSERT INTO institution_aliases VALUES(?,?,?)',('wd:Q1',TARGET,ROR_SHA256))

    def test_exact_reciprocal_bridge_and_resume_do_not_rewrite_events(self):
        s=self.store(archive([]));http=HTTP()
        try:
            self.seed(s);before=list(s.db.execute('SELECT * FROM events'))
            with patch('gapi.institution_bridge.resolve_institutions', side_effect=lambda st, **kw: {'verified_reciprocal':dict(st.db.execute('SELECT alias,institution FROM active_institution_aliases'))}):
                result=resolve_online(s,http)
                self.assertEqual(result['confirmed_identifier_bridges'],1)
                self.assertEqual(s.db.execute("SELECT institution FROM institution_aliases WHERE alias='ringgold:1234'").fetchone()[0],TARGET)
                calls=len(http.calls)
                repeated=resolve_online(s,http)
            self.assertEqual(repeated['processed_aliases'],0)
            self.assertEqual(len(http.calls),calls)
            self.assertEqual(before,list(s.db.execute('SELECT * FROM events')))
            evidence=s.db.execute('SELECT evidence FROM institution_bridge').fetchone()[0]
            self.assertNotIn('private-person',evidence)
            self.assertEqual(json.loads(evidence)['revision'],123)
        finally:s.close()

    def test_duplicate_entities_and_nonreciprocal_ror_do_not_merge(self):
        s=self.store(archive([]));http=HTTP()
        try:
            self.seed(s);http.qids=['Q1','Q2']
            with patch('gapi.institution_bridge.resolve_institutions', side_effect=lambda st, **kw: {'verified_reciprocal':dict(st.db.execute('SELECT alias,institution FROM active_institution_aliases'))}):
                r=resolve_online(s,http)
            self.assertEqual(r['totals'],{'ambiguous':1})
            self.assertEqual(len(http.calls),1)
            self.assertIsNone(s.db.execute("SELECT institution FROM institution_aliases WHERE alias='ringgold:1234'").fetchone())
        finally:s.close()

    def test_failure_leaves_alias_pending_for_a_successful_resume(self):
        s=self.store(archive([]));http=HTTP()
        try:
            self.seed(s);http.fail=True
            with patch('gapi.institution_bridge.resolve_institutions', side_effect=lambda st, **kw: {'verified_reciprocal':dict(st.db.execute('SELECT alias,institution FROM active_institution_aliases'))}):
                failed=resolve_online(s,http)
                self.assertEqual(failed['failed_batches'],1)
                self.assertEqual(s.db.execute('SELECT count(*) FROM institution_bridge').fetchone()[0],0)
                http.fail=False
                self.assertEqual(resolve_online(s,http)['confirmed_identifier_bridges'],1)
        finally:s.close()

    def test_conflicting_existing_alias_is_preserved(self):
        s=self.store(archive([]));http=HTTP()
        try:
            self.seed(s)
            with s.db:s.db.execute('INSERT INTO institution_aliases VALUES(?,?,?)',('ringgold:1234','ror:other','prior-evidence'))
            with patch('gapi.institution_bridge.resolve_institutions', side_effect=lambda st, **kw: {'verified_reciprocal':dict(st.db.execute('SELECT alias,institution FROM active_institution_aliases'))}):
                r=resolve_online(s,http)
            self.assertEqual(r['totals'],{'conflicting_existing_alias':1})
            self.assertEqual(s.db.execute("SELECT institution FROM institution_aliases WHERE alias='ringgold:1234'").fetchone()[0],'ror:other')
        finally:s.close()

    def test_missing_reciprocal_or_multiple_current_identifiers_stays_unresolved(self):
        s=self.store(archive([]));http=HTTP()
        try:
            self.seed(s)
            with s.db:s.db.execute("DELETE FROM institution_aliases WHERE alias='wd:Q1'")
            with patch('gapi.institution_bridge.resolve_institutions', side_effect=lambda st, **kw: {'verified_reciprocal':dict(st.db.execute('SELECT alias,institution FROM active_institution_aliases'))}):
                r=resolve_online(s,http)
            self.assertEqual(r['totals'],{'reciprocal_or_current_identifier_unconfirmed':1})
            self.assertIsNone(s.db.execute("SELECT institution FROM institution_aliases WHERE alias='ringgold:1234'").fetchone())
        finally:s.close()
        original=entity()
        for changes in ('qualified','multiple','deprecated','wrong-alias'):
            row=copy.deepcopy(original)
            if changes=='qualified':row['claims']['P6782'][0]['qualifiers']={'P582':[{}]}
            if changes=='multiple':row['claims']['P3500'].append({'id':'Q1$another','mainsnak':{'datavalue':{'value':'5678'}}})
            if changes=='deprecated':row['claims']['P6782'][0]['rank']='deprecated'
            self.assertIsNone(entity_target(row,'Q1','ringgold:9999' if changes=='wrong-alias' else 'ringgold:1234'))

    def test_allowed_entity_route_is_bounded_and_oversize_does_not_starve_others(self):
        calls=[]
        class Bounded:
            def json(self,url):
                self_url=url.rsplit('/',1)[-1].removesuffix('.json');calls.append(url)
                if self_url=='Q3':raise ValueError('structured response exceeds bound')
                return {'entities':{self_url:dict(entity(),id=self_url)}},'a'*64
        result=fetch_entities(Bounded(),['Q1','Q2','Q3','Q4','Q5'])
        self.assertEqual(set(result),{'Q1','Q2','Q3','Q4','Q5'})
        self.assertIsNone(result['Q3'][0])
        self.assertTrue(all(result[q][0]['id']==q for q in ('Q1','Q2','Q4','Q5')))
        self.assertEqual(len(calls),5)
        self.assertTrue(all('/wiki/Special:EntityData/' in url and 'api.php' not in url for url in calls))
        with self.assertRaisesRegex(ValueError,'invalid bounded'):
            fetch_entities(Bounded(),['Q1|Q2'])
        class Refused:
            def json(self,url):raise PermissionError('source refused')
        with self.assertRaises(PermissionError):fetch_entities(Refused(),['Q1','Q2'])
