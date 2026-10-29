import json
import unittest

import test_streaming as fixtures
from test_streaming import archive, PERSON
from test_official_profiles import URL
from gapi.corroboration import decisions
from gapi.official_profiles import RULE
from gapi.orcid_stream import private_person
from gapi.global_analysis import rank_global


class CorroborationTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def seed(self,s):
        for source in ('wikidata','orcid-public-api'):
            s.start(source)
            locator='Q1@1' if source=='wikidata' else PERSON
            with s.member(source,locator):
                event=dict(source=source,person=private_person(PERSON),institution='ror:05ect4e57',
                    kind='education',stage='doctorate',year_start=None,year_end=1967,role='PhD',
                    locator=locator+'#educations',status='observed',assertion_origin=PERSON,
                    url=f'https://pub.orcid.org/v3.0/{PERSON}/educations',content_sha256='a'*64,
                    references=[{'snaks':{'P854':[{'datavalue':{'value':URL}}]}}])
                s.event(event);s.finish_member(source,locator,'a'*64)
                if source=='orcid-public-api':
                    s.db.execute('INSERT INTO member_parts VALUES(?,?,?,?,?,?,?)',
                        (source,PERSON,'educations',event['url'],'b'*64,100,'2026-10-30'))
        event_id=s.db.execute("SELECT id FROM events WHERE source='wikidata'").fetchone()[0]
        with s.db:
            s.db.execute('INSERT INTO official_checks VALUES(?,?,?,?,?,?,?,?)',
                (event_id,URL,'c'*64,RULE,'2026-10-30','official-underlying-source-confirmed',
                 'same-origin-as-wikidata-reference','d'*64))
            s.db.execute('INSERT INTO official_profile_subjects VALUES(?,?,?,?)',
                (URL,'c'*64,RULE,private_person(PERSON)))

    def test_exact_current_official_and_self_assertion_derive_without_mutation(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            before=list(s.db.execute('SELECT * FROM events ORDER BY id'))
            first=decisions(s)
            self.assertEqual(len(first),2)
            self.assertEqual(first,decisions(s))
            self.assertTrue(all(v['origins']==['official-profile','orcid-self'] for v in first.values()))
            self.assertEqual(before,list(s.db.execute('SELECT * FROM events ORDER BY id')))
            self.assertEqual(decisions(s,allowed_sources={'wikidata'}),{})
        finally:s.close()

    def test_stale_official_rule_and_missing_part_do_not_promote(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:s.db.execute("UPDATE official_checks SET rule_version='official-profile-education-lines/1'")
            self.assertEqual(decisions(s),{})
            with s.db:
                s.db.execute('UPDATE official_checks SET rule_version=?',(RULE,))
                s.db.execute('DELETE FROM member_parts')
            self.assertEqual(decisions(s),{})
        finally:s.close()

    def test_client_copy_date_conflict_and_wrong_identity_fail_closed(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            row=s.db.execute("SELECT id,payload FROM events WHERE source='orcid-public-api'").fetchone()
            for changes in ({'assertion_origin':'client-copy'}, {'year_end':1968},
                            {'person':'same-name-different-person'}, {'role':'PhD not completed'}):
                with self.subTest(changes=changes):
                    event=json.loads(row[1]);event.update(changes)
                    with s.db:s.db.execute('UPDATE events SET payload=? WHERE id=?',(json.dumps(event),row[0]))
                    self.assertEqual(decisions(s),{})
        finally:s.close()

    def test_missing_or_wrong_official_person_binding_cannot_promote(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:s.db.execute("UPDATE official_profile_subjects SET person='another-person'")
            self.assertEqual(decisions(s),{})
            with s.db:s.db.execute('DELETE FROM official_profile_subjects')
            self.assertEqual(decisions(s),{})
        finally:s.close()

    def test_extra_start_year_is_unconfirmed_but_not_a_conflict(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            row=s.db.execute("SELECT id,payload FROM events WHERE source='orcid-public-api'").fetchone()
            event=json.loads(row[1]);event['year_start']=1960
            with s.db:s.db.execute('UPDATE events SET payload=? WHERE id=?',(json.dumps(event),row[0]))
            result=decisions(s)
            self.assertEqual(len(result),2)
            self.assertTrue(all('year_start' not in d['supported_fields'] for d in result.values()))
            row=s.db.execute("SELECT id,payload FROM events WHERE source='wikidata'").fetchone()
            event=json.loads(row[1]);event['year_start']=1961
            with s.db:s.db.execute('UPDATE events SET payload=? WHERE id=?',(json.dumps(event),row[0]))
            self.assertEqual(decisions(s),{})
        finally:s.close()

    def test_verified_degree_and_job_are_consumed_once_by_ranking(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            for source in ('wikidata','orcid-public-api'):
                locator='Q1@1' if source=='wikidata' else PERSON
                event=dict(source=source,person=private_person(PERSON),institution='ror:01mrfdz82',
                    kind='employment',stage='academic',year_start=2010,year_end=None,role='Professor',
                    locator=locator+'#employments',status='observed',assertion_origin=PERSON,
                    url=f'https://pub.orcid.org/v3.0/{PERSON}/employments',content_sha256='a'*64,
                    references=[{'snaks':{'P854':[{'datavalue':{'value':URL}}]}}])
                with s.db:
                    s.event(event)
                    if source=='orcid-public-api':
                        s.db.execute('INSERT INTO member_parts VALUES(?,?,?,?,?,?,?)',
                            (source,PERSON,'employments',event['url'],'b'*64,100,'2026-10-30'))
            job_id=s.db.execute("SELECT id FROM events WHERE source='wikidata' AND kind='employment'").fetchone()[0]
            with s.db:
                s.db.execute('INSERT INTO official_checks VALUES(?,?,?,?,?,?,?,?)',
                    (job_id,URL,'c'*64,RULE,'2026-10-30','official-underlying-source-confirmed',
                     'same-origin-as-wikidata-reference','d'*64))
            result=rank_global(s)
            node=next(n for n in result['nodes'] if n['institution_id']=='ror:05ect4e57')
            self.assertEqual((node['relations'],node['corroborated_relations']),(1,1))
            self.assertEqual(node['independent_source_classes'],['official-profile','orcid-self'])
            self.assertEqual(result['evidence_adjudication']['corroborated_events'],4)
            self.assertEqual(result['edges'][0]['weight'],1)
            self.assertIsNone(node['formal_rank'])
            self.assertNotIn(private_person(PERSON),json.dumps(result))
        finally:s.close()
