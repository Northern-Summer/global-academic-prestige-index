import unittest
from unittest.mock import patch
from gapi.vivo_profiles import VivoEducationParser
from gapi.official_profiles import profile_url

PERSON='0000-0002-1825-0097'
ITEM='''<li>Ph.D., <span itemscope itemtype="http://schema.org/CollegeOrUniversity">
<a itemprop="name" href="/display/unitid_1">Example University</a></span>
<span class="listDateTime">2001</span></li>'''
HTML='''<div id="wrapper-content" role="main"><article><ul id="RO_0000056-EducationalProcess-List">'''+ITEM+'''</ul></article>
<ul id="orcidId-noRangeClass-List"><li><a href="http://orcid.org/'''+PERSON+'''">ORCID</a></li></ul></div>'''


class VivoTests(unittest.TestCase):
    def parse(self,html,chunk=7):
        parser=VivoEducationParser()
        for i in range(0,len(html),chunk):parser.feed(html[i:i+chunk])
        parser.boundary();parser.close()
        return parser

    def test_streamed_structured_degree_and_dedicated_identity(self):
        expected=None
        for chunk in (1,7,1024):
            p=self.parse(HTML,chunk)
            self.assertEqual([(s,n,y) for s,n,y,_ in p.degrees],[('doctorate','example university',2001)])
            self.assertEqual(p.orcids,{PERSON})
            self.assertEqual(p.joined,set());self.assertFalse(p.academic)
            if expected is not None:self.assertEqual(p.degrees,expected)
            expected=p.degrees

    def test_publications_and_outside_profile_do_not_bind_identity_or_degrees(self):
        self.assertEqual(self.parse(HTML.replace('RO_0000056-EducationalProcess-List','publications')).degrees,[])
        self.assertEqual(self.parse(HTML.replace('orcidId-noRangeClass-List','coauthors')).orcids,set())
        self.assertEqual(self.parse(HTML.replace('wrapper-content','unrelated')).degrees,[])
        p=self.parse(HTML.replace(ITEM,'<script>'+ITEM+'</script>'))
        self.assertEqual(p.degrees,[])

    def test_bachelor_abbreviation_reuses_canonical_normalization_rules(self):
        for label in ('B.S.','BSc','B.A.','学士'):
            with self.subTest(label=label):
                p=self.parse(HTML.replace('Ph.D.',label))
                self.assertEqual([stage for stage,_,_,_ in p.degrees],['bachelor'])

    def test_missing_ambiguous_unfinished_and_non_degree_fields_rejected(self):
        variants=[ITEM.replace('2001','unknown'),ITEM.replace('listDateTime','other'),
            ITEM.replace('itemprop="name"','itemprop="other"'),ITEM.replace('Ph.D.','PhD candidate'),
            ITEM.replace('Ph.D.','PhD honorary'),ITEM.replace('Ph.D.','Postdoctoral Fellow'),
            ITEM.replace('</li>','<span class="listDateTime">2002</span></li>')]
        for item in variants:
            with self.subTest(item=item):self.assertEqual(self.parse(HTML.replace(ITEM,item)).degrees,[])
        self.assertEqual(self.parse(HTML.split('</li>')[0]).degrees,[])

    def test_only_exact_https_profile_route_is_supported(self):
        good='https://experts.colorado.edu/display/fisid_1234'
        def event(url):return {'references':[{'snaks':{'P854':[{'datavalue':{'value':url}}]}}]}
        self.assertEqual(profile_url(event(good)),good)
        for bad in (good+'?x=1',good+'#x',good.replace('https:','http:'),
                good.replace('fisid_','pubid_'),good.replace('experts.colorado.edu','experts.colorado.edu:443'),
                good.replace('experts.colorado.edu','user@experts.colorado.edu'),'https://[invalid/',
                good.replace('/display/','/\ndisplay/')):
            with self.subTest(url=bad):self.assertIsNone(profile_url(event(bad)))

    def test_nesting_and_item_size_are_bounded(self):
        with self.assertRaises(ValueError):self.parse('<div>'*130)
        with self.assertRaises(ValueError):self.parse(HTML.replace('Ph.D.','x'*9000))


import test_streaming as fixtures
from test_streaming import archive
from gapi.orcid_stream import private_person
from gapi.official_profiles import check_official_profiles
from gapi.task import BudgetStop


class VivoIntegrationTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_employment_reference_discovers_degree_without_becoming_degree_evidence(self):
        from test_corroboration import CorroborationTests
        from gapi.corroboration import decisions
        s=self.store(archive([]))
        try:
            CorroborationTests.seed(self,s)
            with s.db:
                s.db.execute('DELETE FROM official_checks')
                s.db.execute('INSERT INTO entities VALUES(?,?,?,?,?)',('ror:05ect4e57','Example University','US',1,'{}'))
                row=s.db.execute("SELECT id,payload FROM events WHERE source='wikidata'").fetchone()
                import json
                job=json.loads(row[1]);job.update(kind='employment',stage='academic',year_start=2010,year_end=None)
                s.db.execute('UPDATE events SET kind=?,payload=? WHERE id=?',('employment',json.dumps(job),row[0]))
            # Existing self claim is doctorate, 1967; no Wikidata degree exists.
            p=VivoEducationParser();p.feed(HTML.replace('2001','1967'));p.close()
            before=list(s.db.execute('SELECT * FROM events ORDER BY id'))
            with patch('gapi.official_profiles.fetch_profile',return_value=(p,'b'*64)):
                result=check_official_profiles(s,None)
            self.assertEqual(result['official_self_field_candidates'],1)
            self.assertEqual(result['new_orcid_candidates'],1)
            d=decisions(s)
            self.assertEqual(len(d),1)
            self.assertEqual(d,decisions(s,allowed_sources={'orcid-public-api'}))
            self.assertEqual(before,list(s.db.execute('SELECT * FROM events ORDER BY id')))
            # Successful negative reread retires the derived support.
            p=VivoEducationParser();p.feed(HTML.replace('2001','1968'));p.close()
            with patch('gapi.official_profiles.fetch_profile',return_value=(p,'c'*64)):
                repeated=check_official_profiles(s,None)
            self.assertEqual(repeated['new_orcid_candidates'],0)
            self.assertEqual(decisions(s),{})
        finally:s.close()

    def test_structured_official_check_binds_identity_without_promoting_copied_origin(self):
        s=self.store(archive([]))
        try:
            s.start('wikidata')
            url='https://experts.colorado.edu/display/fisid_1234'
            event=dict(person=private_person(PERSON),institution='ror:05ect4e57',kind='education',
                stage='bachelor',year_end=2001,source='wikidata',locator='Q1@1#claim',status='observed',
                references=[{'snaks':{'P854':[{'datavalue':{'value':url}}]}}])
            with s.member('wikidata','Q1@1'):
                s.event(event);s.finish_member('wikidata','Q1@1','a'*64)
                s.db.execute('INSERT INTO entities VALUES(?,?,?,?,?)',('ror:05ect4e57','Example University','US',1,'{}'))
            p=VivoEducationParser();p.feed(HTML.replace('Ph.D.','B.S.'));p.close()
            with patch('gapi.official_profiles.fetch_profile',return_value=(p,'b'*64)):
                result=check_official_profiles(s,None)
            self.assertEqual(result['official_fields_confirmed'],1)
            self.assertEqual(result['independent_source_promotions'],0)
            self.assertEqual(s.db.execute('SELECT status FROM events').fetchone()[0],'observed')
            with patch('gapi.official_profiles.fetch_profile',side_effect=BudgetStop('space exhausted')):
                with self.assertRaises(BudgetStop):check_official_profiles(s,None)
        finally:s.close()
