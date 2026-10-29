import json
import unittest
from unittest.mock import patch

from gapi.official_profiles import ProfileParser, profile_url, check_official_profiles
import test_streaming as fixtures
from test_streaming import archive

URL='https://www.tamucc.edu/business/about/directory/larry-crumbley.php'
HTML='''<html><script>Ph.D., Fake University, 2000</script><main>
<h1>Official profile</h1><p>Professor of Accounting</p><h2>Education</h2>
<p>Ph.D., Louisiana State University, 1967</p><p>Joined the faculty in 2019</p>
<h2>Biography</h2><p>Ph.D., Fake University, 2000</p></main></html>'''


class OfficialTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_incremental_exact_sections_skip_scripts_and_biography(self):
        p=ProfileParser()
        for i in range(0,len(HTML),7):p.feed(HTML[i:i+7])
        p.close()
        self.assertEqual([(s,n,y) for s,n,y,_ in p.degrees],[('doctorate','louisiana state university',1967)])
        self.assertTrue(p.academic)
        self.assertEqual(p.joined,{2019})

    def test_page_reference_routes_only_exact_official_profile_paths(self):
        event={'references':[{'snaks':{'P854':[{'datavalue':{'value':URL}}]}}]}
        self.assertEqual(profile_url(event),URL)
        for invalid in (URL+'?next=x',URL.replace('https:','http:'),URL.replace('www.tamucc.edu','evil.example'),URL.replace('larry-crumbley','../escape')):
            event['references'][0]['snaks']['P854'][0]['datavalue']['value']=invalid
            self.assertIsNone(profile_url(event))

    def test_official_underlying_confirmation_never_promotes_copied_origin(self):
        s=self.store(archive([]))
        try:
            s.start('wikidata')
            event=dict(person='person',institution='ror:05ect4e57',kind='education',stage='doctorate',
                year_end=1967,source='wikidata',locator='Q1@1#claim',status='observed',
                references=[{'snaks':{'P854':[{'datavalue':{'value':URL}}]}}])
            with s.member('wikidata','Q1@1'):
                s.event(event);s.finish_member('wikidata','Q1@1','a'*64)
                s.db.execute('INSERT INTO entities VALUES(?,?,?,?,?)',('ror:05ect4e57','Louisiana State University','US',1,'{}'))
            p=ProfileParser();p.feed(HTML);p.close()
            with patch('gapi.official_profiles.fetch_profile',return_value=(p,'b'*64)):
                result=check_official_profiles(s,None)
            self.assertEqual(result['official_fields_confirmed'],1)
            self.assertEqual(result['independent_source_promotions'],0)
            self.assertEqual(s.db.execute('SELECT status FROM events').fetchone()[0],'observed')
            self.assertEqual(s.db.execute('SELECT origin_relation FROM official_checks').fetchone()[0],'same-origin-as-wikidata-reference')
            self.assertNotIn('Ph.D.',s.db.execute('SELECT field_hash FROM official_checks').fetchone()[0])
        finally:s.close()

    def test_wrong_year_or_institution_does_not_confirm(self):
        p=ProfileParser();p.feed(HTML);p.close()
        self.assertFalse(any(stage=='doctorate' and name=='stanford university' and date==1967 for stage,name,date,_ in p.degrees))
