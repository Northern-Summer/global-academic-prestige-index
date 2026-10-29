import json
import unittest
from pathlib import Path

from gapi.career_normalization import interpretation, normalize_careers
from gapi.global_analysis import rank_global
from gapi.task import canonical
import test_streaming as fixtures
from test_streaming import archive


class NormalizationTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def event(self, role, kind='education'):
        return dict(source='orcid-public-api',person='private',institution='ror:03vek6s52',
                    role=role,kind=kind,stage=None,status='unresolved',year_end=2000,year_start=1990,locator='x',country='CN')

    def test_multilingual_explicit_labels_and_ambiguous_counterexamples(self):
        for label,kind,stage in [('BS','education','bachelor'),('B.S.','education','bachelor'),('M.S.','education','master'),
            ('M.S.','education','master'),('Doctorado','education','doctorate'),('Doutorado','education','doctorate'),
            ('Maestría','education','master'),('Ricercatori','employment','academic'),
            ('Maître de conférences','employment','academic')]:
            with self.subTest(label=label):self.assertEqual(interpretation(self.event(label,kind))[1],stage)
        for label in ('MD','Dr.','Diploma','Licenciatura','BS in Biology'):
            with self.subTest(label=label):self.assertIsNone(interpretation(self.event(label)))
        for label in ('Doctorat en cours','MS student','honorary BS','Doctorado no finalizado',
                      'Doctorado abandonado','Doutorado interrompido','PhD not completed'):
            with self.subTest(label=label):self.assertEqual(interpretation(self.event(label))[2],'unresolved')
        self.assertIsNone(interpretation(self.event('Graduate Research Associate','employment')))
        self.assertIsNone(interpretation(self.event('Director','employment')))
        missing=self.event('B.S.'); missing['year_end']=None
        self.assertIsNone(interpretation(missing))

    def test_new_interpretation_keeps_raw_events_immutable_and_denominator_consistent(self):
        s=self.store(archive([]))
        try:
            s.start('orcid-public-api')
            training=self.event('BS'); job=self.event('Research Associate','employment')
            job.update(institution='ror:01mrfdz82',year_start=2005,year_end=None)
            with s.member('orcid-public-api','x'):
                s.event(training);s.event(job);s.finish_member('orcid-public-api','x','a'*64)
            before=list(s.db.execute('SELECT * FROM events ORDER BY id'))
            self.assertEqual(rank_global(s)['edges'],[])
            self.assertEqual(normalize_careers(s)['interpreted_events'],2)
            after=list(s.db.execute('SELECT * FROM events ORDER BY id'))
            self.assertEqual(before,after)
            result=rank_global(s)
            self.assertEqual(len(result['edges']),1)
            node=next(n for n in result['nodes'] if n['institution_id']==training['institution'])
            self.assertEqual(node['observed_training_denominator'],1)
            self.assertEqual(node['corroborated_relations'],0)
            self.assertIsNone(node['formal_rank'])
            expected=canonical(result)
            normalize_careers(s)
            self.assertEqual(canonical(rank_global(s)),expected)
            # The default public policy cannot disclose even an interpretation
            # count for a source whose derivatives have not been selected.
            self.assertNotIn('normalization_policy',rank_global(s,public=True)['algorithm'])
        finally:s.close()

    def test_normalization_requires_a_new_version_when_policy_is_changed(self):
        s=self.store(archive([]))
        try:
            s.start('orcid-public-api')
            with s.member('orcid-public-api','x'):
                s.event(self.event('BS'));s.finish_member('orcid-public-api','x','a'*64)
            normalize_careers(s)
            with s.db:s.db.execute("UPDATE event_interpretations SET policy_sha256='old'")
            with self.assertRaisesRegex(ValueError,'version change'):normalize_careers(s)
        finally:s.close()


if __name__=='__main__':unittest.main()
