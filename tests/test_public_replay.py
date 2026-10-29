import copy
import importlib.util
import unittest
from pathlib import Path

spec=importlib.util.spec_from_file_location('public_replay',Path('scripts/verify-public-release.py'))
replay=importlib.util.module_from_spec(spec);spec.loader.exec_module(replay)


class PublicReplayTests(unittest.TestCase):
    def fixture(self):
        # Independent algebra for Y -> X with X dangling:
        # rY=1/(2+a), rX=(1+a)/(2+a).
        alpha=.85
        y=1/(2+alpha);x=(1+alpha)/(2+alpha)
        return {'nodes':[{'institution_id':'X','network_score':x,'observed_rank':1},
                         {'institution_id':'Y','network_score':y,'observed_rank':2}],
            'edges':[{'source':'Y','target':'X','weight':1}],
            'algorithm':{'direction':'employer -> training','network_universe':['X','Y'],'config':{'alpha':alpha}}}

    def test_independent_analytic_direction_and_mutated_outputs(self):
        data=self.fixture();result=replay.replay(data)
        self.assertLess(result['max_score_error'],1e-10)
        for mutation in ['direction','score','edge','rank','endpoint','weight']:
            changed=copy.deepcopy(data)
            if mutation=='direction':changed['algorithm']['direction']='training -> employer'
            elif mutation=='score':changed['nodes'][0]['network_score']=.9
            elif mutation=='edge':changed['edges'][0].update(source='X',target='Y')
            elif mutation=='rank':changed['nodes'][0]['observed_rank']=2
            elif mutation=='endpoint':changed['edges'][0]['target']='outside'
            else:changed['edges'][0]['weight']=float('inf')
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):replay.replay(changed)

    def test_committed_public_slice_replays_without_person_inputs(self):
        result=replay.verify(Path('outputs/global/manifest.json'))
        self.assertEqual(result['status'],'passed')
        self.assertGreater(result['scored_nodes'],0)
