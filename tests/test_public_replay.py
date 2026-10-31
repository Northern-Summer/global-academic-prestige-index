import copy
import gzip
import hashlib
import json
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

    def test_compressed_repository_replay_verifies_stored_and_decoded_identity(self):
        import tempfile
        import os
        parent=Path('D:/GAPI/global-academic-prestige-index') if os.name=='nt' else None
        with tempfile.TemporaryDirectory(dir=parent) as tmp:
            folder=Path(tmp);body=json.dumps(self.fixture()).encode()
            sha=hashlib.sha256(body).hexdigest();name='global-'+sha[:16]+'.json'
            packed=gzip.compress(body,mtime=0);(folder/(name+'.gz')).write_bytes(packed)
            manifest={'snapshot':name,'output_sha256':sha,'repository_snapshot':{
                'filename':name+'.gz','encoding':'gzip','sha256':hashlib.sha256(packed).hexdigest(),
                'bytes':len(packed),'decoded_bytes':len(body)}}
            path=folder/'manifest.json';path.write_text(json.dumps(manifest))
            self.assertEqual(replay.verify(path)['status'],'passed')
            for change in ({'filename':'../escape.gz'},{'encoding':'br'},{'sha256':'0'*64},
                           {'decoded_bytes':1},{'bytes':True}):
                modified=copy.deepcopy(manifest);modified['repository_snapshot'].update(change)
                path.write_text(json.dumps(modified))
                with self.subTest(change=change),self.assertRaises(ValueError):replay.verify(path)
            path.write_text(json.dumps(manifest));(folder/(name+'.gz')).write_bytes(packed[:-1])
            with self.assertRaisesRegex(ValueError,'identity'):replay.verify(path)
