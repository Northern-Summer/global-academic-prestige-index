import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from gapi.derived_export import write_bundle


class DerivedBrowserTests(unittest.TestCase):
    def result(self,count=1):
        return {'source':'fixture','source_census':{'source_complete':True},
            'formal_ranking':False,'global_complete':False,'direction':'employer -> training',
            'nodes':[{'institution_id':str(i),'institution_name':'Institution '+str(i),
                'country':None,'region':'Asia' if i==0 else None,'network_score':.5,'relation_count':count,'observed_order':i+1,
                'institution_identity_status':'unresolved_external_identifier',
                'unranked_reasons':['independent_corroboration_missing','institution_resolution_pending'],
                'private_person':'NEVER EXPORT'} for i in range(2)],
            'edge_count':count,'edges':({'source':'0','target':'1','weight':1.,'stages':{'doctorate':1.}} for _ in range(count)),
            'distinct_retained_people':1,'person_level_funnel':{},'resolved_institutions':0,
            'unresolved_institution_identifiers':2,'deduplicated_relations':count,
            'config':{},'convergence':{'converged':True},'probability_mass':1.}

    def test_complete_graph_is_sharded_indexed_hashed_and_idempotent(self):
        with tempfile.TemporaryDirectory() as folder:
            out=Path(folder);task=SimpleNamespace(check=lambda *a:None,check_memory=lambda:None)
            a=write_bundle(task,self.result(10001),out)
            manifest=json.loads((out/'manifest.json').read_bytes())
            self.assertEqual(len(manifest['edge_chunks']),2)
            nodes=json.loads((out/manifest['nodes']['file']).read_bytes())
            self.assertEqual(nodes[0][6],[0,1]);self.assertEqual(nodes[1][6],[0,1])
            self.assertEqual(nodes[0][9],'Asia');self.assertIsNone(nodes[1][9])
            for asset in [manifest['nodes'],manifest['overview'],*manifest['edge_chunks']]:
                raw=(out/asset['file']).read_bytes()
                self.assertEqual(hashlib.sha256(raw).hexdigest(),asset['sha256'])
                self.assertNotIn(b'NEVER EXPORT',raw)
            before={p.name:(p.stat().st_mtime_ns,p.read_bytes()) for p in out.iterdir()}
            b=write_bundle(task,self.result(10001),out)
            self.assertEqual(a,b)
            self.assertEqual(before,{p.name:(p.stat().st_mtime_ns,p.read_bytes()) for p in out.iterdir()})

    def test_partial_source_cannot_publish_manifest(self):
        with tempfile.TemporaryDirectory() as folder:
            r=self.result();r['source_census']['source_complete']=False
            with self.assertRaisesRegex(ValueError,'complete verified'):
                write_bundle(SimpleNamespace(),r,folder)
            self.assertFalse((Path(folder)/'manifest.json').exists())

    def test_content_conflict_and_unknown_edge_cannot_replace_pointer(self):
        with tempfile.TemporaryDirectory() as folder:
            out=Path(folder);task=SimpleNamespace(check=lambda *a:None,check_memory=lambda:None)
            write_bundle(task,self.result(),out);before=(out/'manifest.json').read_bytes()
            m=json.loads(before);(out/m['nodes']['file']).write_bytes(b'unique recovery bytes')
            with self.assertRaisesRegex(ValueError,'content address conflict'):write_bundle(task,self.result(),out)
            self.assertEqual((out/'manifest.json').read_bytes(),before)
            r=self.result();r['edges']=[{'source':'unknown','target':'1','stages':{},'weight':1}]
            with self.assertRaises(KeyError):write_bundle(task,r,out)
            self.assertEqual((out/'manifest.json').read_bytes(),before)

    def test_budget_stop_before_manifest_keeps_previous_release_and_reuses_assets(self):
        from gapi.task import BudgetStop
        with tempfile.TemporaryDirectory() as folder:
            out=Path(folder);task=SimpleNamespace(check=lambda *a:None,check_memory=lambda:None)
            write_bundle(task,self.result(),out);before=(out/'manifest.json').read_bytes()
            def changed():
                r=self.result();r['nodes'][0]['network_score']=.4;r['nodes'][1]['network_score']=.6
                return r
            calls=0
            def stop(*args):
                nonlocal calls
                calls+=1
                if calls==2:raise BudgetStop('manifest admission')
            task.check=stop
            with self.assertRaises(BudgetStop):write_bundle(task,changed(),out)
            self.assertEqual((out/'manifest.json').read_bytes(),before)
            files={p.name for p in out.iterdir()};task.check=lambda *a:None
            write_bundle(task,changed(),out)
            self.assertEqual({p.name for p in out.iterdir()},files)
            self.assertNotEqual((out/'manifest.json').read_bytes(),before)
