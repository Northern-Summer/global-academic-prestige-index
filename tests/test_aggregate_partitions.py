import gzip
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

import test_streaming as fixtures
from gapi.aggregate_chunks import write, chunks, select_successor, materialize_original
from gapi.aggregate_partitions import repartition, logical_chunks, write_result, read_field
from gapi.task import BudgetStop, canonical


class AggregatePartitionTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_public_candidate_reuses_frozen_assets_and_preserves_current_release(self):
        from gapi.publication import prepare_partition_release
        from gapi.career_store import source_identity
        from gapi.task import digest
        s=self.store(fixtures.archive([]))
        try:
            project=s.task.root.parent/'project'
            self.enterContext(patch('gapi.storage.project_root',return_value=project))
            source=s.task.root/'partitions';data=self.sample();m=write_result(s.task,data,source)
            proof={'status':'passed','manifest_sha256':digest(m),'ordering_consistent':True,'probability_mass':1.0,'max_score_error':1e-18,
                   **{k:m[k] for k in ('logical_sha256','logical_bytes','nodes','edges')}}
            policy=json.loads(Path('configs/publication-20261029.json').read_bytes())
            runs=[{'source':'orcid-2026-summary','identity':source_identity(s.task,'orcid-2026-summary'),'members':1,'status':'verified'}]
            out=project/'outputs/global';out.mkdir(parents=True);(out/'manifest.json').write_bytes(b'{"original":true}')
            selected=prepare_partition_release(s.task,source,out,policy,runs,proof)
            result=json.loads(Path(selected['manifest']).read_bytes())
            self.assertEqual((out/'manifest.json').read_bytes(),b'{"original":true}')
            self.assertEqual((source/'manifest.json').read_bytes(),canonical(m))
            self.assertEqual(result['publication_status'],'public-observations-pending-external-review')
            self.assertFalse(result['publication']['formal_ranking_qualified'])
            self.assertTrue(selected['external_inputs']);self.assertEqual(len(list(out.glob('*.json.gz'))),1)
            oldfield=next(f for f in m['fields'] if f['field']=='publication_status')
            restored=[oldfield if f['field']=='publication_status' else f for f in result['fields']]
            self.assertEqual(restored,m['fields'])
            expected=canonical(dict(data,publication_status=result['publication_status']))
            self.assertEqual(result['logical_sha256'],hashlib.sha256(expected).hexdigest())
            self.assertEqual(result['logical_bytes'],len(expected))
            snapshot=Path(selected['manifest']).read_bytes()
            self.assertEqual(prepare_partition_release(s.task,source,out,policy,runs,proof),selected)
            self.assertEqual(Path(selected['manifest']).read_bytes(),snapshot)
            with self.assertRaises(ValueError):prepare_partition_release(s.task,source,out,None,runs,proof)
            self.assertEqual(Path(selected['manifest']).read_bytes(),snapshot)
        finally:s.close()

    def test_pinned_field_reader_checks_rows_references_and_corruption(self):
        s=self.store(fixtures.archive([]))
        try:
            data=self.sample();data['algorithm']={'network_universe':['a','b','c']}
            target=s.task.root/'fields';m=write_result(s.task,data,target)
            sha=hashlib.sha256((target/'manifest.json').read_bytes()).hexdigest()
            self.assertEqual(list(read_field(target,'nodes',manifest_sha256=sha)),data['nodes'])
            self.assertEqual(list(read_field(target,'edges',manifest_sha256=sha)),data['edges'])
            self.assertEqual(read_field(target,'sensitivity',manifest_sha256=sha),data['sensitivity'])
            self.assertEqual(read_field(target,'algorithm',manifest_sha256=sha),data['algorithm'])
            with self.assertRaises(ValueError):read_field(target,'nodes',manifest_sha256='0'*64)
            with self.assertRaises(ValueError):read_field(target,'missing',manifest_sha256=sha)
            a=next(f for f in m['fields'] if f['field']=='nodes')['assets'][0]
            path=target/a['file'];raw=path.read_bytes();path.write_bytes(b'0'*len(raw))
            with self.assertRaises(ValueError):list(read_field(target,'nodes',manifest_sha256=sha))
            path.write_bytes(raw)
            iterator=read_field(target,'nodes',manifest_sha256=sha);next(iterator)
            (target/'manifest.json').write_bytes((target/'manifest.json').read_bytes()+b' ')
            with self.assertRaises(ValueError):list(iterator)
        finally:s.close()

    def test_direct_result_matches_legacy_partition_and_retains_exact_source_anchor(self):
        from gapi.aggregate_chunks import AggregateRows
        s=self.store(fixtures.archive([]))
        try:
            data=self.sample();legacy=s.task.root/'legacy';target=s.task.root/'direct'
            original=write(s.task,data,legacy);expected=repartition(s.task,legacy)
            rows=data['nodes'];data['nodes']=AggregateRows(len(rows),lambda:iter(rows))
            actual=write_result(s.task,data,target)
            self.assertEqual(actual,expected)
            self.assertEqual(json.loads((target/'source-manifest.json').read_bytes()),original)
            logical=self.sample()
            self.assertEqual(b''.join(logical_chunks(target)),canonical(logical))
            first={p.name:p.read_bytes() for p in target.iterdir()}
            self.assertEqual(write_result(s.task,data,target),actual)
            self.assertEqual({p.name:p.read_bytes() for p in target.iterdir()},first)
            bad=dict(data,publication_status='public-observations-pending-external-review')
            with self.assertRaises(ValueError):write_result(s.task,bad,s.task.root/'forbidden')
            broken=dict(data,nodes=AggregateRows(len(rows)+1,lambda:iter(rows)))
            with self.assertRaises(ValueError):write_result(s.task,broken,s.task.root/'interrupted')
            self.assertFalse((s.task.root/'interrupted'/'manifest.json').exists())
        finally:s.close()

    def test_input_cross_piece_numbers_escapes_and_truncation(self):
        from gapi.aggregate_partitions import _Input
        for value in (1e20,-1.25e-20,True,None,'大学 "x" \\ y',{'nested':[1e20,None,{'x':'y'}]}):
            raw=canonical(value)+b','
            for width in (1,2,7):
                reader=_Input(raw[i:i+width] for i in range(0,len(raw),width))
                self.assertEqual(reader.value(),value)
                reader.expect(',')
                self.assertFalse(reader.peek())
        for body in (b'{"unfinished":',b'1e+',b'NaN',b'nullx'):
            with self.assertRaises(ValueError):_Input([body]).value()

    def sample(self):
        return {'schema_version':'gapi-global-observed/v1',
                'publication_status':'local-only-source-license-review',
                'nodes':[{'institution_id':'a','name':'大学 "A"','network_score':.5,'missing':None},
                         {'institution_id':'b','name':'B\\x','network_score':.5},
                         {'institution_id':'c','name':'C','network_score':None,'empty':[]}],
                'edges':[{'source':'a','target':'b','weight':1.0,'stages':{'doctorate':1.0}}],
                'sensitivity':{'test':{'ranks':{'a':1,'b':2}}},'coverage':{'events':3}}

    def test_plan_write_restore_and_repeat_do_not_change_logical_bytes(self):
        s=self.store(fixtures.archive([]))
        try:
            source=s.task.root/'source';target=s.task.root/'partitions';data=self.sample();write(s.task,data,source)
            plan=repartition(s.task,source)
            self.assertFalse(target.exists())
            result=repartition(s.task,source,target)
            self.assertEqual(plan,result)
            self.assertEqual(b''.join(logical_chunks(target)),canonical(data))
            files={p.name:(p.stat().st_size,p.stat().st_mtime_ns) for p in target.iterdir()}
            self.assertEqual(repartition(s.task,source,target),result)
            self.assertEqual(files,{p.name:(p.stat().st_size,p.stat().st_mtime_ns) for p in target.iterdir()})
            self.assertEqual(result['publication_status'],'local-only-source-license-review')
        finally:s.close()

    def test_successor_keeps_legacy_readers_independent_replay_and_exact_restoration(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('independent_successor','scripts/verify-public-release.py')
        independent=importlib.util.module_from_spec(spec);spec.loader.exec_module(independent)
        s=self.store(fixtures.archive([]))
        try:
            data=self.sample();data['algorithm']={'network_universe':['a','b'],'direction':'employer -> training','config':{'alpha':.85}}
            data['nodes'][0]['observed_rank']=1;data['nodes'][1]['observed_rank']=2
            data['edges'].append({'source':'b','target':'a','weight':1.0,'stages':{'doctorate':1.0}})
            source=s.task.root/'source';target=s.task.root/'target';m=write(s.task,data,source);repartition(s.task,source,target)
            originals={entry['file']:(source/entry['file']).read_bytes() for entry in m['chunks']}
            selected=select_successor(s.task,source,target)
            self.assertEqual(selected['original_chunks_reconstructed'],len(m['chunks']))
            stamp=(source/'successor.json').stat().st_mtime_ns
            self.assertEqual(select_successor(s.task,source,target),selected)
            self.assertEqual((source/'successor.json').stat().st_mtime_ns,stamp)
            self.assertEqual(len(originals),1);name=next(iter(originals));(source/name).unlink()
            self.assertEqual(b''.join(chunks(source)),canonical(data))
            verified=independent.verify_local_chunks(source)
            self.assertEqual(verified['status'],'passed')
            replay=independent.replay_local_chunks(source)
            self.assertEqual(replay['scored_nodes'],2)
            self.assertTrue(replay['ordering_consistent'])
            self.assertEqual(materialize_original(s.task,source)['created_files'],1)
            self.assertEqual((source/name).read_bytes(),originals[name])
            with self.assertRaisesRegex(ValueError,'selected successor'):write(s.task,data,source)
            link=source/'successor.json';old=link.read_bytes();value=json.loads(old);value['directory']='../escape';link.write_bytes(canonical(value))
            with self.assertRaises(ValueError):b''.join(chunks(source))
            with self.assertRaises(ValueError):independent.verify_local_chunks(source)
            link.write_bytes(old)
        finally:s.close()

    def test_invalid_successor_does_not_select_or_retire_source(self):
        s=self.store(fixtures.archive([]))
        try:
            source=s.task.root/'source';target=s.task.root/'target';write(s.task,self.sample(),source);m=repartition(s.task,source,target)
            entry=next(f['asset'] for f in m['fields'] if f['kind']=='value');path=target/entry['file'];path.write_bytes(b'corrupt')
            with self.assertRaises(ValueError):select_successor(s.task,source,target)
            self.assertFalse((source/'successor.json').exists())
            self.assertEqual(b''.join(chunks(source)),canonical(self.sample()))
        finally:s.close()

    def test_asset_bound_splits_rows_and_preserves_missing_null_and_order(self):
        s=self.store(fixtures.archive([]))
        try:
            data=self.sample();data['nodes']=[{'institution_id':str(i),'name':'x'*80,'null':None} for i in range(25)]
            source=s.task.root/'source';target=s.task.root/'target';write(s.task,data,source)
            with patch('gapi.aggregate_partitions.ROW_ASSET_BYTES',512):
                m=repartition(s.task,source,target)
                field=next(f for f in m['fields'] if f['field']=='nodes')
                self.assertGreater(len(field['assets']),1)
                self.assertTrue(all(f['bytes']<=512 for f in field['assets']))
                self.assertEqual(b''.join(logical_chunks(target)),canonical(data))
        finally:s.close()

    def test_edge_references_and_scenario_assets_preserve_exact_logical_input(self):
        s=self.store(fixtures.archive([]))
        try:
            data=self.sample();data['algorithm']={'network_universe':['b','a']}
            data['sensitivity']['other']={'ranks':{'a':2,'b':1}}
            source=s.task.root/'source';target=s.task.root/'target';write(s.task,data,source)
            m=repartition(s.task,source,target)
            edges=next(f for f in m['fields'] if f['field']=='edges');entry=edges['assets'][0]
            encoded=json.loads(gzip.decompress((target/entry['file']).read_bytes()))
            self.assertEqual(encoded['references'],{'source':'algorithm.network_universe','target':'algorithm.network_universe'})
            row=dict(zip(encoded['columns'],encoded['rows'][0]))
            self.assertEqual((row['source'],row['target']),(1,0))
            scenarios=next(f for f in m['fields'] if f['field']=='sensitivity')
            self.assertEqual([v['key'] for v in scenarios['entries']],['other','test'])
            self.assertEqual(b''.join(logical_chunks(target)),canonical(data))
            encoded['rows'][0][encoded['columns'].index('source')]=2
            raw=canonical(encoded);body=gzip.compress(raw,mtime=0);sha=hashlib.sha256(raw).hexdigest();name=sha+'.json.gz';(target/name).write_bytes(body)
            entry.update(file=name,sha256=sha,bytes=len(raw),stored_bytes=len(body),stored_sha256=hashlib.sha256(body).hexdigest())
            (target/'manifest.json').write_bytes(canonical(m))
            with self.assertRaisesRegex(ValueError,'reference index'):b''.join(logical_chunks(target))
        finally:s.close()

    def test_source_count_drift_cannot_replace_committed_partition_pointer(self):
        s=self.store(fixtures.archive([]))
        try:
            source=s.task.root/'source';target=s.task.root/'target';write(s.task,self.sample(),source);repartition(s.task,source,target)
            selected=(target/'manifest.json').read_bytes();pointer=source/'manifest.json';m=json.loads(pointer.read_bytes());m['nodes']+=1;pointer.write_bytes(canonical(m))
            with self.assertRaisesRegex(ValueError,'row count'):repartition(s.task,source,target)
            self.assertEqual((target/'manifest.json').read_bytes(),selected)
        finally:s.close()

    def test_independent_verifier_binds_original_source_and_rejects_changed_identity(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('independent_partition','scripts/verify-public-release.py')
        independent=importlib.util.module_from_spec(spec);spec.loader.exec_module(independent)
        s=self.store(fixtures.archive([]))
        try:
            data=self.sample();data['algorithm']={'network_universe':['a','b']}
            source=s.task.root/'source';target=s.task.root/'target';write(s.task,data,source);repartition(s.task,source,target)
            result=independent.verify_local_partitions(target,source)
            self.assertEqual(result['status'],'passed')
            compress=gzip.compress
            def unix_compress(*args,**kwargs):
                packed=compress(*args,**kwargs)
                return packed[:9]+b'\x03'+packed[10:]
            with patch.object(independent.gzip,'compress',side_effect=unix_compress):
                self.assertEqual(independent.verify_local_partitions(target,source)['status'],'passed')
            self.assertEqual(result['logical_sha256'],hashlib.sha256(canonical(data)).hexdigest())
            self.assertEqual((result['nodes'],result['edges']),(3,1))
            source_pointer=source/'manifest.json';source_raw=source_pointer.read_bytes();read=independent.bounded;changed=[False]
            def drift(path,*args):
                body=read(path,*args)
                if path.parent==target and path.name!='manifest.json' and not changed[0]:
                    source_pointer.write_bytes(source_raw+b' ');changed[0]=True
                return body
            with patch.object(independent,'bounded',side_effect=drift):
                with self.assertRaisesRegex(ValueError,'reconstructed identity'):independent.verify_local_partitions(target,source)
            source_pointer.write_bytes(source_raw)
            pointer=target/'manifest.json';m=json.loads(pointer.read_bytes());m['source_manifest_sha256']='0'*64;pointer.write_bytes(canonical(m))
            with self.assertRaisesRegex(ValueError,'source contract'):independent.verify_local_partitions(target,source)
        finally:s.close()

    def test_interruption_and_capacity_failure_preserve_current_manifest(self):
        s=self.store(fixtures.archive([]))
        try:
            source=s.task.root/'source';target=s.task.root/'target';data=self.sample();write(s.task,data,source);repartition(s.task,source,target)
            selected=(target/'manifest.json').read_bytes();data['coverage']['events']=4;write(s.task,data,source)
            def interrupt(event):raise RuntimeError('interrupted repartition')
            with self.assertRaisesRegex(RuntimeError,'interrupted'):repartition(s.task,source,target,progress=interrupt)
            self.assertEqual((target/'manifest.json').read_bytes(),selected)
            with patch('gapi.global_analysis._write_aggregate',side_effect=BudgetStop('no space')):
                with self.assertRaisesRegex(BudgetStop,'no space'):repartition(s.task,source,target)
            self.assertEqual((target/'manifest.json').read_bytes(),selected)
            result=repartition(s.task,source,target)
            self.assertEqual(b''.join(logical_chunks(target)),canonical(data))
            self.assertEqual(result,repartition(s.task,source,target))
        finally:s.close()

    def test_corruption_wrong_row_count_and_descriptor_escape_are_rejected(self):
        s=self.store(fixtures.archive([]))
        try:
            source=s.task.root/'source';target=s.task.root/'target';write(s.task,self.sample(),source);m=repartition(s.task,source,target)
            pointer=target/'manifest.json';selected=pointer.read_bytes();field=next(f for f in m['fields'] if f['field']=='nodes');entry=field['assets'][0];p=target/entry['file'];body=p.read_bytes()
            p.write_bytes(body[:-1]+bytes([body[-1]^1]))
            with self.assertRaises(ValueError):b''.join(logical_chunks(target))
            p.write_bytes(body)
            for mutation in ('escape','row-count','logical-hash','stored-size'):
                changed=json.loads(selected);f=next(f for f in changed['fields'] if f['field']=='nodes')
                if mutation=='escape':f['assets'][0]['file']='../outside.gz'
                elif mutation=='row-count':f['rows']+=1
                elif mutation=='logical-hash':changed['logical_sha256']='0'*64
                else:f['assets'][0]['stored_bytes']=True
                pointer.write_bytes(canonical(changed))
                with self.assertRaises(ValueError):b''.join(logical_chunks(target))
            pointer.write_bytes(selected)
            self.assertEqual(b''.join(logical_chunks(target)),canonical(self.sample()))
            with self.assertRaises(ValueError):repartition(s.task,source,s.task.root.parent/'escape')
            with self.assertRaises(ValueError):repartition(s.task,source,source)
        finally:s.close()

    def test_large_metadata_is_bounded_and_row_schema_drift_is_lossless(self):
        s=self.store(fixtures.archive([]))
        try:
            source=s.task.root/'source';write(s.task,self.sample(),source)
            with patch('gapi.aggregate_partitions.VALUE_BYTES',32):
                with self.assertRaisesRegex(ValueError,'decoded bound'):repartition(s.task,source)
        finally:s.close()
