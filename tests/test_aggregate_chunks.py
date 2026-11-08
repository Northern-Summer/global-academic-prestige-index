import gzip
import hashlib
import json
import unittest
from unittest.mock import patch

from gapi.aggregate_chunks import write, chunks
from gapi.task import canonical, BudgetStop
import test_streaming as fixtures


class AggregateChunkTests(unittest.TestCase):
    def test_original_gzip_platform_headers_keep_exact_identity(self):
        import importlib.util
        from gapi.aggregate_chunks import original_gzip
        spec=importlib.util.spec_from_file_location('independent_gzip','scripts/verify-public-release.py')
        independent=importlib.util.module_from_spec(spec);spec.loader.exec_module(independent)
        block=b'bounded reconstruction input'*200
        compressed=gzip.compress(block,compresslevel=6,mtime=0)
        for source_os in (3,10,255):
            frozen=compressed[:9]+bytes([source_os])+compressed[10:]
            entry={'bytes':len(frozen),'sha256':hashlib.sha256(frozen).hexdigest()}
            for runtime_os in (3,10,255):
                runtime=compressed[:9]+bytes([runtime_os])+compressed[10:]
                with self.subTest(source=source_os,runtime=runtime_os),patch.object(gzip,'compress',return_value=runtime):
                    self.assertEqual(original_gzip(block,entry),frozen)
                    self.assertEqual(independent.original_gzip(block,entry),frozen)
            for damaged in (frozen[:4]+b'\x01'+frozen[5:],frozen[:-1]+bytes([frozen[-1]^1])):
                bad={'bytes':len(damaged),'sha256':hashlib.sha256(damaged).hexdigest()}
                for reconstruct in (original_gzip,independent.original_gzip):
                    with self.assertRaises(ValueError):reconstruct(block,bad)
        for reconstruct in (original_gzip,independent.original_gzip):
            with self.assertRaises(ValueError):reconstruct(block+b'drift',entry)

    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def test_independent_rank_packed_capacity_and_rejection(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('independent_rank','scripts/verify-public-release.py')
        reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(reader)
        # Complete directed graph has independently known uniform stationary
        # mass. 449*448 crosses the old 200000-edge capacity.
        n=449;nodes=[str(i) for i in range(n)]
        data={'nodes':[{'institution_id':node,'network_score':1/n,'observed_rank':i+1}
                       for i,node in enumerate(sorted(nodes))],
              'edges':[{'source':a,'target':b,'weight':1.0} for a in nodes for b in nodes if a!=b],
              'algorithm':{'direction':'employer -> training','network_universe':nodes,
                           'config':{'alpha':.85}}}
        with self.assertRaisesRegex(ValueError,'edge cap'):reader.replay(data)
        result=reader.replay(data,edge_limit=210000)
        self.assertEqual(result['edges'],n*(n-1))
        self.assertLess(abs(result['probability_mass']-1),1e-10)
        self.assertLess(result['max_score_error'],1e-10)
        data['nodes'][0]['network_score']+=.01
        with self.assertRaisesRegex(ValueError,'mismatch'):reader.replay(data,edge_limit=210000)
        for limit in (True,0,1000001):
            with self.assertRaisesRegex(ValueError,'edge limit'):reader.replay(data,edge_limit=limit)

    def test_independent_reader_and_cli_verify_without_rank_or_public_verdict(self):
        import importlib.util
        import subprocess
        import sys
        from pathlib import Path
        spec=importlib.util.spec_from_file_location('independent_chunks','scripts/verify-public-release.py')
        reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(reader)
        s=self.store(fixtures.archive([]))
        try:
            folder=s.task.root/'chunks';data=self.sample()
            # Tokens cross physical chunk boundaries; no individual chunk is JSON.
            data['note']='x'*(1024**2+17)
            manifest=write(s.task,data,folder)
            result=reader.verify_local_chunks(folder)
            self.assertEqual(result['logical_sha256'],hashlib.sha256(canonical(data)).hexdigest())
            self.assertEqual(result['chunks'],2)
            self.assertIn('graph-to-score reproduction',result['not_verified'])
            run=subprocess.run([sys.executable,'-B','scripts/verify-public-release.py',
                '--local-chunks',str(folder)],capture_output=True,text=True,timeout=10)
            self.assertEqual(run.returncode,0,run.stderr)
            self.assertEqual(json.loads(run.stdout),result)
            pointer=folder/'manifest.json'
            for change in ({'logical_sha256':'0'*64},{'decoded_bytes':1},
                           {'publication_status':'public'},{'nodes':True}):
                pointer.write_text(json.dumps({**manifest,**change}),encoding='utf-8')
                with self.subTest(change=change),self.assertRaises(ValueError):reader.verify_local_chunks(folder)
            pointer.write_text(json.dumps(manifest),encoding='utf-8')
            original=manifest['chunks'][0]
            for change in ({'file':'../escape.gz'},{'bytes':True},{'decoded_sha256':'0'*64},
                           {'decoded_bytes':1024**2+1}):
                altered={**manifest,'chunks':[{**original,**change},*manifest['chunks'][1:]]}
                pointer.write_text(json.dumps(altered),encoding='utf-8')
                with self.subTest(change=change),self.assertRaises(ValueError):reader.verify_local_chunks(folder)
            pointer.write_text(json.dumps(manifest),encoding='utf-8')
            path=folder/original['file'];body=path.read_bytes()
            changed=bytearray(body);changed[4]^=1  # legal gzip mtime, same raw bytes and length
            path.write_bytes(changed)
            with self.assertRaisesRegex(ValueError,'compressed chunk identity'):reader.verify_local_chunks(folder)
            path.write_bytes(body[:-1])
            with self.assertRaisesRegex(ValueError,'compressed chunk identity'):reader.verify_local_chunks(folder)
            # A correctly hashed gzip bomb still exceeds the decoded block bound.
            bomb=gzip.compress(b'x'*(1024**2+1),mtime=0);sha=hashlib.sha256(bomb).hexdigest()
            name=sha+'.json.part.gz';(folder/name).write_bytes(bomb)
            altered={**manifest,'chunks':[{**original,'file':name,'sha256':sha,'bytes':len(bomb)},
                                          *manifest['chunks'][1:]]}
            pointer.write_text(json.dumps(altered),encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'decoded chunk identity'):reader.verify_local_chunks(folder)
        finally:s.close()

    def sample(self):
        return {'schema_version':'gapi-global-observed/v1','nodes':[
            {'institution_id':'a','name':'中文\n"证据','score':None},
            {'institution_id':'b','name':'other','score':.5}],
            'edges':[{'source':'b','target':'a','weight':1}],
            'missing':None,'false':False,'zero':0}

    def test_streaming_semantic_replay_and_failure_controls(self):
        import importlib.util
        from gapi.global_analysis import rank_global
        from test_global_analysis import AnalysisTests
        spec=importlib.util.spec_from_file_location('independent_semantic','scripts/verify-public-release.py')
        reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(reader)
        s=self.store(fixtures.archive([]))
        try:
            AnalysisTests.seed(self,s)
            data=rank_global(s);data['note']='escaped \\" 中文'
            folder=s.task.root/'semantic';write(s.task,data,folder)
            result=reader.replay_local_chunks(folder)
            self.assertTrue(result['ordering_consistent'])
            self.assertEqual(result['edges'],len(data['edges']))
            self.assertNotIn('graph-to-score reproduction',result['not_verified'])
            pointer=folder/'manifest.json';manifest=json.loads(pointer.read_text())
            pointer.write_bytes(canonical(dict(manifest,nodes=manifest['nodes']+1)))
            self.assertEqual(reader.verify_local_chunks(folder)['status'],'passed')
            with self.assertRaisesRegex(ValueError,'manifest mismatch'):
                reader.replay_local_chunks(folder)
            pointer.write_bytes(canonical(manifest))
            # Rehash all transport metadata around trailing JSON garbage:
            # transport checks remain green, semantic admission must fail.
            raw=canonical(data)+b'{}';body=gzip.compress(raw,mtime=0)
            sha=hashlib.sha256(body).hexdigest();name=sha+'.json.part.gz'
            (folder/name).write_bytes(body)
            invalid=dict(manifest,logical_sha256=hashlib.sha256(raw).hexdigest(),decoded_bytes=len(raw),
                chunks=[{'file':name,'sha256':sha,'bytes':len(body),'decoded_bytes':len(raw),
                         'decoded_sha256':hashlib.sha256(raw).hexdigest()}])
            pointer.write_bytes(canonical(invalid))
            self.assertEqual(reader.verify_local_chunks(folder)['status'],'passed')
            with self.assertRaisesRegex(ValueError,'trailing aggregate JSON'):
                reader.replay_local_chunks(folder)
            pointer.write_bytes(canonical(manifest))
            # Exercise every character/chunk split independently of gzip.
            text=json.dumps(data,ensure_ascii=True)
            parsed=reader._JSONStream(bytes([c]) for c in text.encode('ascii')).value()
            self.assertEqual(parsed,data)
            for invalid in ('{"a":1,"a":2}','[1,]','{"a":NaN}',
                            '{"a":1e999}','{"a":','"unterminated'):
                with self.subTest(invalid=invalid),self.assertRaises(ValueError):
                    reader._JSONStream([invalid.encode('ascii')]).value(keep=False)
            with self.assertRaisesRegex(ValueError,'nesting'):
                reader._JSONStream([('['*66+'0'+']'*66).encode()]).value()
            with self.assertRaisesRegex(ValueError,'token bound'):
                reader._JSONStream([b'"'+b'x'*(1024**2)+b'"']).value()
            data['nodes'][0]['network_score']=.123
            write(s.task,data,folder)
            with self.assertRaisesRegex(ValueError,'mismatch'):
                reader.replay_local_chunks(folder)
        finally:s.close()

    def test_repeatable_row_output_equals_materialized_result_and_preserves_pointer(self):
        from gapi.aggregate_chunks import AggregateRows
        from gapi.global_analysis import rank_global
        from test_global_analysis import AnalysisTests
        with self.assertRaisesRegex(ValueError,'stream_rows must be boolean'):
            rank_global(None,stream_rows=1)
        s=self.store(fixtures.archive([]))
        try:
            AnalysisTests.seed(self,s)
            expected=rank_global(s)
            streamed=rank_global(s,stream_rows=True)
            self.assertIsInstance(streamed['nodes'],AggregateRows)
            self.assertEqual(list(streamed['nodes']),expected['nodes'])
            self.assertEqual(list(streamed['nodes']),expected['nodes'])
            self.assertEqual(list(streamed['edges']),expected['edges'])
            folder=s.task.root/'streamed';first=write(s.task,expected,folder)
            stamp=(folder/'manifest.json').stat().st_mtime_ns
            self.assertEqual(write(s.task,streamed,folder),first)
            self.assertEqual((folder/'manifest.json').stat().st_mtime_ns,stamp)
            self.assertEqual(b''.join(chunks(folder)),canonical(expected))
            for count in (len(expected['nodes'])+1,0):
                bad=dict(streamed,nodes=AggregateRows(count,lambda:iter(expected['nodes'])))
                with self.assertRaisesRegex(ValueError,'count mismatch'):write(s.task,bad,folder)
                self.assertEqual(json.loads((folder/'manifest.json').read_text()),first)
            def fail():
                yield expected['nodes'][0]
                raise RuntimeError('interrupted row producer')
            with self.assertRaisesRegex(RuntimeError,'interrupted'):
                write(s.task,dict(streamed,nodes=AggregateRows(len(expected['nodes']),fail)),folder)
            self.assertEqual(json.loads((folder/'manifest.json').read_text()),first)
            self.assertEqual(write(s.task,streamed,folder),first)
        finally:s.close()

    def test_cli_preserves_legacy_files_and_writes_readable_complete_result(self):
        import subprocess
        import sys
        from test_global_analysis import AnalysisTests
        from gapi.global_analysis import rank_global
        s=self.store(fixtures.archive([]))
        try:
            AnalysisTests.seed(self,s)
            expected=rank_global(s)
            folder=s.task.root/'global-local-chunks'
            old=s.task.root/'global-local.json';old.write_bytes(b'predecessor')
        finally:s.close()
        result=subprocess.run([sys.executable,'-B','-m','gapi','rank-global','--task',str(self.path),'--chunked'],capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(b''.join(chunks(folder))),expected)
        self.assertEqual(old.read_bytes(),b'predecessor')
        self.assertEqual(json.loads(result.stdout)['manifest']['publication_status'],'local-only-source-license-review')
        result=subprocess.run([sys.executable,'-B','-m','gapi','rank-global','--task',str(self.path),
                               '--chunked','--progress'],capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout)['manifest']['publication_status'],'local-only-source-license-review')
        phases=[json.loads(line)['analysis_progress']['phase'] for line in result.stderr.splitlines()]
        self.assertIn('checkpoint_load',phases)
        self.assertNotIn('institution_labels',phases)
        self.assertTrue(any(json.loads(line)['analysis_progress'].get('resumed_complete')
            for line in result.stderr.splitlines()
            if json.loads(line)['analysis_progress']['phase']=='institution_directory'))
        self.assertEqual(phases[-1],'chunk_write')

    def test_roundtrip_idempotence_and_unchanged_predecessor_reader(self):
        s=self.store(fixtures.archive([]))
        try:
            folder=s.task.root/'chunks';data=self.sample()
            result=write(s.task,data,folder)
            expected=canonical(data)
            self.assertEqual(b''.join(chunks(folder)),expected)
            self.assertEqual(json.loads(b''.join(chunks(folder))),data)
            self.assertEqual(result['logical_sha256'],hashlib.sha256(expected).hexdigest())
            before={p.name:(p.read_bytes(),p.stat().st_mtime_ns) for p in folder.iterdir()}
            self.assertEqual(write(s.task,data,folder),result)
            self.assertEqual({p.name:(p.read_bytes(),p.stat().st_mtime_ns) for p in folder.iterdir()},before)
        finally:s.close()

    def test_over_old_32mib_limit_is_preserved_in_bounded_chunks(self):
        s=self.store(fixtures.archive([]))
        try:
            data=self.sample();data['evidence_note']='x'*(33*1024**2)
            folder=s.task.root/'chunks';manifest=write(s.task,data,folder)
            h=hashlib.sha256();count=0
            for raw in chunks(folder,check=s.task.check_memory):
                self.assertLessEqual(len(raw),1024**2);count+=len(raw);h.update(raw)
            self.assertGreater(count,32*1024**2)
            self.assertEqual(h.hexdigest(),hashlib.sha256(canonical(data)).hexdigest())
            self.assertEqual(count,manifest['decoded_bytes'])
            import importlib.util
            spec=importlib.util.spec_from_file_location('independent_large_chunks','scripts/verify-public-release.py')
            reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(reader)
            loads=json.loads
            def bounded_json(body,*args,**kwargs):
                self.assertLessEqual(len(body),4*1024**2)
                return loads(body,*args,**kwargs)
            with patch.object(reader.json,'loads',side_effect=bounded_json):
                checked=reader.verify_local_chunks(folder)
            self.assertEqual(checked['decoded_bytes'],count)
            self.assertEqual(checked['logical_sha256'],h.hexdigest())
        finally:s.close()

    def test_capacity_failure_preserves_manifest_and_restart_reuses_chunks(self):
        s=self.store(fixtures.archive([]))
        try:
            folder=s.task.root/'chunks';old=self.sample();write(s.task,old,folder)
            pointer=(folder/'manifest.json').read_bytes();new={**old,'extra':'a'*3000}
            import gapi.aggregate_chunks as codec
            from gapi.global_analysis import _write_aggregate
            calls=[]
            def fail(task,path,body):
                calls.append(path.name)
                if len(calls)==2:raise BudgetStop('injected full capacity')
                return _write_aggregate(task,path,body)
            with patch.object(codec,'CHUNK_BYTES',1024),patch('gapi.global_analysis._write_aggregate',side_effect=fail):
                with self.assertRaisesRegex(BudgetStop,'full capacity'):
                    write(s.task,new,folder)
            self.assertEqual((folder/'manifest.json').read_bytes(),pointer)
            self.assertEqual(json.loads(b''.join(chunks(folder))),old)
            write(s.task,new,folder)
            self.assertEqual(json.loads(b''.join(chunks(folder))),new)
        finally:s.close()

    def test_corruption_missing_chunk_manifest_drift_and_path_escape_rejected(self):
        s=self.store(fixtures.archive([]))
        try:
            folder=s.task.root/'chunks';m=write(s.task,self.sample(),folder)
            pointer=folder/'manifest.json';body=pointer.read_bytes();entry=m['chunks'][0];part=folder/entry['file'];original=part.read_bytes()
            part.write_bytes(original[:-1]+bytes([original[-1]^1]))
            with self.assertRaises(ValueError):list(chunks(folder))
            part.write_bytes(original)
            for changed in ({'decoded_bytes':m['decoded_bytes']+1},{'logical_sha256':'0'*64},
                            {'chunks':[{**entry,'file':'../outside.gz'}]},
                            {'chunks':[{**entry,'bytes':True}]}):
                pointer.write_bytes(canonical({**m,**changed}))
                with self.assertRaises(ValueError):list(chunks(folder))
            pointer.write_bytes(body)
            moved=folder/'retained-corrupt-case.gz';part.rename(moved)
            with self.assertRaises(FileNotFoundError):list(chunks(folder))
            moved.rename(part)
            with self.assertRaises(ValueError):write(s.task,self.sample(),s.task.root.parent/'outside')
            with self.assertRaises(ValueError):write(s.task,{**self.sample(),'bad':float('nan')},folder)
            self.assertEqual(pointer.read_bytes(),body)
        finally:s.close()
