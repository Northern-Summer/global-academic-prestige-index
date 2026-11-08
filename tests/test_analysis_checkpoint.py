import json
import hashlib
import struct
import zlib
import unittest
from unittest.mock import patch

from gapi.analysis_checkpoint import GraphCheckpoint
from gapi.task import BudgetStop
import test_streaming as fixtures


class GraphCheckpointTests(unittest.TestCase):
    setUp=fixtures.StreamingTests.setUp
    tearDown=fixtures.StreamingTests.tearDown
    store=fixtures.StreamingTests.store

    def state(self):
        return {'edges':[[['b','a'],1.0]],'stages':[[['b','a'],{'doctorate':1.0}]],
            'counts':{'a':1},'source_counts':{'a':['wikidata']},'corroborated':{},
            'independent_origins':{},'recent_edges':[],'relation_count':1}

    def compact(self, checkpoint, *, binding=None, nodes=None, rows=None, suffix=b'',
                stage_endpoints=(0,1), recent_rows=()):
        meta={**self.state(),'nodes':['b','a'] if nodes is None else nodes,
              'binding':checkpoint.binding if binding is None else binding,
              'after_person':'person:a','complete':True}
        for key in ('edges','stages','recent_edges'):meta.pop(key)
        edges=struct.pack('<IId',*(rows or (0,1,1.0)))
        stages=struct.pack('<II4d',*stage_endpoints,0,0,1,0)
        recent=b''.join(struct.pack('<IId',*row) for row in recent_rows)
        body=json.dumps(meta).encode()
        raw=edges+stages+recent+body
        return (struct.pack('<8sIIIIII',b'GAPICMP1',len(meta['nodes']),1,1,len(recent_rows),len(body),1)
                +len(raw).to_bytes(8,'big')+hashlib.sha256(raw).digest()
                +zlib.compress(raw)+suffix)

    def test_compact_valid_rows_drift_and_malformed_inputs(self):
        s=self.store(fixtures.archive([]))
        try:
            cp=GraphCheckpoint(s,None);cp.after_person='person:a'
            cp.save(self.state,complete=True)
            path=cp.path.with_suffix('.compact')
            path.write_bytes(self.compact(cp))
            state=GraphCheckpoint(s,None).load()
            self.assertEqual(list(state['edges'].items()),[(('b','a'),1.0)])
            self.assertEqual(list(state['edges'].packed_rows()),[('b','a',1.0)])
            self.assertEqual(state['stages'][('b','a')]['doctorate'],1)
            self.assertEqual(dict(state['stages'][('b','a')]),{'doctorate':1.0})
            from gapi.global_analysis import _rank_graph
            self.assertEqual(_rank_graph(s,['a','b'],state['edges'],.85,1e-12,200),
                             _rank_graph(s,['a','b'],{('b','a'):1.0},.85,1e-12,200))
            for kwargs,reason in [
                ({'binding':'8bd5b1eeef031a179c68ebddd156adb179b56dd619db2e9773ed473b8d1a6f13'},'drift'),
                ({'nodes':['a','a']},'nodes'),
                ({'rows':(0,2,1.)},'row'),
                ({'rows':(0,1,float('nan'))},'row'),
                ({'rows':(0,1,-1.)},'row'),
                ({'stage_endpoints':(1,0)},'stage endpoints mismatch'),
                ({'recent_rows':[(1,0,.5)]},'recent endpoints mismatch'),
                ({'suffix':b'extra'},'stream boundary')]:
                path.write_bytes(self.compact(cp,**kwargs))
                with self.subTest(kwargs=kwargs),self.assertRaisesRegex(ValueError,reason):
                    GraphCheckpoint(s,None).load()
            body=self.compact(cp)
            path.write_bytes(self.compact(cp,recent_rows=[(0,1,.5)]))
            self.assertEqual(list(GraphCheckpoint(s,None).load()['recent_edges'].items()),[(('b','a'),.5)])
            path.write_bytes(body[:-2])
            with self.assertRaises(ValueError):GraphCheckpoint(s,None).load()
            path.write_bytes(body)
            with patch('gapi.analysis_checkpoint.MAX_DECODED',1):
                with self.assertRaisesRegex(ValueError,'decoded bound'):GraphCheckpoint(s,None).load()
            with patch('gapi.analysis_checkpoint.MAX_PACKED',72):
                with self.assertRaisesRegex(ValueError,'compressed bound'):GraphCheckpoint(s,None).load()
            with patch.object(s.task,'check_memory',side_effect=BudgetStop('injected')):
                with self.assertRaises(BudgetStop):GraphCheckpoint(s,None).load()
        finally:s.close()

    def test_compact_empty_graph_round_trip_and_incomplete_refusal(self):
        s=self.store(fixtures.archive([]))
        try:
            state=self.state()
            for name in ('edges','stages','recent_edges'):state[name]=[]
            for name in ('counts','source_counts'):state[name]={}
            state['relation_count']=0
            cp=GraphCheckpoint(s,None)
            with self.assertRaisesRegex(ValueError,'completed'):cp.write_compact(state)
            cp.save(lambda:state,complete=True)
            cp.write_compact(state)
            restored=GraphCheckpoint(s,None);value=restored.load()
            self.assertTrue(restored.complete)
            self.assertIsNone(restored.after_person)
            self.assertEqual(list(value['edges'].items()),[])
            self.assertEqual(value['relation_count'],0)
        finally:s.close()

    def test_compact_migration_binds_input_and_preserves_predecessor(self):
        from pathlib import Path
        import gapi.analysis_checkpoint as codec
        s=self.store(fixtures.archive([]))
        try:
            hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in Path(codec.__file__).parent.glob('*.py')}
            hashes['analysis_checkpoint.py']='0'*64
            old_binding=codec.input_binding(s,None,predecessor_code=hashes)
            with patch('gapi.analysis_checkpoint.input_binding',return_value=old_binding):
                old=GraphCheckpoint(s,None);old.after_person='person:a'
                old.save(self.state,complete=True);old.write_compact(self.state())
            original=old.path.read_bytes();sha=hashlib.sha256(original).hexdigest()
            target=old.path.with_suffix('.compact');before=target.read_bytes()
            with self.assertRaisesRegex(ValueError,'drift'):GraphCheckpoint(s,None).load()
            with self.assertRaisesRegex(ValueError,'identity'):
                GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256='0'*64)
            with patch('gapi.global_analysis._write_aggregate',side_effect=OSError('injected')):
                with self.assertRaises(OSError):GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha)
            self.assertEqual(target.read_bytes(),before)
            receipt=GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha)
            self.assertEqual(receipt['predecessor_binding'],old_binding)
            committed=target.read_bytes()
            self.assertEqual(GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha),receipt)
            self.assertEqual(target.read_bytes(),committed)
            self.assertEqual(old.path.read_bytes(),original)
            self.assertEqual(list(GraphCheckpoint(s,None).load()['edges'].items()),[(('b','a'),1.)])
            # A second code generation must bind the exact committed compact
            # and still reconstruct the unchanged original logical stream.
            def next_generation(store,allowed,*,predecessor_code=None):
                return old_binding if predecessor_code is not None else '1'*64
            with patch('gapi.analysis_checkpoint.input_binding',side_effect=next_generation):
                with self.assertRaisesRegex(ValueError,'drift'):
                    GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha)
                bad=dict(receipt,logical_sha256='0'*64)
                with self.assertRaisesRegex(ValueError,'receipt mismatch'):
                    GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha,predecessor_receipt=bad)
                chained=GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha,predecessor_receipt=receipt)
                self.assertEqual(chained['current_binding'],'1'*64)
                self.assertEqual(GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha,predecessor_receipt=receipt),chained)
                self.assertEqual(old.path.read_bytes(),original)
            target.write_bytes(committed)
            # A validly hashed compact payload with changed business values
            # must still fail the independent original-stream comparison.
            cp=GraphCheckpoint(s,None);cp.complete=True;cp.after_person='person:a'
            changed=self.state();changed['counts']={'a':2};cp.write_compact(changed)
            with self.assertRaisesRegex(ValueError,'logical mismatch'):
                GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha)
            target.write_bytes(before)
            with s.db:s.db.execute("INSERT INTO entities VALUES('drift','changed',NULL,0,'{}')")
            with self.assertRaisesRegex(ValueError,'drift'):
                GraphCheckpoint(s,None).migrate_compact(hashes,original_sha256=sha)
        finally:s.close()

    def test_atomic_capacity_failure_corruption_and_input_drift(self):
        s=self.store(fixtures.archive([]))
        try:
            checkpoint=GraphCheckpoint(s,None);checkpoint.after_person='person:a'
            checkpoint.save(self.state,complete=False)
            before=checkpoint.path.read_bytes();stamp=checkpoint.path.stat().st_mtime_ns
            checkpoint.save(self.state,complete=False)
            self.assertEqual(checkpoint.path.stat().st_mtime_ns,stamp)
            for target,error in [('gapi.global_analysis._write_aggregate',OSError('injected write failure')),
                                 (None,BudgetStop('injected capacity failure'))]:
                context=patch(target,side_effect=error) if target else patch.object(s.task,'check',side_effect=error)
                with context,self.assertRaises(type(error)):
                    checkpoint.save(self.state,complete=True)
                self.assertEqual(checkpoint.path.read_bytes(),before)
                restored=GraphCheckpoint(s,None);self.assertEqual(restored.load(),self.state())
                self.assertFalse(restored.complete)
            checkpoint.path.write_bytes(before[:-3])
            with self.assertRaises((ValueError,OSError,EOFError)):GraphCheckpoint(s,None).load()
            from gapi.analysis_checkpoint import MAGIC
            wrong=bytearray(before);wrong[len(MAGIC)+8]^=1
            checkpoint.path.write_bytes(wrong)
            with self.assertRaisesRegex(ValueError,'content mismatch'):GraphCheckpoint(s,None).load()
            checkpoint.path.write_bytes(before)
            with s.db:s.db.execute("INSERT INTO entities VALUES('test','changed',NULL,0,'{}')")
            with self.assertRaisesRegex(ValueError,'drift'):GraphCheckpoint(s,None).load()
        finally:s.close()

    def test_scope_code_binding_backwards_cursor_and_bounds(self):
        s=self.store(fixtures.archive([]))
        try:
            checkpoint=GraphCheckpoint(s,None);checkpoint.after_person='b'
            checkpoint.save(self.state,complete=False)
            self.assertNotEqual(checkpoint.path,GraphCheckpoint(s,{'wikidata'}).path)
            with self.assertRaisesRegex(ValueError,'backwards'):
                checkpoint.boundary('a',self.state)
            with patch('gapi.analysis_checkpoint.input_binding',return_value='changed'):
                with self.assertRaisesRegex(ValueError,'drift'):GraphCheckpoint(s,None).load()
                with self.assertRaisesRegex(ValueError,'changed'):checkpoint.save(self.state,complete=True)
            original=checkpoint.path.read_bytes()
            with patch('gapi.analysis_checkpoint.MAX_DECODED',1):
                with self.assertRaises(BudgetStop):checkpoint.save(self.state,complete=True)
            self.assertEqual(checkpoint.path.read_bytes(),original)
            with patch('gapi.analysis_checkpoint.MAX_PACKED',20):
                with self.assertRaises(BudgetStop):checkpoint.save(self.state,complete=True)
            self.assertEqual(checkpoint.path.read_bytes(),original)
        finally:s.close()
