import json
from pathlib import Path
import unittest
from unittest.mock import patch
import zipfile
import zlib

from gapi.unified_analysis import UnifiedScan, Graphs
from gapi.global_analysis import analysis_counts_from_stores, relation_rows_from_stores
import test_global_analysis as fixtures


class UnifiedTests(unittest.TestCase):
    setUp=fixtures.AnalysisTests.setUp
    tearDown=fixtures.AnalysisTests.tearDown
    store=fixtures.AnalysisTests.store
    _snapshot_pair=fixtures.AnalysisTests._snapshot_pair
    _snapshot=fixtures.AnalysisTests._snapshot
    _snapshot_events=staticmethod(fixtures.AnalysisTests._snapshot_events)
    # Reuse the existing fixture and snapshot producer, not its whole suite.
    def test_unified_resume_matches_selected_person_oracle(self):
        left,right=self._snapshot_pair()
        try:
            for s in (left,right):self._snapshot(s,'same',self._snapshot_events())
            self._snapshot(right,'empty',[])
            scan=UnifiedScan((left,right));scan.run(seconds=10,max_people=1)
            self.assertFalse(scan.state['complete'])
            again=UnifiedScan((left,right));self.assertTrue(again.load())
            self.assertEqual(again.run(seconds=10)['complete'],True)
            oracle=analysis_counts_from_stores((left,right))
            self.assertEqual(again.state['people'],oracle['coverage']['people'])
            self.assertEqual(again.state['events'],oracle['coverage']['events'])
            self.assertEqual(dict(again.denominators),oracle['training_denominators'])
            self.assertEqual(again.state['relations'],len(list(relation_rows_from_stores((left,right)))))
            before=again.path.read_bytes();repeat=UnifiedScan((left,right));repeat.load();repeat.run(seconds=10)
            self.assertEqual(repeat.path.read_bytes(),before)
            statements=[]
            for store in (left,right):store.db.set_trace_callback(statements.append)
            result=again.result()
            for store in (left,right):store.db.set_trace_callback(None)
            self.assertEqual(result['coverage']['member_status'],{'observed':2})
            queries=[sql for sql in statements if sql.startswith('SELECT') and 'FROM members' in sql]
            self.assertTrue(queries)
            for sql in queries:
                plan=left.db.execute('EXPLAIN QUERY PLAN '+sql).fetchall()
                self.assertFalse(any('TEMP B-TREE' in row[-1] for row in plan))
            old=left.db.execute('EXPLAIN QUERY PLAN SELECT source,status,count(*) FROM members GROUP BY source,status').fetchall()
            self.assertTrue(any('TEMP B-TREE' in row[-1] for row in old),'oracle must detect the rejected external-sort plan')
            self.assertTrue(again.load(),'same reader can reconstruct released indexes from its completed checkpoint')
            self.assertEqual(again.state['events'],2)
            self.assertEqual(result['coverage']['events'],2)
            self.assertEqual(list(result['edges'])[0]['weight'],1.)
            self.assertEqual(result['sensitivity']['delete_source_orcid-public-api']['edges'],0)
            self.assertFalse(result['scope']['complete_global_census'])
        finally:left.close();right.close()

    def test_unified_checkpoint_drift_corruption_and_atomic_failure(self):
        left,right=self._snapshot_pair()
        try:
            self._snapshot(left,'member',self._snapshot_events())
            scan=UnifiedScan((left,right));scan.run(seconds=10,max_people=1);before=scan.path.read_bytes()
            with patch('gapi.unified_analysis._write_aggregate',side_effect=OSError('injected write')):
                with self.assertRaisesRegex(OSError,'injected'):scan.save()
            self.assertEqual(scan.path.read_bytes(),before)
            body=bytearray(before);body[len(body)//2]^=1;scan.path.write_bytes(body)
            with self.assertRaises((ValueError,zipfile.BadZipFile,RuntimeError,zlib.error)):
                UnifiedScan((left,right)).load()
            scan.path.write_bytes(before)
            self._snapshot(right,'changed',[])
            with self.assertRaisesRegex(ValueError,'version drift'):UnifiedScan((left,right)).load()
        finally:left.close();right.close()

    def test_physical_noncontiguous_people_cannot_complete(self):
        from gapi.orcid_stream import stream_archive
        from test_streaming import archive,XML
        left,right=self._snapshot_pair()
        try:
            # Force a legal reader-level interleaving. The independent count
            # still sees one stored person, so a duplicated group must fail.
            self._snapshot(left,'one',self._snapshot_events())
            scan=UnifiedScan((left,right))
            group=list(scan.rows(0,persons=['snapshot-person']))[0]
            scan.archive='synthetic'
            group[2]['end_rowid']=1
            original=scan.rows
            def rows(index,**kw):
                if 'physical_source' in kw:return iter([group,group])
                return original(index,**kw)
            with patch.object(scan,'rows',side_effect=rows):
                with self.assertRaisesRegex(ValueError,'physical person groups'):scan.run(seconds=10)
            self.assertFalse(scan.state['complete'])
        finally:left.close();right.close()

    def test_physical_archive_joins_secondary_fact_before_relation_rules(self):
        from gapi.orcid_stream import stream_archive,private_person
        from test_streaming import archive,XML,PERSON
        from gapi.task import digest
        from gapi.orcid_stream import API_RECORD_TRANSPORT
        data=archive([(f'ORCID_2026_10_summaries/097/{PERSON}.xml',XML)])
        left=self.store(data,work_dir=str(self.root/'left'))
        right=self.store(archive([]),work_dir=str(self.root/'right'))
        try:
            stream_archive(left,'orcid-2026-summary',__import__('io').BytesIO(data))
            source='orcid-public-api';right.start(source)
            with right.member(source,PERSON):
                event=self._snapshot_events()[1]
                right.event(dict(event,person=private_person(PERSON),source=source,
                    locator=PERSON+'#record',status='observed',country='GB'))
                right.db.execute('INSERT INTO member_parts VALUES(?,?,?,?,?,?,?)',
                    (source,PERSON,'record','https://example.invalid/record','a'*64,100,'2026-10-01T00:00:00+00:00'))
                right.finish_member(source,PERSON,digest({'transport':API_RECORD_TRANSPORT,'parts':{'record':'a'*64}}))
            scan=UnifiedScan((left,right));scan.run(seconds=10)
            oracle=analysis_counts_from_stores((left,right))
            self.assertTrue(scan.state['complete'])
            self.assertEqual(scan.state['people'],1)
            self.assertEqual(scan.state['events'],oracle['coverage']['events'])
            self.assertEqual(dict(scan.denominators),oracle['training_denominators'])
            expected=list(relation_rows_from_stores((left,right)))
            self.assertEqual(scan.state['relations'],len(expected))
            self.assertEqual(scan.state['relations'],1)
            self.assertEqual(len(scan.graphs.view(6+scan.sources.index(source))),0)
            self.assertEqual(len(scan.graphs.view(6+scan.sources.index('orcid-2026-summary'))),0)
        finally:left.close();right.close()

    def test_completed_scan_code_migration_preserves_original_and_rejects_other_drift(self):
        import hashlib
        from gapi.task import digest
        from gapi.analysis_checkpoint import input_binding
        left,right=self._snapshot_pair()
        try:
            self._snapshot(left,'member',self._snapshot_events())
            scan=UnifiedScan((left,right));scan.run(seconds=10)
            code={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('src/gapi').glob('*.py')}
            code['unified_analysis.py']='a'*64
            old=digest({'schema':'gapi-unified-input/1','inputs':[input_binding(x,None,predecessor_code=code) for x in (left,right)],'selection':scan.plan})
            scan.state['binding']=old
            scan.save();original=scan.path.read_bytes()
            with self.assertRaisesRegex(ValueError,'version drift'):UnifiedScan((left,right)).load()
            restored=UnifiedScan((left,right));self.assertTrue(restored.load(predecessor_code=code))
            result=restored.result()
            self.assertFalse(restored.graphs.positions)
            self.assertEqual(result['coverage']['people'],1)
            self.assertEqual(result['coverage']['events'],2)
            self.assertEqual(list(result['edges'])[0]['weight'],1.)
            self.assertEqual(restored.path.read_bytes(),original)
            bad={**code,'ranking.py':'b'*64}
            with self.assertRaisesRegex(ValueError,'only explicit bounded representation'):
                UnifiedScan((left,right)).load(predecessor_code=bad)
            # A source generation change cannot be explained as code migration.
            self._snapshot(right,'new',[])
            with self.assertRaisesRegex(ValueError,'version drift'):
                UnifiedScan((left,right)).load(predecessor_code=code)
        finally:left.close();right.close()

    def test_compact_boundary_scenarios_match_full_mapping_without_materializing(self):
        from gapi.global_analysis import scenario_result
        from types import SimpleNamespace
        graph=Graphs(['source'])
        for i in range(30000):graph.add(('e'+str(i%53),'t'+str(i%71)),.2,4)
        graph.add(('outside','t0'),1.,4)
        view=graph.view(4);reference=sorted(set(graph.nodes)-{'outside'})
        store=SimpleNamespace(task=SimpleNamespace(check_memory=lambda:0))
        actual=scenario_result(store,reference,view)
        expected=scenario_result(store,reference,dict(view.items()))
        for left_result,right_result in zip(actual,expected):
            self.assertAlmostEqual(left_result['probability_mass'],right_result['probability_mass'],places=12)
            self.assertEqual({k:v for k,v in left_result.items() if k!='probability_mass'},
                             {k:v for k,v in right_result.items() if k!='probability_mass'})
        self.assertEqual(actual[0]['filtered_boundary_edges'],1)
        self.assertEqual(actual[1]['new_endpoints'],['outside'])
        restricted=view.restrict_to(set(reference))
        self.assertIs(restricted.owner,graph)
        self.assertEqual(len(restricted),len(view)-1)
        self.assertTrue(hasattr(restricted,'packed_rows'))
        self.assertEqual(dict(restricted.items()),{k:v for k,v in view.items() if 'outside' not in k})

    def test_compact_graph_accumulation_is_exact_and_bounded(self):
        graph=Graphs(['a','b'])
        for i in range(100000):graph.add(('e'+str(i%100),'t'+str(i%77)),.5,4,2,i%2==0)
        expected={}
        for i in range(100000):
            key=('e'+str(i%100),'t'+str(i%77));expected[key]=expected.get(key,0)+.5
        self.assertEqual(dict(graph.view(4).items()),dict(sorted(expected.items())))
        self.assertEqual(len(graph.keys),7700)
        self.assertLess(sum(v.buffer_info()[1]*v.itemsize for v in graph.columns)+len(graph.keys)*8,1024**2)


