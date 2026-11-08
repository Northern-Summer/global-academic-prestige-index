import io
import json
import subprocess
import sys
import unittest

from gapi.career_rules import classify
from gapi.global_analysis import export_local, rank_global
from gapi.global_sources import ingest_orcid_api
from gapi.orcid_stream import private_person
import test_streaming as fixtures
from test_streaming import XML, PERSON, archive


class AnalysisTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_partition_publication_preflight_binds_verifier_policy_and_source_generation(self):
        import copy
        from pathlib import Path
        from gapi.publication import partition_release_preflight
        from gapi.career_store import source_identity
        from gapi.task import digest
        s=self.store(archive([]))
        try:
            manifest={'schema_version':'gapi-aggregate-partitions/1','publication_status':'local-only-source-license-review',
                      'logical_sha256':'a'*64,'logical_bytes':123,'nodes':2,'edges':1}
            policy=json.loads(Path('configs/publication-20261029.json').read_bytes())
            runs=[{'source':'orcid-2026-summary','identity':source_identity(s.task,'orcid-2026-summary'),'status':'verified','members':1}]
            independent={'status':'passed','manifest_sha256':digest(manifest),'logical_sha256':'a'*64,'logical_bytes':123,'nodes':2,'edges':1,
                         'ordering_consistent':True,'max_score_error':1e-18,'probability_mass':1.0}
            result=partition_release_preflight(s.task,manifest,policy,runs,independent)
            self.assertEqual(result['qualification'],'public-observations-pending-external-review')
            self.assertFalse(result['formal_ranking_qualified']);self.assertFalse(result['independent_personal_facts_verified'])
            for update in ({'manifest_sha256':'b'*64},{'edges':2},{'ordering_consistent':False},
                           {'max_score_error':float('nan')},{'max_score_error':True},{'probability_mass':float('nan')},
                           {'probability_mass':False},{'probability_mass':.9}):
                with self.assertRaises(ValueError):partition_release_preflight(s.task,manifest,policy,runs,{**independent,**update})
            for update in ({'identity':'c'*64},{'status':'partial'},{'members':False},{'source':'unknown'}):
                with self.assertRaises(ValueError):partition_release_preflight(s.task,manifest,policy,[{**runs[0],**update}],independent)
            with self.assertRaises(ValueError):partition_release_preflight(s.task,manifest,None,runs,independent)
            with self.assertRaises(ValueError):partition_release_preflight(s.task,manifest,policy,runs+runs,independent)
            bad=copy.deepcopy(policy);bad['purpose']='commercial'
            with self.assertRaises(ValueError):partition_release_preflight(s.task,manifest,bad,runs,independent)
        finally:s.close()

    def test_additive_new_reference_endpoint_equals_full_analysis(self):
        self.test_complete_additive_result_equals_full_analysis_and_refuses_drift(new_endpoint=True)

    def test_additive_new_named_and_unscored_directory_rows_equal_full_analysis(self):
        self.test_complete_additive_result_equals_full_analysis_and_refuses_drift(new_endpoint=True,new_directory=True)

    def test_additive_prior_label_witness_preserves_full_label_winner(self):
        self.test_complete_additive_result_equals_full_analysis_and_refuses_drift(label_mode='prior')

    def test_additive_earlier_new_label_matches_full_label_winner(self):
        self.test_complete_additive_result_equals_full_analysis_and_refuses_drift(label_mode='delta')

    def test_complete_additive_result_equals_full_analysis_and_refuses_drift(self,new_endpoint=False,new_directory=False,label_mode=None):
        import gzip,hashlib,copy
        from gapi.analysis_checkpoint import GraphCheckpoint,AdditiveGraph
        from gapi.aggregate_chunks import write
        from gapi.aggregate_partitions import repartition
        from gapi.global_analysis import relation_rows,complete_additive_result,_rank_graph,DEFAULT_CONFIG
        from gapi.task import digest,canonical
        s=self.store(archive([]))
        try:
            annual='orcid-2026-summary';s.start(annual)
            def fact(source,person,inst,kind,stage,start,end,role):
                return dict(source=source,person=person,institution=inst,kind=kind,stage=stage,
                            year_start=start,year_end=end,role=role,locator='fixture',status='observed',country='CN')
            with s.member(annual,'fixture'):
                training=fact(annual,'baseline','A','education','doctorate',1990,1995,'PhD')
                if label_mode:training['institution_name']='Prior A'
                s.event(training)
                s.event(fact(annual,'baseline','B','employment','academic',2000,None,'Professor'))
                if new_endpoint and not new_directory:
                    s.event(dict(fact(annual,'baseline','C','employment',None,None,None,None),status='unresolved'))
                s.finish_member(annual,'fixture','a'*64)
            s.status(annual,'verified')
            with s.db:s.db.execute("INSERT INTO institution_aliases VALUES('alias:A','A','fixture')")
            if new_endpoint and not new_directory:
                with s.db:s.db.execute("INSERT INTO entities VALUES('C','Institution C','CN',1,'{}')")
            baseline=rank_global(s,checkpoint=True)
            cp=GraphCheckpoint(s,None);state=cp.load();cp.write_compact(state)
            legacy=s.task.root/'legacy';previous=s.task.root/'partitions';write(s.task,baseline,legacy);repartition(s.task,legacy,previous)
            pinned=hashlib.sha256((previous/'manifest.json').read_bytes()).hexdigest()
            binding=cp.binding;predecessor={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (
                cp.path.with_suffix('.compact'),s.task.root/'analysis-training-denominators-74234e98afe7498f.json.gz')}
            api='orcid-public-api';s.start(api);person=private_person(PERSON)
            with s.member(api,PERSON):
                training=fact(api,person,'A' if label_mode else ('C' if new_endpoint else 'alias:A'),'education','doctorate',1991,1996,'PhD')
                if new_directory:training['institution_name']='Reported C'
                if label_mode:training['institution_name']='New A'
                if label_mode=='delta':training['put_code']=3
                s.event(dict(training,locator=PERSON))
                s.event(dict(fact(api,person,'B','employment','academic',2001,None,'Professor'),locator=PERSON))
                if new_directory:
                    s.event(dict(fact(api,person,'D','employment',None,None,None,None),locator=PERSON,
                                 status='unresolved',institution_name='Reported D'))
                s.finish_member(api,PERSON,'b'*64)
            events=[{'id':i,'payload':json.loads(text)} for i,text in s.db.execute('SELECT id,payload FROM events WHERE person=? ORDER BY id',(person,))]
            rel=list(relation_rows(s,persons=(person,)))
            delta={'schema_version':'gapi-private-additive-input/1','predecessor_binding':binding,
                   'predecessor_files':predecessor,'people':[person],'events':events,
                   'recovered_member_ledger':[list(s.db.execute('SELECT locator,sha256,status,reason FROM members WHERE source=? AND locator=?',(api,PERSON)).fetchone())],
                   'relations':[{'key':list(k),'training':train,'job':job} for k,train,job in rel],
                   'training_person_institution_pairs':[[person,'C' if new_endpoint else 'A']],'event_delta':len(events),'people_delta':1,'member_delta':1,
                   'observed_event_delta':{'observed':2,**({'unresolved':1} if new_directory else {})}}
            graph=AdditiveGraph(GraphCheckpoint(s,None)._load_compact(cp.path.with_suffix('.compact'),expected_binding=binding)['edges'],{('B','C' if new_endpoint else 'A'):1.})
            nodes=['A','B','C'] if new_endpoint else ['A','B'];scores,conv=_rank_graph(s,nodes,graph,.85,1e-12,200)
            base={'delta_sha256':digest(delta),'complete_aggregate':False,'nodes':nodes,'edges':len(graph),'scores':scores,
                  'ranks':{n:i+1 for i,n in enumerate(sorted(scores,key=lambda n:(-scores[n],n)))},'convergence':conv}
            def witnesses():
                if label_mode is None:return []
                old=s.db.execute("SELECT id FROM events WHERE person='baseline' AND institution='A'").fetchone()[0]
                first=min(e['id'] for e in events if e['payload'].get('institution')=='A')
                if label_mode=='prior':
                    self.assertLess(old,first)
                    return [{'institution':'A','witness_event_id':old,'prior_nonempty_label_before_all_delta_ids':True}]
                self.assertLess(first,old);info=(s.task.root/'careers.sqlite').stat()
                return [{'institution':'A','witness_event_id':first,'delta_label_precedes_prior_winner':True,
                         'predecessor_manifest_sha256':pinned,'prior_label':['Prior A','CN'],
                         'database_identity':{'bytes':info.st_size,'mtime_ns':info.st_mtime_ns},
                         'scanned_through_rowid':s.db.execute('SELECT max(rowid) FROM events').fetchone()[0],
                         'minimum_delta_id':first,'prior_label_absent_below_delta':True}]
            changes=s.db.total_changes
            result=complete_additive_result(s,delta=delta,base_scores=base,previous_directory=previous,previous_manifest_sha256=pinned,label_witnesses=witnesses())
            result['nodes']=list(result['nodes']);result['edges']=list(result['edges'])
            full=rank_global(s)
            self.assertEqual(result['coverage'],full['coverage']);self.assertEqual(result['edges'],full['edges'])
            if new_endpoint:
                self.assertEqual(set(result['sensitivity']),set(full['sensitivity']))
                for name,expected in full['sensitivity'].items():
                    actual=dict(result['sensitivity'][name]);expected=dict(expected)
                    self.assertAlmostEqual(actual.pop('probability_mass'),expected.pop('probability_mass'),places=12)
                    self.assertEqual(actual,expected)
                self.assertEqual([n['institution_id'] for n in result['nodes']],[n['institution_id'] for n in full['nodes']])
                for actual,expected in zip(result['nodes'],full['nodes'],strict=True):
                    actual=dict(actual);expected=dict(expected)
                    for score in ('network_score','network_intensity_score'):
                        self.assertAlmostEqual(actual.pop(score),expected.pop(score),places=12)
                    self.assertEqual(actual,expected)
            else:
                self.assertEqual(result['sensitivity'],full['sensitivity'])
                self.assertEqual(result['nodes'],full['nodes'])
            self.assertEqual(s.db.total_changes,changes)
            if label_mode:
                wrong=witnesses();wrong[0]['witness_event_id']='0'*64
                with self.assertRaises(ValueError):
                    complete_additive_result(s,delta=delta,base_scores=base,previous_directory=previous,previous_manifest_sha256=pinned,label_witnesses=wrong)
            bad=copy.deepcopy(delta);bad['events'][0]['payload']['role']='changed'
            with self.assertRaises(ValueError):complete_additive_result(s,delta=bad,base_scores={**base,'delta_sha256':digest(bad)},previous_directory=previous,previous_manifest_sha256=pinned)
            for field,value in [('people_delta',2),('event_delta',len(events)+1),('member_delta',2),('observed_event_delta',{'observed':1})]:
                bad=copy.deepcopy(delta);bad[field]=value
                with self.subTest(field=field),self.assertRaisesRegex(ValueError,'coverage delta'):
                    complete_additive_result(s,delta=bad,base_scores={**base,'delta_sha256':digest(bad)},previous_directory=previous,previous_manifest_sha256=pinned)
            duplicate=copy.deepcopy(delta);duplicate['recovered_member_ledger']*=2;duplicate['member_delta']=2
            with self.assertRaisesRegex(ValueError,'coverage delta'):
                complete_additive_result(s,delta=duplicate,base_scores={**base,'delta_sha256':digest(duplicate)},previous_directory=previous,previous_manifest_sha256=pinned)
            empty='0000-0001-5109-3700';empty_person=private_person(empty)
            self.assertNotEqual(empty_person,person)
            with s.member(api,empty):s.finish_member(api,empty,'c'*64)
            with_empty=copy.deepcopy(delta);with_empty['people'].append(empty_person);with_empty['member_delta']=2
            with_empty['recovered_member_ledger'].append(list(s.db.execute('SELECT locator,sha256,status,reason FROM members WHERE source=? AND locator=?',(api,empty)).fetchone()))
            result=complete_additive_result(s,delta=with_empty,base_scores={**base,'delta_sha256':digest(with_empty)},previous_directory=previous,previous_manifest_sha256=pinned,label_witnesses=witnesses())
            self.assertEqual(result['coverage'],rank_global(s)['coverage'])
            with_empty['people_delta']=2
            with self.assertRaisesRegex(ValueError,'coverage delta'):
                complete_additive_result(s,delta=with_empty,base_scores={**base,'delta_sha256':digest(with_empty)},previous_directory=previous,previous_manifest_sha256=pinned)
        finally:s.close()

    def test_additive_graph_overlay_preserves_sorted_pagerank_and_baseline(self):
        from gapi.analysis_checkpoint import AdditiveGraph
        from gapi.global_analysis import _rank_graph
        s=self.store(archive([]))
        try:
            base={('b','c'):2.,('c','a'):1.};original=dict(base)
            additions={('a','b'):.6,('b','c'):.35,('z','a'):1.}
            overlay=AdditiveGraph(base,additions);additions[('a','b')]=100.
            expected={('a','b'):.6,('b','c'):2.35,('c','a'):1.,('z','a'):1.}
            self.assertEqual(list(overlay.items()),list(expected.items()))
            self.assertEqual(list(overlay.items()),list(expected.items()))
            self.assertEqual(len(overlay),4);self.assertEqual(base,original)
            nodes=['a','b','c','z','dangling']
            scores,conv=_rank_graph(s,nodes,overlay,.85,1e-12,200)
            reference,_=_rank_graph(s,nodes,expected,.85,1e-12,200)
            self.assertTrue(conv['converged'])
            for n in nodes:self.assertAlmostEqual(scores[n],reference[n],places=14)
            self.assertEqual(sorted(scores,key=lambda n:(-scores[n],n)),sorted(reference,key=lambda n:(-reference[n],n)))
            self.assertEqual(list(AdditiveGraph(base,{}).items()),list(base.items()))
            from gapi.analysis_checkpoint import PackedMap,PackedRows
            import struct
            # Real compact checkpoints retain accumulation order, not key order.
            packed=PackedMap(['a','b','c'],PackedRows(
                struct.pack('<IId',2,0,1.)+struct.pack('<IId',1,2,2.),'<IId'),'edge')
            self.assertEqual(list(AdditiveGraph(packed,{('a','b'):.6,('b','c'):.35}).items()),
                             [(('a','b'),.6),(('b','c'),2.35),(('c','a'),1.)])
            for bad in ({('a','b'):0},{('a','b'):-1},{('a','b'):True},{('a','b'):float('nan')},{'ab':1}):
                with self.assertRaises(ValueError):AdditiveGraph(base,bad)
            with self.assertRaises(ValueError):AdditiveGraph(dict(reversed(list(base.items()))),{})
        finally:s.close()

    def test_bounded_person_relations_match_selected_full_result_without_scan(self):
        from gapi.global_analysis import relation_rows
        s=self.store(archive([]))
        try:
            self.seed(s)
            expected=[row for row in relation_rows(s) if row[1]['person']=='private-person']
            self.assertTrue(expected)
            # Unrelated corrupt payload must not be read by the bounded query.
            with s.db:s.db.execute("UPDATE events SET payload='{' WHERE person='restricted-person'")
            statements=[];s.db.set_trace_callback(statements.append)
            selected=list(relation_rows(s,persons=('private-person',)))
            s.db.set_trace_callback(None)
            self.assertEqual(selected,expected)
            self.assertTrue(any('e.person IN (' in sql for sql in statements))
            self.assertFalse(list(relation_rows(s,persons=('not-present',))))
            self.assertFalse(list(relation_rows(s,allowed_sources={'orcid-public-api'},persons=('private-person',))))
            self.assertEqual(list(relation_rows(s,persons=('private-person',))),selected)
            for value in ([],['private-person']*2,[''],[None],'private-person',list(map(str,range(1001)))):
                with self.assertRaises(ValueError):list(relation_rows(s,persons=value))
            with self.assertRaises(ValueError):list(relation_rows(s,persons=('private-person',),after_person='a'))
        finally:s.close()

    def test_streamed_intensity_matches_reference_with_dangling_nodes(self):
        from collections import Counter
        from unittest.mock import patch
        from gapi.global_analysis import _rank_intensity
        from gapi.ranking import pagerank,pagerank_sparse
        from gapi.analysis_checkpoint import PackedMap,PackedRows
        import struct
        s=self.store(archive([]))
        try:
            nodes=['a','b','c'];edges={('a','b'):2.,('a','c'):.5,('b','a'):1.}
            counts=Counter({'a':3,'b':5})
            expected,_=pagerank(nodes,{pair:w/max(1,counts[pair[1]]) for pair,w in edges.items()},.85,1e-12,200)
            previous,_=pagerank_sparse(nodes,((a,b,w/max(1,counts[b])) for (a,b),w in sorted(edges.items())),.85,1e-12,200)
            packed=PackedMap(nodes,PackedRows(b''.join(struct.pack('<IId',nodes.index(a),nodes.index(b),w)
                for (a,b),w in sorted(edges.items())),'<IId'),'edge')
            with patch('gapi.global_analysis.PACKED_GRAPH_THRESHOLD',0):
                for value in (edges,packed):
                    actual,conv=_rank_intensity(s,nodes,value,counts)
                    self.assertTrue(conv['converged'])
                    self.assertAlmostEqual(sum(actual.values()),1,places=12)
                    for node in nodes:self.assertAlmostEqual(actual[node],expected[node],places=12)
                    self.assertEqual(actual,previous)
                    self.assertEqual(sorted(actual,key=lambda n:(-actual[n],n)),sorted(expected,key=lambda n:(-expected[n],n)))
        finally:s.close()

    def test_analysis_progress_reports_phases_without_changing_result(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            expected=rank_global(s)
            changes=s.db.total_changes;messages=[]
            actual=rank_global(s,progress=messages.append)
            self.assertEqual(actual,expected)
            self.assertEqual(s.db.total_changes,changes)
            phases=[item['phase'] for item in messages]
            for phase in ('relations','base_pagerank','institution_labels','institution_directory',
                          'sensitivity','training_denominators','intensity_pagerank',
                          'coverage_counts','evidence_metadata','aggregate_ready'):
                self.assertIn(phase,phases)
            self.assertEqual(phases[-1],'aggregate_ready')
            self.assertTrue(all(item['elapsed_seconds']>=0 for item in messages))
            self.assertTrue(all(set(item)<= {'phase','elapsed_seconds','resumed_complete',
                'nodes','edges','relations','scanned_rows','institutions','scenario','source'} for item in messages))
        finally:s.close()

    def test_graph_checkpoint_resume_skips_committed_people_and_matches_full_result(self):
        from unittest.mock import patch
        from gapi.analysis_checkpoint import GraphCheckpoint
        s=self.store(archive([]))
        try:
            self.seed(s)
            rows=[json.loads(row[0]) for row in s.db.execute("SELECT payload FROM events WHERE source='wikidata'")]
            with s.db:
                for pid in ('a','z'):
                    for row in rows:s.event(dict(row,person=pid))
            expected=rank_global(s)
            changes=s.db.total_changes
            original=GraphCheckpoint.boundary
            def interrupt(checkpoint,person,state):
                original(checkpoint,person,state)
                raise RuntimeError('interrupted after committed person')
            with patch('gapi.analysis_checkpoint.CHECKPOINT_SECONDS',0),patch.object(GraphCheckpoint,'boundary',interrupt):
                with self.assertRaisesRegex(RuntimeError,'committed person'):
                    rank_global(s,checkpoint=True)
            saved=GraphCheckpoint(s,None);state=saved.load()
            self.assertEqual(saved.after_person,'a')
            self.assertEqual(state['relation_count'],1)
            queries=[];s.db.set_trace_callback(queries.append)
            actual=rank_global(s,checkpoint=True)
            s.db.set_trace_callback(None)
            self.assertEqual(actual,expected)
            self.assertTrue(any("e.person>'a'" in sql for sql in queries))
            # Later sensitivity passes deliberately evaluate their own sources;
            # the first pass itself resumed strictly after a, as SQL seek proves.
            self.assertTrue(GraphCheckpoint(s,None).load())
            self.assertEqual(s.db.total_changes,changes)
            self.assertEqual(rank_global(s,checkpoint=True),expected)
        finally:s.close()

    def test_completed_compact_checkpoint_matches_full_result_without_first_pass(self):
        from unittest.mock import patch
        from gapi.analysis_checkpoint import GraphCheckpoint
        from gapi.global_analysis import relation_rows
        s=self.store(archive([]))
        try:
            self.seed(s)
            expected=rank_global(s,checkpoint=True)
            cp=GraphCheckpoint(s,None);state=cp.load()
            original=cp.path.read_bytes();changes=s.db.total_changes
            target=cp.write_compact(state);first=target.read_bytes()
            self.assertEqual(cp.write_compact(state).read_bytes(),first)
            calls=[]
            def observe(*args,**kwargs):
                calls.append(kwargs)
                self.assertIsNotNone(kwargs.get('allowed_sources'))
                return relation_rows(*args,**kwargs)
            with patch('gapi.global_analysis.relation_rows',side_effect=observe):
                actual=rank_global(s,checkpoint=True)
            self.assertEqual(actual,expected)
            self.assertEqual(cp.path.read_bytes(),original)
            self.assertEqual(s.db.total_changes,changes)
            with patch('gapi.global_analysis._write_aggregate',side_effect=OSError('injected')):
                with self.assertRaises(OSError):cp.write_compact(state)
            self.assertEqual(target.read_bytes(),first)
            with patch('gapi.analysis_checkpoint.input_binding',return_value='drift'):
                with self.assertRaisesRegex(ValueError,'matching input'):cp.write_compact(state)
        finally:s.close()

    def test_graph_checkpoint_interruption_inside_person_reprocesses_only_unsaved_group(self):
        from unittest.mock import patch
        from gapi.global_analysis import observed_relations
        from gapi.analysis_checkpoint import GraphCheckpoint
        s=self.store(archive([]))
        try:
            self.seed(s)
            rows=[json.loads(row[0]) for row in s.db.execute("SELECT payload FROM events WHERE source='wikidata'")]
            with s.db:
                for row in rows:s.event(dict(row,person='a'))
                training=next(row for row in rows if row['kind']=='education')
                s.event(dict(training,institution='wd:Q3',stage='bachelor',year_end=1999))
            expected=rank_global(s)
            def interrupt(events):
                for row in observed_relations(events):
                    yield row
                    if events[0]['person']=='private-person':
                        raise RuntimeError('interrupted inside unsaved person')
            with patch('gapi.analysis_checkpoint.CHECKPOINT_SECONDS',0),patch(
                    'gapi.global_analysis.observed_relations',side_effect=interrupt):
                with self.assertRaisesRegex(RuntimeError,'unsaved person'):
                    rank_global(s,checkpoint=True)
            cursor=GraphCheckpoint(s,None);saved=cursor.load()
            self.assertEqual(cursor.after_person,'a')
            self.assertEqual(saved['relation_count'],1)
            self.assertEqual(rank_global(s,checkpoint=True),expected)
        finally:s.close()

    def test_empty_source_universe_requires_no_database_scan(self):
        from unittest.mock import Mock
        from gapi.global_analysis import relation_rows
        store=Mock()
        store.db.execute.side_effect=AssertionError('empty source universe scanned input')
        self.assertEqual(list(relation_rows(store,allowed_sources=set())),[])
        store.db.execute.assert_not_called()

    def test_relation_alias_lookup_keeps_issue_exclusions_and_refreshes(self):
        from gapi.global_analysis import relation_rows
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:
                s.db.executemany('INSERT INTO institution_aliases VALUES(?,?,?)',[
                    ('wd:Q1','ror:training','test'),('wd:Q2','ror:job','test')])
                s.db.execute('INSERT INTO institution_alias_issues VALUES(?,?,?,?)',
                    ('wd:Q2','test','ambiguous','{}'))
            first=list(relation_rows(s,allowed_sources={'wikidata'}))
            self.assertEqual([row[0] for row in first],[('wd:Q2','ror:training','doctorate')])
            with s.db:
                s.db.execute('DELETE FROM institution_alias_issues WHERE alias=?',('wd:Q2',))
            second=list(relation_rows(s,allowed_sources={'wikidata'}))
            self.assertEqual([row[0] for row in second],[('ror:job','ror:training','doctorate')])
            self.assertEqual(second[0][1]['institution'],'ror:training')
            self.assertEqual(second[0][2]['institution'],'ror:job')
        finally:s.close()

    def test_relation_alias_memory_stop_does_not_mutate_store(self):
        from unittest.mock import patch
        from gapi.global_analysis import relation_rows
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            self.seed(s)
            before=s.db.total_changes
            with patch.object(s.task,'check_memory',side_effect=BudgetStop('memory bound')):
                with self.assertRaisesRegex(BudgetStop,'memory bound'):
                    list(relation_rows(s))
            self.assertEqual(s.db.total_changes,before)
            self.assertFalse(s.db.in_transaction)
        finally:s.close()

    def test_relation_payload_decoding_occurs_only_after_effective_eligibility(self):
        from unittest.mock import patch
        from gapi.global_analysis import relation_rows
        from gapi.event_storage import decode_event,pack
        from gapi.career_normalization import DDL,VERSION,POLICY_SHA256
        s=self.store(archive([]))
        try:
            self.seed(s)
            rows=list(s.db.execute("SELECT id,payload FROM events WHERE source='wikidata' ORDER BY id"))
            identifier,text=rows[0]
            with s.db:
                s.db.execute("UPDATE events SET status='unresolved',payload=? WHERE id=?",(pack(text),identifier))
            with patch('gapi.event_storage.decode_event',wraps=decode_event) as decoder:
                list(relation_rows(s,allowed_sources={'wikidata'}))
            self.assertEqual(decoder.call_count,len(rows)-1)
            # A current interpretation can promote a scalar-unresolved row;
            # delayed decoding must retain that row and original bytes.
            s.db.executescript(DDL)
            value=json.loads(text)
            with s.db:
                s.db.execute('INSERT INTO event_interpretations VALUES(?,?,?,?,?,?,?)',
                    (identifier,VERSION,POLICY_SHA256,value['kind'],value['stage'],'observed',0))
            with patch('gapi.event_storage.decode_event',wraps=decode_event) as decoder:
                actual=list(relation_rows(s,allowed_sources={'wikidata'}))
            self.assertEqual(decoder.call_count,len(rows))
            self.assertEqual(len(actual),1)
        finally:s.close()

    def test_ranking_coverage_matches_report_without_full_report_call(self):
        from unittest.mock import patch
        s=self.store(archive([]))
        try:
            self.seed(s)
            expected=s.report()
            with patch.object(s,'report',side_effect=AssertionError('unbounded report')):
                actual=rank_global(s)
            self.assertEqual(actual['coverage'],{k:expected[k] for k in ('people','events','countries','event_status','member_status')})
        finally:s.close()

    def test_packed_global_route_preserves_all_scenarios_and_output(self):
        from unittest.mock import patch
        s=self.store(archive([]))
        try:
            self.seed(s)
            expected=rank_global(s)
            with patch('gapi.global_analysis.PACKED_GRAPH_THRESHOLD',0):
                actual=rank_global(s)
            def compare(a,b):
                if isinstance(a,float):
                    self.assertAlmostEqual(a,b,places=12)
                elif isinstance(a,dict):
                    self.assertEqual(a.keys(),b.keys())
                    for key in a:compare(a[key],b[key])
                elif isinstance(a,list):
                    self.assertEqual(len(a),len(b))
                    for x,y in zip(a,b):compare(x,y)
                else:self.assertEqual(a,b)
            compare(expected,actual)
            self.assertIn('region_balanced_prior',actual['sensitivity'])
            self.assertTrue(actual['convergence']['converged'])
        finally:s.close()

    def test_large_graph_route_admits_all_edges_and_honors_memory_stop(self):
        from unittest.mock import patch
        from gapi.global_analysis import _rank_graph
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            nodes=[str(i) for i in range(500)]
            edges={(a,b):1.0 for a in nodes for b in nodes[:401]}
            expected,_=__import__('gapi.ranking',fromlist=['pagerank']).pagerank(nodes,edges,.85,1e-12,200)
            actual,conv=_rank_graph(s,nodes,edges,.85,1e-12,200)
            self.assertTrue(conv['converged'])
            for node in nodes:self.assertAlmostEqual(expected[node],actual[node],places=12)
            with patch.object(s.task,'check_memory',side_effect=[1,1,BudgetStop('graph memory boundary')]):
                with self.assertRaisesRegex(BudgetStop,'graph memory boundary'):
                    _rank_graph(s,nodes,edges,.85,1e-12,200)
        finally:s.close()

    def test_crosswalk_admits_more_than_old_count_cap_within_memory(self):
        from gapi.global_analysis import resolve_institutions
        s=self.store(archive([]))
        try:
            result=resolve_institutions(s,extra_identifiers=('external:'+str(i) for i in range(200001)))
            self.assertEqual(result['resolved_identifiers'],0)
            self.assertEqual(result['unresolved_identifiers'],200001)
            self.assertEqual(result['name_similarity_merges'],0)
        finally:s.close()

    def test_crosswalk_memory_stop_bounds_generator_and_preserves_tables(self):
        from unittest.mock import patch
        from gapi.global_analysis import resolve_institutions
        from gapi.task import BudgetStop
        s=self.store(archive([]));consumed=[]
        try:
            before=list(s.db.execute('SELECT * FROM institution_aliases'))
            def identifiers():
                for i in range(10000):
                    consumed.append(i)
                    yield 'external:'+str(i)
            with patch.object(s.task,'check_memory',side_effect=[1,BudgetStop('memory pressure')]):
                with self.assertRaisesRegex(BudgetStop,'memory pressure'):
                    resolve_institutions(s,extra_identifiers=identifiers())
            self.assertEqual(len(consumed),256)
            self.assertEqual(list(s.db.execute('SELECT * FROM institution_aliases')),before)
            self.assertEqual(s.db.execute('SELECT count(*) FROM entities').fetchone()[0],0)
        finally:s.close()
    def seed(self,s):
        for source in ('wikidata','orcid-public-api'):
            s.start(source)
        def event(source,person,inst,kind,stage,start,end):
            return dict(source=source,person=person,institution=inst,kind=kind,stage=stage,year_start=start,year_end=end,locator='statement',status='observed',country='CN')
        with s.member('wikidata','statement'):
            s.event(event('wikidata','private-person','wd:Q1','education','doctorate',2000,2005))
            s.event(event('wikidata','private-person','wd:Q2','employment','academic',2010,None))
            s.finish_member('wikidata','statement','a'*64)
        with s.member('orcid-public-api','statement'):
            s.event(event('orcid-public-api','restricted-person','ringgold:123','education','master',2000,2001))
            s.event(event('orcid-public-api','restricted-person','ringgold:456','employment','academic',2015,None))
            s.finish_member('orcid-public-api','statement','b'*64)

    def test_public_filter_excludes_unapproved_source_in_all_fields(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            result=rank_global(s,public=True)
            text=json.dumps(result)
            self.assertNotIn('ringgold:',text)
            self.assertNotIn('private-person',text)
            self.assertNotIn('restricted-person',text)
            self.assertEqual(result['coverage']['events'],2)
            self.assertEqual(len(result['edges']),1)
            for node in result['nodes']:
                self.assertIsNone(node['award_score'])
                self.assertIsNone(node['composite_score'])
                self.assertIsNone(node['formal_rank'])
        finally:s.close()

    def test_archive_verification_is_year_independent_and_denominators_match(self):
        from gapi.global_analysis import relation_rows
        from gapi.career_store import verified_archive_sql
        from gapi.career_normalization import DDL,VERSION,POLICY_SHA256
        sources=('orcid-2025-summary','annual-careers-snapshot')
        for source in sources:
            self.config['sources'][source]=dict(self.config['sources']['orcid-2026-summary'])
        s=self.store(archive([]))
        try:
            self.seed(s)
            for source in sources:
                s.start(source)
                with s.member(source,'summary.xml'):
                    for kind,stage,role,inst,start,end in (
                            ('education','doctorate','PhD','wd:Q1',2000,2005),
                            ('employment','academic','Professor','wd:Q2',2010,None)):
                        s.event(dict(source=source,person='private-'+source,institution=inst,
                            kind=kind,stage=stage,role=role,year_start=start,year_end=end,
                            locator='summary.xml',status='observed',country='CN'))
                    s.finish_member(source,'summary.xml','c'*64)
            for enriched in (False,True):
                if enriched:
                    s.db.executescript(DDL)
                    with s.db:
                        row=s.db.execute("SELECT id,kind,stage,status FROM events WHERE source='wikidata' LIMIT 1").fetchone()
                        s.db.execute('INSERT INTO event_interpretations VALUES(?,?,?,?,?,?,?)',
                            (row[0],VERSION,POLICY_SHA256,*row[1:],0))
                for state,expected_denominator,expected_relations in (
                        ('provisional',1,1),('failed',1,1),('verified',3,3)):
                    with self.subTest(enriched=enriched,state=state):
                        for source in sources:s.status(source,state)
                        rows=list(relation_rows(s))
                        self.assertEqual(len(rows),expected_relations)
                        result=rank_global(s)
                        node=next(n for n in result['nodes'] if n['institution_id']=='wd:Q1')
                        self.assertEqual(node['observed_training_denominator'],expected_denominator)
                        self.assertAlmostEqual(sum(n['network_score'] or 0 for n in result['nodes']),1)
            with s.db:s.db.execute('DELETE FROM runs WHERE source=?',(sources[0],))
            predicate,params=verified_archive_sql(s.task)
            rejected=s.db.execute('SELECT count(*) FROM events e WHERE NOT '+predicate,params).fetchone()[0]
            self.assertEqual(rejected,2)
            self.assertEqual(len(list(relation_rows(s))),2)
        finally:s.close()

    def test_streamed_directory_labels_match_previous_sorted_query(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:
                s.db.execute("UPDATE events SET payload=json_set(gapi_event_text(payload),'$.institution_name',id)")
            actual=rank_global(s,public=True)
            sql='SELECT id,institution,payload,country,source FROM events NOT INDEXED WHERE institution IS NOT NULL'
            plan=[row[-1] for row in s.db.execute('EXPLAIN QUERY PLAN '+sql)]
            self.assertFalse(any('TEMP B-TREE' in step for step in plan))
            original=s.db
            class PreviousQuery:
                calls=0
                def execute(self,query,*args):
                    if query==sql:
                        self.calls+=1
                        query=sql.replace(' NOT INDEXED','')+' ORDER BY institution,id'
                    return original.execute(query,*args)
                def __getattr__(self,key):return getattr(original,key)
            s.db=PreviousQuery()
            try:
                self.assertEqual(rank_global(s,public=True),actual)
                self.assertEqual(s.db.calls,1)
            finally:s.db=original
            # Independent oracle: the first nonempty label in ID order wins,
            # while an authoritative entity label always takes precedence.
            from gapi.global_analysis import public_sources
            allowed=public_sources(s.task,None)
            expected={}
            for event_id,institution,payload,country,source in original.execute(
                    sql.replace(' NOT INDEXED','')+' ORDER BY id'):
                if source in allowed:
                    label=json.loads(payload).get('institution_name')
                    if label:expected.setdefault(institution,label)
            expected.update((row[0],row[1]) for row in original.execute('SELECT id,name FROM entities'))
            for node in actual['nodes']:
                self.assertEqual(node['name'],expected.get(node['institution_id'],node['institution_id']))
        finally:s.close()

    def test_directory_scan_rejects_corrupt_payload_for_known_institution(self):
        from unittest.mock import patch
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:
                institution=s.db.execute('SELECT institution FROM events WHERE institution IS NOT NULL LIMIT 1').fetchone()[0]
                s.db.execute('INSERT OR REPLACE INTO entities VALUES(?,?,?,?,?)',
                    (institution,'Canonical label',None,1,'{}'))
                s.db.execute('UPDATE events SET payload=? WHERE institution=?',('{',institution))
            phases=[]
            with patch('gapi.global_analysis.relation_rows',return_value=iter(())):
                with self.assertRaises(json.JSONDecodeError):
                    rank_global(s,progress=lambda row:phases.append(row['phase']))
            self.assertEqual(phases[-1],'institution_labels')
        finally:s.close()

    def test_streamed_training_denominators_equal_distinct_sql(self):
        from gapi.career_normalization import normalize_careers
        s=self.store(archive([]))
        try:
            self.seed(s)
            original=s.db;queries=[]
            base=json.loads(original.execute("SELECT payload FROM events WHERE kind='education' LIMIT 1").fetchone()[0])
            with original:
                for person in ('a','a'*64,'A'*64,'wd:Q1'):
                    s.event(dict(base,person=person))
            class Observe:
                def execute(self,query,*args):
                    if query.startswith('SELECT e.person,COALESCE(a.institution,e.institution)'):
                        queries.append((query,args))
                    return original.execute(query,*args)
                def __getattr__(self,key):return getattr(original,key)
            for interpreted in (False,True):
                if interpreted:normalize_careers(s)
                s.db=Observe()
                try:actual=rank_global(s)
                finally:s.db=original
                query,args=queries[-1]
                reference=query.replace('SELECT e.person,COALESCE(a.institution,e.institution)',
                    'SELECT COALESCE(a.institution,e.institution),count(DISTINCT e.person)').replace(
                    ' NOT INDEXED','')+' GROUP BY COALESCE(a.institution,e.institution)'
                expected=dict(original.execute(reference,*args))
                for node in actual['nodes']:
                    self.assertEqual(node['observed_training_denominator'],expected.get(node['institution_id']) or None)
                plan=[row[-1] for row in original.execute('EXPLAIN QUERY PLAN '+query,*args)]
                self.assertFalse(any('TEMP B-TREE' in row for row in plan))
            self.assertEqual(len(queries),2)
        finally:s.close()

    def test_training_denominator_sort_memory_failure_preserves_store(self):
        from unittest.mock import patch
        from gapi.global_analysis import _training_denominators
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            self.seed(s);changes=s.db.total_changes
            with patch.object(s.task,'check_memory',return_value=s.task.config['memory_bytes']):
                with self.assertRaisesRegex(BudgetStop,'denominator sort'):
                    _training_denominators(s,None,lambda *args,**kwargs:None)
            self.assertEqual(s.db.total_changes,changes)
            self.assertTrue(_training_denominators(s,None,lambda *args,**kwargs:None))
        finally:s.close()

    def test_completed_denominator_checkpoint_resume_and_failures(self):
        import gzip,hashlib
        from pathlib import Path
        from unittest.mock import patch
        from gapi.analysis_checkpoint import TrainingDenominatorCheckpoint,input_binding
        from gapi.global_analysis import _training_denominators
        from gapi.task import canonical,BudgetStop
        s=self.store(archive([]))
        try:
            self.seed(s)
            expected=rank_global(s)
            counts=_training_denominators(s,None,lambda *args,**kwargs:None)
            cp=TrainingDenominatorCheckpoint(s,None)
            original=s.task.root/'analysis-training-denominators.json.gz'
            original.write_bytes(gzip.compress(canonical({'schema':'gapi-training-denominators/1',
                'binding':input_binding(s,None),'counts':dict(counts),'source':'fixture'}),mtime=0))
            sha=hashlib.sha256(original.read_bytes()).hexdigest()
            code={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('src/gapi').glob('*.py')}
            with self.assertRaisesRegex(ValueError,'identity mismatch'):
                cp.import_completed(code,original_sha256='0'*64)
            with patch('gapi.global_analysis._write_aggregate',side_effect=BudgetStop('capacity')):
                with self.assertRaises(BudgetStop):cp.import_completed(code,original_sha256=sha)
            self.assertFalse(cp.path.exists())
            receipt=cp.import_completed(code,original_sha256=sha)
            mtime=cp.path.stat().st_mtime_ns
            self.assertEqual(cp.import_completed(code,original_sha256=sha),receipt)
            self.assertEqual(cp.path.stat().st_mtime_ns,mtime)
            with patch('gapi.global_analysis._training_denominators',side_effect=AssertionError('recomputed')):
                self.assertEqual(rank_global(s,checkpoint=True),expected)
            self.assertEqual(hashlib.sha256(original.read_bytes()).hexdigest(),sha)
            content=cp.path.read_bytes()
            def next_generation(store,allowed,*,predecessor_code=None):
                return cp.binding if predecessor_code is not None else '2'*64
            with patch('gapi.analysis_checkpoint.input_binding',side_effect=next_generation):
                advanced=TrainingDenominatorCheckpoint(s,None)
                with self.assertRaisesRegex(ValueError,'receipt mismatch'):
                    advanced.import_completed(code,original_sha256=sha)
                migrated=advanced.import_completed(code,original_sha256=sha,predecessor_receipt=receipt)
                self.assertEqual(advanced.load(),counts)
                self.assertEqual(advanced.import_completed(code,original_sha256=sha,predecessor_receipt=receipt),migrated)
            cp.path.write_bytes(content+b'trailing')
            with self.assertRaisesRegex(ValueError,'stream bound'):cp.load()
            cp.path.write_bytes(content)
            with patch('gapi.analysis_checkpoint.input_binding',return_value='drift'):
                with self.assertRaisesRegex(ValueError,'version drift'):
                    TrainingDenominatorCheckpoint(s,None).load()
            self.assertIsNone(TrainingDenominatorCheckpoint(s,set()).load())
        finally:s.close()

    def test_completed_directory_resume_and_atomic_failure(self):
        from unittest.mock import patch
        from gapi.analysis_checkpoint import InstitutionDirectoryCheckpoint
        from gapi.global_analysis import _institution_directory
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            self.seed(s);expected=rank_global(s)
            directory,ids=_institution_directory(s,None,lambda *args,**kwargs:None)
            cp=InstitutionDirectoryCheckpoint(s,None)
            with patch('gapi.global_analysis._write_aggregate',side_effect=BudgetStop('capacity')):
                with self.assertRaises(BudgetStop):cp.save(directory,ids)
            self.assertFalse(cp.path.exists())
            cp.save(directory,ids);content=cp.path.read_bytes();mtime=cp.path.stat().st_mtime_ns
            cp.save(directory,ids);self.assertEqual(cp.path.stat().st_mtime_ns,mtime)
            with patch('gapi.global_analysis._institution_directory',side_effect=AssertionError('rescanned')):
                self.assertEqual(rank_global(s,checkpoint=True),expected)
            self.assertEqual(cp.load()[1],ids)
            cp.path.write_bytes(content+b'garbage')
            with self.assertRaisesRegex(ValueError,'stream bound'):cp.load()
            cp.path.write_bytes(content)
            with patch('gapi.analysis_checkpoint.input_binding',return_value='changed'):
                with self.assertRaisesRegex(ValueError,'version drift'):
                    InstitutionDirectoryCheckpoint(s,None).load()
            self.assertIsNone(InstitutionDirectoryCheckpoint(s,set()).load())
            with patch('gapi.global_analysis._write_aggregate',side_effect=OSError('write failed')):
                with self.assertRaises(OSError):cp.save(directory,ids)
            self.assertEqual(cp.path.read_bytes(),content)
            import hashlib
            from pathlib import Path
            code={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('src/gapi').glob('*.py')}
            sha=hashlib.sha256(content).hexdigest()
            def generation(store,allowed,*,predecessor_code=None):
                return cp.binding if predecessor_code is not None else '3'*64
            with patch('gapi.analysis_checkpoint.input_binding',side_effect=generation):
                newer=InstitutionDirectoryCheckpoint(s,None)
                with self.assertRaisesRegex(ValueError,'identity mismatch'):
                    newer.migrate_completed(code,original_sha256='0'*64)
                with patch('gapi.global_analysis._write_aggregate',side_effect=OSError('failed')):
                    with self.assertRaises(OSError):newer.migrate_completed(code,original_sha256=sha)
                self.assertEqual(cp.path.read_bytes(),content)
                receipt=newer.migrate_completed(code,original_sha256=sha)
                self.assertEqual(receipt['current_binding'],'3'*64)
                self.assertEqual(newer.load()[1],ids)
                self.assertEqual(newer.load()[0],json.loads(json.dumps(directory)))
        finally:s.close()

    def test_streamed_coverage_matches_original_sql_for_source_scopes(self):
        from gapi.global_analysis import _coverage_counts
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:s.db.execute("UPDATE events SET country='' WHERE source='wikidata'")
            for allowed in (None,{'wikidata'},{'orcid-public-api'},set()):
                where='' if allowed is None else ' WHERE source IN ('+','.join('?' for _ in allowed)+')'
                params=() if allowed is None else tuple(sorted(allowed))
                expected={'people':s.db.execute('SELECT count(DISTINCT person) FROM events'+where,params).fetchone()[0],
                    'events':s.db.execute('SELECT count(*) FROM events'+where,params).fetchone()[0],
                    'countries':[r[0] for r in s.db.execute('SELECT DISTINCT country FROM events'+where+
                        (' WHERE' if allowed is None else ' AND')+' country IS NOT NULL'+
                        ('' if allowed is None else " AND country<>''")+' ORDER BY country',params)],
                    'event_status':dict(s.db.execute('SELECT status,count(*) FROM events'+where+' GROUP BY status',params)),
                    'member_status':dict(s.db.execute('SELECT status,count(*) FROM members'+where+' GROUP BY status',params))}
                changes=s.db.total_changes
                self.assertEqual(_coverage_counts(s,allowed),expected)
                self.assertEqual(s.db.total_changes,changes)
        finally:s.close()

    def test_completed_coverage_checkpoint_reuse_and_faults(self):
        from unittest.mock import patch
        from gapi.analysis_checkpoint import CoverageCheckpoint
        from gapi.global_analysis import _coverage_counts
        from gapi.task import canonical,BudgetStop
        s=self.store(archive([]))
        try:
            self.seed(s);expected=rank_global(s);events=[]
            coverage=_coverage_counts(s,None,lambda phase,**values:events.append(values))
            self.assertEqual([e['source'] for e in events],
                ['event-status-and-countries','unique-people','member-status'])
            cp=CoverageCheckpoint(s,None)
            with patch('gapi.global_analysis._write_aggregate',side_effect=BudgetStop('capacity')):
                with self.assertRaises(BudgetStop):cp.save(coverage)
            self.assertIsNone(cp.load());cp.save(coverage)
            original=cp.path.read_bytes();mtime=cp.path.stat().st_mtime_ns
            cp.save(coverage);self.assertEqual(mtime,cp.path.stat().st_mtime_ns)
            with patch('gapi.global_analysis._coverage_counts',side_effect=AssertionError('recounted')):
                self.assertEqual(rank_global(s,checkpoint=True),expected)
            with self.assertRaisesRegex(ValueError,'reconciliation'):
                cp.save(dict(coverage,events=coverage['events']+1))
            self.assertEqual(cp.path.read_bytes(),original)
            bad=json.loads(original);bad['binding']='changed';cp.path.write_bytes(canonical(bad))
            with self.assertRaisesRegex(ValueError,'version drift'):cp.load()
            cp.path.write_bytes(original+b'{}')
            with self.assertRaises(ValueError):cp.load()
            self.assertIsNone(CoverageCheckpoint(s,set()).load())
        finally:s.close()

    def test_versioned_noncommercial_publication_policy_allows_only_aggregate_fields(self):
        from pathlib import Path
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:
                s.db.execute('''UPDATE events SET role=CASE WHEN kind='education' THEN 'Master' ELSE 'Professor' END,
                    payload=json_set(gapi_event_text(payload),'$.role',CASE WHEN kind='education' THEN 'Master' ELSE 'Professor' END)
                    WHERE source='orcid-public-api' ''')
            policy=json.loads(Path('configs/publication-20261029.json').read_text())
            result=rank_global(s,public=True,publication_policy=policy)
            self.assertEqual(len(result['edges']),2)
            text=json.dumps(result)
            self.assertNotIn('restricted-person',text)
            self.assertNotIn('private-person',text)
            self.assertNotIn('put_code',text)
            self.assertIsNone(result['nodes'][0]['formal_rank'])
            policy['purpose']='revenue-generating'
            with self.assertRaises(ValueError):rank_global(s,public=True,publication_policy=policy)
        finally:s.close()

    def test_repeat_output_is_byte_identical_and_probabilities_conserve(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            a=export_local(s)
            first=(s.task.root/'global-local.json').read_bytes()
            paths=[s.task.root/'global-local.json',s.task.root/'global-local.json.manifest.json']
            mtimes=[path.stat().st_mtime_ns for path in paths]
            b=export_local(s)
            self.assertEqual(first,(s.task.root/'global-local.json').read_bytes())
            self.assertEqual(mtimes,[path.stat().st_mtime_ns for path in paths])
            self.assertEqual(a['manifest'],b['manifest'])
            result=json.loads(first)
            self.assertAlmostEqual(sum(n['network_score'] or 0 for n in result['nodes']),1)
            self.assertTrue(result['sensitivity']['delete_source_wikidata']['converged'])
        finally:s.close()

    def test_compressed_local_export_preserves_decoded_identity_and_is_idempotent(self):
        import gzip
        import hashlib
        s=self.store(archive([]))
        try:
            self.seed(s)
            plain=export_local(s)
            expected=(s.task.root/'global-local.json').read_bytes()
            packed=export_local(s,compress_snapshot=True)
            metadata=packed['manifest']['repository_snapshot']
            path=s.task.root/metadata['filename']
            content=path.read_bytes(); stamp=path.stat().st_mtime_ns
            self.assertEqual(gzip.decompress(content),expected)
            self.assertEqual(hashlib.sha256(content).hexdigest(),metadata['sha256'])
            self.assertEqual(len(content),metadata['bytes'])
            self.assertEqual(len(expected),metadata['decoded_bytes'])
            self.assertEqual(plain['manifest']['output_sha256'],packed['manifest']['output_sha256'])
            self.assertEqual(packed,export_local(s,compress_snapshot=True))
            self.assertEqual(stamp,path.stat().st_mtime_ns)
            self.assertEqual((s.task.root/'global-local.json').read_bytes(),expected)
        finally:s.close()

    def test_aggregate_publication_preserves_predecessor_on_failure_and_foreign_staging(self):
        from unittest.mock import patch
        from gapi.global_analysis import _write_aggregate
        s=self.store(archive([]))
        try:
            path=s.task.root/'aggregate.json'; staging=path.with_name(path.name+'.tmp')
            _write_aggregate(s.task,path,b'old')
            with patch('gapi.global_analysis.os.replace',side_effect=OSError('publication interrupted')):
                with self.assertRaises(OSError):_write_aggregate(s.task,path,b'new')
            self.assertEqual(path.read_bytes(),b'old')
            self.assertFalse(staging.exists())
            staging.write_bytes(b'predecessor recovery')
            with self.assertRaises(FileExistsError):_write_aggregate(s.task,path,b'new')
            self.assertEqual(staging.read_bytes(),b'predecessor recovery')
            self.assertEqual(path.read_bytes(),b'old')
            staging.unlink()
            _write_aggregate(s.task,path,b'new')
            self.assertEqual(path.read_bytes(),b'new')
            with patch.object(s.task,'check',side_effect=RuntimeError('budget')):
                with self.assertRaises(RuntimeError):_write_aggregate(s.task,path,b'changed')
            self.assertEqual(path.read_bytes(),b'new')
            self.assertFalse(staging.exists())
        finally:s.close()

    def test_compressed_public_snapshot_is_lossless_idempotent_and_capacity_bounded(self):
        import gzip
        import hashlib
        from gapi.global_analysis import _write_public_snapshot
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            content=b'{"institution":"'+b'x'*100000+b'"}'
            used=s.task.used()
            s.task.config['budget_bytes']=used+s.task.config['reserve_bytes']+96*1024
            with self.assertRaises(BudgetStop):
                _write_public_snapshot(s.task,s.task.root,'global-test.json',content)
            result=_write_public_snapshot(s.task,s.task.root,'global-test.json',content,compress=True)
            path=s.task.root/result['filename'];packed=path.read_bytes();mtime=path.stat().st_mtime_ns
            self.assertFalse((s.task.root/'global-test.json').exists())
            self.assertEqual(gzip.decompress(packed),content)
            self.assertEqual(result['sha256'],hashlib.sha256(packed).hexdigest())
            self.assertEqual(result['decoded_bytes'],len(content))
            self.assertEqual(_write_public_snapshot(s.task,s.task.root,'global-test.json',content,compress=True),result)
            self.assertEqual(path.stat().st_mtime_ns,mtime)
            self.assertEqual(path.read_bytes(),packed)
        finally:s.close()

    def test_readonly_score_needs_memory_but_not_fictitious_disk_workspace(self):
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            self.seed(s)
            before=s.task.used()
            s.task.config['budget_bytes']=before+s.task.config['reserve_bytes']+8192
            result=rank_global(s,public=True)
            self.assertTrue(result['convergence']['converged'])
            self.assertEqual(s.task.used(),before)
            # Actual publication still enforces its byte-dependent admission.
            with self.assertRaises(BudgetStop):export_local(s)
            self.assertFalse((s.task.root/'global-local.json').exists())
            with s.task.root.joinpath('budget-violation').open('wb') as f:
                f.write(b'x'*8193)
            with self.assertRaises(BudgetStop):rank_global(s,public=True)
        finally:s.close()

    def test_atomic_export_admits_real_staging_and_rejects_insufficient_space(self):
        from gapi.global_analysis import _write_aggregate
        from gapi.task import BudgetStop
        s=self.store(archive([]))
        try:
            content=b'x'*1024**2; path=s.task.root/'sized-aggregate.json'
            # Fits the atomic writer's 2x byte admission, but not the retired 4x.
            s.task.config['budget_bytes']=s.task.used()+s.task.config['reserve_bytes']+3*len(content)
            _write_aggregate(s.task,path,content)
            self.assertEqual(path.read_bytes(),content)
            s.task.config['budget_bytes']=s.task.used()+s.task.config['reserve_bytes']+len(content)
            with self.assertRaises(BudgetStop):_write_aggregate(s.task,path,b'y'*len(content))
            self.assertEqual(path.read_bytes(),content)
        finally:s.close()

    def test_source_deletion_rejoins_surviving_alternative_evidence(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            # The same person's doctorate has a second source, which must survive
            # removal of the first source before the person-level join/dedup.
            with s.member('orcid-public-api','alternative'):
                s.event(dict(source='orcid-public-api',person='private-person',institution='wd:Q1',
                    kind='education',stage='doctorate',role='PhD',year_start=2000,year_end=2005,
                    locator='alternative',status='observed',country='CN'))
                s.event(dict(source='orcid-public-api',person='private-person',institution='wd:Q2',
                    kind='employment',stage='academic',role='Professor',year_start=2010,year_end=None,
                    locator='alternative',status='observed',country='CN'))
                s.finish_member('orcid-public-api','alternative','c'*64)
            result=rank_global(s)
            remaining=result['sensitivity']['delete_source_wikidata']
            self.assertEqual(remaining['edges'],1)
            self.assertFalse(remaining['prior_only'])
            self.assertEqual(result['sensitivity']['delete_source_orcid-public-api']['edges'],1)
        finally:s.close()

    def seed_conflicting_jobs(self, store, *, include_reference=True):
        for source in ('wikidata','orcid-public-api'):
            store.start(source)
        facts = [
            ('orcid-public-api','conflicted-person','X','education','doctorate',1990,1995,'PhD'),
            ('wikidata','conflicted-person','Y','employment','academic',2000,None,'Professor'),
            ('orcid-public-api','conflicted-person','Z','employment','academic',2000,None,'Professor'),
        ]
        if include_reference:
            facts += [
                ('orcid-public-api','reference-person','V','education','doctorate',1990,1995,'PhD'),
                ('orcid-public-api','reference-person','W','employment','academic',2000,None,'Professor'),
            ]
        for source in ('wikidata','orcid-public-api'):
            with store.member(source,'conflict-fixture'):
                for fact_source,person,institution,kind,stage,start,end,role in facts:
                    if fact_source != source:continue
                    store.event(dict(source=source,person=person,institution=institution,kind=kind,
                        stage=stage,year_start=start,year_end=end,role=role,locator='conflict-fixture',
                        status='observed',country='CN'))
                store.finish_member(source,'conflict-fixture','d'*64)

    def test_relation_evidence_is_independent_of_event_id_order(self):
        s=self.store(archive([]))
        try:
            for source in ('wikidata','orcid-public-api'):
                s.start(source)
                with s.member(source,'same-facts'):
                    for inst,kind,stage,start,end,role in (
                        ('X','education','doctorate',2000,2005,'PhD'),
                        ('Y','employment','academic',2010,None,'Professor')):
                        s.event(dict(source=source,person='same-person',institution=inst,kind=kind,
                            stage=stage,year_start=start,year_end=end,role=role,locator='same-facts',
                            status='corroborated' if source=='orcid-public-api' else 'observed'))
                    s.finish_member(source,'same-facts','a'*64)
            original=rank_global(s)
            node=next(n for n in original['nodes'] if n['institution_id']=='X')
            self.assertEqual(node['relations'],1)
            self.assertEqual(node['corroborated_relations'],1)
            self.assertEqual(node['source_classes'],['orcid-public-api','wikidata'])
            # Mutate only ordering keys, never facts or source records.
            with s.db:
                s.db.execute("UPDATE events SET id='z-'||id WHERE source='wikidata'")
            self.assertEqual(original,rank_global(s))
        finally:s.close()

    def test_source_deletion_reports_filtered_edges_and_expanded_universe(self):
        s=self.store(archive([]))
        try:
            self.seed_conflicting_jobs(s)
            result=rank_global(s)
            self.assertEqual(result['algorithm']['network_universe'],['V','W'])
            self.assertEqual(result['algorithm']['method_version'],'gapi-observed-network/4')
            fixed=result['sensitivity']['delete_source_wikidata']
            self.assertEqual((fixed['available_edges'],fixed['edges'],fixed['filtered_boundary_edges']),(2,1,1))
            self.assertEqual(fixed['new_endpoints'],['X','Z'])
            self.assertEqual(set(fixed['ranks']),{'V','W'})
            self.assertAlmostEqual(fixed['probability_mass'],1,places=12)
            expanded=result['expanded_universe_sensitivity']['delete_source_wikidata']
            self.assertEqual((expanded['available_edges'],expanded['edges'],expanded['filtered_boundary_edges']),(2,2,0))
            self.assertEqual(set(expanded['ranks']),{'V','W','X','Z'})
            self.assertAlmostEqual(expanded['probability_mass'],1,places=12)
            self.assertEqual(expanded['universe_kind'],'reference-plus-new-endpoints')
            self.assertIn('fixed-reference-universe',result['uncertainty']['rank_range_scope'])
            by_id={node['institution_id']:node for node in result['nodes']}
            self.assertIsNone(by_id['X']['rank_range'])
            for node in ('V','W'):
                fixed_ranks=[scenario['ranks'][node] for scenario in result['sensitivity'].values()
                             if not scenario['prior_only']]
                self.assertEqual(by_id[node]['rank_range'],[min(fixed_ranks),max(fixed_ranks)])
            self.assertEqual(result,rank_global(s))
        finally:s.close()

    def test_boundary_only_scenario_does_not_claim_fixed_universe_evidence(self):
        s=self.store(archive([]))
        try:
            self.seed_conflicting_jobs(s,include_reference=False)
            result=rank_global(s)
            self.assertEqual(result['algorithm']['network_universe'],[])
            fixed=result['sensitivity']['delete_source_wikidata']
            self.assertEqual((fixed['available_edges'],fixed['edges'],fixed['filtered_boundary_edges']),(1,0,1))
            self.assertTrue(fixed['prior_only'])
            self.assertEqual(fixed['probability_mass'],0)
            expanded=result['expanded_universe_sensitivity']['delete_source_wikidata']
            self.assertFalse(expanded['prior_only'])
            self.assertEqual(expanded['new_endpoints'],['X','Z'])
            self.assertAlmostEqual(expanded['probability_mass'],1,places=12)
            self.assertGreater(expanded['ranks']['Z'],expanded['ranks']['X'])
        finally:s.close()

    def test_process_exit_releases_writer_and_rolls_back_current_member(self):
        s=self.store(archive([]));s.close()
        code='''import os,sys
from pathlib import Path
from gapi.task import Task
from gapi.career_store import CareerStore
s=CareerStore(Task(Path(sys.argv[1])))
s.start('wikidata')
s.db.execute('BEGIN IMMEDIATE')
s.db.execute("INSERT INTO members VALUES('wikidata','uncommitted','dead','pending',NULL)")
os._exit(19)
'''
        import os
        env=dict(os.environ,PYTHONPATH=str(__import__('pathlib').Path('src').resolve()))
        child=subprocess.run([sys.executable,'-c',code,str(self.path)],env=env,timeout=10,capture_output=True)
        self.assertEqual(child.returncode,19,child.stderr)
        from gapi.career_store import CareerStore
        from gapi.task import Task
        s=CareerStore(Task(self.path))
        try:
            self.assertIsNone(s.seen('wikidata','uncommitted'))
            self.assertEqual(s.db.execute('PRAGMA quick_check').fetchone()[0],'ok')
        finally:s.close()

    def test_strict_api_uses_xml_incremental_reads_without_person_tree(self):
        class Response(io.BytesIO):
            headers={'Content-Type':'application/vnd.orcid+xml'}
            def read(self,n=-1):
                self_limit=65536
                if n<0 or n>self_limit: raise AssertionError('unbounded read')
                return super().read(n)
        class HTTP:
            def open(self,url,accept):
                if accept!='application/vnd.orcid+xml': raise AssertionError('must use XML')
                section=url.rsplit('/',1)[-1]
                if section not in ('educations','employments'):raise AssertionError('irrelevant person/works data requested')
                body=XML.replace(b'<record path="/'+PERSON.encode()+b'"',
                                 b'<educations path="/'+PERSON.encode()+b'/educations"').replace(b'</record>',b'</educations>')
                return Response(body if section=='educations' else b'<employments path="/'+PERSON.encode()+b'/employments"/>')
        s=self.store(archive([]))
        try:
            result=ingest_orcid_api(s,HTTP(),[PERSON])
            self.assertEqual(result['events'],1)
            payload=json.loads(s.db.execute('SELECT payload FROM events').fetchone()[0])
            self.assertEqual(payload['transport'],'incremental-affiliation-xml/v1')
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],2)
        finally:s.close()

    def test_candidate_and_honorary_degrees_are_not_promoted(self):
        for value in ('PhD candidate','honorary PhD','PhD student','在读博士'):
            self.assertIsNone(classify('education',value)[1])

    def test_second_affiliation_failure_rolls_back_all_events_and_parts(self):
        class Response(io.BytesIO):
            headers={'Content-Type':'application/vnd.orcid+xml'}
        class HTTP:
            def open(self,url,accept):
                if url.endswith('/employments'):raise OSError('truncated second section')
                return Response(XML.replace(b'<record path="/'+PERSON.encode()+b'"',
                    b'<educations path="/'+PERSON.encode()+b'/educations"').replace(b'</record>',b'</educations>'))
        s=self.store(archive([]))
        try:
            ingest_orcid_api(s,HTTP(),[PERSON])
            self.assertEqual(s.db.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(s.db.execute('SELECT count(*) FROM member_parts').fetchone()[0],0)
            self.assertIsNone(s.seen('orcid-public-api',PERSON))
            self.assertEqual(s.db.execute('SELECT attempts FROM discovery_retries').fetchone()[0],1)
        finally:s.close()

    def test_affiliation_identity_and_wrong_section_fail_closed(self):
        from gapi.orcid_stream import SummaryParser
        for body in (b'<educations path="/different/educations"/>',
                     b'<educations path="/'+PERSON.encode()+b'/educations"><employment-summary/></educations>'):
            parser=SummaryParser('private','source',PERSON,lambda event:None,section='educations')
            with self.subTest(body=body),self.assertRaises(ValueError):
                parser.feed(body,True)

    def test_access_denial_pauses_one_source_without_requesting_other_people(self):
        from urllib.error import HTTPError
        class HTTP:
            calls=0
            def open(self,url,accept):
                self.calls+=1
                raise HTTPError(url,403,'Forbidden',{},None)
        s=self.store(archive([]))
        try:
            http=HTTP()
            ingest_orcid_api(s,http,[PERSON,'0000-0001-5109-3700'])
            self.assertEqual(http.calls,1)
            self.assertEqual(s.db.execute("SELECT status FROM runs WHERE source='orcid-public-api'").fetchone()[0],'access_failed')
        finally:s.close()

    def test_server_pause_survives_reopen_and_prevents_all_source_requests(self):
        from gapi.career_store import CareerStore
        from gapi.task import Task
        from gapi.source_http import SourcePause
        import time
        class HTTP:
            calls=0
            def open(self,url,accept):
                self.calls+=1
                raise SourcePause(429,time.time()+120,'server requested two minutes')
        s=self.store(archive([]))
        http=HTTP()
        ingest_orcid_api(s,http,[PERSON,'0000-0001-5109-3700'])
        self.assertEqual(http.calls,1)
        self.assertEqual(s.db.execute('SELECT count(*) FROM discovery_retries').fetchone()[0],0)
        self.assertEqual(s.db.execute('SELECT count(*) FROM failures').fetchone()[0],0)
        s.close()
        s=CareerStore(Task(self.path))
        try:
            ingest_orcid_api(s,http,[PERSON])
            self.assertEqual(http.calls,1)
            self.assertFalse(s.source_ready('orcid-public-api'))
        finally:s.close()

    def test_intensity_denominator_resolves_aliases_and_deduplicates_people(self):
        s=self.store(archive([]))
        try:
            self.seed(s)
            with s.db:
                s.db.executemany('INSERT INTO institution_aliases VALUES(?,?,?)',[
                    ('wd:Q1','ror:example','test'),('ringgold:123','ror:example','test')])
            result=rank_global(s)
            node=next(n for n in result['nodes'] if n['institution_id']=='ror:example')
            self.assertEqual(node['observed_training_denominator'],2)
            public=rank_global(s,public=True)
            node=next(n for n in public['nodes'] if n['institution_id']=='ror:example')
            self.assertEqual(node['observed_training_denominator'],1)
        finally:s.close()


