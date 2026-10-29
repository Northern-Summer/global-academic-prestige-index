import test from 'node:test';
import assert from 'node:assert/strict';
import {csv,networkSlice,selectRows,validateGlobal} from './global-data.js';

const node = (id,score,country='CN') => ({institution_id:id,name:id,country,network_score:score,award_score:null,composite_score:null,relations:1,unranked_reasons:['independent_review_pending']});
const dataset = () => ({schema_version:'gapi-global-observed/v1',dataset_kind:'real/global-public-observations',nodes:[node('a',.7),node('b',.3,'DE'),node('missing',null)],edges:[{source:'b',target:'a',weight:2,stages:{doctorate:2}}]});

test('real global schema preserves missing scores and rejects synthetic or malformed edges',()=>{
  assert.equal(validateGlobal(dataset()).nodes[2].network_score,null);
  assert.throws(()=>validateGlobal({...dataset(),dataset_kind:'synthetic/demo'}));
  assert.throws(()=>validateGlobal({...dataset(),edges:[{source:'outside',target:'a',weight:1}]}));
});
test('global table search, country, sort and scenario ordering are deterministic',()=>{
  assert.deepEqual(selectRows(dataset(),{country:'DE'}).map(n=>n.institution_id),['b']);
  assert.deepEqual(selectRows(dataset(),{query:'miss'}).map(n=>n.institution_id),['missing']);
  assert.deepEqual(selectRows(dataset()).map(n=>n.institution_id),['a','b','missing']);
});
test('network retains employer to training direction and enforces rendering caps',()=>{
  assert.deepEqual(networkSlice(dataset()).edges.map(e=>[e.source,e.target]),[['b','a']]);
  assert.equal(networkSlice(dataset(),{stage:'postdoc'}).edges.length,0);
  const data={nodes:Array.from({length:1000},(_,i)=>node(String(i),.001)),edges:Array.from({length:999},(_,i)=>({source:String(i),target:String(i+1),weight:1,stages:{doctorate:1}}))};
  assert.ok(networkSlice(data).nodes.length<=500);
  assert.ok(networkSlice(data).edges.length<=2000);
});
test('institution CSV neutralizes formulas and preserves missing values',()=>{
  const data=csv([{...node('=HYPERLINK("bad")',null),name:'\t=1+1'}]);
  assert.match(data,/'=HYPERLINK/);
  assert.match(data,/'\t=1\+1/);
  assert.doesNotMatch(data,/undefined|null/);
});
