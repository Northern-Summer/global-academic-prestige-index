import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {readDerivedAsset} from '../scripts/derived-bundle.mjs';
import {validateDerivedManifest,decodeNodes,decodeEdges,validateAsset,fetchJSON} from './derived-data.js';
const path=new URL('../outputs/easy-orcid-web/manifest.json',import.meta.url);
const manifest=JSON.parse(readFileSync(path));
test('actual complete source decodes with conserved mass and bounded chunks',()=>{
  const m=validateDerivedManifest(manifest),base=new URL('../outputs/easy-orcid-web/',import.meta.url);
  const raw=readDerivedAsset(fileURLToPath(new URL(m.nodes.file,base)),m.nodes.bytes).bytes;
  const nodes=decodeNodes(JSON.parse(raw),m);
  assert.equal(nodes.length,62103);
  assert.equal(nodes.find(n=>n.name==='University of Cambridge').region,'Europe');
  assert.ok(new Set(nodes.map(n=>n.region).filter(Boolean)).size>=6);
  assert.ok(nodes.filter(n=>n.identity==='unresolved_external_identifier').every(n=>n.region===null));
  const corrupt=JSON.parse(raw);corrupt[0][9]='invented';
  assert.throws(()=>decodeNodes(corrupt,m),/Invalid historical institution/);
  const chunk=m.edge_chunks[0];
  const edges=decodeEdges(JSON.parse(readDerivedAsset(fileURLToPath(new URL(chunk.file,base)),chunk.bytes).bytes),nodes);
  assert.equal(edges.length,10000);assert.ok(edges.every(e=>e.weight>0));
});
test('partial, formal, oversized and traversing assets fail closed',()=>{
  for(const changed of [{global_complete:true},{formal_ranking:true},{node_count:200001},{edge_count:1},
    {source_census:{source_complete:false}},{probability_mass:.9}])assert.throws(()=>validateDerivedManifest({...manifest,...changed}));
  assert.throws(()=>validateAsset({...manifest.nodes,file:'../private.json'}));
  assert.throws(()=>validateAsset({...manifest.nodes,bytes:33*1024*1024},32*1024*1024));
});
test('bad endpoints and negative weights do not reach rendering',()=>{
  assert.throws(()=>decodeEdges([[0,2,0,0,1,0]],[{institution_id:'a'}]));
  assert.throws(()=>decodeEdges([[0,0,-1,0,1,0]],[{institution_id:'a'}]));
});
test('oversized response is cancelled while reading',async()=>{
  const original=globalThis.fetch;let cancelled=false;
  globalThis.fetch=async()=>({ok:true,body:new ReadableStream({start(c){c.enqueue(new Uint8Array(10));},cancel(){cancelled=true;}})});
  try{await assert.rejects(fetchJSON('fixture',5),/byte budget/);assert.equal(cancelled,true);}finally{globalThis.fetch=original;}
});
