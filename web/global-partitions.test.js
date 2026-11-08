import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {gzipSync} from 'node:zlib';
import {decodePartitionRows,fetchPartitionAsset,loadPartitionedGlobal,validatePartitionManifest} from './global-partitions.js';
import {validateGlobal} from './global-data.js';

const hash=b=>createHash('sha256').update(b).digest('hex');
function fixture(){
  const files=new Map(),fields=[];
  function asset(value,rows){
    const raw=Buffer.from(JSON.stringify(value)),packed=gzipSync(raw),sha=hash(raw),file=`${sha}.json.gz`;files.set(file,packed);
    return {file,sha256:sha,stored_sha256:hash(packed),bytes:raw.length,stored_bytes:packed.length,encoding:'gzip',...(rows===undefined?{}:{rows})};
  }
  function nodes(id,rank){return {institution_id:id,name:`University ${id}`,network_score:.5,award_score:null,composite_score:null,
    relations:1,corroborated_relations:0,traceability:1,observed_rank:rank,formal_rank:null,source_classes:['orcid-2026-summary'],unranked_reasons:[],rank_range:[rank,rank]};}
  const data={algorithm:{network_universe:['a','b'],config:{alpha:.85}},coverage:{people:2,events:2},dataset_kind:'real/global-public-observations',
    edges:[{source:'a',target:'b',weight:1,stages:{doctorate:1}}],nodes:[nodes('a',1),nodes('b',2)],
    publication_status:'local-only-source-license-review',schema_version:'gapi-global-observed/v1',sensitivity:{base:{ranks:{a:1,b:2},prior_only:false}}};
  for(const name of Object.keys(data).sort()){
    if(['nodes','edges'].includes(name)){
      const columns=Object.keys(data[name][0]).sort();
      const rows=data[name].map(r=>columns.map(k=>name==='edges'&&['source','target'].includes(k)?data.algorithm.network_universe.indexOf(r[k]):r[k]));
      const value={schema_version:'gapi-aggregate-row-columns/1',columns,rows};
      if(name==='edges')value.references={source:'algorithm.network_universe',target:'algorithm.network_universe'};
      fields.push({field:name,kind:'rows',rows:rows.length,assets:[asset(value,rows.length)]});
    }else if(name==='sensitivity')fields.push({field:name,kind:'object',entries:Object.keys(data[name]).sort().map(key=>({key,asset:asset(data[name][key])}))});
    else fields.push({field:name,kind:'value',asset:asset(data[name])});
  }
  const assets=fields.flatMap(f=>f.kind==='rows'?f.assets:f.kind==='object'?f.entries.map(e=>e.asset):[f.asset]);
  const manifest={schema_version:'gapi-aggregate-partitions/1',logical_schema_version:'gapi-global-observed/v1',publication_status:data.publication_status,
    logical_sha256:'a'.repeat(64),source_manifest_sha256:'b'.repeat(64),logical_bytes:JSON.stringify(data).length,nodes:2,edges:1,
    asset_count:assets.length,stored_bytes:assets.reduce((n,a)=>n+a.stored_bytes,0),fields};
  const requests=[],fetcher=async(url)=>{const file=new URL(url).pathname.split('/').at(-1);requests.push(file);return new Response(files.get(file)??'',{status:files.has(file)?200:404});};
  return {files,manifest,data,fetcher,requests};
}

test('partition reader preserves full rows, references, nulls, scenarios and progress',async()=>{
  const f=fixture(),progress=[];const data=await loadPartitionedGlobal(f.manifest,'https://example.test/',{fetcher:f.fetcher,onProgress:v=>progress.push(v)});
  assert.deepEqual(data,f.data);assert.equal(f.requests.length,f.manifest.asset_count);
  assert.equal(data.nodes[0].source_classes,data.nodes[1].source_classes);
  assert.ok(Object.isFrozen(data.nodes[0].source_classes));
  assert.throws(()=>data.nodes[0].source_classes.push('invented-source'));
  assert.equal(progress.at(-1).loadedAssets,f.manifest.asset_count);
  assert.equal(validateGlobal(data,{partitionManifest:f.manifest}),data);
  assert.equal(validateGlobal(structuredClone(data)).nodes.length,2);
});

test('partition manifest rejects forged scope, excessive totals, paths and duplicate fields',()=>{
  const f=fixture();assert.equal(validatePartitionManifest(f.manifest),f.manifest);
  for(const mutate of [m=>m.publication_status='public-observations-pending-external-review',m=>m.nodes=300001,m=>m.edges=600001,
    m=>m.stored_bytes++,m=>m.asset_count++,m=>m.fields.reverse(),m=>m.fields.push(m.fields.at(-1)),
    m=>m.fields[0].asset.file='../escape.gz',m=>m.fields[0].field='__proto__']){
    const m=structuredClone(f.manifest);mutate(m);assert.throws(()=>validatePartitionManifest(m));
  }
});

test('public observation partitions need explicit qualification and matching payload status',async()=>{
  const f=fixture(),m=structuredClone(f.manifest);m.publication_status='public-observations-pending-external-review';
  m.publication={schema_version:'gapi-public-partition-qualification/1',qualification:m.publication_status,
    export_scope:'institution-aggregates-only',purpose:'noncommercial-research',formal_ranking_qualified:false,
    independent_personal_facts_verified:false,local_manifest_sha256:'a'.repeat(64),publication_policy_sha256:'b'.repeat(64),
    local_manifest:'local-input-manifest.json',local_source_manifest:'local-input-source-manifest.json'};
  assert.equal(validatePartitionManifest(m),m);
  await assert.rejects(loadPartitionedGlobal(m,'https://example.test/',{fetcher:f.fetcher}),/payload contract mismatch/);
  for(const update of [{formal_ranking_qualified:true},{purpose:'commercial'},{local_manifest:'../private'},
    {independent_personal_facts_verified:true},{publication_policy_sha256:null}]){
    assert.throws(()=>validatePartitionManifest({...m,publication:{...m.publication,...update}}));
  }
});

test('compressed and decoded corruption, oversized response and failed request stop loading',async()=>{
  const f=fixture(),a=f.manifest.fields[0].asset;
  await assert.rejects(fetchPartitionAsset('https://example.test/',a,{fetcher:async()=>new Response(Buffer.alloc(a.stored_bytes+1))}));
  await assert.rejects(fetchPartitionAsset('https://example.test/',a,{fetcher:async()=>new Response(Buffer.alloc(a.stored_bytes))}));
  await assert.rejects(fetchPartitionAsset('https://example.test/',a,{fetcher:async()=>new Response('',{status:403})}),/403/);
  const bad={...a,sha256:'c'.repeat(64),file:`${'c'.repeat(64)}.json.gz`};
  await assert.rejects(fetchPartitionAsset('https://example.test/',bad,{fetcher:async()=>new Response(f.files.get(a.file))}),/integrity mismatch/);
});

test('row references reject booleans, wrong width, unsupported keys and missing universe',()=>{
  const value={schema_version:'gapi-aggregate-row-columns/1',columns:['source','target'],rows:[[0,1]],references:{source:'algorithm.network_universe',target:'algorithm.network_universe'}};
  assert.deepEqual(decodePartitionRows(value,{rows:1},{field:'edges',universe:['a','b']}),[{source:'a',target:'b'}]);
  for(const index of [true,-1,2,1.5])assert.throws(()=>decodePartitionRows({...value,rows:[[index,1]]},{rows:1},{field:'edges',universe:['a','b']}));
  assert.throws(()=>decodePartitionRows(value,{rows:1},{field:'edges'}));
  assert.throws(()=>decodePartitionRows({...value,rows:[[0]]},{rows:1},{field:'edges',universe:['a','b']}));
  assert.throws(()=>decodePartitionRows({...value,columns:['__proto__','target']},{rows:1},{field:'edges',universe:['a','b']}));
});

test('a mid-stream fetch failure returns no partially accepted dataset',async()=>{
  const f=fixture();let n=0;
  await assert.rejects(loadPartitionedGlobal(f.manifest,'https://example.test/',{fetcher:async(...args)=>{if(++n===3)throw new Error('connection interrupted');return f.fetcher(...args);}}),/interrupted/);
  assert.equal(n,3);
});

test('HTTP-decoded gzip binds full decoded identity without double decompression',async()=>{
  const f=fixture(),a=f.manifest.fields[0].asset;
  const {gunzipSync}=await import('node:zlib');
  const raw=gunzipSync(f.files.get(a.file));
  const decoded=body=>new Response(body,{headers:{'Content-Encoding':'gzip'}});
  assert.deepEqual(await fetchPartitionAsset('https://example.test/',a,{fetcher:async()=>decoded(raw)}),JSON.parse(raw));
  for(const bad of [raw.subarray(1),Buffer.concat([raw,Buffer.from(' ')]),Buffer.alloc(raw.length),f.files.get(a.file)]){
    await assert.rejects(fetchPartitionAsset('https://example.test/',a,{fetcher:async()=>decoded(bad)}));
  }
  const dataset=await loadPartitionedGlobal(f.manifest,'https://example.test/',{fetcher:async url=>{
    const file=new URL(url).pathname.split('/').at(-1);
    return decoded(gunzipSync(f.files.get(file)));
  }});
  assert.deepEqual(dataset,f.data);
});

test('real HTTP gzip content encoding is decoded once by fetch',async()=>{
  const {createServer}=await import('node:http');
  const f=fixture(),a=f.manifest.fields[0].asset,packed=f.files.get(a.file);
  const server=createServer((request,response)=>{
    response.writeHead(200,{'Content-Encoding':'gzip','Content-Type':'application/json','Content-Length':packed.length});
    response.end(packed);
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  try{
    const value=await fetchPartitionAsset(`http://127.0.0.1:${server.address().port}/`,a);
    assert.deepEqual(value,f.data.algorithm);
  }finally{
    server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
  }
});
