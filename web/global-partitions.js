// Reader for gapi.aggregate_partitions. No scoring or publication decisions.
import {verifySnapshotBytes} from './snapshot.js';

const HASH=/^[a-f0-9]{64}$/;
const ROWS='gapi-aggregate-row-columns/1';
const unsafe=new Set(['__proto__','constructor','prototype']);
const integer=n=>Number.isSafeInteger(n)&&n>=0;
const key=k=>typeof k==='string'&&k.length>0&&k.length<=128&&!unsafe.has(k);
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
export const PARTITION_CAPACITY=Object.freeze({nodes:300000,edges:600000,storedBytes:24*1024*1024,logicalBytes:256*1024*1024});

function descriptor(a,rows=false){
  const max=rows?1024*1024:32*1024*1024;
  if(!object(a)||!HASH.test(a.sha256)||!HASH.test(a.stored_sha256)||a.file!==`${a.sha256}.json.gz`
    ||a.encoding!=='gzip'||!integer(a.bytes)||a.bytes===0||a.bytes>max
    ||!integer(a.stored_bytes)||a.stored_bytes===0||a.stored_bytes>max+65536
    ||(rows&&(!integer(a.rows)||a.rows===0)))throw new Error('Invalid partition asset');
  return a;
}

export function validatePartitionManifest(m){
  const publication=m?.publication;
  const publicStatus=m?.publication_status==='public-observations-pending-external-review'
    &&object(publication)&&publication.schema_version==='gapi-public-partition-qualification/1'
    &&publication.qualification===m.publication_status&&publication.export_scope==='institution-aggregates-only'
    &&publication.purpose==='noncommercial-research'&&publication.formal_ranking_qualified===false
    &&publication.independent_personal_facts_verified===false
    &&HASH.test(publication.local_manifest_sha256)&&HASH.test(publication.publication_policy_sha256)
    &&publication.local_manifest==='local-input-manifest.json'&&publication.local_source_manifest==='local-input-source-manifest.json';
  if(!object(m)||m.schema_version!=='gapi-aggregate-partitions/1'||m.logical_schema_version!=='gapi-global-observed/v1'
    ||!(m.publication_status==='local-only-source-license-review'||publicStatus)||!HASH.test(m.logical_sha256)||!HASH.test(m.source_manifest_sha256)
    ||!integer(m.logical_bytes)||m.logical_bytes===0||m.logical_bytes>PARTITION_CAPACITY.logicalBytes
    ||!integer(m.nodes)||m.nodes>PARTITION_CAPACITY.nodes||!integer(m.edges)||m.edges>PARTITION_CAPACITY.edges
    ||!integer(m.asset_count)||m.asset_count>10000||!integer(m.stored_bytes)||m.stored_bytes>PARTITION_CAPACITY.storedBytes
    ||!Array.isArray(m.fields)||m.fields.length===0||m.fields.length>64)throw new Error('Invalid local partition manifest');
  let last='',assets=0,stored=0,decoded=0;const counts={};const seen=new Map();
  const add=(a,rows=false)=>{
    descriptor(a,rows);assets++;stored+=a.stored_bytes;decoded+=a.bytes;
    const previous=seen.get(a.file),identity=JSON.stringify([a.sha256,a.stored_sha256,a.bytes,a.stored_bytes]);
    if(previous&&previous!==identity)throw new Error('Conflicting partition asset identity');
    seen.set(a.file,identity);
  };
  for(const f of m.fields){
    if(!object(f)||!key(f.field)||f.field<=last)throw new Error('Invalid partition field order');
    last=f.field;
    if(f.kind==='rows'&&['nodes','edges'].includes(f.field)){
      if(!integer(f.rows)||f.rows!==m[f.field]||!Array.isArray(f.assets)||f.assets.length>10000)throw new Error('Invalid partition row manifest');
      let n=0;for(const a of f.assets){add(a,true);n+=a.rows;}
      if(n!==f.rows)throw new Error('Partition row total mismatch');counts[f.field]=n;
    }else if(f.kind==='object'&&f.field==='sensitivity'){
      if(!Array.isArray(f.entries)||f.entries.length>256)throw new Error('Invalid partition scenarios');
      let name='';for(const item of f.entries){if(!object(item)||!key(item.key)||item.key<=name)throw new Error('Invalid scenario order');name=item.key;add(item.asset);}
    }else if(f.kind==='value'&&!['nodes','edges','sensitivity'].includes(f.field))add(f.asset);
    else throw new Error('Invalid partition field kind');
  }
  if(counts.nodes!==m.nodes||counts.edges!==m.edges||assets!==m.asset_count||stored!==m.stored_bytes||decoded>PARTITION_CAPACITY.logicalBytes)throw new Error('Partition totals mismatch');
  return m;
}

async function bytes(stream,maximum){
  if(!stream)throw new Error('Missing partition body');
  const reader=stream.getReader(),parts=[];let n=0;
  try{while(true){const {done,value}=await reader.read();if(done)break;n+=value.byteLength;if(n>maximum)throw new Error('Partition response exceeds bound');parts.push(value);}}
  finally{await reader.cancel();reader.releaseLock();}
  const result=new Uint8Array(n);let offset=0;for(const p of parts){result.set(p,offset);offset+=p.length;}return result;
}

export async function fetchPartitionAsset(base,a,{signal,fetcher=fetch,rows=false}={}){
  descriptor(a,rows);
  const response=await fetcher(new URL(a.file,base),{signal});
  if(!response.ok){await response.body?.cancel();throw new Error(`Partition HTTP ${response.status}`);}
  // Fetch removes HTTP gzip Content-Encoding before exposing response.body.
  // The original compressed identity remains checked by build/public replay;
  // this branch binds the exact decoded bytes, without trying to unzip twice.
  if(response.headers.get('Content-Encoding')?.trim().toLowerCase()==='gzip'){
    const raw=await bytes(response.body,a.bytes);
    if(raw.length!==a.bytes)throw new Error('Partition decoded length mismatch');
    await verifySnapshotBytes(raw,{output_sha256:a.sha256});
    return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
  }
  const packed=await bytes(response.body,a.stored_bytes);
  if(packed.length!==a.stored_bytes)throw new Error('Partition compressed length mismatch');
  await verifySnapshotBytes(packed,{output_sha256:a.stored_sha256});
  const raw=await bytes(new Blob([packed]).stream().pipeThrough(new DecompressionStream('gzip')),a.bytes);
  if(raw.length!==a.bytes)throw new Error('Partition decoded length mismatch');
  await verifySnapshotBytes(raw,{output_sha256:a.sha256});
  return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
}

export function decodePartitionRows(value,entry,{field,universe,shared=new Map()}={}){
  const columns=value?.columns,rows=value?.rows,refs=value?.references??{};
  if(value?.schema_version!==ROWS||!Array.isArray(columns)||columns.length===0||columns.length>64
    ||columns.some((k,i)=>!key(k)||(i>0&&k<=columns[i-1]))||!Array.isArray(rows)||rows.length!==entry.rows
    ||!object(refs))throw new Error('Invalid partition rows');
  const names=Object.keys(refs);
  if(names.length&&(field!=='edges'||names.sort().join(',')!=='source,target'
    ||names.some(k=>refs[k]!=='algorithm.network_universe'||!columns.includes(k))||!Array.isArray(universe)))throw new Error('Invalid edge reference contract');
  // Reuse the entry pairs rather than allocating columns.length arrays for
  // every output object. Equal immutable labels/weights may share storage;
  // identity, names, numeric values and all absent/null distinctions survive.
  const pairs=columns.map(k=>[k,null]);
  function intern(name,value){
    const labels=Array.isArray(value)&&value.length<=64&&value.every(v=>typeof v==='string');
    const stages=name==='stages'&&object(value)&&Object.keys(value).length<=4
      &&Object.values(value).every(v=>typeof v==='number'&&Number.isFinite(v)&&!Object.is(v,-0));
    if(!labels&&!stages)return value;
    let cache=shared.get(name);if(!cache){cache=new Map();shared.set(name,cache);}
    const encoded=JSON.stringify(value);if(cache.has(encoded))return cache.get(encoded);
    if(cache.size<64){Object.freeze(value);cache.set(encoded,value);}return value;
  }
  return rows.map(row=>{
    if(!Array.isArray(row)||row.length!==columns.length)throw new Error('Partition row width mismatch');
    for(let i=0;i<columns.length;i++)pairs[i][1]=intern(columns[i],row[i]);
    const result=Object.fromEntries(pairs);
    for(const name of names){const i=result[name];if(!integer(i)||i>=universe.length)throw new Error('Partition edge reference out of bounds');result[name]=universe[i];}
    return result;
  });
}

export async function loadPartitionedGlobal(manifest,base,{signal,fetcher=fetch,onProgress,concurrency=6}={}){
  if(!Number.isInteger(concurrency)||concurrency<1||concurrency>6)throw new Error('Invalid partition concurrency');
  const m=validatePartitionManifest(manifest),data={},shared=new Map();let assets=0;
  const stop=new AbortController(),requestSignal=signal?AbortSignal.any([signal,stop.signal]):stop.signal;
  const plan=m.fields.flatMap(f=>f.kind==='rows'?f.assets.map(a=>({a,rows:true})):
    f.kind==='object'?f.entries.map(v=>({a:v.asset,rows:false})):[{a:f.asset,rows:false}]);
  const pending=new Map();let next=0,consumed=0,pendingBytes=0;
  function fill(){
    requestSignal.throwIfAborted();
    while(next<plan.length&&pending.size<concurrency){
      const item=plan[next];
      // Large scalar values run alone; ordinary row partitions can overlap.
      if(pending.size&&pendingBytes+item.a.bytes>8*1024*1024)break;
      const promise=fetchPartitionAsset(base,item.a,{signal:requestSignal,fetcher,rows:item.rows})
        .then(value=>({value}),error=>{stop.abort(error);return {error};});
      pending.set(next++,{promise,bytes:item.a.bytes});pendingBytes+=item.a.bytes;
    }
  }
  const load=async(a,rows=false)=>{
    const expected=plan[consumed],job=pending.get(consumed);
    if(!job||expected.a!==a||expected.rows!==rows)throw new Error('Partition prefetch order mismatch');
    const result=await job.promise;pending.delete(consumed++);pendingBytes-=job.bytes;
    if(result.error)throw result.error;
    requestSignal.throwIfAborted();assets++;
    onProgress?.({loadedAssets:assets,totalAssets:m.asset_count});fill();return result.value;
  };
  try{
  fill();
  for(const f of m.fields){
    if(f.kind==='rows'){
      const rows=[];
      for(const a of f.assets){
        const part=decodePartitionRows(await load(a,true),a,{field:f.field,universe:data.algorithm?.network_universe,shared});
        for(const row of part)rows.push(row);
      }
      if(rows.length!==m[f.field])throw new Error('Partition loaded row total mismatch');data[f.field]=rows;
    }else if(f.kind==='object'){
      const value={};for(const item of f.entries)value[item.key]=await load(item.asset);data[f.field]=value;
    }else{
      data[f.field]=await load(f.asset);
      if(f.field==='algorithm'){
        const universe=data.algorithm?.network_universe;
        if(!Array.isArray(universe)||universe.length>200000||universe.some(v=>typeof v!=='string')||new Set(universe).size!==universe.length)throw new Error('Invalid partition graph universe');
      }
    }
  }
  if(data.schema_version!==m.logical_schema_version||data.publication_status!==m.publication_status||assets!==m.asset_count)throw new Error('Partition payload contract mismatch');
  return data;
  }finally{stop.abort();pending.clear();}
}
