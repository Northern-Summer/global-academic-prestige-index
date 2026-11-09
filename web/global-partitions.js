// Reader for gapi.aggregate_partitions. No scoring or publication decisions.
import {verifySnapshotBytes} from './snapshot.js';

const HASH=/^[a-f0-9]{64}$/;
const ROWS='gapi-aggregate-row-columns/1';
const unsafe=new Set(['__proto__','constructor','prototype']);
const integer=n=>Number.isSafeInteger(n)&&n>=0;
const key=k=>typeof k==='string'&&k.length>0&&k.length<=128&&!unsafe.has(k);
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
export const PARTITION_CAPACITY=Object.freeze({nodes:300000,edges:600000,storedBytes:24*1024*1024,logicalBytes:384*1024*1024,decodedBytes:256*1024*1024});

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
  if(counts.nodes!==m.nodes||counts.edges!==m.edges||assets!==m.asset_count||stored!==m.stored_bytes||decoded>PARTITION_CAPACITY.decodedBytes)throw new Error('Partition totals mismatch');
  if(m.bundle!=null){
    const b=m.bundle;
    if(!object(b)||b.format!=='gapi-partition-concat/1'||!HASH.test(b.sha256)
      ||b.file!==`${b.sha256}.gapi`||b.bytes!==stored)throw new Error('Invalid partition bundle');
  }
  return m;
}

async function bytes(stream,maximum){
  if(!stream)throw new Error('Missing partition body');
  const reader=stream.getReader(),parts=[];let n=0;
  try{while(true){const {done,value}=await reader.read();if(done)break;n+=value.byteLength;if(n>maximum)throw new Error('Partition response exceeds bound');parts.push(value);}}
  finally{await reader.cancel();reader.releaseLock();}
  const result=new Uint8Array(n);let offset=0;for(const p of parts){result.set(p,offset);offset+=p.length;}return result;
}

export async function fetchPartitionAsset(base,a,{signal,fetcher=fetch,rows=false,onVerified}={}){
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
  const value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
  onVerified?.(packed);return value;
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

export async function loadPartitionedGlobal(manifest,base,{signal,fetcher=fetch,onProgress,concurrency=6,idleTimeoutMs=60000,retryCache}={}){
  if(!Number.isInteger(concurrency)||concurrency<1||concurrency>6)throw new Error('Invalid partition concurrency');
  if(!Number.isInteger(idleTimeoutMs)||idleTimeoutMs<1||idleTimeoutMs>60000)throw new Error('Invalid partition idle timeout');
  const m=validatePartitionManifest(manifest),data={},shared=new Map();let assets=0;
  if(retryCache){
    const identity=JSON.stringify([String(base),m]);
    if(retryCache.identity!==identity){retryCache.identity=identity;retryCache.assets=new Map();retryCache.bytes=0;}
  }
  const cached=a=>retryCache?.assets.get(a.file+':'+a.stored_sha256);
  const remember=(a,packed)=>{
    if(!retryCache||cached(a)||retryCache.bytes+packed.length>PARTITION_CAPACITY.storedBytes)return;
    retryCache.assets.set(a.file+':'+a.stored_sha256,packed);retryCache.bytes+=packed.length;
  };
  if(m.bundle)return loadBundle(m,base,{signal,fetcher,onProgress,idleTimeoutMs,cached,remember});
  const stop=new AbortController(),requestSignal=signal?AbortSignal.any([signal,stop.signal]):stop.signal;
  const plan=m.fields.flatMap(f=>f.kind==='rows'?f.assets.map(a=>({a,rows:true})):
    f.kind==='object'?f.entries.map(v=>({a:v.asset,rows:false})):[{a:f.asset,rows:false}]);
  const pending=new Map();let next=0,consumed=0,pendingBytes=0,active=0;
  let idleTimer;
  const progressDeadline=()=>{
    clearTimeout(idleTimer);
    idleTimer=setTimeout(()=>stop.abort(new DOMException('No verified partition progress within idle deadline','TimeoutError')),idleTimeoutMs);
  };
  function fill(){
    requestSignal.throwIfAborted();
    while(next<plan.length&&active<concurrency){
      const item=plan[next];
      // Large scalar values run alone; ordinary row partitions can overlap.
      if(pending.size&&pendingBytes+item.a.bytes>8*1024*1024)break;
      active++;
      const saved=cached(item.a);
      const promise=fetchPartitionAsset(base,item.a,{signal:requestSignal,rows:item.rows,
        fetcher:saved?async()=>new Response(saved):fetcher,onVerified:packed=>remember(item.a,packed)})
        .then(value=>({value}),error=>{stop.abort(error);return {error};})
        .then(result=>{
          active--;
          // Completed decoded values still reserve their queue bytes, but
          // no longer occupy a network slot behind a slow earlier request.
          if(!requestSignal.aborted)fill();
          return result;
        });
      pending.set(next++,{promise,bytes:item.a.bytes});pendingBytes+=item.a.bytes;
    }
  }
  const load=async(a,rows=false)=>{
    const expected=plan[consumed],job=pending.get(consumed);
    if(!job||expected.a!==a||expected.rows!==rows)throw new Error('Partition prefetch order mismatch');
    const result=await job.promise;pending.delete(consumed++);pendingBytes-=job.bytes;
    if(result.error)throw result.error;
    requestSignal.throwIfAborted();assets++;
    progressDeadline();
    onProgress?.({loadedAssets:assets,totalAssets:m.asset_count});fill();return result.value;
  };
  try{
  progressDeadline();
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
  }finally{clearTimeout(idleTimer);stop.abort();pending.clear();}
}

async function loadBundle(m,base,{signal,fetcher,onProgress,idleTimeoutMs,cached=()=>undefined,remember=()=>{}}){
  const stop=new AbortController(),requestSignal=signal?AbortSignal.any([signal,stop.signal]):stop.signal;
  let timer,reader,chunk=new Uint8Array(),offset=0,total=0;
  const progress=()=>{clearTimeout(timer);timer=setTimeout(()=>stop.abort(new DOMException('No verified bundle progress within idle deadline','TimeoutError')),idleTimeoutMs);};
  const checksum=new SHA256();
  async function exact(n){
    const result=new Uint8Array(n);let written=0;
    while(written<n){
      requestSignal.throwIfAborted();
      if(offset===chunk.length){
        const item=await reader.read();if(item.done)throw new Error('Truncated partition bundle');
        chunk=item.value;offset=0;total+=chunk.length;
        if(total>m.bundle.bytes)throw new Error('Partition bundle exceeds length');
        checksum.update(chunk);
      }
      const count=Math.min(n-written,chunk.length-offset);result.set(chunk.subarray(offset,offset+count),written);written+=count;offset+=count;
    }
    return result;
  }
  const plan=m.fields.flatMap(f=>f.kind==='rows'?f.assets:f.kind==='object'?f.entries.map(v=>v.asset):[f.asset]);
  let prefix=0;for(const a of plan){const saved=cached(a);if(!saved)break;if(saved.length!==a.stored_bytes)throw new Error('Invalid retained partition length');prefix+=saved.length;}
  const data={},shared=new Map();let loaded=0,retained=0;
  async function load(a,rows=false){
    let packed;
    if(retained<prefix){packed=cached(a);retained+=packed.length;checksum.update(packed);}
    else packed=await exact(a.stored_bytes);
    const value=await fetchPartitionAsset(base,a,{signal:requestSignal,rows,
      fetcher:async()=>new Response(packed,{headers:{'Content-Type':'application/gzip'}})});
    remember(a,packed);progress();loaded++;onProgress?.({loadedAssets:loaded,totalAssets:m.asset_count,retainedBytes:prefix});return value;
  }
  try{
    progress();total=prefix;
    if(prefix<m.bundle.bytes){
      const response=await fetcher(new URL(m.bundle.file,base),{signal:requestSignal,...(prefix?{headers:{Range:`bytes=${prefix}-`}}:{})});
      if(!response.ok){await response.body?.cancel();throw new Error(`Partition bundle HTTP ${response.status}`);}
      if(prefix&&(response.status!==206||response.headers.get('Content-Range')!==`bytes ${prefix}-${m.bundle.bytes-1}/${m.bundle.bytes}`)){
        await response.body?.cancel();throw new Error('无法继续加载；已保留校验内容，未重新下载');
      }
      if(!response.body)throw new Error('Missing partition bundle body');reader=response.body.getReader();
    }
    for(const f of m.fields){
      if(f.kind==='rows'){
        const rows=[];
        for(const a of f.assets)for(const row of decodePartitionRows(await load(a,true),a,{field:f.field,universe:data.algorithm?.network_universe,shared}))rows.push(row);
        if(rows.length!==m[f.field])throw new Error('Bundle row total mismatch');data[f.field]=rows;
      }else if(f.kind==='object'){
        const value={};for(const item of f.entries)value[item.key]=await load(item.asset);data[f.field]=value;
      }else{
        data[f.field]=await load(f.asset);
        if(f.field==='algorithm'){
          const u=data.algorithm?.network_universe;
          if(!Array.isArray(u)||u.length>200000||u.some(x=>typeof x!=='string')||new Set(u).size!==u.length)throw new Error('Invalid bundle graph universe');
        }
      }
    }
    if(offset!==chunk.length||reader&&!(await reader.read()).done||total!==m.bundle.bytes)throw new Error('Trailing or incomplete partition bundle');
    if(checksum.hex()!==m.bundle.sha256)throw new Error('Partition bundle SHA-256 mismatch');
    if(loaded!==m.asset_count||data.schema_version!==m.logical_schema_version||data.publication_status!==m.publication_status)throw new Error('Bundle payload contract mismatch');
    return data;
  }finally{clearTimeout(timer);stop.abort();if(reader){await reader.cancel();reader.releaseLock();}}
}

// Incremental SHA-256 keeps bundle verification bounded to one decoded asset.
// Individual stored and decoded assets additionally use the WebCrypto owner.
class SHA256{
  constructor(){this.h=new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]);this.buffer=new Uint8Array(64);this.used=0;this.size=0;this.w=new Uint32Array(64);}
  update(bytes){this.size+=bytes.length;let i=0;while(i<bytes.length){const n=Math.min(64-this.used,bytes.length-i);this.buffer.set(bytes.subarray(i,i+n),this.used);this.used+=n;i+=n;if(this.used===64){this.block(this.buffer);this.used=0;}}}
  block(b){
    const w=this.w,rot=(v,n)=>(v>>>n)|(v<<(32-n));
    for(let i=0;i<16;i++)w[i]=(b[i*4]<<24)|(b[i*4+1]<<16)|(b[i*4+2]<<8)|b[i*4+3];
    for(let i=16;i<64;i++){const a=w[i-15],b=w[i-2];w[i]=(w[i-16]+(rot(a,7)^rot(a,18)^(a>>>3))+w[i-7]+(rot(b,17)^rot(b,19)^(b>>>10)))>>>0;}
    let [a,c,d,e,f,g,h,j]=this.h;
    for(let i=0;i<64;i++){const t1=(j+(rot(f,6)^rot(f,11)^rot(f,25))+((f&g)^(~f&h))+SHA256.k[i]+w[i])>>>0;const t2=((rot(a,2)^rot(a,13)^rot(a,22))+((a&c)^(a&d)^(c&d)))>>>0;j=h;h=g;g=f;f=(e+t1)>>>0;e=d;d=c;c=a;a=(t1+t2)>>>0;}
    const values=[a,c,d,e,f,g,h,j];for(let i=0;i<8;i++)this.h[i]=(this.h[i]+values[i])>>>0;
  }
  hex(){const bits=this.size*8;this.buffer[this.used++]=0x80;if(this.used>56){this.buffer.fill(0,this.used);this.block(this.buffer);this.used=0;}this.buffer.fill(0,this.used,56);const v=new DataView(this.buffer.buffer);v.setUint32(56,Math.floor(bits/0x100000000));v.setUint32(60,bits>>>0);this.block(this.buffer);return [...this.h].map(v=>v.toString(16).padStart(8,'0')).join('');}
  static k=new Uint32Array([0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2]);
}
