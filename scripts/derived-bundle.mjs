// Build-time ownership for the optional frozen, aggregate-only historical view.
import {readFileSync,lstatSync} from 'node:fs';
import {resolve,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {gunzipSync} from 'node:zlib';
import {validateDerivedManifest,decodeNodes,decodeEdges,DERIVED_BYTES} from '../web/derived-data.js';
export const hash=raw=>createHash('sha256').update(raw).digest('hex');
export const DERIVED_MANIFEST='outputs/easy-orcid-web/manifest.json';
export function readDerivedAsset(path,maximum){
  let actual=path,info;
  try{info=lstatSync(actual);}catch(error){if(error.code!=='ENOENT')throw error;actual=path+'.gz';info=lstatSync(actual);}
  if(!info.isFile()||info.isSymbolicLink()||info.size>maximum+(actual===path?0:65536))throw new Error('Invalid historical release file');
  const stored=readFileSync(actual);
  const bytes=actual===path?stored:gunzipSync(stored,{maxOutputLength:maximum});
  return {bytes,path:actual,stored};
}
export function readDerivedBundle(root,selection){
  if(selection==null)return null;
  if(selection.manifest!==DERIVED_MANIFEST||!/^[a-f0-9]{64}$/.test(selection.sha256))throw new Error('Unsafe historical release selection');
  const read=(path,maximum)=>{const info=lstatSync(path);if(!info.isFile()||info.isSymbolicLink()||info.size>maximum)throw new Error('Invalid historical release file');return readFileSync(path);};
  const manifestPath=resolve(root,DERIVED_MANIFEST),raw=read(manifestPath,128*1024);
  if(hash(raw)!==selection.sha256)throw new Error('Historical manifest hash mismatch');
  const manifest=validateDerivedManifest(JSON.parse(raw)),files=new Map([['manifest.json',raw]]);
  const inputs={[DERIVED_MANIFEST]:hash(raw)};
  for(const a of [manifest.nodes,manifest.overview,...manifest.edge_chunks]){
    if(files.has(a.file))continue;
    const asset=readDerivedAsset(resolve(dirname(manifestPath),a.file),a.bytes),bytes=asset.bytes;
    if(bytes.length!==a.bytes||hash(bytes)!==a.sha256)throw new Error('Historical asset hash/length mismatch');
    files.set(a.file,bytes);
    inputs[`outputs/easy-orcid-web/${asset.path.split(/[\\/]/).at(-1)}`]=hash(asset.stored);
  }
  const bytes=[...files.values()].reduce((n,b)=>n+b.length,0);
  if(bytes>DERIVED_BYTES+128*1024)throw new Error('Historical bundle exceeds release budget');
  const nodes=decodeNodes(JSON.parse(files.get(manifest.nodes.file)),manifest);
  let edges=0;
  for(const asset of manifest.edge_chunks)edges+=decodeEdges(JSON.parse(files.get(asset.file)),nodes).length;
  if(edges!==manifest.edge_count)throw new Error('Historical total edge count mismatch');
  decodeEdges(JSON.parse(files.get(manifest.overview.file)),nodes,2000);
  return {files,manifest,identity:{manifest:DERIVED_MANIFEST,manifest_sha256:hash(raw),bytes,
    inputs,assets:Object.fromEntries([...files].map(([name,body])=>[name,hash(body)]))}};
}
