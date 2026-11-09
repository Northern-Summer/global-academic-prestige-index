/* One owner for build-input identity and local publication preflight.
 * Source hashes normalize CRLF/LF, matching Git's text normalization. Output
 * hashes cover exact served bytes. This is provenance, not signed attestation.
 */
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {existsSync,lstatSync,readdirSync,readFileSync,writeFileSync} from 'node:fs';
import {dirname,isAbsolute,relative,resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {gunzipSync} from 'node:zlib';
import {readDerivedBundle} from './derived-bundle.mjs';
import {validatePartitionManifest} from '../web/global-partitions.js';

export const DOCUMENTATION = ['METHODOLOGY.md','DATA_SOURCES.md','DATA_SCHEMA.md','BIAS_AND_LIMITATIONS.md','RED_TEAM_REPORT.md','STREAMING.md','CI_CD.md','OOD.md','FONTS.md','EASY_ORCID_2023_OBSERVATIONS.md'];
export const SOURCE_FILES = ['guide.html','index.html','vite.config.js','package.json','package-lock.json','scripts/build-provenance.mjs','scripts/derived-bundle.mjs','scripts/deploy-pages.mjs','.github/workflows/ci.yml',...DOCUMENTATION.map(name=>`docs/${name}`)];
export const PROVENANCE = 'build-provenance.json';
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const sourceHash = bytes => hash(Buffer.from(bytes.toString('utf8').replaceAll('\r\n','\n')));
const inputHash = (path,bytes) => path.endsWith('.woff2') ? hash(bytes) : sourceHash(bytes);
const ordered = value => Object.fromEntries(Object.entries(value).sort(([a],[b])=>a.localeCompare(b,'en')));
const same = (a,b) => JSON.stringify(ordered(a)) === JSON.stringify(ordered(b));
const canonicalJSON=value=>JSON.stringify((function sort(v){return Array.isArray(v)?v.map(sort):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sort(v[k])])):v;})(value)).replace(/[\u007f-\uffff]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'));

function inside(root,path) {
  const rel = relative(root,path);
  if (isAbsolute(rel) || rel === '..' || rel.startsWith(`..${sep}`)) throw new Error('Path outside build root');
  return rel.split(sep).join('/');
}

function regular(path) {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink() || !stat.isFile()) throw new Error('Build inputs/outputs must be regular files');
  if (stat.size > 32*1024*1024) throw new Error('Build file exceeds 32 MiB bound');
  return readFileSync(path);
}

function walk(folder, root) {
  const files = [];
  for (const entry of readdirSync(folder,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name,'en'))) {
    const path = resolve(folder,entry.name);
    if (entry.isSymbolicLink()) throw new Error('Links forbidden in build tree');
    if (entry.isDirectory()) files.push(...walk(path,root));
    else if (entry.isFile()) files.push(inside(root,path));
    else throw new Error('Unsupported build file type');
  }
  return files;
}

export function sourceInputs(root) {
  const paths = [...SOURCE_FILES,...walk(resolve(root,'web'),root).filter(path=>!path.endsWith('.test.js'))];
  return ordered(Object.fromEntries(paths.map(path=>[path,inputHash(path,regular(resolve(root,path)))])));
}

export function outputHashes(outDir) {
  const files = walk(outDir,outDir).filter(path=>path !== PROVENANCE);
  let total = 0;
  const values = files.map(path=> {
    const bytes = regular(resolve(outDir,path)); total += bytes.length;
    if (total > 64*1024*1024) throw new Error('Build output exceeds 64 MiB bound');
    return [path,hash(bytes)];
  });
  return ordered(Object.fromEntries(values));
}

function git(root,...args) {
  // Committed snapshots have the same 32 MiB bound as local inputs. A smaller
  // subprocess buffer would reject a legitimate release above 2 MiB.
  return execFileSync('git',args,{cwd:root,maxBuffer:32*1024*1024+1,stdio:['ignore','pipe','pipe']});
}

export function readReleaseSnapshot(manifestPath,manifest) {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.json$/.test(manifest.snapshot)) throw new Error('Unsafe release snapshot');
  const storage=manifest.repository_snapshot;
  if ('repository_snapshot' in manifest && (!storage || typeof storage!=='object' || Array.isArray(storage))) throw new Error('Invalid repository snapshot representation');
  if(storage && (storage.encoding!=='gzip' || storage.filename!==manifest.snapshot+'.gz'
      || !/^[a-f0-9]{64}$/.test(storage.sha256)
      || !Number.isSafeInteger(storage.bytes) || storage.bytes<=0 || storage.bytes>32*1024*1024
      || !Number.isSafeInteger(storage.decoded_bytes) || storage.decoded_bytes<=0 || storage.decoded_bytes>32*1024*1024)) {
    throw new Error('Invalid repository snapshot representation');
  }
  const path=resolve(dirname(manifestPath),storage ? storage.filename : manifest.snapshot);
  const stored=regular(path);
  if(storage && (stored.length!==storage.bytes || hash(stored)!==storage.sha256)) throw new Error('Stored snapshot identity mismatch');
  const bytes=storage ? gunzipSync(stored,{maxOutputLength:32*1024*1024}) : stored;
  if(storage && bytes.length!==storage.decoded_bytes) throw new Error('Decoded snapshot size mismatch');
  if(hash(bytes)!==manifest.output_sha256) throw new Error('Selected snapshot identity mismatch');
  return {bytes,path,stored_sha256:hash(stored)};
}

// The transport reader does not grant publication rights. The existing release
// status gate and committed-input checks still apply to every selected asset.
export function readReleasePartitions(manifestPath,manifest) {
  validatePartitionManifest(manifest);
  const descriptors=manifest.fields.flatMap(f=>f.kind==='rows'?f.assets:f.kind==='object'?f.entries.map(e=>e.asset):[f.asset]);
  const files=new Map();
  for(const a of descriptors){
    const local=resolve(dirname(manifestPath),a.file);
    const path=existsSync(local)||!process.env.GAPI_PARTITION_ASSETS?local:resolve(process.env.GAPI_PARTITION_ASSETS,a.file),stored=regular(path);
    if(stored.length!==a.stored_bytes||hash(stored)!==a.stored_sha256)throw new Error('Stored partition identity mismatch');
    const raw=gunzipSync(stored,{maxOutputLength:a.bytes});
    if(raw.length!==a.bytes||hash(raw)!==a.sha256)throw new Error('Decoded partition identity mismatch');
    files.set(a.file,{path,sha256:a.stored_sha256,bytes:stored.length});
  }
  const status=manifest.fields.find(f=>f.field==='publication_status');
  if(status?.kind!=='value')throw new Error('Missing partition publication status');
  const value=JSON.parse(gunzipSync(regular(files.get(status.asset.file).path),{maxOutputLength:status.asset.bytes}));
  if(value!==manifest.publication_status)throw new Error('Partition payload publication status mismatch');
  const bindings=new Map();
  if(manifest.publication_status==='public-observations-pending-external-review'){
    const p=manifest.publication,folder=dirname(manifestPath);
    const original=regular(resolve(folder,p.local_manifest)),source=regular(resolve(folder,p.local_source_manifest));
    if(hash(original)!==p.local_manifest_sha256||hash(source)!==manifest.source_manifest_sha256
      ||hash(Buffer.from(canonicalJSON(p.publication_policy)))!==p.publication_policy_sha256)throw new Error('Public partition policy/source binding mismatch');
    const local=validatePartitionManifest(JSON.parse(original));
    if(local.publication_status!=='local-only-source-license-review'||local.logical_sha256!==p.logical_sha256)throw new Error('Invalid local publication predecessor');
    const old=local.fields.find(f=>f.field==='publication_status');
    const restored=manifest.fields.map(f=>f.field==='publication_status'?old:f);
    if(JSON.stringify(restored)!==JSON.stringify(local.fields)
      ||['nodes','edges','asset_count','source_manifest_sha256'].some(k=>manifest[k]!==local[k])
      ||manifest.stored_bytes!==local.stored_bytes-old.asset.stored_bytes+status.asset.stored_bytes
      ||manifest.logical_bytes!==local.logical_bytes-JSON.stringify(local.publication_status).length+JSON.stringify(value).length)
      throw new Error('Public partition transform changed non-status content');
    bindings.set(p.local_manifest,{path:resolve(folder,p.local_manifest),sha256:hash(original),bytes:original.length});
    bindings.set(p.local_source_manifest,{path:resolve(folder,p.local_source_manifest),sha256:hash(source),bytes:source.length});
  }
  let bundle=null;
  if(manifest.bundle){
    const body=Buffer.concat(descriptors.map(a=>regular(files.get(a.file).path)));
    if(body.length!==manifest.bundle.bytes||hash(body)!==manifest.bundle.sha256)throw new Error('Partition bundle identity mismatch');
    bundle={...manifest.bundle,body};
  }
  // The reader guide fetches only these verified metadata values. Keep them
  // individually addressable even when the interactive view uses the bundle.
  const summaries=new Map();
  if(bundle)for(const name of ['coverage','scope']){
    const field=manifest.fields.find(f=>f.field===name);
    if(field?.kind==='value')summaries.set(field.asset.file,files.get(field.asset.file));
  }
  const outputFiles=new Map([...(bundle?[[bundle.file,{body:bundle.body,sha256:bundle.sha256,bytes:bundle.bytes}]]:files),...summaries,...bindings]);
  return {files,bindings,bundle,outputFiles,bytes:[...outputFiles.values()].reduce((n,f)=>n+f.bytes,0)};
}

export function readLocalPartitionSelection(taskRoot){
  const pointer=resolve(taskRoot,'global-local-partitions-current.json');
  if(!existsSync(pointer))return {directory:resolve(taskRoot,'global-local-partitions'),manifest_sha256:null};
  const raw=regular(pointer);
  if(raw.length>65536)throw new Error('Local partition selection exceeds bound');
  const value=JSON.parse(raw);
  if(value.schema_version!=='gapi-local-partition-selection/1'
    ||typeof value.directory!=='string'||!/^[A-Za-z0-9_-]+$/.test(value.directory)
    ||!/^[a-f0-9]{64}$/.test(value.manifest_sha256))throw new Error('Invalid local partition selection');
  return {directory:resolve(taskRoot,value.directory),manifest_sha256:value.manifest_sha256};
}

function selection(root,manifestPath) {
  const path = resolve(root,manifestPath);
  const manifestBytes = regular(path);
  const manifest = JSON.parse(manifestBytes);
  if(manifest.schema_version==='gapi-aggregate-partitions/1'){
    const partitions=readReleasePartitions(path,manifest);
    return {manifest:inside(root,path),manifest_sha256:hash(manifestBytes),
      partition_inputs:ordered(Object.fromEntries([...partitions.files,...partitions.bindings].map(([name,f])=>[inside(root,resolve(dirname(path),name)),f.sha256]))),
      partition_outputs:ordered(Object.fromEntries([...partitions.outputFiles].map(([name,f])=>[name,f.sha256]))),
      partition_bytes:partitions.bytes,logical_sha256:manifest.logical_sha256,
      publication_status:manifest.publication_status,derived_bundle:readDerivedBundle(root,manifest.derived_bundle)?.identity??null};
  }
  const snapshot=readReleaseSnapshot(path,manifest);
  return {manifest:inside(root,path),snapshot:inside(root,snapshot.path),
    ...(manifest.repository_snapshot ? {stored_snapshot_sha256:snapshot.stored_sha256} : {}),
    manifest_sha256:hash(manifestBytes),snapshot_sha256:hash(snapshot.bytes),
    publication_status:manifest.publication_status,
    derived_bundle:readDerivedBundle(root,manifest.derived_bundle)?.identity??null};
}

export function writeBuildProvenance({root,outDir,mode,manifestPath,base='/',expectedInputs}) {
  const inputs=sourceInputs(root);
  if (expectedInputs && !same(inputs,expectedInputs)) throw new Error('Build input changed during build');
  const value = {schema_version:'gapi-build-provenance/v1',mode,base,
    source_commit_at_build:git(root,'rev-parse','HEAD').toString().trim(),
    inputs,outputs:outputHashes(outDir),
    selection:mode === 'code-only' ? null : selection(root,manifestPath)};
  writeFileSync(resolve(outDir,PROVENANCE),JSON.stringify(value));
  return value;
}

export function verifyBuild({root,outDir,manifestPath,publish=false}) {
  const proof = JSON.parse(regular(resolve(outDir,PROVENANCE)));
  if (proof.schema_version !== 'gapi-build-provenance/v1') throw new Error('Missing/invalid build provenance');
  const inputs = sourceInputs(root);
  if (!same(proof.inputs,inputs)) throw new Error('Source inputs changed after build');
  const outputs = outputHashes(outDir);
  if (!same(proof.outputs,outputs)) throw new Error('Built output changed after provenance');
  if (proof.mode !== 'public-observations') {
    if (publish) throw new Error('Publication requires a public real-observation build');
    if (proof.mode !== 'code-only' && proof.mode !== 'local-only') throw new Error('Unknown build mode');
  }
  if (proof.mode !== 'code-only') {
    const current = selection(root,manifestPath);
    if (JSON.stringify(proof.selection) !== JSON.stringify(current)) throw new Error('Selected release changed after build');
    const built = JSON.parse(regular(resolve(outDir,'data/global/manifest.json')));
    if (JSON.stringify(built) !== JSON.stringify(JSON.parse(regular(resolve(root,current.manifest))))) throw new Error('Built/selected manifest mismatch');
    if(current.partition_inputs){
      for(const [name,expected] of Object.entries(current.partition_outputs))
        if(hash(regular(resolve(outDir,'data/global',name)))!==expected)throw new Error('Built partition mismatch');
    }else if (hash(regular(resolve(outDir,'data/global',built.snapshot))) !== current.snapshot_sha256) throw new Error('Built snapshot mismatch');
    if(current.derived_bundle)for(const [path,expected] of Object.entries(current.derived_bundle.assets??current.derived_bundle.inputs)){
      const name=path.split('/').at(-1);
      if(hash(regular(resolve(outDir,'data/derived',name)))!==expected)throw new Error('Built historical asset mismatch');
    }
  }
  if (publish) {
    if ('site-manifest.json' in outputs) throw new Error('Reserved deployment manifest already exists in build');
    if (proof.base !== '/global-academic-prestige-index/') throw new Error('Public deployment base is incorrect');
    if (proof.selection.publication_status !== 'public-observations-pending-external-review') throw new Error('Unapproved data publication status');
    if (proof.selection.manifest !== 'outputs/global/manifest.json' || (proof.selection.partition_inputs
      ? Object.keys(proof.selection.partition_inputs).some(path=>!path.startsWith('outputs/global/'))
      : !proof.selection.snapshot.startsWith('outputs/global/'))) throw new Error('Public release must use the reviewed aggregate directory');
    // A later data-only commit is valid if the current source bytes are committed.
    // A dirty source tree cannot be attributed to HEAD merely by labeling it so.
    for (const [path,expected] of Object.entries(inputs)) {
      let committed;
      try { committed=git(root,'show',`HEAD:${path}`); }
      catch { throw new Error(`Build input is not committed: ${path}`); }
      if (inputHash(path,committed) !== expected) throw new Error(`Build input differs from committed source: ${path}`);
    }
    for (const [path,expected] of [[proof.selection.manifest,proof.selection.manifest_sha256],...(proof.selection.partition_inputs
      ? Object.entries(proof.selection.partition_inputs)
      : [[proof.selection.snapshot,proof.selection.stored_snapshot_sha256??proof.selection.snapshot_sha256]])]) {
      if (hash(git(root,'show',`HEAD:${path}`)) !== expected) throw new Error(`Release input differs from committed source: ${path}`);
    }
    if(proof.selection.derived_bundle)for(const [path,expected] of Object.entries(proof.selection.derived_bundle.inputs)){
      if(hash(git(root,'show',`HEAD:${path}`))!==expected)throw new Error(`Historical input differs from committed source: ${path}`);
    }
  }
  const result = {status:'verified',mode:proof.mode,files:Object.keys(outputs).length,
    source_commit:git(root,'rev-parse','HEAD').toString().trim(),
    source_commit_at_build:proof.source_commit_at_build,outputs,
    provenance_sha256:hash(regular(resolve(outDir,PROVENANCE)))};
  if (publish) {
    if (Object.keys(outputs).some(path=>path.startsWith('data/local/'))) throw new Error('Private data in public site');
    const bytes=Object.keys(outputs).reduce((sum,path)=>sum+regular(resolve(outDir,path)).length,0);
    // The selected institution slice has its own 32 MiB input cap and exact
    // committed hash above. Keep a separate 8 MiB allowance for application
    // assets; growth of real observations must not admit unrelated extra files.
    const selectedBytes=proof.selection.partition_bytes??readReleaseSnapshot(resolve(root,manifestPath),JSON.parse(regular(resolve(root,manifestPath)))).bytes.length;
    const bound=8*1024*1024+selectedBytes+(proof.selection.derived_bundle?.bytes??0);
    if (bytes+regular(resolve(outDir,PROVENANCE)).length>bound) throw new Error('Public site exceeds reviewed byte bound');
    result.site_manifest={source_commit:result.source_commit,build_provenance_sha256:result.provenance_sha256,
      ranking_status:'observations-only-pending-independent-review',
      files:ordered({...outputs,[PROVENANCE]:result.provenance_sha256,'.nojekyll':hash(Buffer.alloc(0))})};
  }
  return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args[0] !== 'verify') throw new Error('Usage: node scripts/build-provenance.mjs verify [--publish]');
  const root = resolve(dirname(fileURLToPath(import.meta.url)),'..');
  console.log(JSON.stringify(verifyBuild({root,outDir:resolve(root,'dist'),
    manifestPath:process.env.GAPI_RELEASE_MANIFEST || 'outputs/global/manifest.json',publish:args.includes('--publish')})));
}
