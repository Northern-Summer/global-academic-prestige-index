/* One owner for build-input identity and local publication preflight.
 * Source hashes normalize CRLF/LF, matching Git's text normalization. Output
 * hashes cover exact served bytes. This is provenance, not signed attestation.
 */
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {existsSync,lstatSync,readdirSync,readFileSync,writeFileSync} from 'node:fs';
import {dirname,isAbsolute,relative,resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {readDerivedBundle} from './derived-bundle.mjs';

export const DOCUMENTATION = ['METHODOLOGY.md','DATA_SOURCES.md','DATA_SCHEMA.md','BIAS_AND_LIMITATIONS.md','RED_TEAM_REPORT.md','STREAMING.md','CI_CD.md','OOD.md','FONTS.md','EASY_ORCID_2023_OBSERVATIONS.md'];
export const SOURCE_FILES = ['index.html','vite.config.js','package.json','package-lock.json','scripts/build-provenance.mjs','scripts/derived-bundle.mjs','scripts/deploy-pages.mjs','.github/workflows/ci.yml',...DOCUMENTATION.map(name=>`docs/${name}`)];
export const PROVENANCE = 'build-provenance.json';
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const sourceHash = bytes => hash(Buffer.from(bytes.toString('utf8').replaceAll('\r\n','\n')));
const inputHash = (path,bytes) => path.endsWith('.woff2') ? hash(bytes) : sourceHash(bytes);
const ordered = value => Object.fromEntries(Object.entries(value).sort(([a],[b])=>a.localeCompare(b,'en')));
const same = (a,b) => JSON.stringify(ordered(a)) === JSON.stringify(ordered(b));

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

function selection(root,manifestPath) {
  const path = resolve(root,manifestPath);
  const manifestBytes = regular(path);
  const manifest = JSON.parse(manifestBytes);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.json$/.test(manifest.snapshot)) throw new Error('Unsafe release snapshot');
  const snapshotPath = resolve(dirname(path),manifest.snapshot);
  const snapshotBytes = regular(snapshotPath);
  if (hash(snapshotBytes) !== manifest.output_sha256) throw new Error('Selected snapshot identity mismatch');
  return {manifest:inside(root,path),snapshot:inside(root,snapshotPath),
    manifest_sha256:hash(manifestBytes),snapshot_sha256:hash(snapshotBytes),
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
    if (hash(regular(resolve(outDir,'data/global',built.snapshot))) !== current.snapshot_sha256) throw new Error('Built snapshot mismatch');
    if(current.derived_bundle)for(const [path,expected] of Object.entries(current.derived_bundle.inputs)){
      const name=path.split('/').at(-1);
      if(hash(regular(resolve(outDir,'data/derived',name)))!==expected)throw new Error('Built historical asset mismatch');
    }
  }
  if (publish) {
    if ('site-manifest.json' in outputs) throw new Error('Reserved deployment manifest already exists in build');
    if (proof.base !== '/global-academic-prestige-index/') throw new Error('Public deployment base is incorrect');
    if (proof.selection.publication_status !== 'public-observations-pending-external-review') throw new Error('Unapproved data publication status');
    if (proof.selection.manifest !== 'outputs/global/manifest.json' || !proof.selection.snapshot.startsWith('outputs/global/')) throw new Error('Public release must use the reviewed aggregate directory');
    // A later data-only commit is valid if the current source bytes are committed.
    // A dirty source tree cannot be attributed to HEAD merely by labeling it so.
    for (const [path,expected] of Object.entries(inputs)) {
      let committed;
      try { committed=git(root,'show',`HEAD:${path}`); }
      catch { throw new Error(`Build input is not committed: ${path}`); }
      if (inputHash(path,committed) !== expected) throw new Error(`Build input differs from committed source: ${path}`);
    }
    for (const [path,expected] of [[proof.selection.manifest,proof.selection.manifest_sha256],[proof.selection.snapshot,proof.selection.snapshot_sha256]]) {
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
    const bound=8*1024*1024+(proof.selection.derived_bundle?.bytes??0);
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
