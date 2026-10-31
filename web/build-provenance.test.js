import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {mkdirSync,mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {gzipSync} from 'node:zlib';
import {dirname,join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {SOURCE_FILES,PROVENANCE,sourceInputs,writeBuildProvenance,verifyBuild,readReleaseSnapshot} from '../scripts/build-provenance.mjs';
import {readDerivedBundle,readDerivedAsset} from '../scripts/derived-bundle.mjs';

function fixture(run) {
  const parent=process.platform === 'win32' ? 'D:/GAPI/global-academic-prestige-index' : tmpdir();
  const root=mkdtempSync(join(parent,'gapi-provenance-test-'));
  const put=(path,value)=>{mkdirSync(dirname(join(root,path)),{recursive:true});writeFileSync(join(root,path),value);};
  const git=(...args)=>execFileSync('git',args,{cwd:root,stdio:['ignore','pipe','pipe']});
  const commit=()=>{git('add','.');git('-c','user.name=GAPI Test','-c','user.email=test@example.invalid','-c','core.autocrlf=false','commit','-qm','Fixture');};
  try {
    for(const path of SOURCE_FILES)put(path,'fixture\n');
    put('web/main.js','export const fixture=1;\n');
    const snapshot=Buffer.from('{"dataset_kind":"real/global-public-observations"}');
    const sha=createHash('sha256').update(snapshot).digest('hex');
    const manifest={snapshot:'global-test.json',output_sha256:sha,publication_status:'public-observations-pending-external-review'};
    const manifestPath='outputs/global/manifest.json';
    put(manifestPath,JSON.stringify(manifest));put('outputs/global/global-test.json',snapshot);
    git('init','-q');commit();
    put('dist/index.html','<!doctype html>fixture');
    put('dist/assets/app.js','console.log("fixture")');
    put('dist/data/global/manifest.json',JSON.stringify(manifest));
    put('dist/data/global/global-test.json',snapshot);
    const args={root,outDir:join(root,'dist'),mode:'public-observations',manifestPath,base:'/global-academic-prestige-index/'};
    writeBuildProvenance(args);
    run({root,put,git,commit,args,verify:()=>verifyBuild({...args,publish:true})});
  } finally {rmSync(root,{recursive:true,force:true});}
}

test('release provenance is idempotent and accepts later data-only commits',()=>fixture(({put,commit,args,verify})=>{
  const first=verify();assert.equal(first.status,'verified');
  assert.deepEqual(verify(),first);
  const built=readFileSync(join(args.outDir,PROVENANCE));
  writeBuildProvenance(args);
  assert.deepEqual(readFileSync(join(args.outDir,PROVENANCE)),built);
  put('release-receipt.json','{"note":"later data-only receipt"}');commit();
  const current=verify();assert.equal(current.status,'verified');
  assert.notEqual(current.source_commit,current.source_commit_at_build);
}));

test('compressed repository snapshot keeps decoded output identity and committed storage proof',()=>fixture(({root,put,commit,args,verify})=>{
  const manifestPath=join(root,args.manifestPath),manifest=JSON.parse(readFileSync(manifestPath));
  const bytes=readFileSync(join(root,'outputs/global',manifest.snapshot)),packed=gzipSync(bytes);
  manifest.repository_snapshot={encoding:'gzip',filename:manifest.snapshot+'.gz',
    sha256:createHash('sha256').update(packed).digest('hex'),bytes:packed.length,decoded_bytes:bytes.length};
  put('outputs/global/'+manifest.repository_snapshot.filename,packed);
  put(args.manifestPath,JSON.stringify(manifest));put('dist/data/global/manifest.json',JSON.stringify(manifest));
  rmSync(join(root,'outputs/global',manifest.snapshot));commit();writeBuildProvenance(args);
  assert.equal(verify().status,'verified');
  assert.deepEqual(readReleaseSnapshot(manifestPath,manifest).bytes,bytes);
  const first=readFileSync(join(args.outDir,PROVENANCE));writeBuildProvenance(args);
  assert.deepEqual(readFileSync(join(args.outDir,PROVENANCE)),first);
  const bad={...manifest,repository_snapshot:{...manifest.repository_snapshot,filename:'../escape.gz'}};
  assert.throws(()=>readReleaseSnapshot(manifestPath,bad),/Invalid repository/);
  for(const change of [{encoding:'br'},{sha256:'0'.repeat(64)},{bytes:1},{decoded_bytes:1}]){
    assert.throws(()=>readReleaseSnapshot(manifestPath,{...manifest,repository_snapshot:{...manifest.repository_snapshot,...change}}));
  }
  put('outputs/global/'+manifest.repository_snapshot.filename,Buffer.from('tampered'));
  assert.throws(verify,/identity mismatch/);
}));

test('source/output/selected manifest tampering fail before publication',()=>{
  for(const target of ['web/main.js','dist/assets/app.js','outputs/global/manifest.json','dist/data/global/global-test.json']) {
    fixture(({put,verify})=>{
      put(target,'modified');
      assert.throws(verify,/changed|mismatch|Unexpected|JSON/);
    });
  }
});

test('uncommitted build source cannot be labeled as published HEAD',()=>fixture(({put,args,verify})=>{
  put('web/main.js','export const fixture=2;\n');
  writeBuildProvenance(args);
  assert.throws(verify,/differs from committed/);
}));

test('build inputs cannot change while building and text line endings remain portable',()=>fixture(({root,put,args,verify})=>{
  const before=sourceInputs(root);
  put('web/main.js','export const fixture=2;\n');
  assert.throws(()=>writeBuildProvenance({...args,expectedInputs:before}),/during build/);
  put('web/main.js','export const fixture=1;\r\n');
  assert.deepEqual(sourceInputs(root),before);
  writeBuildProvenance({...args,expectedInputs:before});
  assert.equal(verify().status,'verified');
}));

test('code-only and wrong-base builds cannot be published',()=>fixture(({args,verify})=>{
  writeBuildProvenance({...args,mode:'code-only'});
  assert.throws(verify,/public real-observation/);
  writeBuildProvenance({...args,base:'/'});
  assert.throws(verify,/base/);
}));

test('bundled font input identity covers binary bytes without UTF-8 replacement',()=>fixture(({put,commit,args,verify})=>{
  put('web/fonts/test.woff2',Buffer.from([0x77,0x4f,0x46,0x32,0x80]));commit();
  writeBuildProvenance(args);assert.equal(verify().status,'verified');
  put('web/fonts/test.woff2',Buffer.from([0x77,0x4f,0x46,0x32,0x81]));
  assert.throws(verify,/Source inputs changed/);
}));

test('committed real slices larger than 2 MiB remain verifiable',()=>fixture(({put,commit,args,verify})=>{
  const snapshot=JSON.stringify({dataset_kind:'real/global-public-observations',padding:'x'.repeat(2*1024*1024)});
  const manifest={snapshot:'global-test.json',output_sha256:createHash('sha256').update(snapshot).digest('hex'),publication_status:'public-observations-pending-external-review'};
  for(const prefix of ['outputs/global','dist/data/global']){
    put(`${prefix}/global-test.json`,snapshot);put(`${prefix}/manifest.json`,JSON.stringify(manifest));
  }
  commit();writeBuildProvenance(args);
  assert.equal(verify().status,'verified');
}));

test('large verified observation slice has a separate bound from application assets',()=>fixture(({put,commit,args,verify})=>{
  const snapshot=JSON.stringify({dataset_kind:'real/global-public-observations',padding:'x'.repeat(10*1024*1024)});
  const manifest={snapshot:'global-test.json',output_sha256:createHash('sha256').update(snapshot).digest('hex'),publication_status:'public-observations-pending-external-review'};
  for(const prefix of ['outputs/global','dist/data/global']){
    put(`${prefix}/global-test.json`,snapshot);put(`${prefix}/manifest.json`,JSON.stringify(manifest));
  }
  commit();writeBuildProvenance(args);assert.equal(verify().status,'verified');
  put('dist/assets/unrelated.bin',Buffer.alloc(9*1024*1024));
  writeBuildProvenance(args);assert.throws(verify,/reviewed byte bound/);
}));

test('complete historical bundle is identity-bound, committed and emitted without relaxing legacy limits',()=>fixture(({root,put,commit,args,verify})=>{
  const base=new URL('../outputs/easy-orcid-web/',import.meta.url),raw=readFileSync(new URL('manifest.json',base));
  const historical=JSON.parse(raw),selection={manifest:'outputs/easy-orcid-web/manifest.json',sha256:createHash('sha256').update(raw).digest('hex')};
  const files=new Set(['manifest.json',historical.nodes.file,historical.overview.file,...historical.edge_chunks.map(a=>a.file)]);
  for(const name of files){
    const bytes=name==='manifest.json'?raw:readDerivedAsset(fileURLToPath(new URL(name,base)),32*1024*1024).bytes;
    put(`outputs/easy-orcid-web/${name}${name===historical.nodes.file?'.gz':''}`,name===historical.nodes.file?gzipSync(bytes):bytes);
    put(`dist/data/derived/${name}`,bytes);
  }
  const main=JSON.parse(readFileSync(join(root,args.manifestPath)));main.derived_bundle=selection;
  for(const path of [args.manifestPath,'dist/data/global/manifest.json'])put(path,JSON.stringify(main));
  commit();writeBuildProvenance(args);assert.equal(verify().status,'verified');
  const storedPath=`outputs/easy-orcid-web/${historical.nodes.file}.gz`,stored=readFileSync(join(root,storedPath));
  put(storedPath,'tampered');assert.throws(verify);put(storedPath,stored);assert.equal(verify().status,'verified');
  assert.throws(()=>readDerivedBundle(root,{...selection,manifest:'../private.json'}),/Unsafe/);
  put(`dist/data/derived/${historical.nodes.file}`,'tampered');
  assert.throws(verify,/output changed/);
}));
