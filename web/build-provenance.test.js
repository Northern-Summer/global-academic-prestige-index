import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {mkdirSync,mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {gzipSync} from 'node:zlib';
import {dirname,join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {SOURCE_FILES,PROVENANCE,sourceInputs,writeBuildProvenance,verifyBuild,readReleaseSnapshot,readReleasePartitions,readLocalPartitionSelection} from '../scripts/build-provenance.mjs';
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

test('local partition selection preserves predecessor default and rejects unsafe or unpinned choices',()=>fixture(({root,put})=>{
  assert.equal(readLocalPartitionSelection(root).directory,join(root,'global-local-partitions'));
  const selected={schema_version:'gapi-local-partition-selection/1',directory:'global-local-partitions-additions',manifest_sha256:'a'.repeat(64)};
  put('global-local-partitions-current.json',JSON.stringify(selected));
  assert.deepEqual(readLocalPartitionSelection(root),{directory:join(root,selected.directory),manifest_sha256:selected.manifest_sha256});
  for(const change of [{directory:'../escape'},{directory:'D:/escape'},{directory:''},{manifest_sha256:null},{schema_version:'unknown'}]){
    put('global-local-partitions-current.json',JSON.stringify({...selected,...change}));
    assert.throws(()=>readLocalPartitionSelection(root),/Invalid local partition selection/);
  }
}));

function partitionFixture(run){fixture(context=>{
  const {root,put,commit,args}=context,hash=b=>createHash('sha256').update(b).digest('hex');
  const raw=Buffer.from(JSON.stringify('local-only-source-license-review')),packed=gzipSync(raw),file=hash(raw)+'.json.gz';
  const asset={file,encoding:'gzip',bytes:raw.length,sha256:hash(raw),stored_bytes:packed.length,stored_sha256:hash(packed)};
  const manifest={schema_version:'gapi-aggregate-partitions/1',logical_schema_version:'gapi-global-observed/v1',
    publication_status:'local-only-source-license-review',logical_sha256:'a'.repeat(64),source_manifest_sha256:'b'.repeat(64),
    logical_bytes:200,nodes:0,edges:0,stored_bytes:packed.length,asset_count:1,
    fields:[{field:'edges',kind:'rows',rows:0,assets:[]},{field:'nodes',kind:'rows',rows:0,assets:[]},{field:'publication_status',kind:'value',asset}]};
  put(args.manifestPath,JSON.stringify(manifest));put('outputs/global/'+file,packed);
  put('dist/data/global/manifest.json',JSON.stringify(manifest));put('dist/data/global/'+file,packed);commit();
  args.mode='local-only';writeBuildProvenance(args);
  run({...context,manifest,asset,packed});
});}

test('partition build binds all stored inputs and refuses local-only publication',()=>partitionFixture(({args,root,asset})=>{
  const proof=JSON.parse(readFileSync(join(args.outDir,PROVENANCE)));
  assert.equal(proof.selection.partition_inputs['outputs/global/'+asset.file],asset.stored_sha256);
  assert.equal(verifyBuild({...args,publish:false}).status,'verified');
  assert.throws(()=>verifyBuild({...args,publish:true}),/public real-observation build/);
  assert.equal(readReleasePartitions(join(root,args.manifestPath),JSON.parse(readFileSync(join(root,args.manifestPath)))).bytes,asset.stored_bytes);
}));

test('partition bundle is derived from exact committed asset order and rejects a wrong hash',()=>partitionFixture(({args,root,manifest,asset,packed})=>{
  const bundle=Buffer.concat([packed]);
  // The fixture contains one status value and two empty row fields.
  const sha=createHash('sha256').update(bundle).digest('hex');
  const bundled={...manifest,bundle:{format:'gapi-partition-concat/1',file:sha+'.gapi',bytes:bundle.length,sha256:sha}};
  const result=readReleasePartitions(join(root,args.manifestPath),bundled);
  assert.deepEqual(result.bundle.body,bundle);assert.equal(result.bundle.sha256,sha);
  assert.equal(result.bytes,bundle.length);
  assert.deepEqual([...result.outputFiles.keys()],[bundled.bundle.file]);
  const wrong={...bundled,bundle:{...bundled.bundle,sha256:'c'.repeat(64),file:'c'.repeat(64)+'.gapi'}};
  assert.throws(()=>readReleasePartitions(join(root,args.manifestPath),wrong),/bundle identity/);
}));

test('bundled build requires the complete transfer and does not require duplicate served shards',()=>partitionFixture(({args,root,manifest,packed,put,asset})=>{
  const sha=createHash('sha256').update(packed).digest('hex');
  const bundled={...manifest,bundle:{format:'gapi-partition-concat/1',file:sha+'.gapi',bytes:packed.length,sha256:sha}};
  put(args.manifestPath,JSON.stringify(bundled));put('dist/data/global/manifest.json',JSON.stringify(bundled));
  writeBuildProvenance(args);
  assert.throws(()=>verifyBuild({...args,publish:false}),/ENOENT/);
  put('dist/data/global/'+bundled.bundle.file,packed);
  // Source shards remain committed inputs; only their served copies retire.
  rmSync(join(root,'dist/data/global/'+asset.file));
  writeBuildProvenance(args);
  assert.equal(verifyBuild({...args,publish:false}).status,'verified');
  put('dist/data/global/'+bundled.bundle.file,Buffer.alloc(packed.length));
  writeBuildProvenance(args); // A regenerated receipt cannot bless corrupt data.
  assert.throws(()=>verifyBuild({...args,publish:false}),/Built partition mismatch/);
}));

test('bundled production emits hash-bound guide metadata without requiring a full graph download',()=>partitionFixture(({args,root,manifest,packed,put})=>{
  const hash=b=>createHash('sha256').update(b).digest('hex'),metadata=[];
  for(const [field,value] of [['coverage',{events:3,people:2}],['scope',{source_vintages:[{source:'orcid-2026-summary'}]}]]){
    const raw=Buffer.from(JSON.stringify(value)),body=gzipSync(raw);
    const asset={file:hash(raw)+'.json.gz',encoding:'gzip',bytes:raw.length,sha256:hash(raw),stored_bytes:body.length,stored_sha256:hash(body)};
    put('outputs/global/'+asset.file,body);metadata.push({field,kind:'value',asset,body});
  }
  const fields=[...manifest.fields,...metadata.map(({body,...f})=>f)].sort((a,b)=>a.field<b.field?-1:1);
  const body=Buffer.concat(fields.filter(f=>f.kind==='value').map(f=>f.field==='publication_status'?packed:metadata.find(v=>v.field===f.field).body));
  const updated={...manifest,fields,stored_bytes:body.length,asset_count:3,bundle:{format:'gapi-partition-concat/1',file:hash(body)+'.gapi',bytes:body.length,sha256:hash(body)}};
  put(args.manifestPath,JSON.stringify(updated));put('dist/data/global/manifest.json',JSON.stringify(updated));
  const selected=readReleasePartitions(join(root,args.manifestPath),updated);
  for(const item of metadata)assert.ok(selected.outputFiles.has(item.asset.file),'guide metadata must have a real production route');
  for(const [name,item] of selected.outputFiles)put('dist/data/global/'+name,item.body??readFileSync(item.path));
  writeBuildProvenance(args);assert.equal(verifyBuild({...args,publish:false}).status,'verified');
  rmSync(join(root,'dist/data/global',metadata[0].asset.file));
  assert.throws(()=>verifyBuild({...args,publish:false}),/Built output changed after provenance/);
}));

test('partition provenance rejects changed source and built assets',()=>partitionFixture(({args,root,put,asset,packed})=>{
  put('outputs/global/'+asset.file,Buffer.alloc(packed.length));
  assert.throws(()=>verifyBuild({...args,publish:false}),/partition identity mismatch/);
  put('outputs/global/'+asset.file,packed);
  put('dist/data/global/'+asset.file,Buffer.alloc(packed.length));
  assert.throws(()=>verifyBuild({...args,publish:false}),/Built output changed/);
}));

test('changing a partition label cannot grant public release rights',()=>partitionFixture(({args,root,manifest})=>{
  assert.throws(()=>readReleasePartitions(join(root,args.manifestPath),{...manifest,publication_status:'public-observations-pending-external-review'}),/partition manifest/);
  assert.throws(()=>readReleasePartitions(join(root,args.manifestPath),{...manifest,stored_bytes:manifest.stored_bytes+1}),/totals mismatch/);
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
