import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {mkdirSync,mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {tmpdir} from 'node:os';
import {SOURCE_FILES,PROVENANCE,sourceInputs,writeBuildProvenance,verifyBuild} from '../scripts/build-provenance.mjs';

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
