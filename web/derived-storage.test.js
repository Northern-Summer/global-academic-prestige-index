import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync,symlinkSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {gzipSync} from 'node:zlib';
import {readDerivedAsset} from '../scripts/derived-bundle.mjs';

function fixture(run){const root=mkdtempSync(join(tmpdir(),'gapi-asset-'));try{run(join(root,'asset.json'));}finally{rmSync(root,{recursive:true,force:true});}}
test('historical compressed assets preserve exact JSON and input identity',()=>fixture(path=>{
  const raw=Buffer.from('["全球机构",1]'),packed=gzipSync(raw);writeFileSync(path+'.gz',packed);
  const result=readDerivedAsset(path,raw.length);
  assert.deepEqual(result.bytes,raw);assert.deepEqual(result.stored,packed);assert.equal(result.path,path+'.gz');
}));
test('historical plain assets remain authoritative when both exist',()=>fixture(path=>{
  writeFileSync(path,'old');writeFileSync(path+'.gz',gzipSync('replacement'));
  assert.equal(readDerivedAsset(path,100).bytes.toString(),'old');
}));
test('historical compressed corruption and oversized decode fail closed',()=>fixture(path=>{
  writeFileSync(path+'.gz',Buffer.from('corrupt'));assert.throws(()=>readDerivedAsset(path,100));
  writeFileSync(path+'.gz',gzipSync('x'.repeat(100000)));assert.throws(()=>readDerivedAsset(path,1000));
}));
test('historical directories cannot be treated as a missing plain asset',()=>fixture(path=>{
  const parent=join(path,'..');symlinkSync(parent,path,'junction');
  assert.throws(()=>readDerivedAsset(path,100),/Invalid historical/);
}));
