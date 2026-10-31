import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';

test('bundled Chinese subsets cover UI characters and retain licensed hash identities',()=>{
 const coverage=JSON.parse(readFileSync('web/fonts/cjk-coverage.json','utf8'));
 const text=['index.html','vite.config.js',...readdirSync('web').filter(x=>x.endsWith('.js')&&!x.endsWith('.test.js')).map(x=>'web/'+x)]
  .map(x=>readFileSync(x,'utf8')).join('');
 const required=[...new Set(text.match(/[\u3000-\u303f\u3400-\u9fff\uf900-\ufaff\uff01-\uff60]/g)||[])];
 const names=new Intl.DisplayNames(['zh-CN'],{type:'region'});
 for(let a=65;a<=90;a++)for(let b=65;b<=90;b++)required.push(...(names.of(String.fromCharCode(a,b)).match(/[\u3400-\u9fff]/g)||[]));
 const supported=new Set(coverage.assets.flatMap(asset=>asset.codepoints));
 assert.deepEqual(required.filter(char=>!supported.has(char.codePointAt(0))),[]);
 assert.equal(coverage.license,'OFL-1.1');
 const css=readFileSync('web/fonts/cjk.css','utf8');
 for(const asset of coverage.assets){
  const body=readFileSync('web/fonts/'+asset.file);
  assert.equal(body.subarray(0,4).toString(),'wOF2');
  assert.equal(body.length,asset.bytes);
  assert.equal(createHash('sha256').update(body).digest('hex'),asset.sha256);
  for(const point of asset.codepoints)assert.ok(css.includes('U+'+point.toString(16).toUpperCase()));
 }
 for(const family of ['GAPI Song','DM Sans','DM Mono'])assert.ok(css.includes(`font-family:'${family}'`));
 assert.ok(readFileSync('web/style.css','utf8').startsWith("@import './fonts/cjk.css';"));
});
