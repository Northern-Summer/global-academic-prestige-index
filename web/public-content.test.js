import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {institutionCSV,shareExplanation,yearExplanation,sourceEdition} from './public-content.js';

test('editions derive from source identity rather than the current calendar or stale template',()=>{
 assert.equal(sourceEdition({publication:{sources:[{source:'orcid-2026-summary'}]}}).snapshot,'2026');
 assert.equal(sourceEdition({publication:{sources:[{source:'wikidata'}]}}).snapshot,'current');
 assert.equal(sourceEdition({}, {source_vintages:[{source:'orcid-2027-summary'}]}).label,'2027 年度来源');
 assert.equal(sourceEdition({publication:{release_version:'<script>'}}).release,null);
 assert.throws(()=>sourceEdition({publication:{sources:[{source:'orcid-2026-summary'}]}},{source_vintages:[{source:'orcid-2023-summary'}]}),/mismatch/);
});

test('public exports describe snapshot and reference values without empty future-score columns',()=>{
 const rows=[{institution_id:'ror:x',name:'=malicious',country:'CN',observed_rank:2,network_score:.00745,relations:3}];
 const text=institutionCSV(rows,{snapshot:'2026',scenario:'stage_only_doctorate',ranks:{'ror:x':1}});
 assert.match(text,/参考网络份额（%）/);assert.match(text,/研究方案顺序/);
 assert.match(text,/0\.745/);assert.match(text,/'=malicious/);
 assert.doesNotMatch(text,/formal_rank|composite_score|待核验/);
 assert.match(shareExplanation,/不是就业率/);assert.match(yearExplanation,/不能.*相加/);
});
test('public link destinations are readable HTML with return navigation',()=>{
 for(const file of ['../index.html','./global.js','./derived.js']){
  const source=readFileSync(new URL(file,import.meta.url),'utf8');
  assert.doesNotMatch(source,/href=["'][^"']*\.md(?:["'#?])/);
 }
 const guide=readFileSync(new URL('../guide.html',import.meta.url),'utf8');
 for(const section of ['reading','method','sources','coverage','limits'])assert.match(guide,new RegExp(`id="${section}"`));
 assert.match(guide,/返回最新观察/);
 assert.doesNotMatch(guide,/Task\.used|D:\\|configs\/|Not conducted|内部调试/);
});
test('production cannot overwrite the introduction with a stale historical edition',()=>{
 const vite=readFileSync(new URL('../vite.config.js',import.meta.url),'utf8');
 assert.doesNotMatch(vite,/THE CURRENT EDITION|2023 年公开履历已完成/);
 assert.match(vite,/guide:resolve\(buildRoot,'guide.html'\)/);
});
