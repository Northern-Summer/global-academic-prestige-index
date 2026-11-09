import test from 'node:test';
import assert from 'node:assert/strict';
import {processingText,validateProcessing} from './record-processing.js';
import {readFileSync} from 'node:fs';

const coverage=()=>({events:3,processing:{schema_version:'record-governance/1',complete:true,total_events:3,
 primary:{identified:1,missing_institution:2},changes:{newly_identified_events:1},flags:{missing_institution:2},
 history:{retained_historical_people:2,current_people_with_retained_history:1},funnel:{people_contributing_relations:1},
 historical_records_restored_into_current:0,independent_evidence_promotions:0}});
test('complete disposition and missing older summaries remain distinct',()=>{
 assert.match(processingText(coverage())[0],/本版收录/);
 assert.match(processingText({events:3})[0],/没有按原因细分/);
});
test('reject incomplete, mismatched, hostile and promoted summaries',()=>{
 for(const mutate of [c=>c.processing.primary.identified=2,c=>c.processing.complete=false,c=>c.processing.flags.bad=-1,
   c=>c.processing.primary.identified=NaN,c=>c.processing.independent_evidence_promotions=1,
   c=>c.processing.changes.newly_identified_events=4,c=>c.processing.funnel.people_contributing_relations=4,
   c=>c.processing.history.current_people_with_retained_history=3]){
  const c=coverage();mutate(c);assert.throws(()=>validateProcessing(c));
 }
});
test('fixed slots and mutually exclusive lazy module imports replace insertion race',()=>{
 const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');
 const main=readFileSync(new URL('./main.js',import.meta.url),'utf8');
 assert.ok(html.indexOf('id="current-observation-slot"')<html.indexOf('id="historical-observation-slot"'));
 assert.match(main,/if\(historyView\) await import\('\.\/derived\.js'\);\s*else await import\('\.\/global\.js'\)/);
 assert.doesNotMatch(main,/^import '\.\/(global|derived)\.js'/m);
});
