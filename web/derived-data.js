import {verifySnapshotBytes} from './snapshot.js';

export const NODE_COLUMNS=['institution_id','name','country','network_score','relations','observed_order','chunks','institution_identity_status','unranked_reasons','region'];
export const REGIONS=['Asia','Europe','Africa','Oceania','North America','South America','Antarctica'];
export const STAGES=['bachelor','master','doctorate','postdoc'];
export const DERIVED_BYTES=24*1024*1024;
const integer=n=>Number.isSafeInteger(n)&&n>=0;
export function validateAsset(a,max=2*1024*1024){
  if(!a||!/^[a-f0-9]{64}$/.test(a.sha256)||a.file!==`${a.sha256}.json`||!integer(a.bytes)||a.bytes>max)throw new Error('Invalid bounded derived asset');
  return a;
}
export function validateDerivedManifest(m){
  if(m?.schema_version!=='gapi-derived-browser/v1'||m.source!=='easy-orcid-2023.6-full'
    ||m.publication_status!=='public-observations-pending-external-review'||m.formal_ranking!==false||m.global_complete!==false
    ||m.source_census?.source_complete!==true||m.source_census?.all_hq_projections_present!==true
    ||m.direction!=='employer -> training'||JSON.stringify(m.node_columns)!==JSON.stringify(NODE_COLUMNS)
    ||JSON.stringify(m.edge_columns)!==JSON.stringify(['source_index','target_index',...STAGES]))throw new Error('Invalid complete historical observation contract');
  for(const key of ['node_count','edge_count','distinct_retained_people','deduplicated_relations','resolved_institutions','unresolved_institution_identifiers'])
    if(!integer(m[key]))throw new Error('Invalid historical coverage count');
  if(m.node_count>200000||m.resolved_institutions+m.unresolved_institution_identifiers!==m.node_count
    ||!Array.isArray(m.edge_chunks)||m.edge_chunks.length>200||m.edge_chunks.length!==Math.ceil(m.edge_count/10000)
    ||!Number.isFinite(m.probability_mass)||Math.abs(m.probability_mass-1)>1e-9
    ||JSON.stringify(m.missing_scores)!==JSON.stringify(['award_score','composite_score','formal_rank']))throw new Error('Invalid historical network bounds');
  const funnel=m.person_level_funnel;
  if(!funnel||!integer(funnel.people_with_identified_training_and_academic_job)||!integer(funnel.people_with_required_dates_on_both_sides)||!integer(funnel.people_contributing_relations)
    ||m.distinct_retained_people<funnel.people_with_identified_training_and_academic_job
    ||funnel.people_with_identified_training_and_academic_job<funnel.people_with_required_dates_on_both_sides
    ||funnel.people_with_required_dates_on_both_sides<funnel.people_contributing_relations
    ||!integer(m.source_census.counts?.source_rows)||m.source_census.counts.source_rows<m.distinct_retained_people)throw new Error('Invalid historical people funnel');
  validateAsset(m.nodes,32*1024*1024);validateAsset(m.overview);m.edge_chunks.forEach(a=>validateAsset(a));
  const assets=[m.nodes,m.overview,...m.edge_chunks],unique=new Map(assets.map(a=>[a.file,a.bytes]));
  if([...unique.values()].reduce((a,b)=>a+b,0)>DERIVED_BYTES)throw new Error('Derived assets exceed total byte budget');
  return m;
}
export function decodeNodes(rows,m){
  if(!Array.isArray(rows)||rows.length!==m.node_count)throw new Error('Historical node count mismatch');
  const ids=new Set();let mass=0;
  const nodes=rows.map((r,i)=>{
    if(!Array.isArray(r)||r.length!==10||!(r[9]===null||REGIONS.includes(r[9]))||typeof r[0]!=='string'||typeof r[1]!=='string'||ids.has(r[0])
      ||!(r[2]===null||/^[A-Z]{2}$/.test(r[2]))||!Number.isFinite(r[3])||r[3]<0||r[3]>1||!integer(r[4])
      ||r[5]!==i+1||!Array.isArray(r[6])||r[6].some((n,j)=>!integer(n)||n>=m.edge_chunks.length||(j>0&&n<=r[6][j-1]))
      ||!['resolved_exact_ror','unresolved_external_identifier'].includes(r[7])||!Array.isArray(r[8])||r[8].some(v=>typeof v!=='string'))throw new Error('Invalid historical institution');
    ids.add(r[0]);mass+=r[3];
    return {institution_id:r[0],name:r[1],country:r[2],network_score:r[3],relations:r[4],observed_order:r[5],chunks:r[6],identity:r[7],unranked_reasons:r[8],region:r[9],index:i};
  });
  if(Math.abs(mass-1)>1e-9||nodes.filter(n=>n.identity==='resolved_exact_ror').length!==m.resolved_institutions)throw new Error('Historical score/identity totals mismatch');
  return nodes;
}
export function decodeEdges(rows,nodes,maximum=10000){
  if(!Array.isArray(rows)||rows.length>maximum)throw new Error('Historical edge chunk exceeds bound');
  return rows.map(r=>{
    if(!Array.isArray(r)||r.length!==6||!integer(r[0])||!integer(r[1])||!nodes[r[0]]||!nodes[r[1]]
      ||r.slice(2).some(v=>!Number.isFinite(v)||v<0)||r.slice(2).reduce((a,b)=>a+b,0)<=0)throw new Error('Invalid historical edge');
    return {source:nodes[r[0]].institution_id,target:nodes[r[1]].institution_id,weight:r.slice(2).reduce((a,b)=>a+b,0),stages:Object.fromEntries(STAGES.map((s,i)=>[s,r[i+2]]))};
  });
}
export async function fetchJSON(url,max,signal){
  const response=await fetch(url,{signal});
  if(!response.ok){await response.body?.cancel();throw new Error(`HTTP ${response.status}`);}
  const chunks=[];let bytes=0;const reader=response.body.getReader();
  try{while(true){const {done,value}=await reader.read();if(done)break;bytes+=value.length;if(bytes>max)throw new Error('Response exceeds byte budget');chunks.push(value);}}
  finally{await reader.cancel();reader.releaseLock();}
  const raw=new Uint8Array(bytes);let offset=0;for(const part of chunks){raw.set(part,offset);offset+=part.length;}
  return {raw,value:JSON.parse(new TextDecoder().decode(raw))};
}
export async function fetchAsset(base,asset,signal){
  validateAsset(asset,32*1024*1024);
  const {raw,value}=await fetchJSON(new URL(asset.file,base),asset.bytes,signal);
  if(raw.length!==asset.bytes)throw new Error('Derived asset length mismatch');
  await verifySnapshotBytes(raw,{output_sha256:asset.sha256});return value;
}
