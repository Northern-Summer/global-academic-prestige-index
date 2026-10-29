import { verifySnapshotBytes } from './snapshot.js';
import { csv, networkSlice, selectRows, validateGlobal } from './global-data.js';

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const root = document.createElement('section');
root.className = 'panel global-panel'; root.id = 'global-observations'; root.setAttribute('aria-labelledby', 'global-title');
root.innerHTML = `<p class="eyebrow">GLOBAL / REAL PUBLIC OBSERVATIONS</p><h2 id="global-title">Global career observations</h2>
<p id="global-status" role="status">Loading the real observation slice…</p>
<p>Observed scores describe structurally validated source statements in this slice. Independent corroboration and formal ranking eligibility are separate. Missing awards and composite scores stay missing.</p>
<div class="table-tools"><label>Search<input type="search" id="global-search" autocomplete="off"></label>
<label>Country<select id="global-country"><option value="">All observed countries</option></select></label>
<label>Sort<select id="global-sort"><option value="score">Observed network score</option><option value="relations">Observed relations</option><option value="name">Institution name</option></select></label>
<label>Rank scenario<select id="global-scenario"><option value="">Reference α = .85</option><option value="alpha_0.65">α = .65</option><option value="alpha_0.95">α = .95</option></select></label>
<button id="global-csv" type="button">Export filtered institutions</button></div>
<div class="table-wrap" tabindex="0" aria-label="Global real observation table"><table class="directory-table"><thead><tr><th>Observed rank</th><th>Institution</th><th>Network score (α=.85)</th><th>Awards / composite</th><th>Relations / formal qualification</th></tr></thead><tbody id="global-rows"></tbody></table></div>
<div class="directory-pagination"><span id="global-page-status"></span><div><button type="button" id="global-prev" aria-label="Previous global page">←</button><button type="button" id="global-next" aria-label="Next global page">→</button></div></div>
<aside id="global-details" aria-live="polite"><p>Select an institution to inspect score contributions and qualification reasons.</p></aside>
<div class="table-tools"><label>Network stage<select id="global-stage"><option value="all">All stages</option><option value="doctorate">Doctorate</option><option value="master">Master</option><option value="bachelor">Bachelor</option><option value="postdoc">Postdoctoral training</option></select></label>
<button type="button" id="global-show-network">Load 3D network</button><button type="button" id="global-reset">Show whole observed slice</button></div>
<p>Arrows: employer → training institution. Node size: reference network score. Color: region; gray means unknown. Position is a layout, not geography or chronology. At most 500 nodes / 2,000 edges are displayed.</p>
<div id="global-network" role="region" aria-label="Optional 3D network"></div><p id="global-edge-status" role="status"></p>
<div class="table-wrap" tabindex="0" aria-label="Directional relationships"><table class="directory-table"><thead><tr><th>Employer →</th><th>Training institution</th><th>Stage-weighted count</th></tr></thead><tbody id="global-edges"></tbody></table></div>`;
document.querySelector('.metric-strip').before(root);
const $ = selector => root.querySelector(selector);
let data, page = 0, selected = '', dispose, networkEnabled = false, networkGeneration = 0;
const fields = () => ({query:$('#global-search').value, country:$('#global-country').value, sort:$('#global-sort').value, scenario:$('#global-scenario').value});
const value = v => v == null ? 'Missing' : v.toFixed(8);

function details(id) {
  selected = id;
  const n = data.nodes.find(row => row.institution_id === id);
  const incoming = data.edges.filter(e => e.target === id).sort((a,b) => b.weight-a.weight);
  const stages = {};
  incoming.forEach(e => Object.entries(e.stages ?? {}).forEach(([stage,weight]) => {stages[stage] = (stages[stage] ?? 0) + weight;}));
  $('#global-details').innerHTML = `<h3>${esc(n.name)}</h3><p>Observed score ${value(n.network_score)} · country ${esc(n.country || 'unknown')} · parameter rank range ${esc(n.rank_range?.join('–') || 'unavailable')}.</p><p>Formal qualification: ${esc(n.unranked_reasons.join('; '))}.</p><p>Sources: ${esc(n.source_classes.join(', ') || 'no qualifying relations')}; independently corroborated relations: ${n.corroborated_relations}; traceability: ${n.traceability == null ? 'unknown' : n.traceability}. Traceability does not mean independent confirmation.</p><p>Training-stage weighted contributions: ${esc(Object.entries(stages).map(([s,w]) => `${s}: ${w}`).join(' · ') || 'none')}.</p><p>Awards: ${esc(n.award_status)}. Composite: missing. Unknown global missingness is outside the parameter range.</p>`;
  renderNetwork();
}

function renderTable() {
  const rows = selectRows(data,fields());
  page = Math.min(page,Math.max(0,Math.ceil(rows.length/50)-1));
  const ranks = data.sensitivity?.[$('#global-scenario').value]?.ranks;
  $('#global-rows').innerHTML = rows.slice(page*50,page*50+50).map(n => `<tr><td>${esc(ranks?.[n.institution_id] ?? n.observed_rank ?? 'Unranked')}</td><td><button type="button" class="institution-link" data-institution="${esc(n.institution_id)}">${esc(n.name)}</button><small>${esc(n.institution_id)} · ${esc(n.country || 'unknown country')}</small></td><td>${value(n.network_score)}</td><td>Missing / missing</td><td>${n.relations}<small>${esc(n.status)} · formal rank unavailable</small></td></tr>`).join('') || '<tr><td colspan="5">No qualifying real records in this filter.</td></tr>';
  $('#global-page-status').textContent = `${rows.length ? page*50+1 : 0}–${Math.min(rows.length,page*50+50)} / ${rows.length} observed institutions`;
  $('#global-prev').disabled = page === 0; $('#global-next').disabled = (page+1)*50 >= rows.length;
}

async function renderNetwork() {
  const generation = ++networkGeneration;
  const slice = networkSlice(data,{stage:$('#global-stage').value,country:$('#global-country').value,selected});
  const names = new Map(data.nodes.map(n => [n.institution_id,n.name]));
  $('#global-edge-status').textContent = `${slice.nodes.length} displayed nodes · ${slice.edges.length} / ${slice.totalEdges} filtered directed edges. First 100 displayed edges listed below.`;
  $('#global-edges').innerHTML = slice.edges.slice(0,100).map(e => `<tr><td>${esc(names.get(e.source))} →</td><td>${esc(names.get(e.target))}</td><td>${e.visible_weight.toFixed(2)}</td></tr>`).join('') || '<tr><td colspan="3">No qualifying directed relationships in this slice. Missing observations do not imply zero influence.</td></tr>';
  dispose?.(); dispose = undefined;
  if (!networkEnabled || !slice.edges.length) return;
  try {
    const { showNetwork } = await import('./global-network.js');
    if (generation !== networkGeneration) return;
    dispose = showNetwork($('#global-network'),slice,details);
  } catch { $('#global-network').textContent = '3D unavailable; use the directional relationship table.'; }
}

async function load() {
  try {
    const base = import.meta.env.BASE_URL;
    const manifestResponse = await fetch(`${base}data/global/manifest.json`);
    if (!manifestResponse.ok) throw new Error(`manifest HTTP ${manifestResponse.status}`);
    const manifest = await manifestResponse.json();
    if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.json$/.test(manifest.snapshot)) throw new Error('Unsafe snapshot filename');
    const response = await fetch(`${base}data/global/${manifest.snapshot}`);
    if (!response.ok) throw new Error(`snapshot HTTP ${response.status}`);
    if (Number(response.headers.get('Content-Length')) > 32*1024*1024) throw new Error('Snapshot exceeds browser byte budget');
    const reader = response.body.getReader(), chunks = []; let size = 0;
    while (true) {
      const {value:chunk,done} = await reader.read(); if (done) break;
      size += chunk.byteLength;
      if (size > 32*1024*1024) {await reader.cancel(); throw new Error('Snapshot exceeds browser byte budget');}
      chunks.push(chunk);
    }
    const bytes = new Uint8Array(size); let offset = 0; chunks.forEach(c => {bytes.set(c,offset); offset += c.byteLength;});
    await verifySnapshotBytes(bytes,manifest);
    data = validateGlobal(JSON.parse(new TextDecoder().decode(bytes)));
    const countries = [...new Set(data.nodes.map(n => n.country).filter(Boolean))].sort();
    $('#global-country').innerHTML += countries.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('');
    $('#global-scenario').innerHTML = '<option value="">Reference α = .85</option>' + Object.entries(data.sensitivity ?? {}).filter(([key]) => key !== 'alpha_0.85').map(([key,scenario]) => `<option value="${esc(key)}">${esc(key)}${scenario.prior_only ? ' (prior only; no evidence edges)' : ''}</option>`).join('');
    $('#global-status').textContent = `${data.nodes.length} observed institutions / unresolved institution identifiers · ${data.edges.length} qualifying directed edges · ${data.coverage.events} recorded events · ${data.publication_status}. Source enumeration is incomplete; unresolved identifiers may refer to the same institution. Formal global ranking is unavailable.`;
    for (const id of ['#global-search','#global-country','#global-sort','#global-scenario']) $(id).addEventListener('input',() => {page=0;renderTable();renderNetwork();});
    $('#global-prev').onclick = () => {page--;renderTable();}; $('#global-next').onclick = () => {page++;renderTable();};
    $('#global-rows').addEventListener('click',e => {const id=e.target.closest('[data-institution]')?.dataset.institution; if(id) details(id);});
    $('#global-stage').onchange = renderNetwork;
    $('#global-show-network').onclick = () => {networkEnabled=true;renderNetwork();};
    $('#global-reset').onclick = () => {selected='';renderNetwork();};
    $('#global-csv').onclick = () => {const url=URL.createObjectURL(new Blob([csv(selectRows(data,fields()))],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='gapi-real-observed-institutions.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
    renderTable(); renderNetwork();
  } catch(error) {
    $('#global-status').textContent = `Global real observation slice unavailable: ${error.message}. No substitute scores are shown.`;
    for (const input of root.querySelectorAll('input,select,button')) input.disabled = true;
  }
}
load();
