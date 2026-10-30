import { verifySnapshotBytes } from './snapshot.js';
import { csv, networkSlice, selectEdges, selectRows, validateGlobal } from './global-data.js';

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const stageNames = {bachelor:'本科',master:'硕士',doctorate:'博士',postdoc:'博士后训练'};
const reasonNames = {insufficient_independently_corroborated_relations:'独立核验关系不足',insufficient_independent_source_classes:'独立来源类别不足',institution_crosswalk_pending:'机构身份尚待核对',external_dual_review_pending:'第三方双轨审查待完成'};
const sourceName = source => ({'orcid-public-api':'ORCID 公开履历',wikidata:'Wikidata 结构化声明','orcid-2026-summary':'ORCID 年度档案'}[source] ?? source);
const integer = value => new Intl.NumberFormat('zh-CN').format(value);
const share = value => value == null ? '未评分' : `${(value*100).toFixed(3)}%`;
const countryNames = new Intl.DisplayNames(['zh-CN'],{type:'region'});
const countryName = code => {try{return code ? countryNames.of(code) : '地区未知';}catch{return code || '地区未知';}};
const scenarioName = key => key.startsWith('alpha_') ? `阻尼 α = ${key.slice(6)}` : key.startsWith('stage_only_') ? `仅${stageNames[key.slice(11)] || key.slice(11)}关系` : key.startsWith('delete_source_') ? `移除 ${sourceName(key.slice(14))}` : ({employment_since_2016:'2016 年以后任职',region_balanced_prior:'地区平衡先验'}[key] ?? key);

const root = document.createElement('section');
root.className='panel global-panel';root.id='global-observations';root.setAttribute('aria-labelledby','global-title');
root.innerHTML=`<div class="global-heading"><div><p class="eyebrow">01 / INSTITUTIONS</p><h2 id="global-title">机构与得分</h2></div><span class="observation-tag">公开数据 · 持续更新</span></div>
<p id="global-status" class="global-status" role="status">正在校验真实数据与版本清单…</p>
<div id="global-metrics" class="observation-metrics" aria-label="当前切片的实际数据范围"></div>
<p class="scope-caption">仅统计已收录的公开记录。各项计数的口径不同，不能相除得到全球覆盖率或就业率。</p>
<div id="global-evidence" class="evidence-summary"></div>
<details id="global-acquisition" class="reading-note" hidden><summary>采集进度</summary><p>本版本已发现的候选记录，按来源分别计数。候选仍在扩展；这里的完成比例不代表全球学术人员覆盖率。</p><div id="global-acquisition-sources" class="acquisition-progress"></div></details>
<details class="reading-note"><summary>分数与排名说明</summary><p>“网络份额”表示一个机构在本次人才关系网络中分到的影响力，占比合计为 100%。它不是就业率，也不能直接衡量大学整体质量。奖项和综合评分尚未发布。</p></details>
<details id="global-official-awards" class="award-inventory" hidden><summary>官方奖项数据：查看采集范围与归属标签</summary><div id="global-award-content"></div></details>
<div class="table-tools"><label>查找机构<input type="search" id="global-search" placeholder="输入机构名称，例如 Cambridge" autocomplete="off"></label>
<label>国家／地区<select id="global-country"><option value="">全部已观测地区</option></select></label>
<label>排序<select id="global-sort"><option value="score">网络份额</option><option value="relations">可观测关系数</option><option value="name">机构名称</option></select></label>
<label>计算设置<select id="global-scenario"><option value="">参考设置 α = 0.85</option></select></label><button id="global-csv" type="button" class="primary-control">导出筛选结果</button></div>
<p id="global-scenario-note" class="scope-caption"></p>
<div class="table-wrap" tabindex="0" aria-label="真实观测机构分数表"><table class="directory-table"><thead><tr><th scope="col">观测序位</th><th scope="col">机构</th><th scope="col">网络份额</th><th scope="col">去重关系</th><th scope="col">情景名次范围</th><th scope="col">正式排名</th></tr></thead><tbody id="global-rows"></tbody></table></div>
<div class="directory-pagination"><span id="global-page-status"></span><div><button type="button" id="global-prev" aria-label="上一页机构">←</button><button type="button" id="global-next" aria-label="下一页机构">→</button></div></div>
<aside id="global-details" aria-live="polite"><p>点击一个机构，查看分数来源、培养阶段和证据缺口。</p></aside>
<div class="network-heading global-heading"><div><p class="eyebrow">02 / THE NETWORK</p><h3>人才流动网络</h3></div><div class="table-tools"><label>培养阶段<select id="global-stage"><option value="all">全部阶段</option><option value="doctorate">博士</option><option value="master">硕士</option><option value="bachelor">本科</option><option value="postdoc">博士后训练</option></select></label><button type="button" id="global-show-network">打开 3D 网络</button><button type="button" id="global-reset">全部关系</button></div></div>
<p class="scope-caption">箭头固定为“聘用机构 → 培养机构”。节点大小表示参考网络份额，颜色表示地区；灰色为未知。位置不代表地理或时间。国家筛选保留至少一端位于该国的关系。</p>
<div id="global-network" role="region" aria-label="可选三维人才关系网络"></div><p id="global-edge-status" role="status"></p>
<div class="table-wrap" tabindex="0" aria-label="可分页的有向关系列表"><table class="directory-table"><thead><tr><th scope="col">聘用机构 →</th><th scope="col">培养机构</th><th scope="col">阶段加权关系量</th></tr></thead><tbody id="global-edges"></tbody></table></div>
<div class="directory-pagination"><span id="global-edge-page"></span><div><button type="button" id="global-edge-prev" aria-label="上一页关系">←</button><button type="button" id="global-edge-next" aria-label="下一页关系">→</button></div></div>`;
document.querySelector('.intro').after(root);
const $=selector=>root.querySelector(selector);
let data,page=0,edgePage=0,selected='',dispose,networkEnabled=false,networkGeneration=0;
const fields=()=>({query:$('#global-search').value,country:$('#global-country').value,sort:$('#global-sort').value,scenario:$('#global-scenario').value});
const filters=()=>({stage:$('#global-stage').value,country:$('#global-country').value,selected});
function bars(items,format=String){
  const max=Math.max(0,...items.map(item=>item.value));
  return `<ul class="mini-bars">${items.map(item=>`<li><div><span>${esc(item.label)}</span><strong>${esc(format(item.value))}</strong></div><div class="bar-track"><span style="width:${max ? (item.value/max*100).toFixed(4) : 0}%"></span></div></li>`).join('')}</ul>`;
}
function details(id){
  const n=data.nodes.find(row=>row.institution_id===id);if(!n)return;selected=id;edgePage=0;
  const incoming=data.edges.filter(e=>e.target===id);
  const stages={};incoming.forEach(e=>Object.entries(e.stages).forEach(([s,w])=>{stages[s]=(stages[s]??0)+w;}));
  const outgoing=new Map();data.edges.forEach(e=>outgoing.set(e.source,(outgoing.get(e.source)??0)+e.weight));
  const names=new Map(data.nodes.map(row=>[row.institution_id,row.name]));
  const sources=incoming.map(e=>({label:names.get(e.source),value:(data.algorithm.config.alpha??.85)*(data.nodes.find(row=>row.institution_id===e.source)?.network_score??0)*e.weight/outgoing.get(e.source)})).sort((a,b)=>b.value-a.value);
  const residual=n.network_score==null ? null : Math.max(0,n.network_score-sources.reduce((sum,row)=>sum+row.value,0));
  $('#global-details').innerHTML=`<div class="detail-heading"><div><p class="eyebrow">INSTITUTION / 机构详情</p><h3>${esc(n.name)}</h3><p>${esc(countryName(n.country))} · 网络份额 <strong>${esc(share(n.network_score))}</strong></p></div><span class="status unranked">正式排名待核验</span></div>
<div class="detail-charts"><section><h4>哪些聘用机构传来影响力？</h4><p class="scope-caption">按 PageRank 实际传播贡献排序；显示前 8 项。</p>${sources.length ? bars(sources.slice(0,8),share) : '<p>当前没有可计算的聘用机构贡献。</p>'}${residual==null?'':`<p class="scope-caption">另有 ${esc(share(residual))} 来自随机跳转与无出边节点回流。</p>`}</section><section><h4>培养阶段构成</h4><p class="scope-caption">阶段加权的去重关系量，非概率，也非分数占比。</p>${Object.keys(stages).length ? bars(Object.entries(stages).map(([s,w])=>({label:stageNames[s],value:w})),v=>v.toFixed(2)) : '<p>尚无可分层的有效关系。</p>'}</section></div>
<div class="detail-facts"><p><strong>证据：</strong>${esc(n.source_classes.map(sourceName).join('、')||'暂无合格关系来源')}；独立核验关系 ${esc(n.corroborated_relations)} 条；有溯源记录 ${n.traceability==null?'未知':esc((n.traceability*100).toFixed(0)+'%')}。有来源不等于事实已获独立核实。</p><p><strong>情景范围：</strong>${esc(n.rank_range?.join('—')??'暂不可计算')}，仅针对固定参考网络的参数情景，不是全球缺失数据的置信区间。</p><p><strong>暂未正式排名：</strong>${esc(n.unranked_reasons.map(reason=>reasonNames[reason]??reason).join('；'))}。</p></div>
<details><summary>技术标识与缺失分数</summary><p>${esc(n.institution_id)}；奖项评分：缺失；综合评分：缺失。全部原始数值可通过机构表导出。</p></details>`;
  renderNetwork();
}
function renderTable(){
  const rows=selectRows(data,fields());page=Math.min(page,Math.max(0,Math.ceil(rows.length/50)-1));
  const key=$('#global-scenario').value,scenario=data.sensitivity?.[key],ranks=scenario?.ranks;
  $('#global-rows').innerHTML=rows.slice(page*50,page*50+50).map(n=>`<tr><td>${esc(ranks?.[n.institution_id]??n.observed_rank??'—')}</td><td><button type="button" class="institution-link" data-institution="${esc(n.institution_id)}">${esc(n.name)}</button><small>${esc(countryName(n.country))}</small></td><td class="network-share">${esc(share(n.network_score))}</td><td>${esc(integer(n.relations))}</td><td>${esc(n.rank_range?.join('—')??'暂无')}</td><td><span class="status unranked">待核验</span></td></tr>`).join('')||'<tr><td colspan="6">当前筛选下没有已记录机构。缺失不代表低影响力。</td></tr>';
  $('#global-page-status').textContent=`${rows.length?page*50+1:0}—${Math.min(rows.length,page*50+50)} / ${integer(rows.length)} 个机构标识`;
  $('#global-prev').disabled=page===0;$('#global-next').disabled=(page+1)*50>=rows.length;
  const expanded=data.expanded_universe_sensitivity?.[key];
  $('#global-scenario-note').textContent=scenario ? `当前情景只改变序位；份额列仍显示参考设置。${scenario.prior_only?'本情景没有有效关系，仅余先验分布。':''} 固定网络采用 ${scenario.edges} / ${scenario.available_edges??scenario.edges} 条可用边。${expanded ? `另有扩展网络对照：${expanded.node_count} 个节点、${expanded.edges} 条边；新增机构不会被当成“名次稳定”。` : ''}` : '当前为参考设置。情景范围只描述已观测网络；未知全球缺失不在范围内。';
}
async function renderNetwork(){
  const generation=++networkGeneration,slice=networkSlice(data,filters()),edges=selectEdges(data,filters()),names=new Map(data.nodes.map(n=>[n.institution_id,n.name]));
  edgePage=Math.min(edgePage,Math.max(0,Math.ceil(edges.length/50)-1));
  $('#global-edge-status').textContent=`二维列表含当前筛选的全部 ${integer(edges.length)} 条关系，可翻页查看。3D 显示 ${slice.nodes.length} 个节点、${slice.edges.length} 条边，上限为 500 / 2,000。`;
  $('#global-edges').innerHTML=edges.slice(edgePage*50,edgePage*50+50).map(e=>`<tr><td>${esc(names.get(e.source))} →</td><td>${esc(names.get(e.target))}</td><td>${esc(e.visible_weight.toFixed(2))}</td></tr>`).join('')||'<tr><td colspan="3">当前范围暂无有效关系；缺失不等于没有影响力。</td></tr>';
  $('#global-edge-page').textContent=`${edges.length?edgePage*50+1:0}—${Math.min(edges.length,edgePage*50+50)} / ${integer(edges.length)} 条关系`;
  $('#global-edge-prev').disabled=edgePage===0;$('#global-edge-next').disabled=(edgePage+1)*50>=edges.length;
  dispose?.();dispose=undefined;
  if(!networkEnabled||!slice.edges.length)return;
  try{const {showNetwork}=await import('./global-network.js');if(generation!==networkGeneration)return;dispose=showNetwork($('#global-network'),slice,details);}
  catch{$('#global-network').textContent='3D 暂不可用；请使用下方可分页的有向关系列表。';}
}
function renderAwards(){
  const awards=data.official_award_inventory;if(!awards)return;
  $('#global-official-awards').hidden=false;
  $('#global-official-awards summary').textContent=`官方诺贝尔奖数据 · ${integer(awards.processed_recipients)} / ${integer(awards.expected_recipients??0)} 名／家获奖者已处理`;
  const projection=data.official_award_attribution,attributions=new Map((projection?.official_label_attributions??[]).map(row=>[row.official_label_id,row]));
  const fraction=value=>value ? value.denominator==='1'?value.numerator:`${value.numerator} / ${value.denominator}`:'未计算';
  $('#global-award-content').innerHTML=`<p>${esc(integer(awards.award_events))} 个获奖事件，${esc(integer(awards.affiliations.length))} 个官方机构归属标签。标签可能对应同一机构的历史名称，尚未用于大学奖项综合评分。</p><p class="scope-caption">本次仅覆盖诺贝尔奖。单机构份额只累计个人奖项中官方唯一机构的获奖者份额；多机构等分只作情景对照，不能当作官方分配比例。机构奖项单列，不混入个人份额。ROR 匹配仍是待确认候选，不自动合并。</p>${projection?`<p>已计算 ${esc(projection.label_count)} 个官方标签；${esc(projection.unique_candidate_labels)} 个有唯一 ROR 候选，${esc(projection.ambiguous_candidate_labels)} 个有歧义，其余 ${esc(projection.unresolved_labels)} 个待解析。</p>`:''}<label>查找官方奖项机构<input type="search" id="global-award-search" autocomplete="off"></label><p id="global-award-count" role="status"></p><div class="table-wrap" tabindex="0" aria-label="官方获奖时机构归属"><table class="directory-table"><thead><tr><th scope="col">官方机构名称</th><th scope="col">获奖时城市／国家</th><th scope="col">获奖提及（次）</th><th scope="col">单机构个人份额</th><th scope="col">多机构等分情景份额</th><th scope="col">身份映射</th></tr></thead><tbody id="global-award-rows"></tbody></table></div>`;
  const render=()=>{const q=$('#global-award-search').value.trim().toLocaleLowerCase();const rows=awards.affiliations.filter(a=>`${a.name} ${a.city??''} ${a.country??''}`.toLocaleLowerCase().includes(q)).sort((a,b)=>a.name.localeCompare(b.name));$('#global-award-count').textContent=`找到 ${rows.length} 个官方标签，显示前 ${Math.min(50,rows.length)} 项；可继续搜索。`;$('#global-award-rows').innerHTML=rows.slice(0,50).map(a=>{const p=attributions.get(a.official_label_id);return `<tr><td>${esc(a.name)}</td><td>${esc(a.city??'未知')} / ${esc(a.country??'未知')}</td><td>${esc(a.award_mentions)}</td><td>${esc(fraction(p?.nobel_share_single_affiliation))}</td><td>${esc(fraction(p?.nobel_share_equal_split_sensitivity))}</td><td>${esc(p?.identity_status==='candidate-requires-confirmation'?'唯一候选·待确认':p?.identity_status==='ambiguous-candidate'?'歧义待核验':'待解析')}</td></tr>`;}).join('')||'<tr><td colspan="6">未找到匹配的官方标签。</td></tr>';};
  $('#global-award-search').addEventListener('input',render);render();
}
async function load(){
  try{
    const base=import.meta.env.BASE_URL;
    const manifestResponse=await fetch(`${base}data/global/manifest.json`);if(!manifestResponse.ok)throw new Error(`manifest HTTP ${manifestResponse.status}`);
    const manifest=await manifestResponse.json();if(!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.json$/.test(manifest.snapshot))throw new Error('不安全的快照路径');
    const response=await fetch(`${base}data/global/${manifest.snapshot}`);if(!response.ok)throw new Error(`snapshot HTTP ${response.status}`);
    if(Number(response.headers.get('Content-Length'))>32*1024*1024)throw new Error('快照超过浏览器容量预算');
    const reader=response.body.getReader(),chunks=[];let size=0;
    while(true){const {value:chunk,done}=await reader.read();if(done)break;size+=chunk.byteLength;if(size>32*1024*1024){await reader.cancel();throw new Error('快照超过浏览器容量预算');}chunks.push(chunk);}
    const bytes=new Uint8Array(size);let offset=0;chunks.forEach(c=>{bytes.set(c,offset);offset+=c.byteLength;});await verifySnapshotBytes(bytes,manifest);
    data=validateGlobal(JSON.parse(new TextDecoder().decode(bytes)));
    const countryCodes=[...new Set(data.nodes.map(n=>n.country).filter(Boolean))].sort();
    $('#global-country').innerHTML+=countryCodes.map(c=>`<option value="${esc(c)}">${esc(countryName(c))} · ${esc(c)}</option>`).join('');
    $('#global-scenario').innerHTML='<option value="">参考设置 α = 0.85</option>'+Object.entries(data.sensitivity??{}).filter(([key])=>key!=='alpha_0.85').map(([key,s])=>`<option value="${esc(key)}">${esc(scenarioName(key))}${s.prior_only?'（仅先验）':''}</option>`).join('');
    $('#global-status').textContent=`版本已校验 · 数据持续采集中 · 正式全球排名尚未发布`;
    $('#global-status').dataset.snapshot=manifest.output_sha256;
    const scored=data.nodes.filter(n=>n.network_score!=null).length,relations=data.nodes.reduce((sum,n)=>sum+n.relations,0);
    const metrics=[['收录人数',data.coverage.people,'人'],['履历记录',data.coverage.events,'条'],['去重人才关系',relations,'条'],['评分机构',scored,'个']];
    $('#global-metrics').innerHTML=metrics.map(([label,value,unit])=>`<div><span>${esc(label)}</span><strong>${esc(integer(value))}<small>${esc(unit)}</small></strong></div>`).join('');
    const observed=(data.coverage.event_status?.observed??0)+(data.coverage.event_status?.corroborated??0),pending=data.coverage.event_status?.unresolved??0;
    $('#global-evidence').innerHTML=`<div><strong>履历记录</strong><span>已识别 ${esc(integer(observed))} 条 · 待定 ${esc(integer(pending))} 条</span></div><div class="evidence-track" aria-hidden="true"><span style="width:${data.coverage.events ? Math.min(100,Math.max(0,observed/data.coverage.events*100)).toFixed(3) : 0}%"></span></div><p>另有 ${esc(integer(data.algorithm.normalization_policy?.interpreted_events??0))} 条按公开规则识别，原始记录保留。已识别的记录仍需独立核验。</p>`;
    const acquisition=data.acquisition_progress?.sources??[];
    if(acquisition.length){
      $('#global-acquisition').hidden=false;
      $('#global-acquisition-sources').innerHTML=acquisition.map(s=>`<div><div><span>${esc(sourceName(s.source))}</span><span>已处理 ${esc(integer(s.processed))} / ${esc(integer(s.candidates))}</span></div><progress value="${s.processed}" max="${s.candidates}" aria-label="${esc(sourceName(s.source))} 已发现候选处理进度">${esc(integer(s.processed))} / ${esc(integer(s.candidates))}</progress><p>待处理 ${esc(integer(s.pending))} · 其中待重试 ${esc(integer(s.pending_with_errors))} · 已拒绝 ${esc(integer(s.rejected_members))}</p></div>`).join('');
    }
    renderAwards();
    for(const id of ['#global-search','#global-country','#global-sort','#global-scenario'])$(id).addEventListener('input',()=>{page=0;edgePage=0;renderTable();renderNetwork();});
    $('#global-prev').onclick=()=>{page--;renderTable();};$('#global-next').onclick=()=>{page++;renderTable();};
    $('#global-rows').addEventListener('click',e=>{const id=e.target.closest('[data-institution]')?.dataset.institution;if(id)details(id);});
    $('#global-stage').onchange=()=>{edgePage=0;renderNetwork();};$('#global-show-network').onclick=()=>{networkEnabled=true;renderNetwork();};$('#global-reset').onclick=()=>{selected='';edgePage=0;renderNetwork();};
    $('#global-edge-prev').onclick=()=>{edgePage--;renderNetwork();};$('#global-edge-next').onclick=()=>{edgePage++;renderNetwork();};
    $('#global-csv').onclick=()=>{const url=URL.createObjectURL(new Blob([csv(selectRows(data,fields()))],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='gapi-real-observed-institutions.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
    renderTable();renderNetwork();
  }catch(error){$('#global-status').textContent=`真实数据暂不可用：${error.message}。未展示替代评分。`;for(const input of root.querySelectorAll('input,select,button'))input.disabled=true;}
}
load();
