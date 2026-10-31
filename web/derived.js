import {validateDerivedManifest,decodeNodes,decodeEdges,fetchJSON,fetchAsset,STAGES} from './derived-data.js';
import {csv,networkSlice} from './global-data.js';

const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number=v=>new Intl.NumberFormat('zh-CN').format(v),share=v=>`${(v*100).toFixed(4)}%`;
const regions=new Intl.DisplayNames(['zh-CN'],{type:'region'}),region=v=>{try{return v?regions.of(v):'地区未知';}catch{return v??'地区未知';}};
const stageNames={bachelor:'本科',master:'硕士',doctorate:'博士',postdoc:'博士后训练'};
const root=document.createElement('section');root.className='panel global-panel';root.id='historical-observations';
root.setAttribute('aria-labelledby','historical-title');
root.innerHTML=`<div class="global-heading"><div><p class="eyebrow">GLOBAL CAREERS / 2023</p><h2 id="historical-title">全球履历与人才流动</h2></div><span class="observation-tag">完整历史来源 · 2023</span></div>
<p id="historical-status" role="status" class="global-status">正在核对历史来源与文件版本…</p>
<div id="historical-metrics" class="observation-metrics"></div>
<p class="scope-caption">来源：Easy ORCID 2023.6，ORCID 公开履历的历史派生数据。文件已完整处理，履历尚未独立核验；这不是当前全球人员普查或正式大学排名。与下方持续更新的多源切片分别统计。</p>
<details class="reading-note"><summary>怎样理解这些分数？</summary><p>聘用机构 → 培养机构：一个机构聘用某校培养的人才，便向培养机构传播网络影响力。份额由全部已观测关系计算；筛选和三维显示不会重新计算分数。它不表示就业率或大学整体质量。奖项与综合分数仍缺失。</p><p id="historical-funnel"></p><a href="./docs/EASY_ORCID_2023_OBSERVATIONS.md">来源、计数口径与限制 ↗</a></details>
<div class="table-tools"><label>查找机构<input id="historical-search" type="search" placeholder="机构名称或稳定标识"></label><label>国家／地区<select id="historical-country"><option value="">全部已观测地区</option></select></label><label>机构身份<select id="historical-identity"><option value="">全部机构标识</option><option value="resolved_exact_ror">已规范化机构</option><option value="unresolved_external_identifier">待对齐标识</option></select></label><label>排序<select id="historical-sort"><option value="score">网络份额</option><option value="relations">培养关联数</option><option value="name">名称</option></select></label><button type="button" id="historical-csv">导出筛选结果</button></div>
<div class="table-wrap" tabindex="0" aria-label="完整历史来源机构分数"><table class="directory-table"><thead><tr><th scope="col">观测序位</th><th scope="col">机构／标识</th><th scope="col">网络份额</th><th scope="col">培养关联数</th><th scope="col">机构身份</th></tr></thead><tbody id="historical-rows"></tbody></table></div>
<div class="directory-pagination"><span id="historical-page-status"></span><div><button type="button" id="historical-prev" aria-label="上一页历史机构">←</button><button type="button" id="historical-next" aria-label="下一页历史机构">→</button></div></div>
<aside id="historical-details" class="historical-details" aria-live="polite">选择机构，按需读取它在完整网络中的关系。</aside>
<div class="global-heading network-heading"><h3>人才选择的方向</h3><div class="table-tools"><label>培养阶段<select id="historical-stage"><option value="all">全部阶段</option>${STAGES.map(s=>`<option value="${s}">${stageNames[s]}</option>`).join('')}</select></label><button type="button" id="historical-3d">打开 3D 网络</button><button type="button" id="historical-overview">返回概览</button></div></div>
<p class="scope-caption">箭头为“聘用机构 → 培养机构”。节点大小表示全网络份额，颜色来自 ROR 地区信息，未知为灰色；空间位置没有地理含义。3D 最多显示 500 个节点、2,000 条边；下方二维列表可以分页。</p>
<div id="historical-network" role="region" aria-label="历史人才流动三维网络"></div><p id="historical-edge-status" class="scope-caption" role="status"></p>
<div class="table-wrap" tabindex="0" aria-label="历史来源有向关系列表"><table class="directory-table"><thead><tr><th scope="col">聘用机构 →</th><th scope="col">培养机构</th><th scope="col">阶段加权关系量</th></tr></thead><tbody id="historical-edges"></tbody></table></div>
<div class="directory-pagination"><span id="historical-edge-page"></span><div><button type="button" id="historical-edge-prev" aria-label="上一页历史关系">←</button><button type="button" id="historical-edge-next" aria-label="下一页历史关系">→</button></div></div>`;
document.querySelector('.intro').after(root);
const $=s=>root.querySelector(s),base=new URL('data/derived/',new URL(import.meta.env.BASE_URL,location.origin));
let manifest,nodes=[],byId,edges=[],overview=[],selected='',page=0,edgePage=0,controller,dispose,network=false,generation=0,loading=false,failed=false;
function rows(){const q=$('#historical-search').value.trim().toLowerCase(),country=$('#historical-country').value,identity=$('#historical-identity').value,sort=$('#historical-sort').value;
 return nodes.filter(n=>(!q||`${n.name} ${n.institution_id}`.toLowerCase().includes(q))&&(!country||n.country===country)&&(!identity||n.identity===identity))
 .sort((a,b)=>sort==='name'?a.name.localeCompare(b.name)||a.index-b.index:sort==='relations'?b.relations-a.relations||a.index-b.index:a.index-b.index);}
function table(){const list=rows();page=Math.min(page,Math.max(0,Math.ceil(list.length/50)-1));
 $('#historical-rows').innerHTML=list.slice(page*50,page*50+50).map(n=>`<tr><td>${n.observed_order}</td><td><button type="button" class="institution-link" data-history="${esc(n.institution_id)}">${esc(n.name)}</button><small>${esc(region(n.country))}</small></td><td class="network-share">${share(n.network_score)}</td><td>${number(n.relations)}</td><td>${n.identity==='resolved_exact_ror'?'已规范化':'待对齐'}</td></tr>`).join('')||'<tr><td colspan="5">当前筛选没有记录；缺失不代表低影响力。</td></tr>';
 $('#historical-page-status').textContent=`${list.length?page*50+1:0}—${Math.min(list.length,page*50+50)} / ${number(list.length)} 个机构标识`;
 $('#historical-prev').disabled=page===0;$('#historical-next').disabled=(page+1)*50>=list.length;}
async function renderEdges(){const request=++generation,stage=$('#historical-stage').value;
 const visible=edges.map(e=>({...e,visible_weight:stage==='all'?e.weight:e.stages[stage]})).filter(e=>e.visible_weight>0).sort((a,b)=>b.visible_weight-a.visible_weight||a.source.localeCompare(b.source)||a.target.localeCompare(b.target));
 edgePage=Math.min(edgePage,Math.max(0,Math.ceil(visible.length/50)-1));
 $('#historical-edges').innerHTML=visible.slice(edgePage*50,edgePage*50+50).map(e=>`<tr><td>${esc(byId.get(e.source).name)} →</td><td>${esc(byId.get(e.target).name)}</td><td>${e.visible_weight.toFixed(2)}</td></tr>`).join('')||'<tr><td colspan="3">当前范围没有关系。</td></tr>';
 $('#historical-edge-page').textContent=`${visible.length?edgePage*50+1:0}—${Math.min(visible.length,edgePage*50+50)} / ${number(visible.length)} 条关系`;
 $('#historical-edge-prev').disabled=edgePage===0;$('#historical-edge-next').disabled=(edgePage+1)*50>=visible.length;
 $('#historical-edge-status').textContent=loading?'正在读取所选机构关系；校验完成后显示。':failed?'关系尚未完整读取，暂不显示部分结果。':selected?`已读取所选机构全部 ${number(edges.length)} 条直接关系，当前阶段 ${number(visible.length)} 条。`:`概览取全部 ${number(manifest.edge_count)} 条聚合边中最强的 ${number(overview.length)} 条；选择机构可读取其完整直接关系。`;
 dispose?.();dispose=undefined;if(!network||!visible.length)return;
 const {showNetwork}=await import('./global-network.js');if(request!==generation)return;
 const slice=networkSlice({nodes,edges:visible},{stage:'all'},'all');dispose=showNetwork($('#historical-network'),slice,details);
 $('#historical-edge-status').textContent+=` 三维切片含 ${slice.nodes.length} 个节点、${slice.edges.length} 条边。`;}
async function details(id){const n=byId.get(id);if(!n)return;controller?.abort();controller=new AbortController();const own=controller;selected=id;edgePage=0;edges=[];
 $('#historical-details').textContent=`正在读取 ${n.name} 的关系（${n.chunks.length} 个小块）…`;
 loading=true;failed=false;await renderEdges();
 try{for(const chunk of n.chunks){const data=await fetchAsset(base,manifest.edge_chunks[chunk],own.signal);if(own!==controller)return;
   const found=decodeEdges(data,nodes).filter(e=>e.source===id||e.target===id);
   if(edges.length+found.length>50000)throw new Error('直接关系超过单次交互的 50,000 条上限；完整数据仍保存在公开分块中');
   edges.push(...found);}
   if(own!==controller)return;
   loading=false;
   const stages=Object.fromEntries(STAGES.map(s=>[s,edges.filter(e=>e.target===id).reduce((sum,e)=>sum+e.stages[s],0)]));
   $('#historical-details').innerHTML=`<div class="detail-heading"><div><h3>${esc(n.name)}</h3><p>${esc(region(n.country))} · 全网络份额 ${share(n.network_score)}</p></div><span class="status unranked">正式排名待核验</span></div><p>身份：${n.identity==='resolved_exact_ror'?'已通过 ROR 规范化':'机构标识尚待对齐'}。公开履历来自同一 ORCID 派生来源，未达到独立核验条件。奖项与综合分数缺失。</p><p>指向该机构的阶段加权关系量：${STAGES.map(s=>`${stageNames[s]} ${stages[s].toFixed(2)}`).join(' · ')}。这些是观测关系量，不是分数占比或就业概率。</p><p>所选机构的全部直接关系已加载；见下方有向列表。</p>`;
   await renderEdges();
 }catch(error){if(own!==controller||error.name==='AbortError')return;loading=false;failed=true;edges=[];$('#historical-details').textContent=`关系未完整加载：${error.message}。请重新选择机构重试。`;await renderEdges();}}
async function load(){try{
 const received=await fetchJSON(new URL('manifest.json',base),128*1024);manifest=validateDerivedManifest(received.value);
 nodes=decodeNodes(await fetchAsset(base,manifest.nodes),manifest);byId=new Map(nodes.map(n=>[n.institution_id,n]));
 overview=decodeEdges(await fetchAsset(base,manifest.overview),nodes,2000);edges=overview;
 $('#historical-status').textContent='完整历史来源 · 文件版本已校验 · 正式排名待独立审查';
 $('#historical-metrics').innerHTML=[['保留双侧履历',manifest.distinct_retained_people,'人'],['贡献流动关系',manifest.person_level_funnel.people_contributing_relations,'人'],['去重流动关系',manifest.deduplicated_relations,'条'],['已规范化机构',manifest.resolved_institutions,'个']].map(([label,value,unit])=>`<div><span>${label}</span><strong>${number(value)}<small>${unit}</small></strong></div>`).join('');
 $('#historical-funnel').textContent=`${number(manifest.source_census.counts.source_rows)} 条来源记录 → ${number(manifest.distinct_retained_people)} 人有双侧履历 → ${number(manifest.person_level_funnel.people_with_identified_training_and_academic_job)} 人有明确培养阶段与学术岗位 → ${number(manifest.person_level_funnel.people_with_required_dates_on_both_sides)} 人具备必要年份 → ${number(manifest.person_level_funnel.people_contributing_relations)} 人贡献流动。${number(manifest.unresolved_institution_identifiers)} 个机构标识尚待对齐；文件扫描完成不等于全球覆盖完整。`;
 $('#historical-country').insertAdjacentHTML('beforeend',[...new Set(nodes.map(n=>n.country).filter(Boolean))].sort().map(c=>`<option value="${c}">${esc(region(c))}</option>`).join(''));
 for(const id of ['search','country','identity','sort'])$(`#historical-${id}`).addEventListener('input',()=>{page=0;table();});
 $('#historical-prev').onclick=()=>{page--;table();};$('#historical-next').onclick=()=>{page++;table();};
 $('#historical-rows').onclick=e=>{const id=e.target.closest('[data-history]')?.dataset.history;if(id)details(id);};
 $('#historical-stage').onchange=()=>{edgePage=0;renderEdges();};$('#historical-3d').onclick=()=>{network=true;renderEdges();};
 $('#historical-overview').onclick=()=>{controller?.abort();controller=null;loading=false;failed=false;selected='';edges=overview;edgePage=0;$('#historical-details').textContent='选择机构，按需读取它在完整网络中的关系。';renderEdges();};
 $('#historical-edge-prev').onclick=()=>{edgePage--;renderEdges();};$('#historical-edge-next').onclick=()=>{edgePage++;renderEdges();};
 $('#historical-csv').onclick=()=>{const url=URL.createObjectURL(new Blob([csv(rows().map(n=>({...n,observed_rank:n.observed_order,formal_rank:null,award_score:null,composite_score:null,status:n.identity})))],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='gapi-easy-orcid-2023-observed.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
 table();await renderEdges();
 }catch(error){$('#historical-status').textContent=`历史来源暂不可用：${error.message}。未展示替代评分。`;for(const input of root.querySelectorAll('input,select,button'))input.disabled=true;}}
load();
