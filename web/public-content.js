export const publicLabels={rank:'参考顺序',share:'参考网络份额',relations:'去重流动关系'};
// Public vocabulary and exports share one meaning across both snapshots.
export const shareExplanation='网络份额是学校在本次学术人才关系网络中的影响力占比，不是就业率，也不是大学综合质量得分。筛选学校或国家只改变显示范围，不会重算这个数值。';
export const yearExplanation='2026 和 2023 表示来源快照版本，履历本身可跨越多年。同一人的前后记录用于对照，不能将两期人数或关系相加。';
export function sourceEdition(manifest,scope={}){
  const published=manifest.publication?.sources??[],scoped=scope.source_vintages??[];
  const annual=rows=>[...new Set(rows.map(r=>r.source).filter(v=>/^orcid-\d{4}-summary$/.test(v??'')))].sort();
  if(annual(published).length&&annual(scoped).length&&JSON.stringify(annual(published))!==JSON.stringify(annual(scoped)))throw Error('Source edition mismatch');
  const rows=[...published,...scoped];
  const years=[...new Set(rows.map(row=>/^orcid-(\d{4})-summary$/.exec(row.source??'')?.[1]).filter(Boolean))].sort();
  const dates=[...new Set(rows.map(row=>row.snapshot_date).filter(v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)))];
  const release=manifest.publication?.release_version;
  return {label:years.length?years.join('、')+' 年度来源':'多源观察',snapshot:years.join('-')||'current',
    source:years.length?'ORCID '+years.join('、')+' 年度档案 + 已冻结 API / Wikidata':'本版清单所列来源',
    dates,release:typeof release==='string'&&/^v\d+\.\d+\.\d+-observations$/.test(release)?release:null};
}
export function safeCell(value){
  let text=value==null?'':String(value);
  if(/^[\s]*[=+\-@\t\r]/.test(text))text="'"+text;
  return '"'+text.replaceAll('"','""')+'"';
}
export function institutionCSV(rows,{snapshot,scenario='',ranks={}}){
  const fields=[['来源快照',()=>snapshot],['机构名称',n=>n.name],['国家或地区代码',n=>n.country],
    [publicLabels.rank,n=>n.observed_rank??n.observed_order],[publicLabels.share+'（%）',n=>n.network_score==null?null:n.network_score*100],
    [publicLabels.relations+'数',n=>n.relations],['机构标识（数据对照用）',n=>n.institution_id]];
  if(scenario)fields.push(['研究方案',()=>scenario],['研究方案顺序',n=>ranks[n.institution_id]]);
  return '\ufeff'+[fields.map(([name])=>safeCell(name)).join(','),...rows.map(n=>fields.map(([,read])=>safeCell(read(n))).join(','))].join('\r\n');
}
export const loadingFailure='数据暂时无法读取。请检查网络后点击“继续加载”；页面会保留已取得的有效内容。';
