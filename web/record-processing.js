const categories=['conflict','excluded','missing_institution','missing_classification','missing_date','identified'];
const names={conflict:'证据或日期冲突',excluded:'按明确规则排除',missing_institution:'缺少可确认机构',missing_classification:'缺少明确学历或岗位类型',missing_date:'缺少必要日期',identified:'满足本次识别条件'};
const integer=n=>new Intl.NumberFormat('zh-CN').format(n);

export function validateProcessing(coverage){
  const p=coverage.processing;
  if(p==null)return null;
  const count=v=>Number.isSafeInteger(v)&&v>=0;
  if(p.schema_version!=='record-governance/1'||p.complete!==true||p.total_events!==coverage.events
    ||!p.primary||Object.keys(p.primary).some(k=>!categories.includes(k))
    ||Object.values(p.primary).some(v=>!count(v))
    ||Object.values(p.primary).reduce((a,b)=>a+b,0)!==coverage.events
    ||p.historical_records_restored_into_current!==0||p.independent_evidence_promotions!==0
    ||!p.changes||!p.history||!p.funnel||!p.flags
    ||[p.changes,p.history,p.funnel,p.flags].some(v=>Object.values(v).some(n=>!count(n)))
    ||[p.changes,p.flags].some(v=>Object.values(v).some(n=>n>coverage.events))
    ||Object.values(p.funnel).some(n=>n>(coverage.people??coverage.events))
    ||(p.history.current_people_with_retained_history??0)>(coverage.people??coverage.events)
    ||(p.history.current_people_with_retained_history??0)>(p.history.retained_historical_people??0))throw new Error('记录处置摘要与完整输入不一致');
  return p;
}

export function processingText(coverage){
  const p=validateProcessing(coverage);
  if(!p)return ['这版公开统计没有按原因细分；这不表示还有文件没有读取。请结合来源范围理解这些记录。'];
  return [`本版收录 ${integer(p.total_events)} 条教育与任职记录。下面六类互不重复，相加等于全部收录记录。`,
    categories.map(k=>`${names[k]} ${integer(p.primary[k]??0)} 条`).join('；'),
    `依据记录中明确写出的学历或岗位名称，本版增加识别 ${integer(p.changes.newly_identified_events??0)} 条满足机构、类型与日期条件的记录。来源内容没有被改写。`,
    `前期保留了 ${integer(p.history.retained_historical_people??0)} 人的相关记录，其中 ${integer(p.history.current_people_with_retained_history??0)} 人也出现在本版。前后记录一致不代表独立证实，旧记录也不会自动替代本版事实。`,
    `有 ${integer(p.funnel.people_contributing_relations??0)} 人的培养和任职记录满足本次人才关系计算条件。记录条数、参与计算的人数和来源文件处置分别统计。`];
}
