export function validateGlobal(data) {
  if (data?.schema_version !== 'gapi-global-observed/v1' || data.dataset_kind !== 'real/global-public-observations') throw new Error('Invalid real global observation schema');
  if (!Array.isArray(data.nodes) || !Array.isArray(data.edges) || data.nodes.length > 25000 || data.edges.length > 200000) throw new Error('Global slice exceeds browser budget');
  const ids = new Set();
  const scores = new Map();
  for (const node of data.nodes) {
    if (typeof node.institution_id !== 'string' || typeof node.name !== 'string' || ids.has(node.institution_id)) throw new Error('Invalid or duplicate institution');
    ids.add(node.institution_id);
    scores.set(node.institution_id,node.network_score);
    for (const field of ['network_score', 'award_score', 'composite_score']) {
      if (node[field] !== null && (!Number.isFinite(node[field]) || node[field] < 0)) throw new Error(`Invalid ${field}`);
    }
    if (node.network_score !== null && node.network_score > 1) throw new Error('Network score exceeds probability mass');
    if (!Array.isArray(node.unranked_reasons)) throw new Error('Missing qualification reasons');
    for (const field of ['relations','corroborated_relations']) {
      if (!Number.isSafeInteger(node[field]) || node[field] < 0) throw new Error(`Invalid ${field}`);
    }
    if (node.traceability !== null && (!Number.isFinite(node.traceability) || node.traceability < 0 || node.traceability > 1)) throw new Error('Invalid traceability');
    for (const field of ['observed_rank','formal_rank']) {
      if (node[field] !== null && (!Number.isSafeInteger(node[field]) || node[field] < 1)) throw new Error(`Invalid ${field}`);
    }
    if (!Array.isArray(node.source_classes) || node.source_classes.some(v => typeof v !== 'string') || node.unranked_reasons.some(v => typeof v !== 'string')) throw new Error('Invalid source or qualification labels');
    if (node.rank_range !== null && (!Array.isArray(node.rank_range) || node.rank_range.length !== 2 || node.rank_range.some(v => !Number.isSafeInteger(v) || v < 1) || node.rank_range[0] > node.rank_range[1])) throw new Error('Invalid rank range');
  }
  const mass = data.nodes.reduce((sum,node)=>sum+(node.network_score??0),0);
  if (data.nodes.some(node=>node.network_score!==null) && Math.abs(mass-1)>1e-9) throw new Error('Network probability mass is not conserved');
  const alpha=data.algorithm?.config?.alpha;
  if (!Number.isFinite(alpha) || alpha<0 || alpha>=1) throw new Error('Invalid network damping');
  for (const edge of data.edges) {
    if (!ids.has(edge.source) || !ids.has(edge.target) || !Number.isFinite(edge.weight) || edge.weight <= 0) throw new Error('Invalid global edge');
    if (scores.get(edge.source)==null || scores.get(edge.target)==null) throw new Error('Edge endpoint lacks a score');
    if (!edge.stages || typeof edge.stages !== 'object' || Array.isArray(edge.stages)) throw new Error('Invalid stage weights');
    const stages = Object.entries(edge.stages);
    if (stages.some(([stage,weight]) => !['bachelor','master','doctorate','postdoc'].includes(stage) || !Number.isFinite(weight) || weight < 0)) throw new Error('Invalid stage weight');
    const total = stages.reduce((sum,[_stage,weight]) => sum+weight,0);
    if (!Number.isFinite(total) || Math.abs(total-edge.weight) > 1e-9*Math.max(1,edge.weight)) throw new Error('Stage weights do not match edge');
  }
  for (const field of ['events','people']) {
    if (!Number.isSafeInteger(data.coverage?.[field]) || data.coverage[field] < 0) throw new Error('Invalid coverage count');
  }
  const awards = data.official_award_inventory;
  if (awards != null) {
    if (!Array.isArray(awards.affiliations) || awards.affiliations.length > 25000 || typeof awards.categories !== 'object' || awards.categories === null) throw new Error('Invalid official award inventory');
    for (const field of ['processed_recipients','award_events']) {
      if (!Number.isSafeInteger(awards[field]) || awards[field] < 0) throw new Error('Invalid official award count');
    }
    if (awards.expected_recipients !== null && (!Number.isSafeInteger(awards.expected_recipients) || awards.expected_recipients < awards.processed_recipients)) throw new Error('Invalid official award population');
    if (Object.values(awards.categories).some(value => !Number.isSafeInteger(value) || value < 0)) throw new Error('Invalid official award categories');
    for (const affiliation of awards.affiliations) {
      if (typeof affiliation.name !== 'string' || !Number.isSafeInteger(affiliation.award_mentions) || affiliation.award_mentions < 0) throw new Error('Invalid official award affiliation');
    }
  }
  return data;
}

export function selectRows(data, {query = '', country = '', sort = 'score', scenario = ''} = {}) {
  const q = query.trim().toLocaleLowerCase();
  const rows = data.nodes.filter(n => (!q || n.name.toLocaleLowerCase().includes(q)) && (!country || n.country === country));
  const ranks = data.sensitivity?.[scenario]?.ranks;
  return rows.sort((a, b) => {
    if (sort === 'name') return a.name.localeCompare(b.name);
    if (sort === 'relations') return b.relations - a.relations || a.institution_id.localeCompare(b.institution_id);
    if (ranks) return (ranks[a.institution_id] ?? Infinity) - (ranks[b.institution_id] ?? Infinity) || a.institution_id.localeCompare(b.institution_id);
    return (b.network_score ?? -1) - (a.network_score ?? -1) || a.institution_id.localeCompare(b.institution_id);
  });
}

export function selectEdges(data, {stage = 'all', country = '', selected = ''} = {}) {
  const allowed = new Set(data.nodes.filter(n => !country || n.country === country).map(n => n.institution_id));
  return data.edges.filter(e => (!country || allowed.has(e.source) || allowed.has(e.target)) && (!selected || e.source === selected || e.target === selected))
    .map(e => ({...e, visible_weight: stage === 'all' ? e.weight : (e.stages?.[stage] ?? 0)}))
    .filter(e => e.visible_weight > 0).sort((a,b) => b.visible_weight - a.visible_weight || a.source.localeCompare(b.source) || a.target.localeCompare(b.target));
}

export function networkSlice(data, filters = {}) {
  const edges = selectEdges(data, filters);
  const ids = new Set();
  const visible = [];
  for (const edge of edges) {
    const extra = Number(!ids.has(edge.source)) + Number(!ids.has(edge.target));
    if (ids.size + extra > 500) continue;
    ids.add(edge.source); ids.add(edge.target); visible.push(edge);
    if (visible.length === 2000) break;
  }
  return {nodes: data.nodes.filter(n => ids.has(n.institution_id)), edges: visible, totalEdges: edges.length};
}

export function csv(rows) {
  const safe = value => {
    let text = value == null ? '' : String(value);
    if (/^[\s]*[=+\-@\t\r]/.test(text)) text = `'${text}`;
    return `"${text.replaceAll('"', '""')}"`;
  };
  const fields = ['institution_id','name','country','network_score','award_score','composite_score','observed_rank','formal_rank','status'];
  return [fields.join(','), ...rows.map(row => fields.map(f => safe(row[f])).join(','))].join('\r\n');
}
