export function validateGlobal(data) {
  if (data?.schema_version !== 'gapi-global-observed/v1' || data.dataset_kind !== 'real/global-public-observations') throw new Error('Invalid real global observation schema');
  if (!Array.isArray(data.nodes) || !Array.isArray(data.edges) || data.nodes.length > 25000 || data.edges.length > 200000) throw new Error('Global slice exceeds browser budget');
  const ids = new Set();
  for (const node of data.nodes) {
    if (typeof node.institution_id !== 'string' || typeof node.name !== 'string' || ids.has(node.institution_id)) throw new Error('Invalid or duplicate institution');
    ids.add(node.institution_id);
    for (const field of ['network_score', 'award_score', 'composite_score']) {
      if (node[field] !== null && (!Number.isFinite(node[field]) || node[field] < 0)) throw new Error(`Invalid ${field}`);
    }
    if (!Array.isArray(node.unranked_reasons)) throw new Error('Missing qualification reasons');
  }
  for (const edge of data.edges) {
    if (!ids.has(edge.source) || !ids.has(edge.target) || !Number.isFinite(edge.weight) || edge.weight <= 0) throw new Error('Invalid global edge');
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

export function networkSlice(data, {stage = 'all', country = '', selected = ''} = {}) {
  const allowed = new Set(data.nodes.filter(n => !country || n.country === country).map(n => n.institution_id));
  const edges = data.edges.filter(e => allowed.has(e.source) && allowed.has(e.target) && (!selected || e.source === selected || e.target === selected))
    .map(e => ({...e, visible_weight: stage === 'all' ? e.weight : (e.stages?.[stage] ?? 0)}))
    .filter(e => e.visible_weight > 0).sort((a,b) => b.visible_weight - a.visible_weight || a.source.localeCompare(b.source) || a.target.localeCompare(b.target));
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
