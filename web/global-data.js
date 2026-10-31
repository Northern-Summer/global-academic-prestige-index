// Directory capacity is independent of the much smaller rendered graph.
export const MAX_DIRECTORY_NODES = 65536;

export function validateGlobal(data) {
  if (data?.schema_version !== 'gapi-global-observed/v1' || data.dataset_kind !== 'real/global-public-observations') throw new Error('Invalid real global observation schema');
  if (!Array.isArray(data.nodes) || !Array.isArray(data.edges) || data.nodes.length > MAX_DIRECTORY_NODES || data.edges.length > 200000) throw new Error('Global slice exceeds browser budget');
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
  const acquisition = data.acquisition_progress;
  if (acquisition != null) {
    if (acquisition.scope !== 'current-discovered-candidates-only' || acquisition.complete_global_census !== false
        || acquisition.world_personnel_denominator !== null || !Array.isArray(acquisition.sources)
        || acquisition.sources.length > 20) throw new Error('Invalid acquisition scope');
    const sources = new Set();
    for (const source of acquisition.sources) {
      if (typeof source.source !== 'string' || !source.source || sources.has(source.source)
          || !/^[a-f0-9]{64}$/.test(source.selection_sha256)) throw new Error('Invalid acquisition source');
      sources.add(source.source);
      for (const field of ['candidates','processed','observed_members','rejected_members','pending','pending_with_errors']) {
        if (!Number.isSafeInteger(source[field]) || source[field] < 0) throw new Error('Invalid acquisition count');
      }
      if (source.candidates === 0 || source.processed > source.candidates
          || source.observed_members + source.rejected_members !== source.processed
          || source.pending !== source.candidates - source.processed || source.pending_with_errors > source.pending
          || source.enumeration_complete !== (source.pending === 0)) throw new Error('Acquisition totals mismatch');
    }
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
  const projection=data.official_award_attribution;
  if(projection!=null){
    const labels=projection.official_label_attributions;
    if(!awards||!Array.isArray(labels)||labels.length>25000||projection.label_count!==labels.length
        ||projection.canonical_identity_promotions!==0||projection.network_and_composite_scores_imputed!==false)throw new Error('Invalid award attribution contract');
    for(const field of ['label_count','unique_candidate_labels','ambiguous_candidate_labels','unresolved_labels'])
      if(!Number.isSafeInteger(projection[field])||projection[field]<0)throw new Error('Invalid award mapping count');
    if(projection.unique_candidate_labels+projection.ambiguous_candidate_labels+projection.unresolved_labels!==labels.length)throw new Error('Award mapping totals mismatch');
    const seen=new Set();
    for(const label of labels){
      if(typeof label.name!=='string'||typeof label.official_label_id!=='string'||seen.has(label.official_label_id)
          ||!['candidate-requires-confirmation','ambiguous-candidate','unresolved'].includes(label.identity_status)
          ||!Array.isArray(label.ror_candidates)||label.ror_candidates.length>1000)throw new Error('Invalid award label identity');
      seen.add(label.official_label_id);
      for(const field of ['award_mentions','personal_single_affiliation_events','personal_multi_affiliation_events','institutional_events'])
        if(!Number.isSafeInteger(label[field])||label[field]<0)throw new Error('Invalid award attribution count');
      for(const field of ['nobel_share_single_affiliation','nobel_share_equal_split_sensitivity']){
        const value=label[field];
        if(!value||typeof value.numerator!=='string'||typeof value.denominator!=='string'
            ||!/^(0|[1-9]\d{0,511})$/.test(value.numerator)||!/^[1-9]\d{0,511}$/.test(value.denominator))throw new Error('Invalid exact award share');
        if(BigInt(value.numerator)>BigInt(label.award_mentions)*BigInt(value.denominator))throw new Error('Award share exceeds observed mentions');
      }
      const main=label.nobel_share_single_affiliation,split=label.nobel_share_equal_split_sensitivity;
      if(BigInt(main.numerator)*BigInt(split.denominator)>BigInt(split.numerator)*BigInt(main.denominator))throw new Error('Award scenario omits main attribution');
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

// Rendering limits only; table selection and ranking inputs remain complete.
export const NETWORK_VIEWS = Object.freeze({
  focus: Object.freeze({nodes:80, edges:40}),
  all: Object.freeze({nodes:500, edges:2000})
});

export function networkSlice(data, filters = {}, view = 'all') {
  const limits = Object.hasOwn(NETWORK_VIEWS, view) ? NETWORK_VIEWS[view] : null;
  if (!limits) throw new Error('Unknown network view');
  const edges = selectEdges(data, filters);
  const ids = new Set();
  const visible = [];
  for (const edge of edges) {
    const extra = Number(!ids.has(edge.source)) + Number(!ids.has(edge.target));
    if (ids.size + extra > limits.nodes) continue;
    ids.add(edge.source); ids.add(edge.target); visible.push(edge);
    if (visible.length === limits.edges) break;
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
