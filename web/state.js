export function edgeWeight(edge, stage = 'all') {
  return stage === 'all' ? edge.weight : (edge.stages?.[stage] ?? 0);
}

export function filterEdges(edges, { stage = 'all', minWeight = 0 } = {}) {
  return edges.filter((edge) => {
    const weight = edgeWeight(edge, stage);
    return weight > 0 && weight >= minWeight;
  }).map((edge) => ({ ...edge, visible_weight: edgeWeight(edge, stage) }));
}

export function edgeWeightRange(edges, stage = 'all') {
  const weights = edges.map((edge) => edgeWeight(edge, stage)).filter((weight) => weight > 0).sort((a, b) => a - b);
  return { min: weights[0] ?? 0, max: weights.at(-1) ?? 0, count: weights.length };
}

export function edgesForInstitution(edges, institutionId) {
  return {
    incoming: edges.filter((edge) => edge.target === institutionId),
    outgoing: edges.filter((edge) => edge.source === institutionId),
  };
}

export function nodeColor(status) {
  return status === 'ranked' ? '#79b5a1' : '#d88a70';
}
