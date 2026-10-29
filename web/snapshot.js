export function validateScoreSnapshot(snapshot) {
  const revision = snapshot?.snapshot_revision ?? 'unknown revision';
  const failures = [];
  const requireNumber = (object, path) => {
    const value = path.split('.').reduce((current, part) => current?.[part], object);
    if (!Number.isFinite(value)) failures.push(path);
  };

  if (snapshot?.dataset_kind !== 'real/derived-research-sample') failures.push('dataset_kind');
  if (typeof snapshot?.snapshot_revision !== 'string') failures.push('snapshot_revision');
  if (!Array.isArray(snapshot?.nodes) || snapshot.nodes.length === 0) failures.push('nodes');
  for (const path of [
    'algorithm.nodes', 'algorithm.edges', 'algorithm.within_ranked_network_edges',
    'algorithm.external_doctoral_origin_edges', 'algorithm.external_doctoral_origin_count',
    'algorithm.iterations', 'scope.employment_sample_start', 'scope.employment_sample_end',
    'boundary_sensitivity.induced_employer_subgraph.nodes',
    'boundary_sensitivity.all_observed_origins_as_nodes.nodes',
    'boundary_sensitivity.rank_comparison_among_employer_nodes.top_10.overlap_count',
    'boundary_sensitivity.rank_comparison_among_employer_nodes.top_25.overlap_count',
    'boundary_sensitivity.rank_comparison_among_employer_nodes.top_50.overlap_count',
  ]) requireNumber(snapshot, path);
  for (const [index, node] of (Array.isArray(snapshot?.nodes) ? snapshot.nodes : []).entries()) {
    for (const field of [
      'rank', 'gapi_network_score', 'source_recorded_doctoral_origin_count',
      'source_recorded_faculty_count', 'ranking_network_doctoral_origin_count',
      'ranking_network_employer_count', 'excluded_external_doctoral_origin_edges',
      'excluded_external_doctoral_origin_count',
    ]) {
      if (!Number.isFinite(node?.[field])) failures.push(`nodes[${index}].${field}`);
    }
    for (const field of ['institution_id', 'name']) {
      if (typeof node?.[field] !== 'string' || !node[field]) failures.push(`nodes[${index}].${field}`);
    }
    if (!Array.isArray(node?.roles)) failures.push(`nodes[${index}].roles`);
  }
  if (failures.length) throw new Error(`Snapshot ${revision} schema invalid: ${[...new Set(failures)].join(', ')}`);
  return snapshot;
}

export async function verifySnapshotBytes(bytes, manifest) {
  const revision = manifest?.snapshot_revision ?? 'unknown revision';
  if (!/^[a-f0-9]{64}$/i.test(manifest?.output_sha256 ?? '')) {
    throw new Error(`Snapshot ${revision} manifest invalid: output_sha256`);
  }
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  const actual = [...new Uint8Array(digest)].map((value) => value.toString(16).padStart(2, '0')).join('');
  if (actual !== manifest.output_sha256.toLowerCase()) {
    throw new Error(`Snapshot ${revision} integrity mismatch: output_sha256`);
  }
}
