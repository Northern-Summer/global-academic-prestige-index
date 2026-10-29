import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { edgeWeightRange, edgesForInstitution, filterEdges, nodeColor } from './state.js';
import { validateScoreSnapshot, verifySnapshotBytes } from './snapshot.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

test('public demo exports are labeled and contain no person identifiers', async () => {
  const data = JSON.parse(await readFile(resolve(root, 'data/demo/rankings.json'), 'utf8'));
  assert.equal(data.dataset_kind, 'synthetic/demo');
  assert.match(data.demo_notice, /Synthetic records only/);
  assert.ok(data.nodes.length >= 1);
  assert.ok(data.nodes.every((node) => !('person_id' in node)));
  assert.ok(data.nodes.every((node) => Array.isArray(node.unranked_reasons)));
});

test('synthetic scenarios are multistage, asymmetric, and visibly separated from real observations', async () => {
  const data = JSON.parse(await readFile(resolve(root, 'data/demo/scenarios.json'), 'utf8'));
  assert.equal(data.dataset_kind, 'synthetic/demo');
  assert.match(data.notice, /constructed teaching fixture/);
  const baseline = data.scenarios.find((scenario) => scenario.id === 'baseline');
  assert.ok(baseline.result.edges.some((edge) => Object.keys(edge.stages).length > 1));
  const weights = new Map(baseline.result.edges.map((edge) => [`${edge.source}|${edge.target}`, edge.weight]));
  assert.notEqual(weights.get('gapi:alpha|gapi:bravo'), weights.get('gapi:bravo|gapi:alpha'));
  assert.deepEqual(new Set(data.scenarios.map((scenario) => scenario.id)).size, data.scenarios.length);
});

test('stage and minimum-weight filters define one shared edge slice for network and details', () => {
  const edges = [
    { source: 'hire-a', target: 'train-a', weight: 4, stages: { doctorate: 3, master: 1 } },
    { source: 'hire-b', target: 'train-a', weight: 1, stages: { master: 1 } },
    { source: 'hire-a', target: 'train-b', weight: 2, stages: { doctorate: 2 } },
  ];
  const filtered = filterEdges(edges, { stage: 'doctorate', minWeight: 2 });
  assert.deepEqual(filtered.map((edge) => [edge.source, edge.target, edge.visible_weight]), [['hire-a', 'train-a', 3], ['hire-a', 'train-b', 2]]);
  const details = edgesForInstitution(filtered, 'train-a');
  assert.deepEqual(details.incoming.map((edge) => edge.target), ['train-a']);
  assert.deepEqual(details.outgoing, []);
});

test('flow threshold range follows selected stage and high threshold excludes flows', () => {
  const edges = [
    { source: 'a', target: 'b', weight: 7, stages: { doctorate: 6, master: 1 } },
    { source: 'b', target: 'a', weight: 2, stages: { doctorate: 1, master: 1 } },
  ];
  assert.deepEqual(edgeWeightRange(edges, 'all'), { min: 2, max: 7, count: 2 });
  assert.deepEqual(edgeWeightRange(edges, 'doctorate'), { min: 1, max: 6, count: 2 });
  assert.equal(filterEdges(edges, { stage: 'all', minWeight: 6 }).length, 1);
  assert.equal(filterEdges(edges, { stage: 'doctorate', minWeight: 6 }).length, 1);
});

test('scenario node color maps eligibility only and never encodes a score', async () => {
  const data = JSON.parse(await readFile(resolve(root, 'data/demo/scenarios.json'), 'utf8'));
  const baseline = data.scenarios.find((scenario) => scenario.id === 'baseline').result.nodes;
  const removedHub = data.scenarios.find((scenario) => scenario.id === 'without_alpha_hiring').result.nodes;
  const baselineById = new Map(baseline.map((node) => [node.institution_id, node]));
  const hub = removedHub.find((node) => baselineById.get(node.institution_id)?.status === 'ranked' && node.status !== 'ranked');

  assert.equal(nodeColor('ranked'), '#79b5a1');
  assert.equal(nodeColor('unranked_low_coverage'), '#d88a70');
  assert.ok(hub && nodeColor(hub.status) === '#d88a70');
});

test('static HTML exposes a real-score ranking and contains no synthetic-score controls', async () => {
  const html = await readFile(resolve(root, 'index.html'), 'utf8');
  assert.match(html, /id="directory-rows"/);
  assert.match(html, /REAL SAMPLE SCORES/);
  assert.match(html, /2011–2020/);
  assert.match(html, /GAPI network score/);
  assert.doesNotMatch(html, /id="scenario-filter"|id="network-canvas"|demo score|demo ranking/i);
  assert.match(html, /role="status"/);
});

test('frontend loads only the local real-data score snapshot and has no synthetic fallback', async () => {
  const source = await readFile(resolve(root, 'web/main.js'), 'utf8');
  assert.match(source, /data\/local\/us-academia-network\.json/);
  assert.match(source, /validateScoreSnapshot/);
  assert.match(source, /verifySnapshotBytes/);
  assert.match(source, /snapshot\.algorithm\.nodes/);
  assert.match(source, /Synthetic\/demo scores are never used as a fallback/);
  assert.doesNotMatch(source, /data\/demo|scenarios\.json/);
});

test('snapshot validation names missing fields and binds errors to a revision', () => {
  assert.throws(
    () => validateScoreSnapshot({ dataset_kind: 'real/derived-research-sample', snapshot_revision: 'test-r4', nodes: [], algorithm: {} }),
    /Snapshot test-r4 schema invalid: nodes, algorithm\.nodes/,
  );
});

test('snapshot hash mismatch fails closed with the pinned revision in its diagnostic', async () => {
  const bytes = new TextEncoder().encode('{"real":true}');
  await assert.rejects(
    verifySnapshotBytes(bytes, { snapshot_revision: 'test-r5', output_sha256: '0'.repeat(64) }),
    /Snapshot test-r5 integrity mismatch: output_sha256/,
  );
});
