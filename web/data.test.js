import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

test('public demo export is labeled and contains no person identifiers', async () => {
  const data = JSON.parse(await readFile(resolve(root, 'data/demo/rankings.json'), 'utf8'));
  assert.equal(data.dataset_kind, 'synthetic/demo');
  assert.match(data.demo_notice, /Synthetic records only/);
  assert.ok(data.nodes.length >= 1);
  assert.ok(data.nodes.every((node) => !('person_id' in node)));
});

test('network edges have a valid direction and positive weight', async () => {
  const data = JSON.parse(await readFile(resolve(root, 'data/demo/rankings.json'), 'utf8'));
  const nodeIds = new Set(data.nodes.map((node) => node.institution_id));
  assert.ok(data.edges.length > 0);
  assert.ok(data.edges.every((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target) && edge.weight > 0));
});
