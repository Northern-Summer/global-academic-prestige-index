import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {decodeGlobalTransport, MAX_DIRECTORY_NODES} from './global-transport.js';
import {validateGlobal} from './global-data.js';

const fixture = () => JSON.parse(readFileSync(new URL('../data/samples/public-transport-contract.json', import.meta.url)));

test('shared Python/JavaScript protocol fixture preserves every field, type and legacy input', () => {
  const {logical, wire} = fixture();
  const before = JSON.stringify(wire);
  const restored = decodeGlobalTransport(wire);
  assert.deepEqual(restored, logical);
  assert.equal(restored.nodes[0].flag, false);
  assert.equal(restored.nodes[1].flag, 0);
  assert.equal(JSON.stringify(wire), before);
  assert.equal(decodeGlobalTransport(logical), logical);
  const missing = fixture().wire;
  missing.node_rows[0].optional = null;
  assert.equal(Object.hasOwn(decodeGlobalTransport(missing).nodes[1], 'optional'), false);
});

test('compact hostile envelopes reject identity defaults, pollution, oversized defaults and node fanout', () => {
  const mutations = [
    v => {v.extra = true;}, v => {v.payload.nodes = [];},
    v => {v.payload.schema_version = 'synthetic/demo';},
    v => {v.node_defaults.name = 'hidden identity';},
    v => {v.node_defaults = JSON.parse('{"__proto__":{}}');},
    v => {v.node_rows = [{}];},
    v => {v.node_rows = Array(MAX_DIRECTORY_NODES + 1).fill(v.node_rows[0]);},
    v => {v.node_defaults.large = 'x'.repeat(65536);},
    v => {v.node_defaults.large = '中'.repeat(12000);},
    v => {v.node_defaults.large = '😀'.repeat(6000);},
    v => {Object.assign(v.node_rows[0], Object.fromEntries(Array.from({length: 65}, (_, i) => [String(i), null])));},
  ];
  for (const mutate of mutations) {
    const wire = fixture().wire;
    mutate(wire);
    assert.throws(() => decodeGlobalTransport(wire));
  }
  assert.equal({}.polluted, undefined);
});

test('transport decoding never waives score validation or duplicate identity rejection', () => {
  const wire = fixture().wire;
  assert.throws(() => validateGlobal(wire)); // incomplete logical fixture stays invalid
  wire.node_rows[1].institution_id = wire.node_rows[0].institution_id;
  assert.throws(() => validateGlobal(wire));
});
