// Versioned storage protocol only; eligibility and scores remain server outputs.
// Python owner: gapi.public_transport. Cross-runtime tests prove equivalence.
export const MAX_DIRECTORY_NODES = 131072;
const FORMAT = 'gapi-institution-defaults/1';
const forbidden = new Set(['__proto__', 'constructor', 'prototype']);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const fields = value => object(value) && Object.keys(value).length <= 64
  && Object.keys(value).every(key => key.length > 0 && key.length <= 64 && !forbidden.has(key));
// Match the ASCII-escaped representation used by the Python format guard.
// A surrogate pair occupies two escapes, including for astral characters.
const asciiJSONLength = value => JSON.stringify(value).replace(/[^\x00-\x7f]/g,
  ch => `\\u${ch.charCodeAt(0).toString(16).padStart(4, '0')}`).length;

export function decodeGlobalTransport(data) {
  if (data?.schema_version !== FORMAT) return data;
  if (Object.keys(data).sort().join(',') !== 'node_defaults,node_rows,payload,schema_version') throw new Error('Invalid compact institution envelope');
  const {payload, node_defaults: defaults, node_rows: rows} = data;
  if (!object(payload) || payload.schema_version !== 'gapi-global-observed/v1'
      || Object.hasOwn(payload, 'nodes') || Object.keys(payload).some(key => forbidden.has(key))
      || !fields(defaults) || Object.hasOwn(defaults, 'institution_id') || Object.hasOwn(defaults, 'name')
      || asciiJSONLength(defaults) > 65536
      || !Array.isArray(rows) || rows.length > MAX_DIRECTORY_NODES) throw new Error('Invalid compact institution structure');
  const nodes = rows.map(row => {
    if (!fields(row) || !Object.hasOwn(row, 'institution_id') || !Object.hasOwn(row, 'name')
        || new Set([...Object.keys(defaults), ...Object.keys(row)]).size > 64) throw new Error('Invalid compact institution row');
    return {...defaults, ...row};
  });
  return {...payload, nodes};
}
