"""Lossless public node defaults; no score, evidence or eligibility decisions.

The versioned transport has independent Python/JavaScript readers. Their exact
logical equivalence is exercised on common fixtures and actual public data.
"""
from collections import Counter

from .task import canonical

FORMAT = 'gapi-institution-defaults/1'
# Directory entries include unranked observed institutions. Independent readers
# mirror this transport bound; shared boundary tests enforce compatibility.
MAX_NODES = 131072
MAX_FIELDS = 64
MAX_DEFAULT_BYTES = 65536
FORBIDDEN = {'__proto__', 'constructor', 'prototype'}


def _object(value):
    return (isinstance(value, dict) and len(value) <= MAX_FIELDS
            and all(isinstance(k, str) and 0 < len(k) <= 64 and k not in FORBIDDEN for k in value))


def unpack(data):
    """Expand only an explicit envelope; legacy logical snapshots pass through."""
    if not isinstance(data, dict) or data.get('schema_version') != FORMAT:
        return data
    if set(data) != {'schema_version', 'payload', 'node_defaults', 'node_rows'}:
        raise ValueError('invalid compact institution envelope')
    payload, defaults, rows = data['payload'], data['node_defaults'], data['node_rows']
    if (not isinstance(payload, dict) or payload.get('schema_version') != 'gapi-global-observed/v1'
            or 'nodes' in payload or any(k in FORBIDDEN for k in payload)
            or not _object(defaults) or {'institution_id', 'name'} & defaults.keys()
            or len(canonical(defaults)) > MAX_DEFAULT_BYTES
            or not isinstance(rows, list) or len(rows) > MAX_NODES):
        raise ValueError('invalid compact institution structure')
    nodes = []
    for row in rows:
        if (not _object(row) or not {'institution_id', 'name'} <= row.keys()
                or len(set(defaults) | set(row)) > MAX_FIELDS):
            raise ValueError('invalid compact institution row')
        nodes.append({**defaults, **row})
    return {**payload, 'nodes': nodes}


def pack(data):
    """Deterministic modal defaults, preserving absent fields and value types.

    Fields with many distinct values are skipped using bounded counters. Values
    are compared as canonical JSON, so false and zero are never conflated.
    Identity and name always remain explicit per institution. If compacting
    cannot save bytes, retain the original representation.
    """
    if not isinstance(data, dict) or data.get('schema_version') != 'gapi-global-observed/v1':
        raise ValueError('logical public observation snapshot required')
    nodes = data.get('nodes')
    if not isinstance(nodes, list) or len(nodes) > MAX_NODES or any(not _object(n) for n in nodes):
        raise ValueError('invalid institution directory')
    if not nodes:
        return data
    common = set.intersection(*(set(n) for n in nodes)) - {'institution_id', 'name'}
    defaults, encoded = {}, {}
    for field in sorted(common):
        counts = Counter()
        values = {}
        for node in nodes:
            value = canonical(node[field])
            if len(value) > 16384:
                break
            counts[value] += 1
            values[value] = node[field]
            if len(counts) > 64:
                break
        else:
            value = min(counts, key=lambda v: (-counts[v], v))
            # Account for one default copy and repeated key/value punctuation.
            if counts[value] > 1 and (counts[value] - 1) * (len(canonical(field)) + len(value) + 2) > 32:
                defaults[field] = values[value]
                encoded[field] = value
    if len(canonical(defaults)) > MAX_DEFAULT_BYTES:
        raise ValueError('compact defaults exceed bound')
    rows = [{key: value for key, value in node.items()
             if key not in defaults or canonical(value) != encoded[key]} for node in nodes]
    result = {'schema_version': FORMAT, 'payload': {k: v for k, v in data.items() if k != 'nodes'},
              'node_defaults': defaults, 'node_rows': rows}
    # Equivalence is a mandatory contract check, not an inferred compression claim.
    if canonical(unpack(result)) != canonical(data):
        raise ValueError('compact institution reconstruction mismatch')
    return result if len(canonical(result)) < len(canonical(data)) else data
