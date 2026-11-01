"""Logical preservation and hostile compact envelopes, separate from ranking."""
import copy
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gapi.public_transport import FORMAT, MAX_NODES, pack, unpack
from gapi.task import canonical

spec = importlib.util.spec_from_file_location('independent_transport_reader', 'scripts/verify-public-release.py')
independent = importlib.util.module_from_spec(spec)
spec.loader.exec_module(independent)


class PublicTransportTests(unittest.TestCase):
    def fixture(self):
        return json.loads(Path('data/samples/public-transport-contract.json').read_bytes())

    def test_shared_contract_exact_types_missing_values_and_legacy(self):
        fixture = self.fixture()
        for reader in (unpack, independent.expand_transport):
            restored = reader(fixture['wire'])
            self.assertEqual(canonical(restored), canonical(fixture['logical']))
            self.assertIs(restored['nodes'][0]['flag'], False)
            self.assertIs(type(restored['nodes'][1]['flag']), int)
            self.assertIs(reader(fixture['logical']), fixture['logical'])
        data = fixture['logical']
        for i, row in enumerate(data['nodes']):
            row['repeated'] = 'unranked explanation ' * 60
            if i:
                row['optional'] = None
        wanted = canonical(data)
        packed = pack(data)
        self.assertEqual(packed['schema_version'], FORMAT)
        self.assertEqual(canonical(unpack(packed)), wanted)
        self.assertEqual(canonical(independent.expand_transport(packed)), wanted)
        self.assertEqual(canonical(data), wanted)
        self.assertNotIn('optional', unpack(packed)['nodes'][0])
        self.assertEqual(canonical(pack(data)), canonical(packed))
        self.assertLess(len(canonical(packed)), len(wanted))

    def test_invalid_transport_not_silently_accepted(self):
        base = self.fixture()['wire']
        cases = []
        def changed(mutator):
            value = copy.deepcopy(base)
            mutator(value)
            cases.append(value)
        changed(lambda v: v.update(extra=True))
        changed(lambda v: v['payload'].update(nodes=[]))
        changed(lambda v: v['payload'].update(schema_version='synthetic/demo'))
        changed(lambda v: v['node_defaults'].update(name='hidden identity'))
        changed(lambda v: v['node_defaults'].update(__proto__={}))
        changed(lambda v: v.update(node_rows=[{}]))
        changed(lambda v: v.update(node_rows=[base['node_rows'][0]] * (MAX_NODES + 1)))
        changed(lambda v: v['node_defaults'].update(overlarge='x' * 65536))
        changed(lambda v: v['node_defaults'].update(overlarge='中' * 12000))
        changed(lambda v: v['node_defaults'].update(overlarge='😀' * 6000))
        changed(lambda v: v['node_rows'][0].update({str(i): None for i in range(65)}))
        for n, case in enumerate(cases):
            for reader in (unpack, independent.expand_transport):
                with self.subTest(case=n, reader=reader.__name__), self.assertRaises(ValueError):
                    reader(case)

    def test_actual_public_data_roundtrip_and_no_input_mutation(self):
        manifest = json.loads(Path('outputs/global/manifest.json').read_bytes())
        path = Path('outputs/global') / manifest.get('repository_snapshot', {}).get('filename', manifest['snapshot'])
        raw = path.read_bytes()
        if path.suffix == '.gz':
            raw = gzip.decompress(raw)
        original = unpack(json.loads(raw))
        logical = canonical(original)
        encoded = pack(original)
        self.assertEqual(canonical(unpack(encoded)), logical)
        self.assertEqual(canonical(independent.expand_transport(encoded)), logical)
        self.assertEqual(canonical(original), logical)
        self.assertLess(len(canonical(encoded)), 32 * 1024**2)

    def test_manifest_logical_hash_and_independent_score_replay(self):
        data = self.fixture()['logical']
        # Closed-form Y -> X PageRank, independent of the ranking implementation.
        a = 0.85
        data['algorithm'] = {'direction': 'employer -> training', 'network_universe': ['test:A', 'test:B'], 'config': {'alpha': a}}
        for node, score, rank in zip(data['nodes'], [(1 + a) / (2 + a), 1 / (2 + a), None], [1, 2, None]):
            node.update(network_score=score, observed_rank=rank)
        wire = self.fixture()['wire']
        wire['payload'] = {k: v for k, v in data.items() if k != 'nodes'}
        wire['node_defaults'] = {}
        wire['node_rows'] = data['nodes']
        raw = canonical(wire)
        sha = hashlib.sha256(raw).hexdigest()
        parent = 'D:/GAPI/global-academic-prestige-index' if os.name == 'nt' else None
        with tempfile.TemporaryDirectory(dir=parent) as folder:
            path = Path(folder)
            name = 'global-' + sha[:16] + '.json'
            (path / name).write_bytes(raw)
            manifest = {'snapshot': name, 'output_sha256': sha,
                        'logical_output_sha256': hashlib.sha256(canonical(data)).hexdigest()}
            p = path / 'manifest.json'
            p.write_bytes(canonical(manifest))
            self.assertEqual(independent.verify(p)['status'], 'passed')
            manifest['logical_output_sha256'] = '0' * 64
            p.write_bytes(canonical(manifest))
            with self.assertRaisesRegex(ValueError, 'logical snapshot identity'):
                independent.verify(p)

    def test_compact_export_integration_is_repeatable_and_replayable(self):
        from gapi.global_analysis import export_release
        from test_global_analysis import AnalysisTests
        fixture = AnalysisTests()
        fixture.setUp()
        store = None
        try:
            from test_streaming import archive
            store = fixture.store(archive([]))
            fixture.seed(store)
            out = fixture.root / 'outputs/global'
            # Only the isolated public path and code revision are substituted;
            # scoring, evidence selection, manifest and file writers are real.
            with patch('gapi.global_analysis.project_root', return_value=fixture.root), \
                    patch('gapi.global_analysis.subprocess.check_output', return_value='test-revision\n'):
                first = export_release(store, out, compact_nodes=True, compress_snapshot=True)
                snapshot = out / first['repository_snapshot']['filename']
                before = snapshot.read_bytes()
                pointer = (out / 'manifest.json').read_bytes()
                second = export_release(store, out, compact_nodes=True, compress_snapshot=True)
                self.assertEqual(first, second)
                self.assertEqual(before, snapshot.read_bytes())
                self.assertEqual(pointer, (out / 'manifest.json').read_bytes())
                restored = unpack(json.loads(gzip.decompress(before)))
                self.assertEqual(first['logical_output_sha256'], hashlib.sha256(canonical(restored)).hexdigest())
                self.assertEqual(independent.verify(out / 'manifest.json')['status'], 'passed')
                self.assertNotIn(b'private-person', gzip.decompress(before))
        finally:
            if store is not None:
                store.close()
            fixture.tearDown()
