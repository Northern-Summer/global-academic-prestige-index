import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from gapi.ood import MEMORY_BYTES, SECONDS, main, run_ood
from gapi.task import BudgetStop, canonical


class OODTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = run_ood()

    def test_report_is_repeatable_and_explicitly_synthetic(self):
        self.assertEqual(canonical(self.report), canonical(run_ood()))
        self.assertEqual(self.report['dataset_kind'], 'synthetic/deidentified-test-fixtures')
        self.assertTrue(self.report['passed'])
        self.assertEqual(self.report['case_count'], 9)
        self.assertGreaterEqual(self.report['check_count'], 25)
        self.assertNotIn('execution', self.report)
        self.assertEqual(self.report['budget']['process_resident_bytes'], 128*1024**2)

    def test_degree_completion_and_postdoc_expected_counts(self):
        degree = self.report['cases']['multilingual_degree_completion']['metrics']
        self.assertEqual(degree['accepted_relations_by_stage'], {'doctorate': 6, 'master': 4, 'bachelor': 2})
        self.assertEqual(degree['accepted_relations'], 12)
        postdoc = self.report['cases']['postdoc_density']['metrics']
        self.assertEqual(postdoc['relations_by_stage'], {'postdoc': 1})
        self.assertEqual(postdoc['doctorate_relations'], 0)
        self.assertEqual(postdoc['edge_weight'], .6)

    def test_low_evidence_missing_data_and_name_collision(self):
        cases = self.report['cases']
        self.assertIsNone(cases['single_source_low_evidence_injection']['metrics']['injection_institution_score'])
        self.assertEqual(cases['single_source_low_evidence_injection']['metrics']['formal_ranks'], 0)
        self.assertEqual(cases['missing_and_reversed_years']['metrics']['accepted_relations'], 0)
        self.assertEqual(cases['same_name_distinct_ids']['metrics']['relations'], 1)
        self.assertEqual(cases['same_name_distinct_ids']['metrics']['fixture_false_person_joins'], 0)
        duplicate = cases['duplicate_and_alternative_evidence']['metrics']
        self.assertEqual((duplicate['before_weight'], duplicate['after_weight']), (1, 1))

    def test_source_deletion_keeps_boundaries_distinct(self):
        metrics = self.report['cases']['source_loss_network_boundary']['metrics']
        self.assertEqual(metrics['reference_nodes'], ['V', 'W'])
        fixed, expanded = metrics['fixed'], metrics['expanded']
        self.assertEqual((fixed['available_edges'], fixed['edges'], fixed['filtered_boundary_edges']), (2, 1, 1))
        self.assertEqual((expanded['edges'], expanded['node_count']), (2, 4))
        self.assertEqual(fixed['new_endpoints'], ['X', 'Z'])
        self.assertEqual(set(fixed['ranks']), {'V', 'W'})
        self.assertEqual(set(expanded['ranks']), {'V', 'W', 'X', 'Z'})

    def test_reversed_edges_and_invalid_weights_are_discriminated(self):
        metrics = self.report['cases']['direction_isolation_and_extreme_weights']['metrics']
        self.assertGreater(metrics['forward_training_score'], .5)
        self.assertLess(metrics['reverse_training_score'], .5)
        self.assertAlmostEqual(metrics['bidirectional_training_score'], .5)
        self.assertAlmostEqual(metrics['probability_mass'], 1)
        self.assertEqual(metrics['invalid_weight_cases_rejected'], ['overflow', 'nan', 'infinity', 'negative'])

    def test_region_prior_and_row_scaling_are_exercised(self):
        metrics = self.report['cases']['region_and_observed_scale']['metrics']
        self.assertEqual(metrics['fixture_region_group_sizes'], {'Asia': 1, 'Europe': 2})
        self.assertLess(metrics['max_score_drift_after_row_scaling'], 1e-12)
        self.assertAlmostEqual(metrics['region_balanced_prior']['probability_mass'], 1)
        self.assertIn('not all graduates', metrics['intensity_denominator'])

    def test_no_network_or_filesystem_database(self):
        import sqlite3
        connect = sqlite3.connect
        opened = []
        def memory_only(path, *args, **kwargs):
            opened.append(path)
            self.assertEqual(path, ':memory:')
            return connect(path, *args, **kwargs)
        with patch('sqlite3.connect', side_effect=memory_only), \
             patch('urllib.request.urlopen', side_effect=AssertionError('network forbidden')), \
             patch('socket.create_connection', side_effect=AssertionError('network forbidden')):
            self.assertTrue(run_ood()['passed'])
        self.assertEqual(len(opened), 8)

    def test_runtime_budget_failures_are_not_passes(self):
        # The existing production sampler owns the memory comparison.
        with patch('gapi.ood.Task.check_memory', side_effect=BudgetStop('memory exhausted')):
            with self.assertRaises(BudgetStop): run_ood()
        with patch('gapi.ood.time.monotonic', side_effect=[0, SECONDS+1]):
            with self.assertRaises(BudgetStop): run_ood()

    def test_failed_check_cli_returns_nonzero(self):
        with patch('gapi.ood.run_ood', return_value={'passed': False}), patch('builtins.print'):
            self.assertEqual(main([]), 1)

    def test_standalone_cli_resource_budget_and_stdout(self):
        env = dict(os.environ, PYTHONPATH=str(Path('src').resolve()), PYTHONDONTWRITEBYTECODE='1')
        child = subprocess.run([sys.executable, '-m', 'gapi.ood', '--measure'],
                               capture_output=True, text=True, env=env, timeout=SECONDS+2)
        self.assertEqual(child.returncode, 0, child.stderr)
        report = json.loads(child.stdout)
        runtime = report.pop('execution')
        self.assertEqual(report, self.report)
        self.assertLess(runtime['elapsed_seconds'], SECONDS)
        self.assertLess(runtime['max_sampled_rss_bytes'], MEMORY_BYTES)


if __name__ == '__main__':
    unittest.main()
