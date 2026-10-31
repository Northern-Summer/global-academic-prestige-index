"""Physical packing preserves original inputs, descendants and scientific output."""
import json
import lzma
import unittest
from unittest.mock import patch

import test_streaming as fixtures
import test_global_analysis as analysis
from gapi.task import BudgetStop, canonical
from gapi.row_snapshot import freeze_rows, archive_path, inspect, lines
from gapi.row_packing import FORMAT, repack_full
from gapi.frozen_input import frozen_reader, file_hash
from gapi.global_analysis import rank_global


class FullRowPackingTests(unittest.TestCase):
    setUp = fixtures.StreamingTests.setUp
    tearDown = fixtures.StreamingTests.tearDown
    store = fixtures.StreamingTests.store

    def test_cli_dispatches_under_the_existing_runtime_boundary(self):
        import argparse
        from gapi.stream_cli import register, execute
        parser = argparse.ArgumentParser()
        register(parser.add_subparsers(dest='command'))
        args = parser.parse_args(['repack-full-input', '--filename', 'a' * 64 + '.rows', '--dry-run'])
        with patch('gapi.stream_cli.Task') as task, patch('gapi.stream_cli.execute_task', return_value=True) as run:
            self.assertTrue(execute(args))
            task.return_value.activate_runtime.assert_called_once()
            run.assert_called_once_with(task.return_value, args)
            task.return_value.restore_runtime.assert_called_once()

    def seed(self, store):
        analysis.AnalysisTests.seed(self, store)
        with store.member('wikidata', 'packed-full-extra'):
            for n in range(80):
                store.event(dict(person=f'packed-{n}', institution='wd:Q1',
                    kind='education', stage='doctorate', year_end=2000,
                    source='wikidata', locator='packed-full-extra', status='observed',
                    evidence='repeatable source evidence ' * 150))

    def test_exact_restore_scores_descendants_preview_and_idempotence(self):
        s = self.store(fixtures.archive([]))
        try:
            self.seed(s)
            full = freeze_rows(s)
            path = archive_path(s, full['filename'])
            before = b''.join(lines(path, s.task))
            with s.member('wikidata', 'delta-after-full'):
                s.event(dict(person='next', institution='wd:Q1', kind='education',
                    stage='doctorate', year_end=2001, source='wikidata',
                    locator='delta-after-full', status='observed'))
            delta = freeze_rows(s, base_filename=full['filename'])
            expected = canonical(rank_global(s))
            contents = path.read_bytes()
            preview = repack_full(s, full['filename'], dry_run=True)
            self.assertEqual(path.read_bytes(), contents)
            result = repack_full(s, full['filename'])
            self.assertGreater(result['saved_bytes'], 0)
            self.assertEqual(result['packed_bytes'], preview['candidate_bytes'])
            self.assertEqual(b''.join(lines(path, s.task)), before)
            self.assertEqual(inspect(path, s.task), (full['sha256'], full['bytes']))
            stamp = path.stat().st_mtime_ns
            self.assertTrue(repack_full(s, full['filename'])['already_repacked'])
            self.assertEqual(path.stat().st_mtime_ns, stamp)
            with frozen_reader(s, delta['filename']):
                self.assertEqual(canonical(rank_global(s)), expected)
            self.assertFalse(list(path.parent.glob('*.partial')))
        finally:
            s.close()

    def test_failed_write_and_foreign_partial_preserve_original(self):
        s = self.store(fixtures.archive([]))
        try:
            self.seed(s)
            full = freeze_rows(s)
            path = archive_path(s, full['filename'])
            wanted = file_hash(path)
            original_check = s.task.check
            calls = 0
            def denied(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls > 1:
                    raise BudgetStop('injected capacity exhaustion')
                return original_check(*args, **kwargs)
            with patch.object(s.task, 'check', side_effect=denied):
                with self.assertRaises(BudgetStop):
                    repack_full(s, full['filename'])
            self.assertEqual(file_hash(path), wanted)
            partial = path.with_suffix('.xz.repack-full.partial')
            self.assertFalse(partial.exists())
            partial.write_bytes(b'foreign recovery state')
            with self.assertRaises(FileExistsError):
                repack_full(s, full['filename'])
            self.assertEqual(partial.read_bytes(), b'foreign recovery state')
            self.assertEqual(file_hash(path), wanted)
        finally:
            s.close()

    def test_corruption_missing_footer_unknown_records_and_wrong_filename(self):
        s = self.store(fixtures.archive([]))
        try:
            self.seed(s)
            full = freeze_rows(s)
            path = archive_path(s, full['filename'])
            repack_full(s, full['filename'])
            packed = path.read_bytes()
            raw = lzma.decompress(packed)
            records = raw.splitlines(keepends=True)
            self.assertEqual(json.loads(records[0])['format'], FORMAT)
            cases = [b''.join(records[:-1]), raw + b'A{}\n',
                     b''.join([records[0], b'Xbad\n', *records[2:]])]
            altered = json.loads(records[0]); altered['bytes'] += 1
            cases.append(canonical(altered) + b'\n' + b''.join(records[1:]))
            for bad in cases:
                path.write_bytes(lzma.compress(bad, preset=3))
                with self.assertRaises(ValueError):
                    inspect(path, s.task)
            path.write_bytes(packed[:-5])
            with self.assertRaises((EOFError, lzma.LZMAError)):
                inspect(path, s.task)
            path.write_bytes(packed)
            other = path.with_name('0' * 64 + '.rows.xz')
            other.write_bytes(packed)
            with self.assertRaisesRegex(ValueError, 'filename identity'):
                inspect(other, s.task)
        finally:
            s.close()

    def test_source_change_and_unprofitable_candidate_preserve_predecessor(self):
        s = self.store(fixtures.archive([]))
        try:
            self.seed(s)
            full = freeze_rows(s)
            path = archive_path(s, full['filename'])
            wanted = file_hash(path)
            with patch('gapi.row_packing.monotonic', side_effect=[0, 2]):
                result = repack_full(s, full['filename'], max_decode_seconds=1)
            self.assertTrue(result['retained_predecessor'])
            self.assertEqual(file_hash(path), wanted)
            self.assertFalse(list(path.parent.glob('*.partial')))
            for invalid in (True, 0, -1, float('nan'), float('inf'), '1'):
                with self.assertRaisesRegex(ValueError, 'time limit'):
                    repack_full(s, full['filename'], max_decode_seconds=invalid)
            with patch('gapi.frozen_input.file_hash', side_effect=[wanted, '0' * 64]):
                with self.assertRaisesRegex(ValueError, 'changed during packing'):
                    repack_full(s, full['filename'])
            self.assertEqual(file_hash(path), wanted)
            # Reproduce a valid original stream exactly: no saving means keep
            # the predecessor, even though reconstruction itself succeeds.
            def oversized(source, expected_sha, expected_bytes, emit):
                # A legal legacy stream remains readable by the same reader.
                for line in source:
                    emit(line)
            with patch('gapi.row_packing._emit', side_effect=oversized):
                # Preset 3 reproduces the already packed original exactly.
                result = repack_full(s, full['filename'], compression_preset=3)
            self.assertTrue(result['retained_predecessor'])
            self.assertEqual(file_hash(path), wanted)
            self.assertFalse(list(path.parent.glob('*.partial')))
            for bad in (1, 'yes', None):
                with self.assertRaises(ValueError):
                    repack_full(s, full['filename'], dry_run=bad)
        finally:
            s.close()
