"""Real Git object tests; remote effects are an explicit in-memory transport."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('code_publication', Path(__file__).resolve().parents[1]/'scripts/publish-code.py')
publication = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publication)


class CodePublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        (self.root/'code.py').write_text('old\n')
        self.git('add', 'code.py')
        self.git('commit', '-qm', 'public base')
        self.base = self.git('rev-parse', 'HEAD')
        # Local ancestry contains an unrelated private path. It must not escape.
        (self.root/'private.txt').write_text('synthetic private marker\n')
        self.git('add', 'private.txt')
        self.git('commit', '-qm', 'private local ancestor')
        (self.root/'code.py').write_text('reviewed change\n')
        self.remote = self.base
        self.pushes = 0
        self.ambiguous = False
        self.origin = 'https://github.com/example/project.git'
        self.push_urls = [self.origin]
        self.receipt = self.root/'receipt.json'

    def git(self, *args, env=None, input=None):
        run = subprocess.run(['git', *args], cwd=self.root, env=env, input=input,
                             capture_output=True, timeout=15)
        if run.returncode:
            raise RuntimeError(run.stderr.decode(errors='replace'))
        return run.stdout.decode().strip()

    def transport(self, *args, **kwargs):
        if args[:3] == ('remote', 'get-url', 'origin'):
            return self.origin
        if args[:4] == ('remote', 'get-url', '--push', '--all'):
            return '\n'.join(self.push_urls)
        if args[0] == 'ls-remote':
            return self.remote+'\trefs/heads/public'
        if args[0] == 'push':
            self.assertEqual(args[1], 'origin')
            self.assertNotIn('--force', args)
            self.pushes += 1
            self.remote = args[2].split(':')[0]
            if self.ambiguous:
                self.ambiguous = False
                raise RuntimeError('response lost after accepted push')
            return ''
        return self.git(*args, **kwargs)

    def publish(self, **changes):
        args = dict(repository='example/project', base=self.base, branch='refs/heads/public', paths=['code.py'],
                    receipt=self.receipt, message='Reviewed update', git=self.transport)
        args.update(changes)
        return publication.publish_code(self.root, **args)

    def test_public_parent_paths_and_repeat_are_exact(self):
        result = self.publish()
        self.assertEqual(self.git('show', '-s', '--format=%P', result['public_commit']), self.base)
        self.assertEqual(self.git('ls-tree', '--name-only', result['public_commit']), 'code.py')
        before = self.receipt.stat().st_mtime_ns
        self.assertEqual(self.publish(), result)
        self.assertEqual(self.pushes, 1)
        self.assertEqual(self.receipt.stat().st_mtime_ns, before)
        self.assertFalse(self.receipt.with_suffix('.index').exists())
        self.assertTrue((self.root/'private.txt').exists())

    def test_ambiguous_push_reconciles_without_second_push(self):
        self.ambiguous = True
        with self.assertRaisesRegex(RuntimeError, 'response lost'):
            self.publish()
        self.assertEqual(json.loads(self.receipt.read_text())['state'], 'prepared')
        self.assertEqual(self.publish()['state'], 'pushed-and-read-back')
        self.assertEqual(self.pushes, 1)

    def test_remote_drift_never_pushes(self):
        self.remote = self.git('rev-parse', 'HEAD')
        with self.assertRaisesRegex(RuntimeError, 'branch changed'):
            self.publish()
        self.assertEqual(self.pushes, 0)
        self.assertFalse(self.receipt.exists())
        self.assertFalse(self.receipt.with_suffix('.index').exists())

    def test_changed_reviewed_bytes_refused_on_retry(self):
        self.publish()
        (self.root/'code.py').write_text('unreviewed change\n')
        with self.assertRaisesRegex(RuntimeError, 'operation changed'):
            self.publish()
        self.assertEqual(self.pushes, 1)

    def test_private_ancestry_and_transport_cannot_be_selected_implicitly(self):
        for origin in ('git@github.com:example/project.git', 'https://token@github.com/example/project.git'):
            self.origin = origin
            with self.assertRaisesRegex(RuntimeError, 'HTTPS origin'):
                self.publish()
        self.assertEqual(self.pushes, 0)

    def test_paths_and_unfinished_index_fail_closed(self):
        for name in ('../private.txt', ':(glob)**', 'code.py*', '.git/config'):
            with self.subTest(path=name), self.assertRaises(RuntimeError):
                self.publish(paths=[name])
        self.receipt.with_suffix('.index').write_bytes(b'unsettled')
        with self.assertRaisesRegex(RuntimeError, 'unfinished publication index'):
            self.publish()
        self.assertEqual(self.receipt.with_suffix('.index').read_bytes(), b'unsettled')
        self.assertEqual(self.pushes, 0)

    def test_no_changes_do_not_create_commit_or_receipt(self):
        (self.root/'code.py').write_text('old\n')
        with self.assertRaisesRegex(RuntimeError, 'path set differs'):
            self.publish()
        self.assertFalse(self.receipt.exists())
        self.assertEqual(self.pushes, 0)

    def test_different_https_repository_refused(self):
        self.origin = 'https://github.com/example/other-project.git'
        with self.assertRaisesRegex(RuntimeError, 'reviewed repository'):
            self.publish()
        self.assertEqual(self.pushes, 0)

    def test_push_url_and_multiple_push_targets_are_refused(self):
        for urls in (['https://github.com/example/unreviewed.git'],
                     [self.origin, 'https://github.com/example/other.git'],
                     ['git@github.com:example/project.git']):
            self.push_urls = urls
            with self.subTest(urls=urls), self.assertRaises(RuntimeError):
                self.publish()
        self.assertEqual(self.pushes, 0)

    def test_receipt_is_bound_to_original_branch(self):
        self.publish()
        self.remote = self.base  # Another branch still points to the old base.
        with self.assertRaisesRegex(RuntimeError, 'branch'):
            self.publish(branch='refs/heads/other')
        self.assertEqual(self.pushes, 1)

    def test_legacy_receipt_missing_target_is_not_rebound_by_retry(self):
        self.publish()
        for field in ('branch', 'repository'):
            original = json.loads(self.receipt.read_text())
            broken = dict(original)
            broken.pop(field)
            self.receipt.write_text(json.dumps(broken))
            before = self.receipt.read_bytes()
            with self.assertRaisesRegex(RuntimeError, field):
                self.publish()
            self.assertEqual(self.receipt.read_bytes(), before)
            self.receipt.write_text(json.dumps(original))
        self.assertEqual(self.pushes, 1)

    def test_real_git_pushurl_resolution_refuses_other_target(self):
        self.git('remote', 'add', 'origin', self.origin)
        self.git('config', '--add', 'remote.origin.pushurl',
                 'https://github.com/example/unreviewed.git')
        def real_urls(*args, **kwargs):
            if args[0] == 'remote':
                return self.git(*args, **kwargs)
            return self.transport(*args, **kwargs)
        with self.assertRaisesRegex(RuntimeError, 'push URLs'):
            self.publish(git=real_urls)
        self.assertEqual(self.pushes, 0)
        self.assertFalse(self.receipt.exists())

    def test_tampered_receipt_commit_cannot_publish_private_tree(self):
        self.publish()
        record = json.loads(self.receipt.read_text())
        record['public_commit'] = self.git('rev-parse', 'HEAD')
        self.receipt.write_text(json.dumps(record))
        with self.assertRaisesRegex(RuntimeError, 'receipt commit'):
            self.publish()
        self.assertEqual(self.pushes, 1)
