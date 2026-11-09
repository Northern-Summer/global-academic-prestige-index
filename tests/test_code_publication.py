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
    def test_memory_assets_prepare_exact_git_identity_without_local_blob_or_push(self):
        import gzip,hashlib
        raw=b'{"institution_count":2}';body=gzip.compress(raw,mtime=0)
        name='outputs/global/'+hashlib.sha256(raw).hexdigest()+'.json.gz'
        options=dict(paths=['code.py',name],external_bytes={name:body},prepare_only=True)
        result=self.publish(**options)
        blob=hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()
        self.assertIn(blob,self.git('ls-tree','-r',result['public_commit']))
        missing=subprocess.run(['git','cat-file','-e',blob],cwd=self.root,capture_output=True)
        self.assertNotEqual(missing.returncode,0);self.assertFalse((self.root/name).exists())
        self.assertEqual(self.pushes,0);self.assertEqual(self.publish(**options),result)
        for changed in ({'prepare_only':False},{'external_bytes':{name:body+b'drift'}},
                        {'external_bytes':{'private.txt':body},'paths':['code.py','private.txt']},
                        {'external_bytes':{name:b'x'*(24*1024**2+1)}}):
            with self.assertRaises(RuntimeError):self.publish(**{**options,**changed})
        self.assertEqual(self.pushes,0)

    def test_offline_preparation_has_no_remote_effect_and_drift_still_blocks_push(self):
        def offline(*args,**kwargs):
            if args[0] in ('ls-remote','push'):raise AssertionError('offline preparation contacted remote')
            return self.transport(*args,**kwargs)
        prepared=self.publish(prepare_only=True,git=offline)
        self.assertEqual(prepared['state'],'prepared');self.assertEqual(self.pushes,0)
        self.assertEqual(self.publish(prepare_only=True,git=offline),prepared)
        self.remote=self.git('rev-parse','HEAD')
        with self.assertRaisesRegex(RuntimeError,'branch changed'):self.publish()
        self.assertEqual(self.pushes,0)
    def test_frozen_external_asset_is_committed_without_checkout_copy_and_retries_once(self):
        import gzip,hashlib
        with tempfile.TemporaryDirectory(prefix='gapi-frozen-') as directory:
            frozen=Path(directory).resolve();raw=b'{"institution_count":2}';body=gzip.compress(raw,mtime=0)
            name=hashlib.sha256(raw).hexdigest()+'.json.gz';source=frozen/name;source.write_bytes(body)
            destination='outputs/global/'+name
            options=dict(paths=['code.py',destination],external_inputs={destination:source},external_root=frozen)
            result=self.publish(**options)
            actual=subprocess.run(['git','show',result['public_commit']+':'+destination],cwd=self.root,capture_output=True,check=True).stdout
            self.assertEqual(actual,body);self.assertFalse((self.root/destination).exists())
            self.assertNotIn('private.txt',self.git('ls-tree','-r','--name-only',result['public_commit']))
            self.assertEqual(self.publish(**options),result);self.assertEqual(self.pushes,1)
            source.write_bytes(body+b'drift')
            with self.assertRaises(RuntimeError):self.publish(**options)
            self.assertEqual(self.pushes,1)

    def test_external_input_boundary_and_mid_prepare_drift_prevent_push(self):
        import gzip,hashlib
        with tempfile.TemporaryDirectory(prefix='gapi-frozen-') as directory:
            frozen=Path(directory).resolve();raw=b'{}';name=hashlib.sha256(raw).hexdigest()+'.json.gz';source=frozen/name;source.write_bytes(gzip.compress(raw,mtime=0));destination='outputs/global/'+name
            options=dict(paths=['code.py',destination],external_inputs={destination:source},external_root=frozen)
            for changed in ({'external_root':self.root},{'external_root':None},
                            {'paths':['code.py']},{'external_inputs':{'private.txt':source},'paths':['code.py','private.txt']}):
                with self.assertRaises(RuntimeError):self.publish(**{**options,**changed})
            normal=self.transport
            def mutate(*args,**kwargs):
                value=normal(*args,**kwargs)
                if args[0]=='hash-object':source.write_bytes(b'changed during preparation')
                return value
            with self.assertRaisesRegex(RuntimeError,'changed while preparing'):
                self.publish(**options,git=mutate)
            self.assertEqual(self.pushes,0);self.assertFalse(self.receipt.exists())

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

    def test_prepared_memory_objects_settle_once_without_private_ancestry(self):
        import hashlib,base64
        self.git('remote','add','origin',self.origin)
        name='outputs/global/'+'a'*64+'.json.gz';body=b'reviewed aggregate fixture'
        prepared=self.publish(paths=['code.py',name],external_bytes={name:body},prepare_only=True)
        objects={};calls=[];remote=[self.base];lose=[True]
        def api(method,path,value):
            calls.append((method,path))
            if '/git/ref/' in path:return {'object':{'sha':remote[0]}}
            if method=='PATCH':
                self.assertFalse(value['force']);self.assertEqual(remote[0],self.base);remote[0]=value['sha']
                if lose[0]:lose[0]=False;raise RuntimeError('response lost')
            if method=='GET':return objects.get(path)
            if path.endswith('/blobs'):
                raw=base64.b64decode(value['content']);sha=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
                result={'sha':sha}
            elif path.endswith('/trees'):
                self.assertEqual({e['path'] for e in value['tree']},{'code.py',name})
                result={'sha':prepared['tree']}
            else:
                self.assertEqual(value['parents'],[self.base]);result={'sha':prepared['public_commit'],
                    'tree':{'sha':prepared['tree']},'parents':[{'sha':self.base}]}
            objects[path+'/'+result['sha']]=result;return result
        result=publication.publish_prepared_objects(self.root,receipt=self.receipt,api=api,external_bytes={name:body})
        self.assertEqual(result['state'],'pushed-and-read-back')
        self.assertEqual(sum(m=='PATCH' for m,_ in calls),1)
        publication.publish_prepared_objects(self.root,receipt=self.receipt,api=api,external_bytes={name:body})
        self.assertEqual(sum(m=='PATCH' for m,_ in calls),1)
        self.assertNotIn('private.txt',self.git('ls-tree','--name-only',prepared['public_commit']))
        remote[0]=self.git('rev-parse','HEAD')
        with self.assertRaisesRegex(RuntimeError,'advanced'):
            publication.publish_prepared_objects(self.root,receipt=self.receipt,api=api,external_bytes={name:body})
        self.assertEqual(sum(m=='PATCH' for m,_ in calls),1)

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
