"""Optional native checksum recovery, checked against hashlib in real processes."""
import copy
import hashlib
import json
import os
import random
import subprocess
import sys
import unittest

from gapi.archive_hash import ArchiveHash, verify_native_hashes
from gapi.task import digest


class ArchiveHashTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.name != 'nt':
            raise unittest.SkipTest('optional interpreter-local Windows OpenSSL backend')
        # On supported Windows runtimes, missing capabilities must fail visibly.
        verify_native_hashes()

    def test_repeated_snapshots_partial_blocks_and_nonmutating_digest(self):
        rng = random.Random(271828)
        for algorithm in ('md5', 'sha256'):
            reference = hashlib.new(algorithm, usedforsecurity=False)
            native = ArchiveHash(algorithm)
            for size in (0, 1, 55, 56, 63, 64, 65, 1023, 65536, 7, 65535):
                data = rng.randbytes(size)
                reference.update(data)
                native.update(data)
                self.assertEqual(native.hexdigest(), reference.hexdigest())
                self.assertEqual(native.hexdigest(), reference.hexdigest())
                native = ArchiveHash.restore(json.loads(json.dumps(native.snapshot())))
                self.assertEqual(native.hexdigest(), reference.hexdigest())

    def test_new_process_continues_both_checksums_without_prefix(self):
        prefix = random.Random(17).randbytes(65539)
        suffix = b'new bytes only\x00\xff'
        states = []
        for algorithm in ('md5', 'sha256'):
            native = ArchiveHash(algorithm)
            native.update(prefix[:65536]); native.update(prefix[65536:])
            states.append(native.snapshot())
        code = '''
import json,sys
from gapi.archive_hash import ArchiveHash
states=json.load(sys.stdin)
result=[]
for state in states:
    h=ArchiveHash.restore(state)
    h.update(bytes.fromhex(sys.argv[1]))
    result.append(h.hexdigest())
print(json.dumps(result))
'''
        run = subprocess.run([sys.executable, '-B', '-c', code, suffix.hex()],
            input=json.dumps(states), text=True, capture_output=True, timeout=20)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(json.loads(run.stdout), [
            hashlib.new(a, prefix+suffix, usedforsecurity=False).hexdigest()
            for a in ('md5', 'sha256')])

    def test_corrupt_truncated_and_version_drift_states_fail_closed(self):
        native = ArchiveHash('sha256'); native.update(b'partial block')
        original = native.snapshot()
        broken = copy.deepcopy(original); broken['fields']['h'][0] ^= 1
        with self.assertRaisesRegex(ValueError, 'checksum'):
            ArchiveHash.restore(broken)
        mutations = [
            ('openssl', 'OpenSSL 3.different'), ('context_bytes', 1),
            ('byte_count', original['byte_count']+1), ('byte_count', True)]
        for key, value in mutations:
            broken = copy.deepcopy(original); broken[key] = value
            broken['sha256'] = digest({k:v for k,v in broken.items() if k != 'sha256'})
            with self.assertRaises(ValueError): ArchiveHash.restore(broken)
        for name, value in [('data', [0]), ('num', 64), ('md_len', 16), ('Nl', -1)]:
            broken = copy.deepcopy(original); broken['fields'][name] = value
            broken['sha256'] = digest({k:v for k,v in broken.items() if k != 'sha256'})
            with self.assertRaises(ValueError): ArchiveHash.restore(broken)
        with self.assertRaises(ValueError): ArchiveHash.restore({})

    def test_algorithm_and_update_bounds(self):
        with self.assertRaises(ValueError): ArchiveHash('sha1')
        native = ArchiveHash('md5')
        for bad in (b'x'*65537, bytearray(b'x'), 'x', None):
            with self.assertRaises(ValueError): native.update(bad)
        self.assertEqual(native.size, 0)
        self.assertEqual(native.hexdigest(), hashlib.md5(b'').hexdigest())


if __name__ == '__main__': unittest.main()
