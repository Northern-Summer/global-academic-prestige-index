"""Optional process-restorable archive checksums, bound to the native ABI.

Only the archive recovery path may use these states. Ordinary hashing retains
hashlib. OpenSSL's public low-level contexts contain no pointers; snapshots use
named integer fields rather than dumping Python or provider-owned memory.
The caller must atomically bind the state to frozen source and byte position.
"""
from __future__ import annotations

import ctypes
import hashlib
import json
from pathlib import Path

from .task import canonical, digest


class _MD5(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint32) for name in ('A', 'B', 'C', 'D', 'Nl', 'Nh')]
    _fields_ += [('data', ctypes.c_uint32 * 16), ('num', ctypes.c_uint32)]


class _SHA256(ctypes.Structure):
    _fields_ = [('h', ctypes.c_uint32 * 8), ('Nl', ctypes.c_uint32),
                ('Nh', ctypes.c_uint32), ('data', ctypes.c_uint32 * 16),
                ('num', ctypes.c_uint32), ('md_len', ctypes.c_uint32)]


def _native():
    # Reuse the DLL shipped beside the interpreter's actual _hashlib extension;
    # never load a name from CWD/PATH or silently install a crypto provider.
    import _hashlib
    import sys
    extension=Path(_hashlib.__file__).resolve()
    if sys.platform=='win32':
        candidates=[extension.parent/name for name in ('libcrypto-3.dll','libcrypto-3-x64.dll')]
        found=[p for p in candidates if p.is_file()]
        if len(found)!=1:raise RuntimeError('restorable archive hash requires one interpreter-local OpenSSL 3 DLL')
        path=found[0]
    else:
        # Symbols resolve through this exact interpreter extension's linked
        # OpenSSL dependency; never search CWD/PATH or select another provider.
        path=extension
    library = ctypes.CDLL(str(path))
    library.OpenSSL_version.argtypes = [ctypes.c_int]
    library.OpenSSL_version.restype = ctypes.c_char_p
    version = library.OpenSSL_version(0).decode('ascii')
    if not version.startswith('OpenSSL 3.'):
        raise RuntimeError('unsupported archive checksum native version')
    for algorithm, context, prefix in [('md5', _MD5, 'MD5'), ('sha256', _SHA256, 'SHA256')]:
        for suffix, args in [('Init', [ctypes.POINTER(context)]),
                             ('Update', [ctypes.POINTER(context), ctypes.c_void_p, ctypes.c_size_t]),
                             ('Final', [ctypes.c_void_p, ctypes.POINTER(context)])]:
            method = getattr(library, prefix + '_' + suffix)
            method.argtypes, method.restype = args, ctypes.c_int
    return library, version


class ArchiveHash:
    """Hashlib-like bounded update plus a validated, version-bound snapshot."""
    def __init__(self, algorithm):
        if algorithm not in ('md5', 'sha256'):
            raise ValueError('unsupported archive checksum')
        self.algorithm = algorithm
        self.library, self.version = _native()
        self.context = (_MD5 if algorithm == 'md5' else _SHA256)()
        self.prefix = 'MD5' if algorithm == 'md5' else 'SHA256'
        self.size = 0
        if getattr(self.library, self.prefix + '_Init')(ctypes.byref(self.context)) != 1:
            raise RuntimeError('archive checksum initialization failed')

    def update(self, data):
        if not isinstance(data, bytes) or len(data) > 65536:
            raise ValueError('archive checksum requires bytes of at most 64 KiB')
        if self.size + len(data) >= 2**61:
            raise ValueError('archive checksum bit count overflow')
        if getattr(self.library, self.prefix + '_Update')(
                ctypes.byref(self.context), data, len(data)) != 1:
            raise RuntimeError('archive checksum update failed')
        self.size += len(data)

    def hexdigest(self):
        clone = type(self.context).from_buffer_copy(self.context)
        output = ctypes.create_string_buffer(16 if self.algorithm == 'md5' else 32)
        if getattr(self.library, self.prefix + '_Final')(output, ctypes.byref(clone)) != 1:
            raise RuntimeError('archive checksum finalization failed')
        return output.raw.hex()

    def snapshot(self):
        fields = {}
        for name, _ in self.context._fields_:
            value = getattr(self.context, name)
            fields[name] = list(value) if isinstance(value, ctypes.Array) else value
        value = {'schema': 'archive-hash-state/v1', 'algorithm': self.algorithm,
                 'openssl': self.version, 'context_bytes': ctypes.sizeof(self.context),
                 'byte_count': self.size, 'fields': fields}
        return {**value, 'sha256': digest(value)}

    @classmethod
    def restore(cls, value):
        if not isinstance(value, dict) or len(canonical(value)) > 4096:
            raise ValueError('invalid archive hash state')
        keys = {'schema', 'algorithm', 'openssl', 'context_bytes', 'byte_count', 'fields', 'sha256'}
        if set(value) != keys or value['schema'] != 'archive-hash-state/v1':
            raise ValueError('archive hash state schema mismatch')
        if digest({k:v for k,v in value.items() if k != 'sha256'}) != value['sha256']:
            raise ValueError('archive hash state checksum mismatch')
        restored = cls(value['algorithm'])
        if (value['openssl'] != restored.version
                or value['context_bytes'] != ctypes.sizeof(restored.context)):
            raise ValueError('archive hash state native version drift')
        size = value['byte_count']
        fields = value['fields']
        if type(size) is not int or not 0 <= size < 2**61:
            raise ValueError('archive hash byte count invalid')
        if not isinstance(fields, dict) or set(fields) != {n for n, _ in restored.context._fields_}:
            raise ValueError('archive hash fields mismatch')
        for name, _ in restored.context._fields_:
            current, incoming = getattr(restored.context, name), fields[name]
            items = incoming if isinstance(current, ctypes.Array) else [incoming]
            if (not isinstance(items, list)
                    or len(items) != (len(current) if isinstance(current, ctypes.Array) else 1)
                    or any(type(x) is not int or not 0 <= x < 2**32 for x in items)):
                raise ValueError('archive hash integer field invalid')
            if isinstance(current, ctypes.Array):
                current[:] = items
            else:
                setattr(restored.context, name, incoming)
        if (fields['num'] != size % 64 or fields['Nl'] != (size * 8) % 2**32
                or fields['Nh'] != (size * 8) >> 32
                or (restored.algorithm == 'sha256' and fields['md_len'] != 32)):
            raise ValueError('archive hash byte count/context mismatch')
        restored.size = size
        return restored


def verify_native_hashes():
    """Caller must run before using this optional ABI for production bytes."""
    for algorithm in ('md5', 'sha256'):
        for size in (0, 1, 55, 56, 63, 64, 65, 65536, 65539):
            data = (bytes(range(256)) * (size // 256 + 1))[:size]
            native = ArchiveHash(algorithm)
            for offset in range(0, len(data), 32771):
                native.update(data[offset:offset+32771])
                native = ArchiveHash.restore(json.loads(json.dumps(native.snapshot())))
            expected = hashlib.new(algorithm, data, usedforsecurity=False).hexdigest()
            if native.hexdigest() != expected:
                raise RuntimeError('native archive checksum ABI self-check failed')
