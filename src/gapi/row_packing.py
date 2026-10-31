"""Physical full-row packing; exact logical rows remain the input identity.

Reuse the delta codec's reversible event expansion instead of compressing
Base64-encoded, already compressed payloads. No external dependencies or new
scientific rules. Callers serialize replacement with frozen readers/writers.
"""
import hashlib
import json
import lzma
import math
import os
import re
from time import monotonic

from .task import canonical

FORMAT = 'gapi-private-packed-ranking-rows/1'


def decoded_lines(path, task):
    from .row_delta import restore_addition
    from .row_snapshot import _packed_lines, MAX_LINE
    records = _packed_lines(path, task, max_line=MAX_LINE + 1)
    try:
        first = next(records, None)
        if first is None:
            raise ValueError('empty full row archive')
        header = json.loads(first)
        if not isinstance(header, dict) or header.get('format') != FORMAT:
            # Keep the original full-row format readable, with its old bound.
            if len(first) > MAX_LINE:
                raise ValueError('row archive line exceeds bound')
            yield first
            for record in records:
                if len(record) > MAX_LINE:
                    raise ValueError('row archive line exceeds bound')
                yield record
            return
        if (set(header) != {'format', 'sha256', 'bytes'}
                or not isinstance(header['sha256'], str)
                or not re.fullmatch('[a-f0-9]{64}', header['sha256'])
                or type(header['bytes']) is not int
                or not 0 < header['bytes'] <= task.config['budget_bytes'] * 4):
            raise ValueError('invalid packed full-row header')
        if re.match('[a-f0-9]{64}\\.', path.name) and path.name[:64] != header['sha256']:
            raise ValueError('packed full-row filename identity mismatch')
        sha = hashlib.sha256()
        size = count = 0
        ended = False
        for record in records:
            if ended:
                raise ValueError('trailing packed full-row records')
            if record.startswith(b'F '):
                footer = json.loads(record[2:])
                if (footer != {'sha256': sha.hexdigest(), 'bytes': size, 'lines': count}
                        or sha.hexdigest() != header['sha256']
                        or size != header['bytes'] or count < 2):
                    raise ValueError('packed full-row identity mismatch')
                ended = True
                continue
            if record.startswith(b'E'):
                value = restore_addition(record)
            elif record.startswith(b'A'):
                value = record[1:]
            else:
                raise ValueError('invalid packed full-row record')
            if not 1 < len(value) <= MAX_LINE:
                raise ValueError('invalid packed full-row length')
            size += len(value)
            count += 1
            if size > header['bytes']:
                raise ValueError('packed full-row decoded bound exceeded')
            sha.update(value)
            yield value
        if not ended:
            raise ValueError('incomplete packed full-row archive')
    finally:
        records.close()


def _emit(source, expected_sha, expected_bytes, emit):
    from .row_delta import addition_record
    emit(canonical({'format': FORMAT, 'sha256': expected_sha, 'bytes': expected_bytes}) + b'\n')
    sha = hashlib.sha256()
    size = count = 0
    for line in source:
        sha.update(line)
        size += len(line)
        count += 1
        emit(addition_record(line))
    if (sha.hexdigest(), size) != (expected_sha, expected_bytes) or count < 2:
        raise ValueError('full-row source identity mismatch')
    emit(b'F ' + canonical({'sha256': expected_sha, 'bytes': size, 'lines': count}) + b'\n')


def repack_full(store, filename, *, compression_preset=6, dry_run=False, max_decode_seconds=None):
    """Estimate in memory or atomically replace a verified retained full input.

    The candidate must fit beside the predecessor without spending the reserve.
    Never replace on size regression, changed source or reconstruction failure.
    Only our exclusively created partial file is cleaned on failure.
    """
    from .frozen_input import snapshot_path, file_hash
    from .row_snapshot import _packed_lines, inspect, lines, validate_compression_preset
    validate_compression_preset(compression_preset)
    if type(dry_run) is not bool:
        raise ValueError('dry_run must be boolean')
    if max_decode_seconds is not None and (type(max_decode_seconds) not in (int, float)
            or not math.isfinite(max_decode_seconds) or max_decode_seconds <= 0):
        raise ValueError('invalid candidate decode time limit')
    if store.db.in_transaction:
        raise ValueError('cannot repack unfinished transaction')
    path = snapshot_path(store, filename).with_suffix('.rows.xz')
    if not filename.endswith('.rows') or not path.is_file():
        raise ValueError('retained full-row input required')
    task = store.task
    expected = inspect(path, task)
    if expected[0] != filename[:64]:
        raise ValueError('retained full-row identity mismatch')
    reader = _packed_lines(path, task)
    try:
        header = json.loads(next(reader))
    finally:
        reader.close()
    if header.get('format') == FORMAT:
        return {'filename': filename, 'already_repacked': True, 'saved_bytes': 0,
                'sha256': expected[0], 'bytes': expected[1]}
    original_hash = file_hash(path)
    before = path.stat().st_size
    encoder = lzma.LZMACompressor(format=lzma.FORMAT_XZ, preset=compression_preset)
    source = lines(path, task)
    written = 0
    if dry_run:
        def measure(value):
            nonlocal written
            written += len(encoder.compress(value))
            task.check_memory()
        try:
            _emit(source, *expected, measure)
            written += len(encoder.flush())
        finally:
            source.close()
        if file_hash(path) != original_hash:
            raise ValueError('full-row source changed during preview')
        return {'filename': filename, 'dry_run': True, 'sha256': expected[0],
                'bytes': expected[1], 'previous_packed_bytes': before,
                'candidate_bytes': written, 'estimated_saved_bytes': before - written}
    initial = task.check(65536)
    temporary = path.with_suffix('.xz.repack-full.partial')
    created = False
    try:
        with temporary.open('xb') as output:
            created = True
            def write(value):
                nonlocal written
                task.check(len(value) + 65536, minimum_used=initial + written)
                task.check_memory()
                output.write(value)
                written += len(value)
            def emit(value):
                packed = encoder.compress(value)
                if packed:
                    write(packed)
            _emit(source, *expected, emit)
            write(encoder.flush())
            output.flush()
            os.fsync(output.fileno())
        started = monotonic()
        if inspect(temporary, task) != expected:
            raise ValueError('packed full-row reconstruction mismatch')
        decoded_seconds = monotonic() - started
        if file_hash(path) != original_hash:
            raise ValueError('full-row source changed during packing')
        after = temporary.stat().st_size
        if max_decode_seconds is not None and decoded_seconds > max_decode_seconds:
            return {'filename': filename, 'retained_predecessor': True, 'saved_bytes': 0,
                    'reason': 'candidate decode time exceeds acceptance limit',
                    'candidate_decode_seconds': decoded_seconds, 'max_decode_seconds': max_decode_seconds}
        if after >= before:
            return {'filename': filename, 'retained_predecessor': True, 'saved_bytes': 0}
        os.replace(temporary, path)
        return {'filename': filename, 'sha256': expected[0], 'bytes': expected[1],
                'format': FORMAT, 'previous_packed_bytes': before, 'packed_bytes': after,
                'saved_bytes': before - after, 'previous_storage_sha256': original_hash,
                'storage_sha256': file_hash(path), 'logical_identity_unchanged': True,
                'candidate_decode_seconds': decoded_seconds}
    finally:
        source.close()
        if created:
            temporary.unlink(missing_ok=True)
