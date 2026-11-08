"""One temporary official archive, outside the unchanged database budget.

The caller explicitly authorizes this file exception. No ingestion, extraction,
ranking or automatic deletion occurs here. Delete after full task acceptance.
HTTP identity, pacing, refusal and Range validation reuse PublicHTTP.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
from types import SimpleNamespace
from urllib.error import URLError

from .career_store import exclusive_writer
from .source_http import PublicHTTP, StageDeadline
from .task import Task, digest

CHUNK = 65536
RESERVE = 256 * 1024**2


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        stream.write(json.dumps(value, indent=2) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def download(task, http, source, destination, *, max_seconds=21600, progress=None):
    """Resume file bytes only; full official MD5 gates final rename."""
    task.require_frozen()
    spec = task.config['sources'][source]
    total = spec['size_bytes']
    md5 = spec['checksum'].removeprefix('md5:')
    if not http._frozen_orcid_range(spec['url'], total, md5):
        raise ValueError('download requires exact frozen official archive')
    destination = Path(destination).absolute()
    for path in (destination, *destination.parents):
        if path.is_symlink() or (path.exists() and path.is_junction()):
            raise ValueError('download path contains a link')
    part = destination.with_suffix(destination.suffix + '.part')
    receipt = destination.with_suffix(destination.suffix + '.download.json')
    for path in (part, receipt, receipt.with_suffix(receipt.suffix + '.tmp')):
        if path.is_symlink() or (path.exists() and path.is_junction()):
            raise ValueError('download artifact contains a link')
    identity = digest(spec)
    state = json.loads(receipt.read_text()) if receipt.exists() else None
    if state and (state.get('source_identity') != identity or state.get('destination') != str(destination)):
        raise ValueError('download receipt source/destination drift')
    if destination.exists() and (not state or state.get('state') != 'verified-file'):
        # A rename can settle before receipt replacement. Reconcile below.
        if not state or part.exists() or destination.stat().st_size != total:
            raise ValueError('unowned existing destination')
    if part.exists() and not state:
        raise ValueError('partial file has no identity receipt')
    if destination.exists() and part.exists():
        raise ValueError('duplicate final/partial archive')
    position = (destination if destination.exists() else part).stat().st_size if destination.exists() or part.exists() else 0
    if position > total:
        raise ValueError('partial file exceeds frozen source length')
    state = state or {'source_identity': identity, 'destination': str(destination),
        'source': source, 'expected_bytes': total, 'expected_md5': md5,
        'headers': {}, 'retention': 'delete immediately after full task acceptance',
        'storage_exception': 'this one compressed file only; task budget unchanged'}
    if position and not state['headers']:
        raise ValueError('partial file has no source validators')
    started = time.monotonic()
    start_bytes = position
    next_report = started
    reconnects = stalls = 0
    if 'error_type' in state:
        state['previous_failure'] = {'error_type': state.pop('error_type'),
                                     'error': state.pop('error', '')}
    state.update(state='downloading', bytes=position)
    save(receipt, state)
    try:
        with http.within(max_seconds):
            while position < total:
                task.check_memory()
                if shutil.disk_usage(destination.parent).free < total-position+RESERVE:
                    raise OSError('insufficient physical space for remaining archive and reserve')
                response = (http.open_range(spec['url'], position, total, state['headers'], expected_md5=md5)
                    if position else http.open(spec['url'], headers={'Accept-Encoding': 'identity'}))
                previous = position
                try:
                    with response:
                        if not position:
                            if int(response.headers.get('Content-Length', '-1')) != total or response.headers.get('Content-Encoding', 'identity') != 'identity':
                                raise ValueError('initial archive representation/length mismatch')
                            state['headers'] = {key: value for key, value in response.headers.items()
                                if key.lower() in ('etag', 'last-modified')}
                            # A sentinel keeps validator-free, checksum-pinned sources resumable.
                            state['headers']['Content-Length'] = str(total)
                            save(receipt, state)
                        with part.open('ab') as output:
                            while position < total:
                                http.remaining()
                                chunk = response.read(min(CHUNK, total-position))
                                if not chunk:
                                    raise ConnectionError('early archive EOF')
                                output.write(chunk)
                                position += len(chunk)
                                now = time.monotonic()
                                if now >= next_report:
                                    output.flush(); os.fsync(output.fileno())
                                    task.check_memory()
                                    if shutil.disk_usage(destination.parent).free < RESERVE:
                                        raise OSError('download reserve exhausted')
                                    state.update(bytes=position, elapsed_seconds=now-started,
                                                 bytes_per_second=(position-start_bytes)/max(now-started,0.001))
                                    if hasattr(task, 'used'):
                                        state['normal_task_bytes'] = task.used()
                                        state['combined_bytes'] = state['normal_task_bytes'] + position
                                        state['unchanged_task_budget_bytes'] = task.config['budget_bytes']
                                    save(receipt, state)
                                    if progress: progress(state)
                                    next_report = now + 60
                            if response.read(1):
                                raise ValueError('archive exceeds declared length')
                            output.flush(); os.fsync(output.fileno())
                except StageDeadline:
                    raise
                except (ConnectionError, TimeoutError, URLError):
                    reconnects += 1
                    stalls = stalls+1 if position == previous else 0
                    if reconnects >= 32 or stalls >= 3:
                        raise
            state.update(state='verifying-file', bytes=position)
            save(receipt,state)
            actual = hashlib.md5()
            sha256 = hashlib.sha256()
            with (destination if destination.exists() else part).open('rb') as stream:
                while chunk := stream.read(CHUNK):
                    http.remaining(); task.check_memory()
                    actual.update(chunk); sha256.update(chunk)
            if actual.hexdigest() != md5:
                raise ValueError('official archive MD5 mismatch; retained for diagnosis')
            if not destination.exists(): os.replace(part, destination)
            state.update(state='verified-file', bytes=total, md5=actual.hexdigest(),
                         sha256=sha256.hexdigest(), elapsed_seconds=time.monotonic()-started)
            save(receipt,state)
            return state
    except BaseException as error:
        state.update(state='resumable-time-boundary' if isinstance(error,StageDeadline) else 'stopped',
                     bytes=part.stat().st_size if part.exists() else position,
                     error_type=type(error).__name__, error=str(error)[:250])
        if hasattr(task, 'used'):
            state['normal_task_bytes'] = task.used()
            state['combined_bytes'] = state['normal_task_bytes'] + state['bytes']
        save(receipt,state)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task', type=Path, required=True)
    parser.add_argument('--max-seconds', type=float, default=21600)
    args = parser.parse_args()
    if not 0 < args.max_seconds <= 21600: raise ValueError('invalid download time limit')
    task = Task(args.task)
    source = 'orcid-2026-summary'
    # One explicitly authorized compressed file, separately metered, no raw extraction.
    directory = task.resource_root.parent / 'archive-download'
    for path in (directory, *directory.parents):
        if path.is_symlink() or (path.exists() and path.is_junction()):
            raise ValueError('download directory contains a link')
    directory.mkdir(exist_ok=True)
    destination = directory / task.config['sources'][source]['filename']
    lock_owner = SimpleNamespace(root=directory, require_frozen=task.require_frozen, check=task.check)
    http = PublicHTTP(task)
    try:
        with exclusive_writer(lock_owner):
            result = download(task,http,source,destination,max_seconds=args.max_seconds,
                              progress=lambda state:print(json.dumps(state),flush=True))
            print(json.dumps(result),flush=True)
    finally: http.close()


if __name__ == '__main__': main()
