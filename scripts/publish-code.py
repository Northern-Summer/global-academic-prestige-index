"""Publish an explicit reviewed path set on a verified public-only Git parent.

Git refs use the repository's HTTPS origin and normal GCM interface. The caller
owns review, tests and release authorization. A receipt is one retry boundary;
this module does not create tags, Releases, data snapshots or deployments.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def file_hash(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def publish_code(root, *, repository, base, branch, paths, receipt, message, git=None):
    root = Path(root).resolve()
    receipt = Path(receipt)
    require(receipt.is_absolute() and receipt.resolve().is_relative_to(root),
            'receipt must belong to this checkout')
    require(not receipt.is_symlink(), 'receipt must not be a symlink')
    require(bool(re.fullmatch('[0-9a-f]{40}', base)), 'invalid public base')
    require(branch.startswith('refs/heads/') and bool(re.fullmatch('[A-Za-z0-9_./-]+', branch))
            and '..' not in branch, 'invalid branch')
    require(bool(paths) and len(paths) == len(set(paths)), 'reviewed paths must be nonempty and unique')
    for name in paths:
        path = PurePosixPath(name)
        require(not path.is_absolute() and str(path) == name and
                not any(part in ('.', '..', '.git') for part in path.parts)
                and not any(c in name for c in ':*?[\\'), 'invalid reviewed path')
        actual = root / name
        require(actual.is_file() and actual.resolve().is_relative_to(root)
                and not any(p.is_symlink() for p in (actual, *actual.parents)),
                'reviewed files must be regular checkout files')
    if git is None:
        def git(*args, env=None, input=None):
            run = subprocess.run(['git', '-c', 'http.version=HTTP/1.1', *args],
                                 cwd=root, env=env, input=input, capture_output=True, timeout=90)
            if run.returncode:
                raise RuntimeError(run.stderr.decode(errors='replace')[-2000:])
            return run.stdout.decode().strip()
    origin = git('remote', 'get-url', 'origin')
    require(bool(re.fullmatch(r'https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:\.git)?', origin)),
            'publication requires the existing GitHub HTTPS origin')
    require(origin.removeprefix('https://github.com/').removesuffix('.git') == repository,
            'origin differs from the explicitly reviewed repository')
    push_urls = git('remote', 'get-url', '--push', '--all', 'origin').splitlines()
    require(len(push_urls) == 1 and push_urls[0].startswith('https://github.com/')
            and push_urls[0].removeprefix('https://github.com/').removesuffix('.git') == repository,
            'push URLs differ from the single explicitly reviewed HTTPS repository')
    hashes = {p: file_hash(root / p) for p in paths}
    index = receipt.with_suffix('.index')
    require(not index.exists() and not index.is_symlink(), 'unfinished publication index requires inspection')
    env = {**os.environ, 'GIT_INDEX_FILE': str(index)}
    try:
        git('read-tree', base, env=env)
        git('add', '--', *paths, env=env)
        tree = git('write-tree', env=env)
        changed = git('diff-tree', '--no-commit-id', '--name-only', '-r', base, tree).splitlines()
        require(set(changed) == set(paths), 'reviewed path set differs from actual changes')
        require(hashes == {p: file_hash(root / p) for p in paths},
                'reviewed worktree changed while preparing the tree')
        if receipt.exists():
            record = json.loads(receipt.read_text(encoding='utf-8'))
            require(record.get('repository') == repository, 'receipt repository missing or changed')
            require(record.get('branch') == branch, 'receipt branch missing or changed')
            require(record['base'] == base and record['tree'] == tree and record['paths'] == paths,
                    'existing operation changed; reconcile its receipt')
            require(record.get('tested_worktree_sha256') == hashes,
                    'reviewed worktree bytes differ from the existing operation')
        else:
            require(git('ls-remote', 'origin', branch).split()[0] == base,
                    'public branch changed; reconcile before publication')
            sha = git('commit-tree', tree, '-p', base, input=(message.rstrip() + '\n').encode())
            require(git('show', '-s', '--format=%P', sha) == base, 'public parent mismatch')
            record = {'state': 'prepared', 'repository': repository, 'branch': branch, 'base': base, 'tree': tree,
                      'public_commit': sha, 'paths': paths, 'tested_worktree_sha256': hashes}
            # Persist the commit identity before any possibly ambiguous push.
            receipt.write_text(json.dumps(record, indent=2) + '\n', encoding='utf-8')
    finally:
        index.unlink(missing_ok=True)
    sha = record['public_commit']
    require(git('show', '-s', '--format=%P', sha) == base and git('rev-parse', sha+':') == tree,
            'receipt commit differs from the reviewed public tree')
    remote = git('ls-remote', 'origin', branch).split()[0]
    if remote != sha:
        require(remote == base, 'public branch changed; preserve concurrent work')
        git('push', 'origin', sha + ':' + branch)
    require(git('ls-remote', 'origin', branch).split()[0] == sha, 'push readback mismatch')
    record['state'] = 'pushed-and-read-back'
    body = json.dumps(record, indent=2) + '\n'
    if receipt.read_text(encoding='utf-8') != body:
        receipt.write_text(body, encoding='utf-8')
    return record
