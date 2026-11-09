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


def publish_prepared_objects(root, *, receipt, api, external_bytes):
    """Settle an existing prepared public commit through the GCM HTTPS client.

    Content-addressed uploads avoid a second local copy of bounded aggregate
    assets. The existing reviewed parent/tree and non-force ref boundary remain
    authoritative. api(method, repo-relative-path, body) is the normal client,
    returning None only for an absent immutable object or ref.
    """
    import base64
    import datetime
    root=Path(root).resolve();receipt=Path(receipt)
    require(receipt.is_absolute() and receipt.resolve().is_relative_to(root)
            and not receipt.is_symlink(),'receipt must belong to reviewed checkout')
    record=json.loads(receipt.read_text('utf-8'))
    repository=record['repository'];branch=record['branch'];commit=record['public_commit']
    require(bool(re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repository)),'invalid reviewed repository')
    require(branch.startswith('refs/heads/') and '..' not in branch,'invalid reviewed branch')
    require(set(external_bytes)<=set(record['paths']),'memory assets outside reviewed set')
    require(sum(len(v) for v in external_bytes.values())<=24*1024**2,'memory publication exceeds bound')
    def git(*args):return subprocess.check_output(['git',*args],cwd=root)
    origin=git('remote','get-url','origin').decode().strip()
    require(origin.removesuffix('.git')=='https://github.com/'+repository,'publication origin changed')
    require(git('rev-parse',commit+':').decode().strip()==record['tree'],'prepared tree changed')
    require(git('show','-s','--format=%P',commit).decode().strip()==record['base'],'prepared parent changed')
    prefix='/repos/'+repository;ref='/git/'+branch;read_ref='/git/ref/'+branch.removeprefix('refs/')
    remote=api('GET',prefix+read_ref,None)
    require(remote is not None,'reviewed public branch missing')
    current=remote['object']['sha']
    require(current in (record['base'],commit),'public branch advanced; preserve concurrent work')
    def save():receipt.write_text(json.dumps(record,indent=2)+'\n','utf-8')
    def immutable(path,sha,body):
        existing=api('GET',prefix+path+'/'+sha,None)
        if existing is None:
            try:existing=api('POST',prefix+path,body)
            except Exception:
                existing=api('GET',prefix+path+'/'+sha,None)
                if existing is None:raise
        require(existing.get('sha')==sha,'immutable remote object mismatch')
    entries=[]
    for name in record['paths']:
        relative=PurePosixPath(name)
        require(not relative.is_absolute() and '..' not in relative.parts,'invalid reviewed object path')
        if name in external_bytes:
            require(bool(re.fullmatch(r'outputs/global/[a-f0-9]{64}\.json\.gz',name)),'invalid memory aggregate path')
            body=external_bytes[name]
        else:
            require(file_hash(root/name)==record['tested_worktree_sha256'][name],'reviewed worktree changed')
            body=git('show',commit+':'+name)
        require(hashlib.sha256(body).hexdigest()==record['tested_worktree_sha256'][name]
                or name not in external_bytes and body.replace(b'\r\n',b'\n')==(root/name).read_bytes().replace(b'\r\n',b'\n'),
                'reviewed publication bytes changed')
        blob=hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()
        tree_entry=git('ls-tree',commit,'--',name).decode().split()
        require(len(tree_entry)>=3 and tree_entry[2]==blob,'prepared blob differs from reviewed bytes')
        if current!=commit:
            immutable('/git/blobs',blob,{'content':base64.b64encode(body).decode('ascii'),'encoding':'base64'})
        entries.append({'path':name,'mode':'100644','type':'blob','sha':blob})
    if current!=commit:
        base_tree=git('rev-parse',record['base']+':').decode().strip()
        immutable('/git/trees',record['tree'],{'base_tree':base_tree,'tree':entries})
        raw=git('cat-file','commit',commit).decode('utf-8');header,message=raw.split('\n\n',1)
        fields=dict(line.split(' ',1) for line in header.splitlines())
        def identity(value):
            match=re.fullmatch(r'(.*) <([^<>]*)> (\d+) ([+-]\d{4})',value)
            require(match is not None,'unsupported reviewed Git identity')
            name,email,seconds,offset=match.groups()
            zone=datetime.timezone(datetime.timedelta(minutes=(1 if offset[0]=='+' else -1)*(int(offset[1:3])*60+int(offset[3:5]))))
            return {'name':name,'email':email,'date':datetime.datetime.fromtimestamp(int(seconds),zone).isoformat()}
        immutable('/git/commits',commit,{'message':message,'tree':record['tree'],'parents':[record['base']],
            'author':identity(fields['author']),'committer':identity(fields['committer'])})
        remote=api('GET',prefix+read_ref,None)
        require(remote['object']['sha']==record['base'],'public branch changed before settlement')
        record['state']='ref-write-prepared';save()
        try:api('PATCH',prefix+ref,{'sha':commit,'force':False})
        except Exception:
            remote=api('GET',prefix+read_ref,None)
            if remote is None or remote['object']['sha']!=commit:raise
    remote=api('GET',prefix+read_ref,None)
    require(remote is not None and remote['object']['sha']==commit,'public ref readback mismatch')
    settled=api('GET',prefix+'/git/commits/'+commit,None)
    require(settled is not None and settled['tree']['sha']==record['tree']
            and [p['sha'] for p in settled['parents']]==[record['base']],'remote public ancestry mismatch')
    record.update(state='pushed-and-read-back',transport='existing GCM HTTPS immutable-object transport');save()
    return record


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def file_hash(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def publish_code(root, *, repository, base, branch, paths, receipt, message, git=None,
                 external_inputs=None, external_root=None, prepare_only=False, external_bytes=None):
    root = Path(root).resolve()
    require(type(prepare_only) is bool,'prepare_only must be boolean')
    receipt = Path(receipt)
    require(receipt.is_absolute() and receipt.resolve().is_relative_to(root),
            'receipt must belong to this checkout')
    require(not receipt.is_symlink(), 'receipt must not be a symlink')
    require(bool(re.fullmatch('[0-9a-f]{40}', base)), 'invalid public base')
    require(branch.startswith('refs/heads/') and bool(re.fullmatch('[A-Za-z0-9_./-]+', branch))
            and '..' not in branch, 'invalid branch')
    require(bool(paths) and len(paths) == len(set(paths)), 'reviewed paths must be nonempty and unique')
    external_inputs={} if external_inputs is None else dict(external_inputs)
    external_bytes={} if external_bytes is None else dict(external_bytes)
    require(set(external_inputs).issubset(paths),'external inputs outside reviewed paths')
    require(set(external_bytes).issubset(paths) and not set(external_bytes)&set(external_inputs),
            'memory inputs outside reviewed paths or conflicting sources')
    require(not external_bytes or prepare_only,'memory assets require prepared immutable-object publication')
    require(sum(len(v) for v in external_bytes.values() if isinstance(v,bytes))<=24*1024**2,
            'memory aggregate storage exceeds bound')
    if external_inputs:
        require(external_root is not None,'external inputs need one explicitly reviewed root')
        external_root=Path(external_root).absolute()
    actual_inputs={}
    for name in paths:
        path = PurePosixPath(name)
        require(not path.is_absolute() and str(path) == name and
                not any(part in ('.', '..', '.git') for part in path.parts)
                and not any(c in name for c in ':*?[\\'), 'invalid reviewed path')
        if name in external_bytes:
            require(bool(re.fullmatch(r'outputs/global/[a-f0-9]{64}\.json\.gz',name))
                    and isinstance(external_bytes[name],bytes) and 0<len(external_bytes[name])<=32*1024**2,
                    'memory publication permits only bounded content-addressed aggregate assets')
            continue
        actual=Path(external_inputs[name]).absolute() if name in external_inputs else root/name
        boundary=external_root if name in external_inputs else root
        if name in external_inputs:
            require(bool(re.fullmatch(r'outputs/global/[a-f0-9]{64}\.json\.gz',name))
                    and actual.name==path.name and actual.stat().st_size<=32*1024**2,
                    'external publication permits only bounded content-addressed aggregate assets')
        require(actual.is_file() and actual.resolve().is_relative_to(boundary)
                and not any(p.is_symlink() for p in (actual, *actual.parents)),
                'reviewed files must be regular files inside their declared root')
        actual_inputs[name]=actual
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
    def fingerprint(name):
        return hashlib.sha256(external_bytes[name]).hexdigest() if name in external_bytes else file_hash(actual_inputs[name])
    hashes = {p:fingerprint(p) for p in paths}
    index = receipt.with_suffix('.index')
    require(not index.exists() and not index.is_symlink(), 'unfinished publication index requires inspection')
    env = {**os.environ, 'GIT_INDEX_FILE': str(index)}
    try:
        git('read-tree', base, env=env)
        ordinary=[p for p in paths if p not in external_inputs and p not in external_bytes]
        if ordinary:git('add', '--', *ordinary, env=env)
        for name in paths:
            if name in external_bytes:
                body=external_bytes[name]
                blob=hashlib.sha1(b'blob '+str(len(body)).encode('ascii')+b'\0'+body).hexdigest()
            elif name in external_inputs:blob=git('hash-object','-w','--no-filters','--',str(actual_inputs[name]))
            else:continue
            require(bool(re.fullmatch('[0-9a-f]{40}',blob)),'invalid external Git blob identity')
            git('update-index','--add','--cacheinfo','100644,'+blob+','+name,env=env)
        tree = git('write-tree',*(['--missing-ok'] if external_bytes else []),env=env)
        changed = git('diff-tree', '--no-commit-id', '--name-only', '-r', base, tree).splitlines()
        require(set(changed) == set(paths), 'reviewed path set differs from actual changes')
        require(hashes == {p:fingerprint(p) for p in paths},
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
            if not prepare_only:
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
    if prepare_only:return record
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
