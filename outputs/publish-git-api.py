"""One-task immutable Git-object transport; existing GCM, no config/auth changes.

Every object SHA must match local Git before a remote ref is created. This is a
transport fallback for an unreachable Git HTTPS endpoint, not an auth workaround.
"""
import base64
import datetime
import importlib.util
import json
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('github_transport', Path.home()/'.codex/bin/github_api.py')
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
client = transport.Client()
repo = '/repos/Northern-Summer/global-academic-prestige-index'
branch = 'codex/global-streaming'
receipt = root/'outputs/github-push-receipt.json'


def git(*args):
    return subprocess.check_output(['git',*args],cwd=root)


def read(path):
    try:
        return client.request('GET',repo+path)
    except transport.Failure as exc:
        if exc.code == 'NOT_FOUND_OR_NOT_AUTHORIZED':
            return None
        raise


def immutable(path, expected, payload):
    # Content addressing is the idempotency boundary, including ambiguous POSTs.
    existing = read(path+'/'+expected)
    if existing is not None:
        if existing['sha'] != expected:
            raise RuntimeError('existing object readback identity mismatch')
        return
    try:
        result = client.request('POST',repo+path,payload)
    except transport.Failure as exc:
        if exc.code != 'WRITE_OUTCOME_UNKNOWN': raise
        result = read(path+'/'+expected)
        if result is None: raise
    if result['sha'] != expected:
        raise RuntimeError('object identity mismatch: '+expected+' != '+result['sha'])


def identity(value):
    import re
    match=re.fullmatch(r'(.*) <([^<>]*)> (\d+) ([+-]\d{4})',value)
    if not match: raise ValueError('unsupported Git identity')
    name,email,seconds,offset=match.groups()
    zone=datetime.timezone(datetime.timedelta(minutes=(1 if offset[0]=='+' else -1)*(int(offset[1:3])*60+int(offset[3:5]))))
    date=datetime.datetime.fromtimestamp(int(seconds),zone).isoformat()
    return {'name':name,'email':email,'date':date}


def main():
    if client.request('GET','/user')['login'] != 'Northern-Summer':
        raise RuntimeError('account changed')
    head=git('rev-parse','HEAD').decode().strip()
    try:
        client.request('GET',repo+'/git/refs')
    except transport.Failure as exc:
        if exc.details.get('status')!=409:raise
        # GitHub refuses Git object creation in a wholly empty repository. Seed
        # its bootstrap main branch, preserve it, and publish exact local history
        # on a separate branch. No force or history replacement is involved.
        seed_path=repo+'/contents/README.md'
        body={'message':'Initialize publication transport bootstrap',
              'content':base64.b64encode(b'# GAPI publication bootstrap\n\nThe complete source history is published on the repository default branch codex/global-streaming.\n').decode()}
        marker=root/'outputs/github-bootstrap-receipt.json'
        if marker.exists():
            raise RuntimeError('bootstrap outcome requires readback before another mutation')
        marker.write_text(json.dumps({'state':'write_outcome_unknown','path':seed_path}),encoding='utf-8')
        status,_,result=transport.transport('PUT',seed_path,client.acquire(),body,15)
        if status!=201:raise RuntimeError(f'bootstrap rejected HTTP {status}; preserve receipt')
        confirmed=client.request('GET',seed_path)
        if confirmed['sha']!=result['content']['sha']:raise RuntimeError('bootstrap readback mismatch')
        marker.write_text(json.dumps({'state':'confirmed','sha':confirmed['sha']}),encoding='utf-8')
    known=set(json.loads(receipt.read_text()).get('verified_objects',[])) if receipt.exists() else set()
    def save(state):
        receipt.write_text(json.dumps({'state':state,'local_head':head,'verified_objects':sorted(known)},indent=2),encoding='utf-8')
    def send(kind,sha):
        if sha in known:return
        body=git('cat-file',kind,sha)
        if kind=='blob':
            immutable('/git/blobs',sha,{'content':base64.b64encode(body).decode(),'encoding':'base64'})
        elif kind=='tree':
            entries=[]
            for row in git('ls-tree','-r','-z',sha).split(b'\0'):
                if not row:continue
                meta,name=row.split(b'\t',1)
                mode,child_type,child_sha=meta.decode().split()
                if child_type=='commit':raise ValueError('embedded submodule requires separate ownership')
                entry={'path':name.decode(),'mode':mode.zfill(6),'type':child_type}
                if child_sha in known:
                    entry['sha']=child_sha
                else:
                    blob=git('cat-file','blob',child_sha)
                    try:entry['content']=blob.decode('utf-8')
                    except UnicodeDecodeError:
                        send(child_type,child_sha)
                        entry['sha']=child_sha
                entries.append(entry)
            immutable('/git/trees',sha,{'tree':entries})
        elif kind=='commit':
            headers,message=body.decode().split('\n\n',1)
            fields={};parents=[]
            for line in headers.splitlines():
                key,value=line.split(' ',1)
                if key=='parent': parents.append(value)
                elif key in ('tree','author','committer'):fields[key]=value
                else:raise ValueError('unsupported signed/extended commit header; refusing changed identity')
            send('tree',fields['tree'])
            for parent in parents:send('commit',parent)
            immutable('/git/commits',sha,{'tree':fields['tree'],'parents':parents,'message':message,
                'author':identity(fields['author']),'committer':identity(fields['committer'])})
        else:raise ValueError('unsupported object kind')
        known.add(sha);save('uploading-immutable-objects')
        if len(known)%20==0:print(json.dumps({'verified_objects':len(known)}),flush=True)
    send('commit',head)
    existing=read('/git/ref/heads/'+branch)
    if existing:
        if existing['object']['sha']!=head:
            previous=existing['object']['sha']
            if subprocess.run(['git','merge-base','--is-ancestor',previous,head],cwd=root,capture_output=True).returncode:
                raise RuntimeError('remote branch is not a local ancestor; refusing overwrite')
            save('fast-forward-outcome-unknown')
            status,_,_=transport.transport('PATCH',repo+'/git/refs/heads/'+branch,client.acquire(),{'sha':head,'force':False},15)
            if status!=200:
                settled=read('/git/ref/heads/'+branch)
                if not settled or settled['object']['sha']!=head:
                    raise RuntimeError(f'fast-forward unconfirmed HTTP {status}')
    else:
        save('ref-write-outcome-unknown')
        try:client.request('POST',repo+'/git/refs',{'ref':'refs/heads/'+branch,'sha':head})
        except transport.Failure:
            settled=read('/git/ref/heads/'+branch)
            if not settled or settled['object']['sha']!=head:raise
    settled=read('/git/ref/heads/'+branch)
    if settled['object']['sha']!=head:raise RuntimeError('remote readback mismatch')
    save('confirmed')
    current=client.request('GET',repo)
    if current['default_branch']!=branch:
        status,_,_=transport.transport('PATCH',repo,client.acquire(),{'default_branch':branch},15)
        if status!=200:raise RuntimeError(f'default branch update rejected HTTP {status}')
        if client.request('GET',repo)['default_branch']!=branch:raise RuntimeError('default branch readback mismatch')
    print(json.dumps({'state':'confirmed','head':head,'verified_objects':len(known)}))


if __name__=='__main__':main()
