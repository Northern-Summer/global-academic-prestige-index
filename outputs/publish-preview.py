"""Task-owned, bounded publication of reviewed dist and an honest prerelease.

Uses existing GCM transport. No credentials, personal inputs or raw source bodies
are persisted. Remote refs use non-force writes and SHA/content readback.
"""
import hashlib
import argparse
import re
import importlib.util
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('gapi_publish', root/'outputs/publish-git-api.py')
pub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pub)
receipt = root/'outputs/github-preview-receipt.json'


def git_sha(kind, body):
    return hashlib.sha1(kind.encode()+b' '+str(len(body)).encode()+b'\0'+body).hexdigest()


def tree_sha(files):
    tree = {}
    for path, content in files.items():
        cursor = tree
        parts = path.split('/')
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = content
    def encode(folder):
        body = b''
        for name, content in sorted(folder.items(), key=lambda row:row[0]+('/' if isinstance(row[1],dict) else '')):
            mode, sha = ('40000', encode(content)) if isinstance(content,dict) else ('100644',git_sha('blob',content))
            body += mode.encode()+b' '+name.encode()+b'\0'+bytes.fromhex(sha)
        return git_sha('tree',body)
    return encode(tree)


def mutate(method, path, payload, expected_status):
    try:
        status, _, result = pub.transport.transport(method,pub.repo+path,pub.client.acquire(),payload,15)
    except pub.transport.Failure as exc:
        if exc.code != 'NETWORK_UNAVAILABLE':
            raise
        return None  # Caller must reconcile; never blindly repeat this mutation.
    if status not in expected_status:
        raise RuntimeError(f'{method} {path} returned HTTP {status}; reconcile before retry')
    return result


def main():
    global receipt
    parser=argparse.ArgumentParser()
    parser.add_argument('--tag',default='v0.2.0-preview')
    args=parser.parse_args()
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+-[a-z0-9.-]+',args.tag):
        raise ValueError('only an explicit prerelease tag is supported')
    receipt=root/('outputs/github-preview-'+args.tag+'-receipt.json')
    head = pub.git('rev-parse','HEAD').decode().strip()
    if pub.client.request('GET','/user')['login'] != 'Northern-Summer':
        raise RuntimeError('account identity changed')
    if pub.read('/git/ref/heads/'+pub.branch)['object']['sha'] != head:
        raise RuntimeError('publish the exact source revision first')
    selected = json.loads((root/'outputs/global/manifest.json').read_text())
    built = json.loads((root/'dist/data/global/manifest.json').read_text())
    if selected != built or built['publication_status'] != 'public-observations-pending-external-review':
        raise RuntimeError('selected/built public manifest mismatch')
    files = {p.relative_to(root/'dist').as_posix():p.read_bytes() for p in (root/'dist').rglob('*') if p.is_file()}
    if sum(map(len,files.values())) > 8*1024**2 or any(path.startswith('data/local/') for path in files):
        raise RuntimeError('site exceeds reviewed bounds or contains private dataset')
    if hashlib.sha256(files['data/global/'+built['snapshot']]).hexdigest() != built['output_sha256']:
        raise RuntimeError('site snapshot identity mismatch')
    files['.nojekyll'] = b''
    manifest = {'source_commit':head, 'ranking_status':'observations-only-pending-independent-review',
        'files':{path:hashlib.sha256(body).hexdigest() for path,body in sorted(files.items())}}
    files['site-manifest.json'] = json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
    expected_tree = tree_sha(files)
    receipt.write_text(json.dumps({'state':'prepared','source_commit':head,'site_tree':expected_tree}),encoding='utf-8')
    entries = [{'path':p,'mode':'100644','type':'blob','content':content.decode('utf-8')} for p,content in sorted(files.items())]
    pub.immutable('/git/trees',expected_tree,{'tree':entries})
    previous = pub.read('/git/ref/heads/gh-pages')
    previous_sha = previous['object']['sha'] if previous else None
    if previous and pub.read('/git/commits/'+previous_sha)['tree']['sha'] == expected_tree:
        site_commit = previous_sha
    else:
        commit = pub.git('cat-file','commit',head).decode()
        author = next(line[7:] for line in commit.splitlines() if line.startswith('author '))
        message = f'Publish source-permitted observation preview from {head}\n'
        body = f'tree {expected_tree}\n'+(f'parent {previous_sha}\n' if previous else '')+f'author {author}\ncommitter {author}\n\n'+message
        site_commit = git_sha('commit',body.encode())
        pub.immutable('/git/commits',site_commit,{'tree':expected_tree,'parents':[previous_sha] if previous else [],
            'message':message,'author':pub.identity(author),'committer':pub.identity(author)})
        if previous:
            mutate('PATCH','/git/refs/heads/gh-pages',{'sha':site_commit,'force':False},(200,))
        else:
            mutate('POST','/git/refs',{'ref':'refs/heads/gh-pages','sha':site_commit},(201,))
        settled = pub.read('/git/ref/heads/gh-pages')
        if not settled or settled['object']['sha'] != site_commit:
            raise RuntimeError('site branch settlement unknown; preserve receipt')
    pages = pub.read('/pages')
    desired = {'source':{'branch':'gh-pages','path':'/'},'build_type':'legacy'}
    if not pages:
        mutate('POST','/pages',desired,(201,))
    elif pages.get('source') != desired['source'] or pages.get('build_type') != 'legacy':
        raise RuntimeError('existing Pages configuration differs; preserve it')
    pages = pub.read('/pages')
    if not pages or pages.get('source') != desired['source']:
        raise RuntimeError('Pages configuration not confirmed')
    tag = args.tag
    release = pub.read('/releases/tags/'+tag)
    if not release:
        description = ('Streaming real-data observation preview. No formal global ranking or independent red-team approval.\n\n'
            'The source-permitted institution aggregate is a bounded real observation slice; source enumeration and official corroboration are incomplete. '
            'ORCID derived aggregates use the explicit noncommercial publication policy in the manifest. '
            'Awards and composite scores are missing. Personal histories and the US study are excluded.\n\n'
            f'Frozen source revision: {head}\nSnapshot SHA-256: {built["output_sha256"]}\n'
            'See README, docs/STREAMING.md and outputs/RED_TEAM_HANDOFF.md for exact scope, reproducibility and review gates.\n')
        mutate('POST','/releases',{'tag_name':tag,'target_commitish':head,'name':'GAPI '+tag+' — real observation streaming preview',
            'body':description,'draft':False,'prerelease':True},(201,))
        release = pub.read('/releases/tags/'+tag)
    if not release or not release['prerelease'] or release['target_commitish'] != head:
        raise RuntimeError('prerelease identity/status mismatch; never retarget an existing tag')
    tag_ref = pub.read('/git/ref/tags/'+tag)
    if not tag_ref or tag_ref['object']['sha'] != head:
        raise RuntimeError('prerelease tag SHA mismatch')
    result = {'state':'remote_refs_confirmed_http_site_pending','source_commit':head,'site_commit':site_commit,
        'site_tree':expected_tree,'site_url':pages['html_url'],'release_url':release['html_url'],
        'snapshot_sha256':built['output_sha256'],'site_manifest':manifest}
    receipt.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='site_manifest'}))


if __name__ == '__main__':
    main()
