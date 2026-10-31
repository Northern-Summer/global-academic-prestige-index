"""Stand-alone public aggregate replay; stdlib only, no gapi imports or person DB.

This checks graph-to-score reproduction. It cannot confirm the truth of private
career inputs, source independence, licensing or complete world coverage.
"""
import argparse
import hashlib
import gzip
import io
import json
import math
from pathlib import Path
import re


def bounded(path, maximum=32*1024**2):
    with path.open('rb') as stream:body=stream.read(maximum+1)
    if len(body)>maximum:raise ValueError('public input exceeds byte cap')
    return body


def replay(data):
    rows=data['nodes']; nodes=sorted(row['institution_id'] for row in rows if row['network_score'] is not None)
    if not nodes or len(nodes)>200000 or len(set(nodes))!=len(nodes):raise ValueError('invalid graph universe')
    if data['algorithm']['direction']!='employer -> training':raise ValueError('wrong declared edge direction')
    if sorted(data['algorithm']['network_universe'])!=nodes:raise ValueError('universe mismatch')
    alpha=data['algorithm']['config']['alpha']
    if type(alpha) not in (float,int) or not math.isfinite(alpha) or not 0<=alpha<1:raise ValueError('invalid alpha')
    totals={node:0.0 for node in nodes};edges=[];seen=set()
    if len(data['edges'])>200000:raise ValueError('edge cap')
    for edge in data['edges']:
        source,target,weight=edge['source'],edge['target'],edge['weight']
        if source not in totals or target not in totals or (source,target) in seen:raise ValueError('invalid edge endpoints/duplicate')
        if type(weight) not in (float,int) or not math.isfinite(weight) or weight<=0:raise ValueError('invalid edge weight')
        seen.add((source,target));totals[source]+=weight;edges.append((source,target,weight))
    if not all(math.isfinite(value) for value in totals.values()):raise ValueError('row sum overflow')
    n=len(nodes);scores=dict.fromkeys(nodes,1/n);residual=None
    for iteration in range(1,201):
        dangling=math.fsum(scores[node] for node in nodes if not totals[node])
        new=dict.fromkeys(nodes,((1-alpha)+alpha*dangling)/n)
        for source,target,weight in edges:new[target]+=alpha*scores[source]*weight/totals[source]
        residual=math.fsum(abs(new[node]-scores[node]) for node in nodes);scores=new
        if residual<1e-12:break
    if residual>=1e-12:raise ValueError('independent replay did not converge')
    stored={row['institution_id']:row['network_score'] for row in rows if row['network_score'] is not None}
    if any(type(value) not in (float,int) or not math.isfinite(value) or not 0<=value<=1 for value in stored.values()):
        raise ValueError('invalid stored score')
    error=max(abs(scores[node]-stored[node]) for node in nodes)
    mass=math.fsum(scores.values())
    if error>1e-10 or abs(mass-1)>1e-10:raise ValueError('graph/score mismatch or mass loss')
    ranks={node:i for i,node in enumerate(sorted(nodes,key=lambda node:(-stored[node],node)),1)}
    if any(row['observed_rank']!=ranks[row['institution_id']] for row in rows if row['institution_id'] in ranks):
        raise ValueError('stored ordering mismatch')
    return {'scored_nodes':n,'edges':len(edges),'iterations':iteration,'l1_residual':residual,
            'max_score_error':error,'probability_mass':mass,'ordering_consistent':True}


def verify(manifest_path):
    manifest_bytes=bounded(manifest_path,1024**2);manifest=json.loads(manifest_bytes)
    name=manifest.get('snapshot')
    if not isinstance(name,str) or not re.fullmatch(r'global-[a-f0-9]{16}\.json',name):raise ValueError('unsafe snapshot locator')
    storage=manifest.get('repository_snapshot')
    if 'repository_snapshot' in manifest:
        if (not isinstance(storage,dict) or storage.get('encoding')!='gzip'
                or storage.get('filename')!=name+'.gz'
                or not isinstance(storage.get('sha256'),str) or not re.fullmatch(r'[a-f0-9]{64}',storage['sha256'])
                or any(type(storage.get(k)) is not int or not 0<storage[k]<=32*1024**2 for k in ('bytes','decoded_bytes'))):
            raise ValueError('invalid repository snapshot representation')
        packed=bounded(manifest_path.parent/storage['filename'])
        if len(packed)!=storage['bytes'] or hashlib.sha256(packed).hexdigest()!=storage['sha256']:
            raise ValueError('stored snapshot identity mismatch')
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:body=stream.read(32*1024**2+1)
        if len(body)>32*1024**2 or len(body)!=storage['decoded_bytes']:raise ValueError('decoded snapshot size mismatch')
    else:body=bounded(manifest_path.parent/name)
    sha=hashlib.sha256(body).hexdigest()
    if sha!=manifest['output_sha256']:raise ValueError('snapshot hash mismatch')
    data=json.loads(body);result=replay(data)
    return {'schema_version':'gapi-public-independent-replay/v1','status':'passed',
        'snapshot_sha256':sha,'manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest(),**result,
        'scope':'independently implemented public graph-to-score replay; no canonical gapi ranking imports',
        'not_verified':['underlying personal facts','source independence','world coverage','source rights','third-party auditor identity']}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,default=Path('outputs/global/manifest.json'))
    args=parser.parse_args();print(json.dumps(verify(args.manifest),sort_keys=True))


if __name__=='__main__':main()
