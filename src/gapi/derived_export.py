"""Bounded browser assets for the complete derived observation network.

Scores come from the ranking owner. Rendering slices never recalculate them.
Only aggregate institution fields are allowed into this separate public contract.
"""
import hashlib
import heapq
from pathlib import Path

from .task import canonical

STAGES=('bachelor','master','doctorate','postdoc')


def write_bundle(task,result,directory):
    directory=Path(directory)
    if directory.is_symlink() or getattr(directory,'is_junction',lambda:False)():
        raise ValueError('browser output links forbidden')
    if result['source_census'].get('source_complete') is not True:
        raise ValueError('complete verified source required for browser export')
    if result.get('formal_ranking') is not False or result.get('global_complete') is not False:
        raise ValueError('derived browser view cannot assert formal/global completion')
    nodes=result['nodes']
    if len(nodes)>200000:raise ValueError('browser institution directory exceeds bounded contract')
    ids={n['institution_id']:i for i,n in enumerate(nodes)}
    if len(ids)!=len(nodes):raise ValueError('duplicate browser institution')
    directory.mkdir(parents=True,exist_ok=True)
    assets=[]
    def asset(value,limit=2*1024**2):
        task.check_memory()
        raw=canonical(value)
        task.check_memory()
        if len(raw)>limit:raise ValueError('browser asset exceeds contract')
        sha=hashlib.sha256(raw).hexdigest();name=sha+'.json';path=directory/name
        if path.exists():
            if path.is_symlink() or path.read_bytes()!=raw:raise ValueError('browser content address conflict')
        else:
            task.check(len(raw)*2+1024**2);temporary=directory/(sha+'.tmp');created=False
            try:
                with temporary.open('xb') as handle:created=True;handle.write(raw)
                temporary.replace(path)
            finally:
                if created and temporary.exists():temporary.unlink()
        entry={'file':name,'sha256':sha,'bytes':len(raw)};assets.append(entry)
        return entry
    chunks=[];pending=[];adjacency=[[] for _ in nodes];overview=[];edge_count=0
    def flush():
        if pending:chunks.append(asset(pending));pending.clear()
    for edge in result['edges']:
        a,b=ids[edge['source']],ids[edge['target']]
        stages=[edge['stages'].get(s,0) for s in STAGES]
        packed=[a,b,*stages];chunk=len(chunks)
        for index in {a,b}:
            if not adjacency[index] or adjacency[index][-1]!=chunk:adjacency[index].append(chunk)
        pending.append(packed);edge_count+=1
        # A separate explicit overview, not a replacement for full adjacency.
        item=(edge['weight'],-a,-b,packed)
        if len(overview)<2000:heapq.heappush(overview,item)
        elif item>overview[0]:heapq.heapreplace(overview,item)
        if len(pending)==10000:flush();task.check_memory()
    flush()
    if edge_count!=result['edge_count']:raise ValueError('browser edge count mismatch')
    packed_nodes=[[n['institution_id'],n['institution_name'],n['country'],
                   n['network_score'],n['relation_count'],n['observed_order'],adjacency[i],
                   n['institution_identity_status'],n['unranked_reasons'],n.get('region')]
                  for i,n in enumerate(nodes)]
    node_asset=asset(packed_nodes,32*1024**2)
    overview_asset=asset([item[3] for item in sorted(overview,reverse=True)])
    manifest={
        'schema_version':'gapi-derived-browser/v1',
        'publication_status':'public-observations-pending-external-review',
        'source':result['source'],'source_census':result['source_census'],
        'formal_ranking':False,'global_complete':False,'direction':result['direction'],
        'node_columns':['institution_id','name','country','network_score','relations','observed_order',
                        'chunks','institution_identity_status','unranked_reasons','region'],
        'region_provenance':'fixed ROR institution metadata; unknown remains null',
        'edge_columns':['source_index','target_index',*STAGES],
        'node_count':len(nodes),'edge_count':edge_count,'nodes':node_asset,
        'edge_chunks':chunks,'overview':overview_asset,
        'overview_scope':'up to 2000 strongest aggregate edges; scores use the full network',
        **{key:result[key] for key in ('distinct_retained_people','person_level_funnel',
            'resolved_institutions','unresolved_institution_identifiers','deduplicated_relations',
            'config','convergence','probability_mass')},
        'missing_scores':['award_score','composite_score','formal_rank'],
        'evidence_status':'observed historical ORCID derivative; no independent corroboration',
    }
    # The pointer becomes readable only after every content-addressed asset is
    # written. Identical reruns neither append files nor rewrite the pointer.
    raw=canonical(manifest);pointer=directory/'manifest.json'
    if pointer.is_symlink():raise ValueError('browser manifest link forbidden')
    if not pointer.exists() or pointer.read_bytes()!=raw:
        task.check(len(raw)*2+1024**2);temporary=directory/'manifest.tmp';created=False
        try:
            with temporary.open('xb') as handle:created=True;handle.write(raw)
            temporary.replace(pointer)
        finally:
            if created and temporary.exists():temporary.unlink()
    return {'manifest':str(pointer),'sha256':hashlib.sha256(raw).hexdigest(),
            'asset_bytes':sum({a['file']:a['bytes'] for a in assets}.values()),
            'nodes':len(nodes),'edges':edge_count}
