"""Versioned release permissions; acquisition locks are never rewritten for reuse.

This validates the adopted project policy, not general legal compliance. Official
terms, decision rationale, scope and residual restrictions are auditable inputs.
"""
import re
from urllib.parse import urlsplit


def public_sources(task,policy=None):
    allowed={name for name,spec in task.config['sources'].items() if spec.get('allow_derivatives')}
    if policy is None:return allowed
    if not isinstance(policy,dict) or policy.get('schema_version')!='gapi-publication-policy/v1':
        raise ValueError('unsupported publication policy')
    if policy.get('purpose')!='noncommercial-research' or policy.get('export_scope')!='institution-aggregates-only':
        raise ValueError('publication purpose/scope not permitted')
    permissions=policy.get('permissions')
    if not isinstance(permissions,dict) or not permissions:
        raise ValueError('publication permissions missing')
    for source,decision in permissions.items():
        if source not in task.config['sources'] or not isinstance(decision,dict):
            raise ValueError('publication permission source unknown')
        if decision.get('decision')!='allow-public-institution-derivatives' or not decision.get('license') or not decision.get('rationale'):
            raise ValueError('publication decision lacks declared rights/rationale')
        evidence=decision.get('evidence')
        if not isinstance(evidence,list) or not evidence:raise ValueError('publication decision needs evidence')
        for item in evidence:
            if not isinstance(item,dict):raise ValueError('publication evidence must be an object')
            url=urlsplit(item.get('url',''))
            if url.scheme!='https' or url.username or url.password or url.hostname not in ('info.orcid.org','www.wikidata.org','openalex.org'):
                raise ValueError('publication evidence requires an official policy URL')
            if not re.fullmatch(r'[a-f0-9]{64}',item.get('sha256','')) or not item.get('retrieved_at'):
                raise ValueError('publication evidence requires content hash/time')
        allowed.add(source)
    return allowed


def partition_release_preflight(task,manifest,policy,source_runs,independent):
    """Bind an independently reproduced local aggregate to the adopted source
    policy. This only qualifies observation publication, never formal rankings
    or personal facts. Changing a transport label is not this gate.
    """
    from .task import digest
    from .career_store import source_identity
    task.require_frozen()
    if (manifest.get('schema_version')!='gapi-aggregate-partitions/1'
            or manifest.get('publication_status')!='local-only-source-license-review'
            or independent.get('status')!='passed'
            or independent.get('manifest_sha256')!=digest(manifest)
            or any(independent.get(k)!=manifest.get(k) for k in ('logical_sha256','logical_bytes','nodes','edges'))
            or independent.get('ordering_consistent') is not True
            or type(independent.get('max_score_error')) not in (int,float)
            or not 0<=independent['max_score_error']<=1e-10
            or type(independent.get('probability_mass')) not in (int,float)
            or not 1-1e-10<=independent['probability_mass']<=1+1e-10):
        raise ValueError('partition publication requires matching independent full verification')
    allowed=public_sources(task,policy)
    # A locally computed result did not source-filter. All registered sources
    # must therefore be allowed, in addition to every actual settled run.
    if not set(task.config['sources']).issubset(allowed):raise ValueError('local aggregate contains potentially restricted sources')
    if not isinstance(source_runs,list) or not source_runs:raise ValueError('missing source run binding')
    seen=set()
    for row in source_runs:
        if (not isinstance(row,dict) or row.get('source') in seen or row.get('source') not in allowed
                or row.get('identity')!=source_identity(task,row['source'])
                or type(row.get('members')) is not int or row['members']<=0):
            raise ValueError('invalid publication source identity')
        source=row['source'];seen.add(source)
        if task.config['sources'][source].get('kind')=='orcid-summary-tar-gzip' and row.get('status')!='verified':
            raise ValueError('annual source not integrity verified')
    return {'schema_version':'gapi-public-partition-qualification/1',
            'purpose':'noncommercial-research','export_scope':'institution-aggregates-only',
            'local_manifest_sha256':digest(manifest),'logical_sha256':manifest['logical_sha256'],
            'publication_policy_sha256':digest(policy),'sources':source_runs,
            'qualification':'public-observations-pending-external-review',
            'formal_ranking_qualified':False,'independent_personal_facts_verified':False}


def prepare_partition_release(task,directory,out,policy,source_runs,independent):
    """Prepare a reviewed institution-observation candidate, leaving the current
    release manifest untouched. Reuse frozen assets; only the status asset and
    explicit source/policy bindings are new. No Git or network effects.
    """
    import gzip,hashlib,json
    from pathlib import Path
    from .task import canonical,digest
    from .storage import project_root
    from .global_analysis import _write_aggregate
    from .aggregate_partitions import logical_chunks
    directory=Path(directory).absolute();out=Path(out).absolute()
    if not directory.is_relative_to(task.root.absolute()) or out.resolve()!=(project_root()/'outputs/global').resolve():
        raise ValueError('partition release must use frozen task inputs and reviewed public output')
    original=(directory/'manifest.json').read_bytes();m=json.loads(original)
    qualification=partition_release_preflight(task,m,policy,source_runs,independent)
    if hashlib.sha256(original).hexdigest()!=qualification['local_manifest_sha256']:raise ValueError('noncanonical local release manifest')
    old='local-only-source-license-review';status='public-observations-pending-external-review';raw=canonical(status)
    packed=gzip.compress(raw,compresslevel=9,mtime=0);sha=hashlib.sha256(raw).hexdigest()
    asset={'file':sha+'.json.gz','encoding':'gzip','bytes':len(raw),'sha256':sha,
           'stored_bytes':len(packed),'stored_sha256':hashlib.sha256(packed).hexdigest()}
    changed=0;size=0;checksum=hashlib.sha256();expect_status=False
    for block in logical_chunks(directory,check=task.check_memory):
        if expect_status:
            if block!=canonical(old):raise ValueError('unexpected logical publication status')
            block=raw;expect_status=False;changed+=1
        elif block==b'"publication_status":':expect_status=True
        checksum.update(block);size+=len(block)
    if changed!=1 or expect_status or (directory/'manifest.json').read_bytes()!=original:raise ValueError('publication transform scope/drift')
    candidate=json.loads(original)
    entry=next(f for f in candidate['fields'] if f['field']=='publication_status')
    candidate['stored_bytes']+=asset['stored_bytes']-entry['asset']['stored_bytes'];entry['asset']=asset
    candidate.update(publication_status=status,logical_sha256=checksum.hexdigest(),logical_bytes=size,
                     publication={**qualification,'publication_policy':policy,
                       'local_manifest':'local-input-manifest.json','local_source_manifest':'local-input-source-manifest.json'})
    current=out/'manifest.json'
    if current.exists():
        previous=json.loads(current.read_bytes())
        if previous.get('derived_bundle'):candidate['derived_bundle']=previous['derived_bundle']
    source=(directory/'source-manifest.json').read_bytes()
    if hashlib.sha256(source).hexdigest()!=m['source_manifest_sha256']:raise ValueError('local source anchor drift')
    out.mkdir(parents=True,exist_ok=True)
    _write_aggregate(task,out/asset['file'],packed)
    _write_aggregate(task,out/'local-input-manifest.json',original)
    _write_aggregate(task,out/'local-input-source-manifest.json',source)
    _write_aggregate(task,out/'manifest.partitions-candidate.json',canonical(candidate))
    descriptors=[a for f in m['fields'] for a in (f['assets'] if f['kind']=='rows' else [v['asset'] for v in f['entries']] if f['kind']=='object' else [f['asset']])]
    return {'state':'prepared-public-observation-candidate','manifest':str(out/'manifest.partitions-candidate.json'),
            'manifest_sha256':digest(candidate),'logical_sha256':candidate['logical_sha256'],
            'external_root':str(directory),'external_inputs':{'outputs/global/'+a['file']:str(directory/a['file']) for a in descriptors},
            'new_status_asset':'outputs/global/'+asset['file'],'qualification':qualification,
            'current_release_manifest_replaced':False,'public_release_created':False}
