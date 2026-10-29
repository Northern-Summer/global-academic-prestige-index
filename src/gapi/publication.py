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
