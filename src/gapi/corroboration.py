"""Deterministic evidence adjudication, derived from immutable source records.

Only an exactly checked university profile plus the same ORCID owner's explicit
self assertion qualifies here. Wikidata and its cited profile remain one origin.
Old profile rules, missing part hashes, unresolved identities and copied/client
assertions fail closed. No original event status is rewritten.
"""
import json
import re

from .career_rules import classify, valid_orcid, QUALIFICATION_RULE_VERSION
from .career_normalization import interpretation, POLICY_SHA256 as NORMALIZATION_POLICY
from .official_profiles import RULE as PROFILE_RULE, profile_url,supported_profile_url
from .orcid_stream import private_person
from .career_store import member_evidence_sql
from .task import BudgetStop, digest

VERSION = 'official-self-corroboration/2'
POLICY_SHA256 = digest({'version': VERSION, 'profile_rule': PROFILE_RULE,
    'qualification_rule': QUALIFICATION_RULE_VERSION, 'normalization_policy': NORMALIZATION_POLICY,
    'identity': 'same-valid-orcid', 'origins': ['official-profile', 'orcid-self'],
    'facts': 'institution,kind,stage,degree-end-or-job-start; optional-dates-must-not-conflict',
    'requires': 'official-same-orcid, current-profile-rule, committed-members, hashed-affiliation-part',
    'official_degree_discovery':'reference URL discovers page; explicit official degree matches same-ORCID self claim'})


def _hash(value):
    return isinstance(value, str) and bool(re.fullmatch('[a-f0-9]{64}', value))


def _fact(event, institution):
    row = dict(event, institution=institution)
    if event['source'] == 'orcid-public-api':
        row['kind'], row['stage'] = classify(row['kind'], row.get('role'), completed_only=True)
    interpreted = interpretation(event)
    if interpreted is not None:
        row['kind'], row['stage'], row['status'], _ = interpreted
    if row['status'] not in ('observed', 'corroborated') or not row.get('stage'):
        return None
    if not institution or not institution.startswith('ror:'):
        return None
    if not row.get('year_end' if row['kind'] in ('education', 'training') else 'year_start'):
        return None
    return tuple(row.get(k) for k in ('institution', 'kind', 'stage', 'year_start', 'year_end'))


def overlap_report(store):
    """Count exact normalized facts, never elevate them to corroborated evidence.

    Stream by existing stable person key, with the same bounded per-person fanout
    as adjudication. Distinct source names do not establish independent origins.
    """
    from collections import Counter, defaultdict
    from itertools import groupby, combinations
    shared_people=Counter();matched_facts=Counter();candidates=0
    rows=store.db.execute('''SELECT e.person,e.source,e.payload,
        COALESCE(a.institution,e.institution) FROM events e
        LEFT JOIN active_institution_aliases a ON a.alias=e.institution
        WHERE '''+member_evidence_sql()+''' ORDER BY e.person,e.source,e.id''')
    for _,events in groupby(rows,key=lambda r:r[0]):
        sources=set();facts=defaultdict(set)
        for i,(_,source,payload,institution) in enumerate(events):
            if i>=4096:raise BudgetStop('person overlap fanout exceeds bounded analysis')
            sources.add(source);fact=_fact(json.loads(payload),institution)
            if fact:facts[fact].add(source)
        shared_people.update(combinations(sorted(sources),2))
        for supports in facts.values():
            if len(supports)>1:
                candidates+=1;matched_facts.update(combinations(sorted(supports),2))
        store.task.check_memory()
    return {'normalized_exact_fact_candidates':candidates,
        'source_pairs':[{'sources':list(pair),'shared_people':shared_people[pair],
            'normalized_exact_fact_candidates':matched_facts[pair]} for pair in sorted(shared_people)],
        'scope':'stable person identity, resolved ROR institution, explicit stage and required date; both dates exact',
        'independent_origins_verified':False,'promotes_event_status':False}


def decisions(store, *, allowed_sources=None):
    if allowed_sources is not None and 'orcid-public-api' not in allowed_sources:
        return {}
    result = {}
    checks = store.db.execute('''SELECT e.id,e.payload,COALESCE(a.institution,e.institution),
            c.url,c.content_sha256,c.field_hash,c.origin_relation
        FROM official_checks c JOIN events e ON e.id=c.event_id
        LEFT JOIN active_institution_aliases a ON a.alias=e.institution
        WHERE c.rule_version=? AND c.status='official-underlying-source-confirmed'
        AND ((c.origin_relation='same-origin-as-wikidata-reference' AND e.source='wikidata')
          OR (c.origin_relation='independent-official-profile' AND e.source='orcid-public-api'))
        AND ''' + member_evidence_sql() + '''
        ORDER BY e.id,c.url''', (PROFILE_RULE,))
    for event_id, payload, institution, url, page_hash, field_hash, origin in checks:
        official = json.loads(payload)
        direct=origin=='independent-official-profile'
        if not direct and allowed_sources is not None and 'wikidata' not in allowed_sources:continue
        fact = _fact(official, institution)
        route_valid=supported_profile_url(url) if direct else profile_url(official)==url
        # XML affiliation ingress binds full source bytes in member_parts, not
        # an invented per-event content_sha256. Legacy JSON events may carry it.
        source_hash=official.get('content_sha256')
        if (not fact or not route_valid or (direct and fact[1]!='education')
                or not all(map(_hash,(page_hash,field_hash)))
                or ((not direct or source_hash is not None) and not _hash(source_hash))):
            continue
        subject=store.db.execute('''SELECT person FROM official_profile_subjects
            WHERE url=? AND content_sha256=? AND rule_version=?''',(url,page_hash,PROFILE_RULE)).fetchone()
        if not subject or subject[0]!=official['person']:
            continue
        candidates = store.db.execute('''SELECT e.id,e.payload,COALESCE(a.institution,e.institution)
            FROM events e LEFT JOIN active_institution_aliases a ON a.alias=e.institution
            WHERE e.person=? AND e.source='orcid-public-api'
            AND ''' + member_evidence_sql() + '''
            ORDER BY e.id LIMIT 4097''', (official['person'],))
        for index, (other_id, other_payload, other_institution) in enumerate(candidates):
            if index >= 4096:
                raise BudgetStop('person corroboration fanout exceeds bounded analysis')
            if direct and other_id!=event_id:continue
            other = json.loads(other_payload)
            orcid = other['locator'].split('#', 1)[0]
            if other.get('person') != official['person'] or not valid_orcid(orcid) or private_person(orcid) != official['person']:
                continue
            if other.get('assertion_origin') not in ('self', orcid):
                continue
            other_fact=_fact(other, other_institution)
            if not other_fact or fact[:3]!=other_fact[:3]:
                continue
            required=4 if official['kind'] in ('education','training') else 3
            optional=3 if required==4 else 4
            if fact[required]!=other_fact[required]:
                continue
            if fact[optional] is not None and other_fact[optional] is not None and fact[optional]!=other_fact[optional]:
                continue
            section = 'educations' if other['kind'] in ('education', 'training') else 'employments'
            part = store.db.execute('''SELECT url,sha256 FROM member_parts
                WHERE source='orcid-public-api' AND locator=? AND part=?''', (orcid, section)).fetchone()
            if not part or part[0] != other.get('url') or not _hash(part[1]):
                continue
            decision = {'rule_version': VERSION, 'policy_sha256': POLICY_SHA256,
                'origins': ['official-profile', 'orcid-self'],
                'supported_fields': ['institution','kind','stage','year_end' if required==4 else 'year_start'],
                'evidence_sha256': digest({'official_event': digest(official),
                    'self_event': digest(other), 'profile_url': url, 'profile_sha256': page_hash,
                    'field_sha256': field_hash, 'part_sha256': part[1], 'fact': fact})}
            # Multiple valid supports are deterministic; no first-ID policy choice.
            for key in (event_id, other_id):
                if key not in result or decision['evidence_sha256'] < result[key]['evidence_sha256']:
                    result[key] = decision
            if len(result) > 200000:
                raise BudgetStop('corroboration decision index exceeds bounded analysis')
    return result
