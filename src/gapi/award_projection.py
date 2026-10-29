"""Auditable Nobel label-level attribution and conservative identity candidates.

Official award-time labels own attribution. ROR exact-name/country matches are
explicit candidates, never automatic institutional merges or network scores.
"""
from collections import defaultdict
from fractions import Fraction
import hashlib
import json
import unicodedata

from .task import canonical, digest, BudgetStop

VERSION = 'nobel-label-attribution/2'
ROR_SHA256 = '959747b1ee6075d5598cc658d5599051714d4e054e93a706e422c43071f07db6'
COUNTRIES = {'usa':'united states', 'uk':'united kingdom', 'the netherlands':'netherlands'}
POLICY = {'version': VERSION, 'main':'personal-prize-portion; single official affiliation only',
          'multi_affiliation':'equal split sensitivity only; no main attribution',
          'identity':'exact normalized ROR name/alias and country; candidate only',
          'country_aliases': COUNTRIES, 'network_or_composite_imputation':False}
DDL = '''CREATE TABLE IF NOT EXISTS award_label_projection(
    source TEXT NOT NULL, label_id TEXT NOT NULL, rule_version TEXT NOT NULL,
    input_sha256 TEXT NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY(source,label_id,rule_version));'''


def norm(text):
    return ' '.join(unicodedata.normalize('NFKC', text or '').casefold().split())


def country(text):
    value=norm(text)
    return COUNTRIES.get(value,value)


def rational(value):
    # JSON integer strings retain exactness beyond JavaScript's safe integer.
    return {'numerator':str(value.numerator),'denominator':str(value.denominator)}


def label_attributions(rows):
    labels={}; index=hashlib.sha256(); events=0
    for event in rows:
        index.update(canonical(event)); events+=1
        if events>100000:raise BudgetStop('award attribution row limit')
        key, recipient, year, category, kind, portion, affiliations, page=event
        values=json.loads(affiliations); share=Fraction(portion)
        if not 0<share<=1:raise ValueError('invalid prize share')
        if len({digest(label) for label in values})!=len(values):
            raise ValueError('duplicate official affiliation within award')
        for label in values:
            ident=digest(label)
            item=labels.setdefault(ident,{**label,'official_label_id':ident,'award_mentions':0,
                'personal_single_affiliation_events':0,'personal_multi_affiliation_events':0,
                'institutional_events':0,'single_share':Fraction(0),'split_share':Fraction(0),
                'first_award_year':year,'last_award_year':year})
            if len(labels)>25000:raise BudgetStop('award label projection exceeds bounded public slice')
            item['award_mentions']+=1
            item['first_award_year']=min(year,item['first_award_year'])
            item['last_award_year']=max(year,item['last_award_year'])
            if kind=='personal':
                item['split_share']+=share/len(values)
                if len(values)==1:
                    item['single_share']+=share;item['personal_single_affiliation_events']+=1
                else:item['personal_multi_affiliation_events']+=1
            elif kind=='institutional':item['institutional_events']+=1
            else:raise ValueError('unsupported Nobel recipient type')
    for item in labels.values():
        item['nobel_share_single_affiliation']=rational(item.pop('single_share'))
        item['nobel_share_equal_split_sensitivity']=rational(item.pop('split_share'))
    return labels,index.hexdigest()


def exact_candidates(labels, records, check=lambda:None):
    wanted=defaultdict(list); result={key:[] for key in labels}
    for key,label in labels.items():wanted[norm(label['name'])].append(key)
    for i,record in enumerate(records):
        if i%1000==0:check()
        if record.get('status')!='active':continue
        names={norm(n) for n in [record['name'],*record.get('aliases',[])]}
        record_country=country(record.get('geo',{}).get('country_name'))
        for name in sorted(names & wanted.keys()):
            for key in wanted[name]:
                label=labels[key]
                if not label.get('country') or country(label['country'])!=record_country:continue
                candidate={'institution_id':'ror:'+record['id'],'name':record['name'],
                    'country':record.get('country_code'),'evidence':'exact-name-or-alias-and-country',
                    'historical_identity_requires_review':record.get('established') is None or
                        record['established']>label['first_award_year']}
                if candidate not in result[key]:result[key].append(candidate)
                if len(result[key])>1000:raise BudgetStop('award identity ambiguity exceeds bounded candidate list')
    for key in result:result[key].sort(key=lambda row:row['institution_id'])
    return result


def project_awards(store):
    from .global_analysis import ror_records
    from .storage import project_root
    from .nobel import public_inventory
    inventory=public_inventory(store)
    if not inventory:raise ValueError('official award inventory unavailable')
    source=store.db.execute('SELECT id FROM award_sources').fetchone()[0]
    store.task.check(16*1024**2)
    rows=store.db.execute('''SELECT event_key,recipient_key,award_year,category,recipient_type,
        portion,affiliations,page_offset FROM official_awards WHERE source=? ORDER BY event_key''',(source,))
    labels,input_hash=label_attributions(rows)
    directory=project_root()/'data/real/institutions.json.gz'
    with directory.open('rb') as stream:
        if hashlib.file_digest(stream,'sha256').hexdigest()!=ROR_SHA256:
            raise ValueError('ROR projection identity mismatch')
    matches=exact_candidates(labels,ror_records(directory),store.task.check_memory)
    identity=digest({'award_index':input_hash,'ror_sha256':ROR_SHA256,'policy':POLICY})
    store.db.executescript(DDL)
    with store.member('award-projection',VERSION):
        for key,item in sorted(labels.items()):
            item.update(ror_candidates=matches[key],identity_status='candidate-requires-confirmation'
                if len(matches[key])==1 else 'ambiguous-candidate' if matches[key] else 'unresolved')
            payload=canonical(item).decode()
            old=store.db.execute('SELECT input_sha256,payload FROM award_label_projection WHERE source=? AND label_id=? AND rule_version=?',
                (source,key,VERSION)).fetchone()
            if old and old!=(identity,payload):raise ValueError('award projection input changed; new version required')
            store.db.execute('INSERT OR IGNORE INTO award_label_projection VALUES(?,?,?,?,?)',
                (source,key,VERSION,identity,payload))
    return public_projection(store)


def public_projection(store):
    if not store.db.execute("SELECT 1 FROM sqlite_master WHERE name='award_label_projection'").fetchone():return None
    rows=list(store.db.execute('SELECT input_sha256,payload,source FROM award_label_projection WHERE rule_version=? ORDER BY label_id',(VERSION,)))
    if not rows:return None
    identities={row[0] for row in rows}
    sources={row[2] for row in rows}
    if len(identities)!=1 or len(sources)!=1:raise ValueError('mixed award projection inputs')
    current=hashlib.sha256()
    for row in store.db.execute('''SELECT event_key,recipient_key,award_year,category,recipient_type,
        portion,affiliations,page_offset FROM official_awards WHERE source=? ORDER BY event_key''',(next(iter(sources)),)):
        current.update(canonical(row))
    expected=digest({'award_index':current.hexdigest(),'ror_sha256':ROR_SHA256,'policy':POLICY})
    if next(iter(identities))!=expected:raise ValueError('stale award projection; inputs or policy changed')
    labels=[json.loads(row[1]) for row in rows]
    return {'policy':POLICY,'input_sha256':next(iter(identities)),'ror_sha256':ROR_SHA256,'label_count':len(labels),
        'unique_candidate_labels':sum(len(row['ror_candidates'])==1 for row in labels),
        'ambiguous_candidate_labels':sum(len(row['ror_candidates'])>1 for row in labels),
        'unresolved_labels':sum(not row['ror_candidates'] for row in labels),
        'official_label_attributions':labels,'canonical_identity_promotions':0,
        'network_and_composite_scores_imputed':False,
        'scope':'Nobel-only official award-time labels; rational prize shares; exact ROR matches remain candidates'}
