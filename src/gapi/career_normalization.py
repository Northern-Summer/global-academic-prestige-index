"""Versioned deterministic interpretations; immutable acquisition stays intact.

These narrow lexical rules fill missing stages and explicitly flag unfinished
education as unresolved. They never infer a degree
from a subject, occupation, institution, name or publication. Research employment
and postdoctoral training stay distinct. Original evidence and confidence remain
visible, and a newer rule cannot retroactively rewrite an older frozen input.
"""
import json
import re

from .career_rules import classify, normalized, EXCLUSIONS, excluded_qualification
from .task import digest

VERSION = 'career-interpretation/3'
RULES = (
    ('education', 'bachelor', r'\b(b\.?\s*s\.?|s\.?\s*b\.?)'),
    ('education', 'master', r'\b(m\.?\s*s\.?|s\.?\s*m\.?|masters)'),
    ('education', 'doctorate', r'\b(doctorat|doctorado|doutorado|doktor der naturwissenschaften)\b|박사'),
    ('education', 'master', r'\b(mestrado|maestría|maestria|máster|master of science)\b|석사'),
    ('education', 'bachelor', r'\b(bacharelado|baccalauréat universitaire)\b|학사'),
    ('employment', 'academic', r'\b(research associate|(?:senior |staff |visiting |project )?scientist|ricercator[ei]|professeur|profesor(?:a)?|investigador(?:a)?|pesquisador(?:a)?|maître de conférences|maitre de conferences)\b'),
)
POLICY_SHA256 = digest({'version': VERSION, 'fills_missing_and_excludes_unfinished': True,
                        'exclusions': EXCLUSIONS, 'rules': RULES})
DDL = '''CREATE TABLE IF NOT EXISTS event_interpretations(
    event_id TEXT NOT NULL, rule_version TEXT NOT NULL, policy_sha256 TEXT NOT NULL,
    kind TEXT NOT NULL, stage TEXT, status TEXT NOT NULL, rule_index INTEGER NOT NULL,
    PRIMARY KEY(event_id,rule_version));'''


def completed_education_stage(role):
    """Source-neutral use of the existing qualification and lexical rule owners."""
    kind,stage=classify('education',role,completed_only=True)
    if kind!='education' or excluded_qualification(role):return None
    if stage:return stage
    text=normalized(role)
    for input_kind,candidate,expression in RULES:
        if input_kind=='education' and re.fullmatch(expression,text):return candidate
    return None


def interpretation(event):
    kind = event['kind']
    if event['source'] != 'orcid-public-api': return None
    text = normalized(event.get('role'))
    if excluded_qualification(text):
        if event.get('status') in ('observed', 'unresolved'):
            return kind, None, 'unresolved', -1
        return None
    base_kind, base_stage = classify(kind, event.get('role'))
    if event.get('stage') or base_stage:
        return None
    if kind not in ('education', 'employment'):
        return None
    for index, (input_kind, stage, expression) in enumerate(RULES):
        # New multilingual labels are deliberately full-match only: arbitrary
        # trailing completion/withdrawal prose is never silently interpreted.
        if input_kind != kind or not re.fullmatch(expression, text): continue
        # Explicit jobs do not convert student/graduate assistance into a faculty role.
        if kind == 'employment' and re.search(r'\b(graduate|undergraduate|assistantship)\b', text):
            return None
        end = event.get('year_end') if kind == 'education' else event.get('year_start')
        if not event.get('institution') or not end or event.get('status') not in ('observed', 'unresolved'):
            return None
        return kind, stage, 'observed', index
    return None


def normalize_careers(store):
    store.task.check(4*1024**2)
    store.db.executescript(DDL)
    count = 0
    after = ''
    while True:
        rows = list(store.db.execute('SELECT id,payload FROM events WHERE id>? ORDER BY id LIMIT 500', (after,)))
        if not rows: break
        store.task.check(); store.task.check_memory()
        with store.member('normalization', VERSION):
            for event_id, payload in rows:
                result = interpretation(json.loads(payload))
                if result is None: continue
                row = (event_id, VERSION, POLICY_SHA256, *result)
                old = store.db.execute('SELECT * FROM event_interpretations WHERE event_id=? AND rule_version=?', (event_id, VERSION)).fetchone()
                if old and old != row: raise ValueError('normalization policy changed without version change')
                store.db.execute('INSERT OR IGNORE INTO event_interpretations VALUES(?,?,?,?,?,?,?)', row)
                count += 1
        after = rows[-1][0]
    return {'rule_version': VERSION, 'policy_sha256': POLICY_SHA256, 'interpreted_events': count,
            'canonical_events_modified': 0, 'independent_evidence_promotions': 0}


def has_interpretations(store):
    return bool(store.db.execute("SELECT 1 FROM sqlite_master WHERE name='event_interpretations'").fetchone())
