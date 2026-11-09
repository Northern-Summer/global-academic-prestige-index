"""Derived record disposition and historical concordance; never source writes.

Historical projections can confirm concordance, not restore missing current
facts or supply independent corroboration. Numeric summaries alone are public.
"""
from collections import Counter, defaultdict
import json
import re

from .career_rules import (classify, excluded_qualification, normalized,
                           valid_interval, training_observation_eligible)
from .career_normalization import completed_education_stage, RULES, POLICY_SHA256
from .projection_storage import plain_payload
from .task import BudgetStop, digest

VERSION = 'record-governance/1'
CATEGORIES = ('conflict', 'excluded', 'missing_institution', 'missing_classification',
              'missing_date', 'identified')
POLICY = digest({'version': VERSION, 'classification': POLICY_SHA256,
                 'primary_order': CATEGORIES, 'history': 'concordance-only-no-current-restoration'})


def effective_record(row):
    """Apply existing reviewed lexical owners to every ORCID snapshot equally."""
    value = dict(row)
    if '_governance_interpretation' in value:
        value['kind'], value['stage'] = value['_governance_interpretation']
        return value
    if not value['source'].startswith('orcid-'):
        return value
    kind, stage = classify(value['kind'], value.get('role'), completed_only=True)
    if kind == 'education' and stage is None:
        stage = completed_education_stage(value.get('role'))
    if kind == 'employment' and stage is None:
        text = normalized(value.get('role'))
        for input_kind, candidate, pattern in RULES:
            if input_kind == kind and re.fullmatch(pattern, text):
                stage = candidate
                break
    value['kind'], value['stage'] = kind, stage
    return value


def disposition(row):
    """One primary reason, plus overlapping informational flags."""
    flags = []
    if not valid_interval(row.get('year_start'), row.get('year_end')):
        flags.append('conflict')
    if (row['kind'] in ('education', 'training') and excluded_qualification(row.get('role'))
            or row.get('status') not in ('observed', 'corroborated', 'unresolved')):
        flags.append('excluded')
    if not row.get('institution'):
        flags.append('missing_institution')
    if row.get('stage') not in ('bachelor', 'master', 'doctorate', 'postdoc', 'academic'):
        flags.append('missing_classification')
    if not row.get('year_start' if row['kind'] == 'employment' else 'year_end'):
        flags.append('missing_date')
    return next((c for c in CATEGORIES[:-1] if c in flags), 'identified'), flags


def signature(row, aliases):
    return (row['kind'], aliases.get(row.get('institution'), row.get('institution')),
            row.get('year_start'), row.get('year_end'), normalized(row.get('role')))


class HistoricalConcordance:
    """Reuse existing binary-person indexes; at most one 1000-person batch."""
    def __init__(self, stores):
        self.readers = []
        self.cache = {}
        self.binding = []
        for store in stores:
            names = {r[0] for r in store.db.execute('SELECT name FROM sqlite_master')}
            for state in ('easy_career_state', 'easy_full_state'):
                if state in names:
                    # Freeze identities/statuses without scanning private rows.
                    value = store.db.execute('SELECT * FROM '+state).fetchall()
                    status_column = 3 if state == 'easy_career_state' else 2
                    if len(value) != 1 or value[0][status_column] != 'verified':
                        raise ValueError('historical source is not verified')
                    self.binding.append(digest([list(r) for r in value]))
            for table in ('easy_careers', 'easy_full_delta'):
                if table in names:
                    if 'easy_career_state' not in names or table == 'easy_full_delta' and 'easy_full_state' not in names:
                        raise ValueError('historical source identity missing')
                    if table+'_person' not in names:
                        raise ValueError('historical person index missing')
                    self.readers.append((store, table))

    def population(self):
        from heapq import merge
        cursors = []
        try:
            for store, table in self.readers:
                cursor=store.db.cursor();cursor.row_factory=None;cursors.append(cursor)
                cursor.execute(f'SELECT person FROM {table} INDEXED BY {table}_person GROUP BY person ORDER BY person')
            previous=None;count=0
            for (pid,) in merge(*cursors):
                if pid != previous:count+=1;previous=pid
                if count and count%100000==0:self.readers[0][0].task.check_memory()
            return count
        finally:
            for cursor in cursors:cursor.close()

    def prepare(self, people):
        self.cache = {}
        people = [bytes.fromhex(p) for p in people if re.fullmatch('[a-f0-9]{64}', p)]
        if len(people) > 1000:
            raise BudgetStop('historical lookup batch exceeds bound')
        size = 0
        for store, table in self.readers:
            for offset in range(0, len(people), 500):
                batch = people[offset:offset+500]
                if not batch:
                    continue
                cursor = store.db.cursor(); cursor.row_factory = None
                try:
                    sql = f'SELECT person,payload FROM {table} INDEXED BY {table}_person WHERE person IN ('+','.join('?' for _ in batch)+')'
                    for pid, payload in cursor.execute(sql, batch):
                        raw = plain_payload(store.db, payload)
                        size += len(raw)
                        if size > 8*1024**2:
                            raise BudgetStop('historical batch decoded bound')
                        target = self.cache.setdefault(pid.hex(), [])
                        target.extend(json.loads(raw))
                        if len(target) > 4096:
                            raise BudgetStop('historical person fanout exceeds bound')
                finally:
                    cursor.close()


class RecordGovernance:
    def __init__(self):
        self.primary = Counter(); self.flags = Counter(); self.changes = Counter()
        self.history = Counter(); self.funnel = Counter()

    def consume(self, rows, historical, aliases, contributed):
        old = defaultdict(set)
        for kind, stage, inst, start, end, role in historical:
            old[signature({'kind': kind, 'institution': inst, 'year_start': start,
                           'year_end': end, 'role': role}, aliases)].add(stage)
        if historical:
            self.history['current_people_with_retained_history'] += 1
        current = set()
        for raw in rows:
            row = effective_record(raw)
            primary, flags = disposition(row)
            self.primary[primary] += 1; self.flags.update(flags)
            before = disposition(raw)[0]
            changed = (raw.get('kind'), raw.get('stage')) != (row['kind'], row.get('stage'))
            self.changes['rule_changed_events'] += changed
            self.changes['newly_identified_events'] += before != 'identified' and primary == 'identified'
            key = signature(row, aliases); current.add(key)
            if key in old:
                self.history['exact_projection_matches'] += 1
                if row.get('stage') not in old[key]:
                    self.history['stage_disagreements_current_preserved'] += 1
            elif historical:
                self.history['unmatched_current_events'] += 1
        self.history['historical_affiliations_absent_from_current'] += len(set(old)-current)
        trainings = [r for r in rows if training_observation_eligible(
            r['kind'], r.get('role'), r.get('year_start'), r.get('year_end'))]
        jobs = [r for r in rows if r['kind'] == 'employment']
        identified_trainings = [effective_record(r) for r in trainings
                               if effective_record(r).get('stage') in ('bachelor','master','doctorate','postdoc')]
        identified_jobs = [effective_record(r) for r in jobs if effective_record(r).get('stage') == 'academic']
        self.funnel['people_with_training'] += bool(trainings)
        self.funnel['people_with_academic_employment'] += bool(identified_jobs)
        self.funnel['people_with_identified_both_sides'] += bool(identified_trainings and identified_jobs)
        self.funnel['people_with_dates_on_both_sides'] += bool(
            any(r.get('year_end') for r in identified_trainings) and any(r.get('year_start') for r in identified_jobs))
        self.funnel['people_contributing_relations'] += contributed

    def snapshot(self):
        return {k:dict(getattr(self,k)) for k in ('primary','flags','changes','history','funnel')}

    def restore(self, value, events):
        if set(value) != {'primary','flags','changes','history','funnel'}:
            raise ValueError('invalid governance checkpoint fields')
        for key, counts in value.items():
            if not isinstance(counts,dict) or any(type(v) is not int or v < 0 for v in counts.values()):
                raise ValueError('invalid governance checkpoint counts')
            setattr(self,key,Counter(counts))
        if set(self.primary)-set(CATEGORIES) or sum(self.primary.values()) != events:
            raise ValueError('governance event accounting mismatch')

    def public(self, events):
        value = self.snapshot()
        self.restore(value, events)
        return dict(value, schema_version=VERSION, policy_sha256=POLICY,
                    complete=True, total_events=events, primary_order=list(CATEGORIES),
                    history_scope='retained-2023-projections-only; concordance is not independent verification',
                    historical_records_restored_into_current=0, independent_evidence_promotions=0)
