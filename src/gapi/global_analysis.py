"""Bounded institution-level analysis; privacy and release decisions live here."""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import math
import os
import platform
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path

from .ranking import DEFAULT_CONFIG, pagerank
from .career_rules import classify, valid_interval, training_observation_eligible
from .storage import project_root
from .career_store import member_evidence_sql, verified_archive_sql
from .task import BudgetStop, canonical, digest
from .publication import public_sources


PACKED_GRAPH_THRESHOLD = 200000


def _rank_intensity(store, nodes, edges, denominators):
    """Apply exact denominator weights without duplicating the large graph."""
    if len(edges)<=PACKED_GRAPH_THRESHOLD:
        return _rank_graph(store,nodes,{(s,t):w/max(1,denominators[t])
            for (s,t),w in sorted(edges.items())},.85,1e-12,200)
    from .ranking import pagerank_sparse
    if hasattr(edges,'packed_rows'):
        rows=edges.packed_rows()  # Completed checkpoint rows are endpoint-sorted.
    else:
        rows=((s,t,w) for (s,t),w in sorted(edges.items()))
    store.task.check_memory()
    return pagerank_sparse(nodes,((s,t,w/max(1,denominators[t])) for s,t,w in rows),
        .85,1e-12,200,check=store.task.check_memory)


def _rank_graph(store, nodes, edges, alpha, tolerance, iterations, personalization=None):
    """Preserve sorted-edge model; large graphs avoid a second edge dict.

    The threshold selects a representation, never truncates the graph or raises
    a resource allowance. Packed admission and each iteration observe the same
    task memory budget.
    """
    store.task.check_memory()
    if hasattr(edges,'packed_rows') and len(edges)>PACKED_GRAPH_THRESHOLD:
        from .ranking import pagerank_sparse
        store.task.check_memory()
        return pagerank_sparse(nodes, edges.packed_rows(), alpha,tolerance,iterations,
            check=store.task.check_memory,personalization=personalization)
    if len(edges) <= PACKED_GRAPH_THRESHOLD:
        return pagerank(nodes, dict(sorted(edges.items())), alpha, tolerance, iterations, personalization)
    from .ranking import pagerank_sparse
    if hasattr(edges,'packed_rows'):
        return pagerank_sparse(nodes,edges.packed_rows(),alpha,tolerance,iterations,
            check=store.task.check_memory,personalization=personalization)
    keys = sorted(edges)
    store.task.check_memory()
    return pagerank_sparse(nodes, ((a,b,edges[(a,b)]) for a,b in keys),
        alpha,tolerance,iterations,check=store.task.check_memory,personalization=personalization)


def ror_records(path):
    """Incrementally decode the existing normalized ROR directory, not its raw dump."""
    decoder = json.JSONDecoder()
    with gzip.open(path, "rt", encoding="utf-8") as f:
        buffer = f.read(65536)
        marker = '"records":['
        position = buffer.find(marker)
        if position < 0:
            raise ValueError("ROR normalized records array missing")
        buffer = buffer[position+len(marker):]
        while True:
            buffer = buffer.lstrip(", \r\n\t")
            if buffer.startswith("]"):
                return
            try:
                value, end = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                more = f.read(65536)
                if not more:
                    raise ValueError("truncated normalized ROR directory")
                buffer += more
                if len(buffer) > 1024**2:
                    raise ValueError("ROR record exceeds bound")
                continue
            yield value
            buffer = buffer[end:]


def resolve_institutions(store, *, extra_identifiers=()):
    # Admit by the task's actual memory budget, not an identifier-count proxy.
    # Consume supplied iterables incrementally as well as database identifiers;
    # failure here precedes all persistent crosswalk writes.
    store.task.check_memory()
    wanted = set()
    for number, identifier in enumerate(extra_identifiers, 1):
        wanted.add(identifier)
        if number % 256 == 0:
            store.task.check_memory()
    for number, row in enumerate(store.db.execute("SELECT DISTINCT institution FROM events WHERE institution IS NOT NULL"), 1):
        wanted.add(row[0])
        if number % 256 == 0:
            store.task.check_memory()
    store.task.check_memory()
    directory = project_root() / "data/real/institutions.json.gz"
    resolved, ambiguous = {}, set()
    if directory.exists() and wanted:
        # Verify the preserved input identity before using any crosswalk.
        expected = "959747b1ee6075d5598cc658d5599051714d4e054e93a706e422c43071f07db6"
        with directory.open("rb") as f:
            if hashlib.file_digest(f, "sha256").hexdigest() != expected:
                raise ValueError("ROR normalized directory identity changed")
        for number, record in enumerate(ror_records(directory), 1):
            if number % 256 == 0:
                store.task.check_memory()
            canonical_id = "ror:" + record["id"]
            ids = {canonical_id}
            for authority in ("grid", "wikidata"):
                ids.update(("wd:" if authority == "wikidata" else authority + ":") + x for x in record.get("external_ids", {}).get(authority, []))
            matched = ids & wanted
            if matched:
                for alias in matched:
                    if alias in ambiguous:
                        continue
                    # Refuse ambiguous crosswalk rather than last-row-wins.
                    if alias in resolved and resolved[alias][0] != canonical_id:
                        ambiguous.add(alias)
                        resolved.pop(alias)
                        continue
                    resolved[alias] = (canonical_id, record)
    batch = []
    conflicts = set()
    policy='ror-exact-crosswalk/2'
    with store.db:
        for alias in sorted(ambiguous):
            store.db.execute('INSERT OR IGNORE INTO institution_alias_issues VALUES(?,?,?,?)',
                (alias,policy,'ambiguous_frozen_ror_identifier',canonical({'ror_sha256':expected}).decode()))
    def commit_batch(rows):
        with store.member("identity", "ror-crosswalk"):
            for alias, (target, record) in rows:
                old=store.db.execute('SELECT institution,provenance FROM institution_aliases WHERE alias=?',(alias,)).fetchone()
                if old and old[0]!=target:
                    conflicts.add(alias)
                    store.db.execute('INSERT OR IGNORE INTO institution_alias_issues VALUES(?,?,?,?)',
                        (alias,policy,'existing_alias_conflict',canonical({'existing':old,'proposed':target,'ror_sha256':expected}).decode()))
                else:
                    store.db.execute("INSERT OR IGNORE INTO institution_aliases VALUES(?,?,?)",(alias,target,expected))
                store.db.execute("INSERT OR REPLACE INTO entities VALUES(?,?,?,?,?)", (target, record["name"], record.get("country_code"), 1,
                    canonical({"region": record.get("geo", {}).get("continent_name"), "type": record.get("type"), "ror": record["ror"]}).decode()))
    for item in resolved.items():
        batch.append(item)
        if len(batch) == 1000:
            commit_batch(batch)
            batch.clear()
    if batch:
        commit_batch(batch)
    active=dict(store.db.execute('SELECT alias,institution FROM active_institution_aliases'))
    # The bridge consumes this scan's agreement, never an old reciprocal row.
    verified={alias:target for alias,(target,_) in resolved.items() if active.get(alias)==target}
    total_resolved = sum(alias in wanted for alias in active)
    return {"resolved_identifiers": total_resolved, "unresolved_identifiers": len(wanted)-total_resolved,
            "ambiguous_identifiers": sorted(ambiguous), "conflicting_identifiers":sorted(conflicts),
            "verified_reciprocal":verified, "name_similarity_merges": 0}


def corroborate(store):
    from .corroboration import decisions, overlap_report, VERSION, POLICY_SHA256
    resolution = resolve_institutions(store)
    candidates = store.db.execute('''SELECT count(*) FROM (
        SELECT person,institution,kind,stage,year_start,year_end FROM events
        WHERE institution IS NOT NULL AND stage IS NOT NULL
        GROUP BY person,institution,kind,stage,year_start,year_end HAVING count(DISTINCT source)>1)''').fetchone()[0]
    overlap=overlap_report(store)
    verified = decisions(store)
    return {"identity": resolution, "raw_identifier_exact_candidates": candidates,
            "cross_source_candidates": overlap['normalized_exact_fact_candidates'],
            "cross_source_overlap":overlap,
            "independent_official_confirmations": len(verified),
            "rule_version": VERSION, "policy_sha256": POLICY_SHA256,
            "decision_index_sha256": digest(verified), "canonical_events_modified": 0,
            "policy": "exact official profile and same-ORCID self assertion; copied official reference counts once"}


def observed_relations(items):
    trainings = [e for e in items if training_observation_eligible(
        e['kind'], e.get('role'), e.get('year_start'), e.get('year_end'))
        and e.get("year_end") and e.get("stage") in DEFAULT_CONFIG["stage_weights"]]
    jobs = [e for e in items if e["kind"] == "employment" and e.get("stage") == "academic"
            and e.get("year_start") and valid_interval(e.get('year_start'), e.get('year_end'))]
    grouped = {}
    for train in trainings:
        for job in jobs:
            key = (job["institution"], train["institution"], train["stage"])
            if key[0] == key[1] or job["year_start"] < train["year_end"]:
                continue
            # Detect overlapping jobs at different institutions. Missing end means ongoing.
            if any(other["institution"] != job["institution"] and
                   other["year_start"] <= (job.get("year_end") or 2100) and
                   job["year_start"] <= (other.get("year_end") or 2100) for other in jobs):
                continue
            # Count the relation once, retaining every valid evidence source.
            # Strong evidence cannot disappear when an unrelated ID sorts first.
            priority = (not (train.get('evidence_origins') and job.get('evidence_origins')),
                not (train['status'] == job['status'] == 'corroborated'),
                -(int(train['status']=='corroborated')+int(job['status']=='corroborated')),
                job['year_start'], train['year_end'], train['source'], job['source'],
                train['locator'], job['locator'])
            if key not in grouped:
                if len(grouped) >= 200000:
                    raise BudgetStop('person relation fanout exceeds bounded analysis')
                grouped[key] = [priority, train, job, set(), set()]
            entry = grouped[key]
            entry[3].add(train['source']); entry[4].add(job['source'])
            if priority < entry[0]: entry[:3] = [priority, train, job]
    for key in sorted(grouped):
        _, train, job, training_sources, job_sources = grouped[key]
        yield key, dict(train, supporting_sources=sorted(training_sources)), dict(job, supporting_sources=sorted(job_sources))


def relation_rows(store, *, allowed_sources=None, after_person=None, person_complete=None, persons=None):
    for person, events, _ in _relation_people(store, allowed_sources=allowed_sources,
                                         after_person=after_person, persons=persons):
        yield from observed_relations(events)
        if person_complete is not None:
            person_complete(person)


def _relation_people(store, *, allowed_sources=None, after_person=None, persons=None,
                     excluded_members=(), shared_aliases=None, prepared=None,
                     physical_source=None, after_rowid=0, capture_directory=False):
    """Read one person's bounded events at a time. No population-sized JSON/list."""
    # Source-deletion sensitivity can remove the only available source. That
    # graph is exactly empty: do not scan the population merely to discard it.
    if allowed_sources is not None and not allowed_sources:
        return
    selected=()
    if persons is not None:
        if (not isinstance(persons,(tuple,list)) or not 1<=len(persons)<=1000
                or any(not isinstance(p,str) or not p for p in persons)
                or len(set(persons))!=len(persons) or after_person is not None):
            raise ValueError('invalid bounded relation person selection')
        selected=tuple(sorted(persons))
    from .career_normalization import has_interpretations, VERSION, POLICY_SHA256
    enriched = has_interpretations(store) if prepared is None else prepared['enriched']
    from .corroboration import decisions
    verified = decisions(store, allowed_sources=allowed_sources) if prepared is None else prepared['verified']
    # This reader already owns a stable analysis transaction under the writer
    # lock. Reuse the authoritative active view once instead of probing its
    # alias and issue indexes for every event, including ineligible events.
    aliases = shared_aliases
    if aliases is None:
        aliases = {}
        for number, (alias, target) in enumerate(store.db.execute(
                'SELECT alias,institution FROM active_institution_aliases'), 1):
            aliases[alias] = target
            if number % 1000 == 0:
                store.task.check_memory()
    store.task.check_memory()
    archive_predicate,archive_params=verified_archive_sql(store.task)
    columns = ',i.kind,i.stage,i.status' if enriched else ',NULL,NULL,NULL'
    if excluded_members:
        columns += ',e.locator'
    columns += ',e.country'
    if capture_directory:columns += ',e.rowid'
    join = " LEFT JOIN event_interpretations i ON i.event_id=e.id AND i.rule_version=? AND i.policy_sha256=?" if enriched else ''
    seek = ' AND e.person>?' if after_person is not None else ''
    if selected:seek=' AND e.person IN ('+','.join('?' for _ in selected)+')'
    if after_person is not None and not isinstance(after_person,str):
        raise ValueError('invalid relation person cursor')
    # Keep physical payload bytes until scalar eligibility has been checked.
    # The reader otherwise decompresses every unresolved event before the loop
    # discards it. Preserve row order, interpretation overrides and fanout gates.
    if physical_source is not None:
        if after_person is not None or selected or type(after_rowid) is not int or after_rowid<0:
            raise ValueError('invalid physical person cursor')
        seek=' AND e.source=? AND e.rowid>?'
    cursor = store.db.cursor()
    # These columns contain no encoded digest/URL. Decode payloads explicitly
    # only after eligibility; the generic factory inspects every scalar column.
    cursor.row_factory=None
    cursor.execute("""SELECT e.person,e.payload AS stored_payload,e.institution,e.status,e.source,e.id""" + columns + """
        FROM events e""" + (' NOT INDEXED' if physical_source is not None else '') + join + """
        WHERE """ + member_evidence_sql() + " AND " + archive_predicate + seek + """
        ORDER BY """ + ('e.rowid' if physical_source is not None else 'e.person,e.id'),
        ((VERSION, POLICY_SHA256) if enriched else ()) + archive_params
        + ((physical_source,after_rowid) if physical_source is not None else
           (after_person,) if after_person is not None else selected))
    try:
        person, events = None, []
        summary = {'events': 0, 'event_status': Counter(), 'countries': set()}
        if capture_directory:summary.update(labels={},directory_ids=set(),end_rowid=after_rowid)
        for record in cursor:
            pid, payload, institution, status, source, event_id, interpreted_kind, interpreted_stage, interpreted_status = record[:9]
            institution = aliases.get(institution, institution)
            if person is not None and pid != person:
                yield person, events, summary
                events = []
                summary = {'events': 0, 'event_status': Counter(), 'countries': set()}
                if capture_directory:summary.update(labels={},directory_ids=set(),end_rowid=after_rowid)
            person = pid
            if excluded_members and (source, record[9].split('#', 1)[0]) in excluded_members:
                continue
            if allowed_sources is not None and source not in allowed_sources:
                continue
            summary['events'] += 1
            summary['event_status'][status] += 1
            country = record[-2] if capture_directory else record[-1]
            if capture_directory:summary['end_rowid']=record[-1]
            if country is not None and (allowed_sources is None or country != ''):
                summary['countries'].add(country)
            if len(events) >= 4096 or capture_directory and summary['events']>4096:
                raise BudgetStop("person event fanout exceeds bounded relation analysis")
            row=None
            if capture_directory and institution:
                summary['directory_ids'].add(institution)
                if institution not in prepared['canonical_ids']:
                    from .event_storage import decode_event
                    row=json.loads(decode_event(store.db,payload))
                    label=row.get('institution_name')
                    if label:
                        previous=summary['labels'].get(institution)
                        if previous is None or event_id<previous[0]:
                            summary['labels'][institution]=(event_id,label,country)
            if interpreted_status is not None:
                status = interpreted_status
            if status not in ("observed", "corroborated") or not institution:
                continue
            from .event_storage import decode_event
            if row is None:row = json.loads(decode_event(store.db,payload))
            row["institution"], row["status"] = institution, status
            if source.startswith('orcid-'):
                row['kind'], row['stage'] = classify(row['kind'],row.get('role'),completed_only=True)
            if interpreted_status is not None:
                row['kind'], row['stage'] = interpreted_kind, interpreted_stage
            if event_id in verified:
                row['status'] = 'corroborated'
                row['evidence_origins'] = verified[event_id]['origins']
            events.append(row)
        if person is not None:
            yield person, events, summary
    finally:
        cursor.close()


def select_analysis_members(stores, *, allowed_sources=None):
    """Select duplicate member versions for a two-store analysis, without writes.

    Callers hold both stores' existing writer exclusions. Source identities must
    agree before identical locators are compared. Different API bodies require
    complete, hash-bound activity captures with strictly ordered time windows.
    This is an input selection, not a combined coverage or release manifest.
    """
    from datetime import datetime
    from .career_store import source_identity
    from .orcid_stream import API_RECORD_TRANSPORT
    if not isinstance(stores, (tuple, list)) or len(stores) != 2 or stores[0] is stores[1]:
        raise ValueError('analysis requires two distinct stores')
    generations = []
    sources = []
    for store in stores:
        store.task.require_frozen()
        path = store.task.root / 'careers.sqlite'
        actual = next((row[2] for row in store.db.execute('PRAGMA database_list') if row[1] == 'main'), None)
        if not actual or Path(actual).resolve() != path.resolve():
            raise ValueError('analysis connection differs from its bound database')
        if store.db.in_transaction or any(Path(str(path)+suffix).exists() for suffix in ('-wal', '-journal')):
            raise ValueError('multi-store analysis requires settled inputs')
        info = path.stat()
        generations.append((str(path.resolve()), info.st_size, info.st_mtime_ns))
        active = {}
        for source, identity, members in store.db.execute('SELECT source,identity,members FROM runs WHERE members>0'):
            if allowed_sources is not None and source not in allowed_sources:
                continue
            if source not in store.task.config['sources'] or identity != source_identity(store.task, source):
                raise ValueError('analysis source identity differs from its freeze')
            active[source] = (identity, members)
        sources.append(active)
    if generations[0][0] == generations[1][0]:
        raise ValueError('analysis stores refer to the same database')

    def window(store, source, locator, checksum):
        parts = list(store.db.execute('SELECT part,sha256,retrieved_at FROM member_parts WHERE source=? AND locator=?',
                                      (source, locator)))
        names = {part for part, _, _ in parts}
        if len(names) != len(parts) or names not in ({'record'}, {'educations', 'employments'}):
            raise ValueError('changed member lacks complete activity evidence')
        transport = API_RECORD_TRANSPORT if names == {'record'} else 'incremental-affiliation-xml/v1'
        if digest({'transport': transport, 'parts': {part: sha for part, sha, _ in parts}}) != checksum:
            raise ValueError('changed member evidence hash mismatch')
        times = []
        for _, _, stamp in parts:
            try:
                value = datetime.fromisoformat(stamp)
                if value.utcoffset() is None:
                    raise ValueError('unqualified timestamp')
            except (TypeError, ValueError):
                raise ValueError('invalid member retrieval timestamp') from None
            times.append(value)
        return min(times), max(times)

    excluded = [set(), set()]
    same = changed = rejected = 0
    for source in sorted(set(sources[0]) & set(sources[1])):
        if sources[0][source][0] != sources[1][source][0]:
            raise ValueError('cannot combine different frozen identities under one source name')
        small = 0 if sources[0][source][1] <= sources[1][source][1] else 1
        other = 1-small
        for locator, sha, status in stores[small].db.execute(
                'SELECT locator,sha256,status FROM members WHERE source=?', (source,)):
            old = stores[other].db.execute('SELECT sha256,status FROM members WHERE source=? AND locator=?',
                                           (source, locator)).fetchone()
            if old is None:
                continue
            if len(excluded[0])+len(excluded[1]) >= 1000:
                raise BudgetStop('duplicate member selection exceeds bounded plan')
            if sha == old[0] and status == old[1]:
                loser = 1
                same += 1
            elif status != 'observed' or old[1] != 'observed':
                if (status == 'observed') == (old[1] == 'observed'):
                    raise ValueError('conflicting rejected member versions')
                loser = other if status == 'observed' else small
                rejected += 1
            else:
                if source != 'orcid-public-api':
                    raise ValueError('changed member has no supported snapshot selection rule')
                left = window(stores[small], source, locator, sha)
                right = window(stores[other], source, locator, old[0])
                if left[1] < right[0]:
                    loser = small
                elif right[1] < left[0]:
                    loser = other
                else:
                    raise ValueError('changed member capture windows overlap')
                changed += 1
            excluded[loser].add((source, locator))
            stores[small].task.check_memory()
    for store, expected in zip(stores, generations):
        path = store.task.root / 'careers.sqlite'
        info = path.stat()
        if (str(path.resolve()), info.st_size, info.st_mtime_ns) != expected:
            raise ValueError('analysis input changed during member selection')
    return {'schema': 'gapi-analysis-member-selection/1', 'generations': generations,
            'excluded_members': [sorted(values) for values in excluded],
            'identical_members': same, 'changed_members': changed,
            'failed_versions_excluded': rejected}


def relation_rows_from_stores(stores, *, allowed_sources=None, persons=None):
    """Stream both stores through the existing per-person relation rule owner.

    The selection's capture policy removes superseded API members before person
    grouping. This iterator does not authorize a merged score publication:
    coverage, denominator, directory and checkpoint inputs need the same stores.
    """
    for _, events, _ in analysis_people_from_stores(stores, allowed_sources=allowed_sources, persons=persons):
        yield from observed_relations(events)


def analysis_people_from_stores(stores, *, allowed_sources=None, persons=None, after_person=None):
    """One selected person group for relations, coverage and training counts.

    Coverage retains raw observation statuses; interpretation never converts a
    coverage event into independent evidence. Event payloads retain the existing
    relation-reader eligibility contract. All consumers must drain this stream
    before accepting the final generation check.
    """
    from heapq import merge
    from itertools import groupby
    if allowed_sources is not None and not allowed_sources:
        return
    versions=[(s.db.total_changes,s.db.execute('PRAGMA data_version').fetchone()[0]) for s in stores]
    plan = select_analysis_members(stores, allowed_sources=allowed_sources)
    aliases = {}
    for store in stores:
        for alias, target in store.db.execute('SELECT alias,institution FROM active_institution_aliases'):
            if alias in aliases and aliases[alias] != target:
                raise ValueError('analysis institution aliases disagree')
            aliases[alias] = target
        store.task.check_memory()
    streams = [_relation_people(store, allowed_sources=allowed_sources, persons=persons,
                               after_person=after_person,
                               excluded_members=set(map(tuple, excluded)), shared_aliases=aliases)
               for store, excluded in zip(stores, plan['excluded_members'])]
    for _, groups in groupby(merge(*streams, key=lambda item: item[0]), key=lambda item: item[0]):
        events = []
        summary = {'events': 0, 'event_status': Counter(), 'countries': set()}
        for person, rows, counts in groups:
            if len(events)+len(rows) > 4096:
                raise BudgetStop('combined person event fanout exceeds bounded relation analysis')
            events.extend(rows)
            summary['events'] += counts['events']
            summary['event_status'].update(counts['event_status'])
            summary['countries'].update(counts['countries'])
        yield person, events, summary
    for store, version, expected in zip(stores, versions, plan['generations']):
        if (store.db.total_changes,store.db.execute('PRAGMA data_version').fetchone()[0])!=version:
            raise ValueError('analysis input changed during relation reading')
        path = store.task.root / 'careers.sqlite'
        info = path.stat()
        if (str(path.resolve()), info.st_size, info.st_mtime_ns) != tuple(expected):
            raise ValueError('analysis input changed during relation reading')


def analysis_counts_from_stores(stores, *, allowed_sources=None, persons=None, progress=None):
    """Reconcile selected observation coverage and training denominators together.

    Returns scalar/organization aggregates only. No source rows or database copies
    are written. A bounded person selection is diagnostic and explicitly labeled.
    Member totals remain separate until the full selected-member ledger is read.
    """
    events = people = 0
    statuses = Counter()
    countries = set()
    denominators = Counter()
    for _, rows, summary in analysis_people_from_stores(stores, allowed_sources=allowed_sources, persons=persons):
        events += summary['events']
        people += bool(summary['events'])
        statuses.update(summary['event_status'])
        countries.update(summary['countries'])
        institutions = {row['institution'] for row in rows if training_observation_eligible(
            row['kind'], row.get('role'), row.get('year_start'), row.get('year_end'))}
        denominators.update(institutions)
        if people and people % 10000 == 0:
            for store in stores:
                store.task.check_memory()
            if progress:
                progress({'people': people, 'events': events})
    return {'scope': 'selected-people-diagnostic' if persons is not None else 'selected-two-store-observations',
            'coverage': {'people': people, 'events': events, 'event_status': dict(statuses),
                         'countries': sorted(countries)},
            'training_denominators': dict(denominators), 'complete_release': False}


def _accumulate_relation(relation, edges, stages, counts, source_counts, corroborated,
                         independent_origins, recent_edges):
    (employer, training, stage), train, job = relation
    key = employer, training
    counts[training] += 1
    source_counts[training].update(train['supporting_sources'])
    source_counts[training].update(job['supporting_sources'])
    if train['status'] == job['status'] == 'corroborated':
        corroborated[training] += 1
        independent_origins[training].update(set(train.get('evidence_origins', ())) & set(job.get('evidence_origins', ())))
    weight = DEFAULT_CONFIG['stage_weights'][stage]
    edges[key] += weight
    stages[key][stage] += weight
    if job['year_start'] >= 2016:
        recent_edges[key] += weight


def scan_analysis_stores(stores, *, allowed_sources=None, max_seconds=300, max_people=None, progress=None):
    """Resume one atomic graph/coverage/denominator pass at person boundaries.

    Callers hold the existing writer exclusions. A time/person boundary saves a
    partial, never a completed result. The output is private analysis state, not
    scores, publication qualification or a source completion assertion.
    """
    from .analysis_checkpoint import MultiStoreGraphCheckpoint
    if (isinstance(max_seconds,bool) or not isinstance(max_seconds,(int,float))
            or not math.isfinite(max_seconds) or max_seconds<=0
            or max_people is not None and (type(max_people) is not int or max_people<1)):
        raise ValueError('invalid multi-store scan bound')
    checkpoint=MultiStoreGraphCheckpoint(stores,allowed_sources)
    saved=checkpoint.load()
    edges,stages,counts=defaultdict(float),defaultdict(Counter),Counter()
    source_counts,corroborated=defaultdict(set),Counter()
    independent_origins,recent_edges=defaultdict(set),defaultdict(float)
    relation_count=people=events=0
    statuses=Counter();countries=set();denominators=Counter()
    if saved:
        graph=saved['graph']
        edges.update((tuple(k),n) for k,n in graph['edges'])
        stages.update((tuple(k),Counter(n)) for k,n in graph['stages'])
        counts.update(graph['counts']);source_counts.update((k,set(n)) for k,n in graph['source_counts'].items())
        corroborated.update(graph['corroborated']);independent_origins.update((k,set(n)) for k,n in graph['independent_origins'].items())
        recent_edges.update((tuple(k),n) for k,n in graph['recent_edges']);relation_count=graph['relation_count']
        coverage=saved['coverage'];people=coverage['people'];events=coverage['events']
        statuses.update(coverage['event_status']);countries.update(coverage['countries'])
        denominators.update(saved['training_denominators'])
        del saved
    def state():
        # Return the persisted JSON representation on both fresh and resumed
        # scans, so callers can compare state without a second full encoding.
        return {'graph':{'edges':[[list(k),n] for k,n in edges.items()],
                'stages':[[list(k),dict(n)] for k,n in stages.items()],'counts':dict(counts),
                'source_counts':{k:sorted(n) for k,n in source_counts.items()},'corroborated':dict(corroborated),
                'independent_origins':{k:sorted(n) for k,n in independent_origins.items()},
                'recent_edges':[[list(k),n] for k,n in recent_edges.items()],'relation_count':relation_count},
                'coverage':{'people':people,'events':events,'event_status':dict(statuses),'countries':sorted(countries)},
                'training_denominators':dict(denominators)}
    started=time.monotonic();processed=0;last=started
    if not checkpoint.complete:
        rows=analysis_people_from_stores(stores,allowed_sources=allowed_sources,after_person=checkpoint.after_person)
        try:
            for person,items,summary in rows:
                for relation in observed_relations(items):
                    _accumulate_relation(relation,edges,stages,counts,source_counts,corroborated,independent_origins,recent_edges)
                    relation_count+=1
                people+=bool(summary['events']);events+=summary['events']
                statuses.update(summary['event_status']);countries.update(summary['countries'])
                denominators.update({row['institution'] for row in items if training_observation_eligible(
                    row['kind'],row.get('role'),row.get('year_start'),row.get('year_end'))})
                checkpoint.boundary(person,state);processed+=1
                if processed%1000==0:
                    for store in stores:store.task.check_memory()
                if progress and time.monotonic()-last>=30:
                    progress({'phase':'selected-person-scan','people':people,'events':events,'relations':relation_count})
                    last=time.monotonic()
                if time.monotonic()-started>=max_seconds or max_people is not None and processed>=max_people:
                    checkpoint.save(state,complete=False)
                    break
            else:checkpoint.finish(state)
        finally:rows.close()
    return {'state':state(),'complete':checkpoint.complete,'checkpoint':str(checkpoint.path),
            'input_binding':checkpoint.binding,'people_processed_this_run':processed,
            'elapsed_seconds':time.monotonic()-started,'publication_ready':False}


def _training_denominators(store, allowed, report):
    # Conditional intensity: denominator is observed training people, never graduates at risk.
    from .career_normalization import has_interpretations, VERSION, POLICY_SHA256
    denominators = Counter()
    report('training_denominators')
    store.db.create_function('gapi_training_observation', 4,
                             training_observation_eligible, deterministic=True)
    effective_kind = 'e.kind'
    denominator_sql = """SELECT e.person,COALESCE(a.institution,e.institution)
        FROM events e NOT INDEXED LEFT JOIN active_institution_aliases a ON a.alias=e.institution
        WHERE e.kind IN ('education','training') AND e.status IN ('observed','corroborated')"""
    params = ()
    if has_interpretations(store):
        # The explicit v2 view counts its interpreted training observations in
        # the same denominator; old frozen inputs retain their predecessor view.
        denominator_sql = """SELECT e.person,COALESCE(a.institution,e.institution)
            FROM events e NOT INDEXED LEFT JOIN active_institution_aliases a ON a.alias=e.institution
            LEFT JOIN event_interpretations i ON i.event_id=e.id AND i.rule_version=? AND i.policy_sha256=?
            WHERE COALESCE(i.kind,e.kind) IN ('education','training')
            AND COALESCE(i.status,e.status) IN ('observed','corroborated')"""
        params = (VERSION, POLICY_SHA256)
        effective_kind = 'COALESCE(i.kind,e.kind)'
    archive_predicate,archive_params=verified_archive_sql(store.task)
    denominator_sql += " AND " + member_evidence_sql() + " AND " + archive_predicate
    denominator_sql += (' AND gapi_training_observation('
                        + effective_kind + ',e.role,e.year_start,e.year_end)')
    params += archive_params
    if allowed is not None:
        denominator_sql += " AND e.source IN (" + ','.join('?' for _ in allowed) + ")"
        params += tuple(sorted(allowed))
    # Read physical rows, then sort injective compact keys in memory. A hash
    # shortcut would risk merging distinct people. Lowercase SHA256 IDs use a
    # tagged lossless binary encoding; other identifiers retain exact UTF-8.
    records=[];institution_ids={}
    cursor=store.db.execute(denominator_sql,params)
    try:
        for number,(person,institution) in enumerate(cursor,1):
            if institution:
                if institution not in institution_ids:
                    institution_ids[institution]=len(institution_ids)
                raw=(b'\0'+bytes.fromhex(person) if len(person)==64
                     and all(c in '0123456789abcdef' for c in person)
                     else b'\1'+person.encode('utf-8'))
                records.append(institution_ids[institution].to_bytes(4,'big')+raw)
            if number%1000==0:
                store.task.check_memory()
                if number%100000==0:report('training_denominators',scanned_rows=number)
    finally:cursor.close()
    # CPython's sort may reserve half a pointer array. Admit that additional
    # demand against the unchanged process allowance before starting the sort.
    import struct
    if store.task.check_memory()+len(records)*struct.calcsize('P')//2>store.task.config['memory_bytes']:
        raise BudgetStop('training denominator sort exceeds process memory budget')
    records.sort();previous=None;counts=Counter()
    for number,key in enumerate(records,1):
        if key!=previous:
            counts[int.from_bytes(key[:4],'big')]+=1;previous=key
        if number%100000==0:store.task.check_memory()
    for institution,index in institution_ids.items():denominators[institution]=counts[index]
    return denominators


def _institution_directory(store, allowed, report):
    report('institution_labels')
    directory = {r[0]: (r[1], r[2], json.loads(r[3])) for r in store.db.execute("SELECT id,name,country,payload FROM entities")}
    # A reported label is presentation only, never a name-based identity merge.
    # Select the same smallest eligible event ID while reading physical rows.
    # ID-index order caused random table/payload lookups across the full store.
    # All allowed payloads are still decoded, including known institutions, so
    # invalid input remains an error rather than silently disappearing.
    canonical_directory=set(directory)
    label_ids={}
    for number, (event_id, institution, payload, country, source) in enumerate(store.db.execute("SELECT id,institution,payload,country,source FROM events NOT INDEXED WHERE institution IS NOT NULL"),1):
        if number % 100000 == 0:
            report('institution_labels',scanned_rows=number,institutions=len(directory))
        if number % 1000 == 0:
            store.task.check_memory()
        if allowed is not None and source not in allowed:
            continue
        reported = json.loads(payload).get("institution_name")
        if (institution not in canonical_directory and reported
                and (institution not in label_ids or event_id<label_ids[institution])):
            directory[institution] = (reported, country, {"identity_status":"reported-label-unresolved"})
            label_ids[institution]=event_id
    del canonical_directory,label_ids
    # Observed event endpoints belong in directory even when they have no qualifying edge.
    directory_ids = set()
    report('institution_directory')
    for number, row in enumerate(store.db.execute("SELECT DISTINCT COALESCE(a.institution,e.institution),e.source FROM events e LEFT JOIN active_institution_aliases a ON a.alias=e.institution WHERE e.institution IS NOT NULL"),1):
        if allowed is None or row[1] in allowed:
            directory_ids.add(row[0])
        if number % 1000 == 0:
            store.task.check_memory()
    return directory,directory_ids


def _coverage_counts(store, allowed, report=None):
    """Exact scalar counts without population-sized GROUP BY temporary trees."""
    where='' if allowed is None else ' WHERE source IN ('+','.join('?' for _ in allowed)+')'
    params=() if allowed is None else tuple(sorted(allowed))
    statuses=Counter();countries=set();events=people=0
    def progress(source,number):
        store.task.check_memory()
        if report:report('coverage_counts',source=source,scanned_rows=number)
    def scalar_cursor(sql):
        # Only country/status/person columns: none uses a payload, URL or
        # digest codec. Bypass the generic per-column decoding checks while
        # preserving the connection's configured reader for other queries.
        cursor=store.db.cursor();cursor.row_factory=None
        try:return cursor.execute(sql,params)
        except BaseException:
            cursor.close();raise
    if report:report('coverage_counts',source='event-status-and-countries',scanned_rows=0)
    cursor=scalar_cursor('SELECT country,status FROM events NOT INDEXED'+where)
    try:
        for country,status in cursor:
            events+=1;statuses[status]+=1
            if country is not None and (allowed is None or country!=''):countries.add(country)
            if events%1000000==0:progress('event-status-and-countries',events)
    finally:cursor.close()
    if report:report('coverage_counts',source='unique-people',scanned_rows=0)
    cursor=scalar_cursor('SELECT person FROM events INDEXED BY events_person'+where+' ORDER BY person')
    previous=None
    try:
        for number,(person,) in enumerate(cursor,1):
            if person is not None and person!=previous:people+=1
            previous=person
            if number%1000000==0:progress('unique-people',number)
    finally:cursor.close()
    members=Counter();cursor=scalar_cursor('SELECT status FROM members'+where)
    if report:report('coverage_counts',source='member-status',scanned_rows=0)
    try:
        for number,(status,) in enumerate(cursor,1):
            members[status]+=1
            if number%1000000==0:progress('member-status',number)
    finally:cursor.close()
    return {'people':people,'events':events,'countries':sorted(countries),
            'event_status':dict(statuses),'member_status':dict(members)}


def _scenario_summary(universe, scores, converged, *, edges, available_edges, new_endpoints, boundary):
    return {'converged':converged,'edges':edges,'available_edges':available_edges,
            'filtered_boundary_edges':available_edges-edges,'new_endpoints':new_endpoints,
            'universe_kind':boundary,'node_count':len(universe),'probability_mass':math.fsum(scores.values()),
            'prior_only':not bool(edges),
            'ranks':{node:i+1 for i,node in enumerate(sorted(scores,key=lambda n:(-scores[n],n)))}}


def scenario_result(store, nodes, changed_edges, alpha=.85, personalization=None):
    """Authoritative fixed/expanded-universe scenario calculation for full and
    incrementally restored graphs. No source or evidence policy lives here.
    """
    store.task.check_memory();reference_nodes=set(nodes)
    positive_edges = (changed_edges if all(weight>0 for _,weight in changed_edges.items())
                      else {edge:weight for edge,weight in changed_edges.items() if weight > 0})
    new_endpoints=sorted({endpoint for edge in positive_edges for endpoint in edge}-reference_nodes)
    fixed_edges=(positive_edges if not new_endpoints else
                 positive_edges.restrict_to(reference_nodes) if hasattr(positive_edges,'restrict_to') else
                 {edge:weight for edge,weight in positive_edges.items() if edge[0] in reference_nodes and edge[1] in reference_nodes})
    def evaluate(universe, valid_edges, boundary):
        scores,conv=_rank_graph(store,universe,valid_edges,alpha,1e-12,1000,personalization)
        return _scenario_summary(universe,scores,conv['converged'],edges=len(valid_edges),available_edges=len(positive_edges),
                                 new_endpoints=new_endpoints,boundary=boundary)
    fixed=evaluate(nodes,fixed_edges,'fixed-reference-universe')
    expanded=(evaluate(sorted(reference_nodes|set(new_endpoints)),positive_edges,'reference-plus-new-endpoints')
              if new_endpoints else None)
    return fixed,expanded


def _institution_node_row(node, directory, network, order, intensity, denominators,
                          counts, source_counts, corroborated, independent_origins, scenarios):
    """Shared full/incremental institution-row semantics."""
    name,country,meta=directory.get(node,(node,None,{}))
    reasons=[]
    if corroborated[node]<10:reasons.append('insufficient_independently_corroborated_relations')
    if len(independent_origins.get(node,()))<2:reasons.append('insufficient_independent_source_classes')
    if node not in directory or meta.get('identity_status')=='reported-label-unresolved':reasons.append('institution_crosswalk_pending')
    reasons.append('external_dual_review_pending')
    return {'institution_id':node,'name':name,'country':country,'region':meta.get('region'),
            'institution_type':meta.get('type'),'observed_rank':order.get(node),'formal_rank':None,
            'network_score':network.get(node),'award_score':None,'composite_score':None,
            'network_intensity_score':intensity.get(node),'observed_training_denominator':denominators[node] or None,
            'award_status':'missing_official_award_data','status':'observed_only' if node in network else 'unranked_no_qualifying_relations',
            'unranked_reasons':reasons,'relations':counts[node],'source_classes':sorted(source_counts.get(node,())),
            'independent_source_classes':sorted(independent_origins.get(node,())),
            'corroborated_relations':corroborated[node],'traceability':1.0 if counts[node] else None,
            'rank_range':[min(s['ranks'][node] for s in scenarios.values() if not s['prior_only']),
                          max(s['ranks'][node] for s in scenarios.values() if not s['prior_only'])] if node in network else None}


def _verify_prior_label_witness(store, witness, node, label, events):
    changed=[e for e in events if e['payload'].get('institution')==node
             and e['payload'].get('institution_name')
             and [e['payload']['institution_name'],e['payload'].get('country')]!=list(label)]
    if not changed:return
    if (not witness or not witness.get('prior_nonempty_label_before_all_delta_ids')
            or not witness['witness_event_id']<min(e['id'] for e in changed)):
        raise ValueError('directory label winner needs bounded evidence')
    previous=store.db.execute('SELECT institution,payload FROM events WHERE id=?',(witness['witness_event_id'],)).fetchone()
    value=json.loads(previous[1]) if previous else {}
    if (not previous or previous[0]!=node or not value.get('institution_name')
            or [value.get('institution_name'),value.get('country')]!=list(label)
            or witness['witness_event_id'] in {e['id'] for e in events}):
        raise ValueError('directory label witness does not match retained event')


def _verify_new_label_witness(store, witness, node, label, events, manifest_sha256):
    candidates=[e for e in events if e['payload'].get('institution')==node and e['payload'].get('institution_name')]
    first=min(candidates,key=lambda e:e['id']) if candidates else None
    info=(store.task.root/'careers.sqlite').stat()
    if (not first or witness.get('witness_event_id')!=first['id']
            or witness.get('predecessor_manifest_sha256')!=manifest_sha256
            or witness.get('prior_label')!=list(label)
            or witness.get('database_identity')!={'bytes':info.st_size,'mtime_ns':info.st_mtime_ns}
            or witness.get('scanned_through_rowid')!=store.db.execute('SELECT max(rowid) FROM events').fetchone()[0]
            or witness.get('minimum_delta_id')!=first['id']
            or witness.get('prior_label_absent_below_delta') is not True):
        raise ValueError('directory new label winner lacks complete predecessor evidence')
    return first['payload']['institution_name'],first['payload'].get('country')


def complete_unresolved_additions(store, *, events, after_rowid, previous_directory,
                                  previous_manifest_sha256, asset_reader=None,label_witnesses=()):
    """Append only new unresolved, non-training people to a pinned full result.

    This narrow path preserves every graph, score and scenario byte value.
    The caller holds writer exclusion and binds input before/after export.
    Conflicting existing raw labels require the ordinary evidence route.
    """
    from .aggregate_partitions import read_field
    from .aggregate_chunks import AggregateRows
    if type(after_rowid) is not int or after_rowid<0 or not 1<=len(events)<=1000:
        raise ValueError('invalid unresolved addition boundary')
    rows=list(store.db.execute('SELECT rowid,id,payload FROM events WHERE rowid>? ORDER BY rowid LIMIT 1001',(after_rowid,)))
    actual=[{'id':eid,'payload':json.loads(text)} for _,eid,text in rows]
    if (actual!=events or [r[0] for r in rows]!=list(range(after_rowid+1,after_rowid+1+len(events)))
            or len({e['id'] for e in events})!=len(events)):
        raise ValueError('unresolved additions differ from committed append')
    if any(e['payload'].get('source')!='orcid-public-api' or e['payload'].get('status')!='unresolved'
           or training_observation_eligible(e['payload'].get('kind'),e['payload'].get('role'),
                 e['payload'].get('year_start'),e['payload'].get('year_end')) for e in events):
        raise ValueError('unresolved additions could change graph or denominator')
    people=sorted({e['payload']['person'] for e in events})
    for person in people:
        if store.db.execute('SELECT 1 FROM events WHERE person=? AND rowid<=? LIMIT 1',(person,after_rowid)).fetchone():
            raise ValueError('unresolved addition person overlaps predecessor')
    if list(relation_rows(store,persons=people,allowed_sources={'orcid-public-api'})):
        raise ValueError('unresolved additions produced a qualifying relation')
    load=lambda name:read_field(previous_directory,name,manifest_sha256=previous_manifest_sha256,asset_reader=asset_reader)
    manifest=json.loads((Path(previous_directory)/'manifest.json').read_bytes())
    metadata={f['field']:load(f['field']) for f in manifest['fields'] if f['kind']!='rows'}
    if metadata['coverage']['events']!=after_rowid or metadata['algorithm']['config']!=DEFAULT_CONFIG:
        raise ValueError('unresolved predecessor coverage or method mismatch')
    total_members=store.db.execute('SELECT COALESCE(sum(members),0) FROM runs').fetchone()[0]
    if total_members!=sum(metadata['coverage']['member_status'].values())+len(people):
        raise ValueError('unresolved additions do not bind new member counts')
    affected={e['payload']['institution'] for e in events if e['payload'].get('institution')}
    new_directory={}
    for node in affected:
        alias=store.db.execute('SELECT institution FROM active_institution_aliases WHERE alias=?',(node,)).fetchone()
        if alias and alias[0]!=node:raise ValueError('unresolved alias requires ordinary incremental route')
        entity=store.db.execute('SELECT name,country,payload FROM entities WHERE id=?',(node,)).fetchone()
        if entity:new_directory[node]=(entity[0],entity[1],json.loads(entity[2]))
        else:
            candidates=[e for e in events if e['payload'].get('institution')==node and e['payload'].get('institution_name')]
            if candidates:
                first=min(candidates,key=lambda e:e['id'])['payload']
                new_directory[node]=(first['institution_name'],first.get('country'),{'identity_status':'reported-label-unresolved'})
    seen=set();label_updates={}
    for row in load('nodes'):
        node=row['institution_id']
        if node not in affected:continue
        seen.add(node)
        if not store.db.execute('SELECT 1 FROM entities WHERE id=?',(node,)).fetchone():
            witness=next((w for w in label_witnesses if w['institution']==node),None)
            if witness and witness.get('delta_label_precedes_prior_winner'):
                label_updates[node]=_verify_new_label_witness(store,witness,node,[row['name'],row.get('country')],events,previous_manifest_sha256)
            else:_verify_prior_label_witness(store,witness,node,[row['name'],row.get('country')],events)
    new_ids=sorted(affected-seen)
    def nodes():
        missing=iter(new_ids);n=next(missing,None)
        def fresh(node):
            return _institution_node_row(node,new_directory,{}, {}, {},Counter(),Counter(),{},Counter(),{},metadata['sensitivity'])
        for row in load('nodes'):
            while n is not None and n<row['institution_id']:
                yield fresh(n);n=next(missing,None)
            if row['institution_id'] in label_updates:row['name'],row['country']=label_updates[row['institution_id']]
            yield row
        while n is not None:
            yield fresh(n);n=next(missing,None)
    coverage=metadata['coverage'];coverage['people']+=len(people);coverage['events']+=len(events)
    coverage['event_status']['unresolved']=coverage['event_status'].get('unresolved',0)+len(events)
    coverage['member_status']['observed']+=len(people)
    coverage['countries']=sorted(set(coverage['countries'])|{e['payload']['country'] for e in events if e['payload'].get('country') is not None})
    metadata.update(publication_status='local-only-source-license-review',
        nodes=AggregateRows(manifest['nodes']+len(new_ids),nodes),
        edges=AggregateRows(manifest['edges'],lambda:load('edges')),acquisition_progress=store.candidate_progress())
    return metadata


def complete_additive_result(store, *, delta, base_scores, previous_directory, previous_manifest_sha256,
                             label_witnesses=(), progress=None):
    """Complete the narrow append-only recovery contract without a population
    scan. The caller pins delta/base-score bytes and holds writer exclusion.
    Every recovered person must have only the frozen new API events, and the
    predecessor graph must have annual-only support. Other updates are refused.
    Returned rows stream the retained aggregate; no previous output is mutated.
    """
    from .analysis_checkpoint import GraphCheckpoint,AdditiveGraph,TrainingDenominatorCheckpoint
    from .aggregate_partitions import read_field
    from .aggregate_chunks import AggregateRows
    def report(phase,**values):
        store.task.check_memory()
        if progress:progress({'phase':phase,**values})
    if (delta.get('schema_version')!='gapi-private-additive-input/1'
            or base_scores.get('delta_sha256')!=digest(delta)
            or base_scores.get('complete_aggregate') is not False):raise ValueError('unbound additive inputs')
    people=tuple(delta['people'])
    if not 1<=len(people)<=1000 or len(set(people))!=len(people):raise ValueError('invalid additive people')
    current=[]
    for person in people:
        current.extend({'id':eid,'payload':json.loads(text)} for eid,text in store.db.execute(
            'SELECT id,payload FROM events WHERE person=? ORDER BY id',(person,)))
    if sorted(current,key=lambda e:e['id'])!=sorted(delta['events'],key=lambda e:e['id']):raise ValueError('additive person input drift')
    if any(e['payload']['source']!='orcid-public-api' for e in current):raise ValueError('additive source overlap')
    # Coverage people are distinct event subjects, not successfully fetched
    # members: a valid empty API record contributes one member and zero people.
    expected_people=len({e['payload']['person'] for e in current})
    expected_status=Counter(e['payload']['status'] for e in current)
    if (type(delta.get('people_delta')) is not int or delta['people_delta']!=expected_people
            or type(delta.get('event_delta')) is not int or delta['event_delta']!=len(current)
            or delta.get('observed_event_delta')!=dict(expected_status)
            or any(type(n) is not int for n in delta.get('observed_event_delta',{}).values())
            or type(delta.get('member_delta')) is not int
            or delta['member_delta']!=len(delta['recovered_member_ledger'])
            or len({row[0] for row in delta['recovered_member_ledger']})!=delta['member_delta']):
        raise ValueError('additive coverage delta differs from committed input')
    for row in delta['recovered_member_ledger']:
        if list(store.db.execute("SELECT locator,sha256,status,reason FROM members WHERE source='orcid-public-api' AND locator=?",(row[0],)).fetchone() or ())!=row:
            raise ValueError('additive member drift')
    cp=GraphCheckpoint(store,None);path=cp.path.with_suffix('.compact')
    if hashlib.sha256(path.read_bytes()).hexdigest()!=delta['predecessor_files'][path.name]:raise ValueError('additive graph predecessor drift')
    state=cp._load_compact(path,expected_binding=delta['predecessor_binding'])
    if any(set(v)-{'orcid-2026-summary'} for v in state['source_counts'].values()):raise ValueError('mixed-source predecessor requires explicit subtraction')
    # API deletion can reuse the old graph only if all current API relations
    # are exactly the frozen additions; this query visits API people, not a census.
    from .orcid_stream import private_person
    api_people=tuple(private_person(r[0]) for r in store.db.execute("SELECT locator FROM members WHERE source='orcid-public-api' AND status='observed'"))
    actual=list(relation_rows(store,persons=api_people,allowed_sources={'orcid-public-api'}))
    expected=[(tuple(r['key']),r['training'],r['job']) for r in delta['relations']]
    if sorted(actual,key=lambda r:r[0])!=sorted(expected,key=lambda r:r[0]):raise ValueError('additional API relations outside frozen additions')
    add=defaultdict(float);recent_add=defaultdict(float);stage_add=defaultdict(lambda:defaultdict(float));new_counts=Counter();new_sources=defaultdict(set)
    for key,train,job in expected:
        a,b,stage=key;w=DEFAULT_CONFIG['stage_weights'][stage];add[(a,b)]+=w;stage_add[stage][(a,b)]+=w
        if job['year_start']>=2016:recent_add[(a,b)]+=w
        new_counts[b]+=1;new_sources[b].update(train['supporting_sources']);new_sources[b].update(job['supporting_sources'])
    graph=AdditiveGraph(state['edges'],dict(add));nodes=base_scores['nodes']
    if sorted(set(state['edges'].nodes)|{n for edge in add for n in edge})!=nodes or len(graph)!=base_scores['edges']:
        raise ValueError('base-score graph identity mismatch')
    load=lambda name:read_field(previous_directory,name,manifest_sha256=previous_manifest_sha256)
    metadata={f['field']:load(f['field']) for f in json.loads((Path(previous_directory)/'manifest.json').read_bytes())['fields']
              if f['kind']=='value'}
    if (metadata['algorithm']['config']!=DEFAULT_CONFIG
            or metadata['algorithm']['network_universe']!=sorted(state['edges'].nodes)):
        raise ValueError('predecessor method or universe drift')
    metadata['algorithm']['network_universe']=nodes
    den=TrainingDenominatorCheckpoint(store,None)
    if hashlib.sha256(den.path.read_bytes()).hexdigest()!=delta['predecessor_files'][den.path.name]:raise ValueError('denominator predecessor drift')
    denominators=den._read(den.path,delta['predecessor_binding'])
    for _,institution in delta['training_person_institution_pairs']:denominators[institution]+=1
    # Existing raw-institution labels stay authoritative only when the new
    # label agrees, is canonical, or an earlier nonempty event wins by ID.
    raw_affected={e['payload']['institution'] for e in current if e['payload'].get('institution')}
    aliases={raw:(store.db.execute('SELECT institution FROM active_institution_aliases WHERE alias=?',(raw,)).fetchone() or (raw,))[0]
             for raw in raw_affected}
    affected=set(aliases.values());seen=set();regions={};label_updates={}
    witness={w['institution']:w for w in label_witnesses}
    for row in load('nodes'):
        node=row['institution_id']
        if node in base_scores['scores']:regions[node]=row.get('region') or 'unknown'
        if node not in affected:continue
        seen.add(node);canonical_entity=store.db.execute('SELECT 1 FROM entities WHERE id=?',(node,)).fetchone()
        w=witness.get(node)
        if not canonical_entity and w and w.get('delta_label_precedes_prior_winner'):
            # The frozen predecessor already binds its own minimum-ID label.
            # A complete search found no event backing that label before the
            # new minimum, so this exact committed new event now wins.
            label_updates[node]=_verify_new_label_witness(store,w,node,[row['name'],row.get('country')],current,previous_manifest_sha256)
            continue
        if not canonical_entity:
            _verify_prior_label_witness(store,witness.get(node),node,[row['name'],row.get('country')],current)
    new_directory={}
    for node in sorted(affected-seen):
        # The verified predecessor includes every prior event institution.
        # A new raw identity may be introduced only by these exact new events.
        # Aliased identities use the canonical entity, never a guessed label.
        entity=store.db.execute('SELECT name,country,payload FROM entities WHERE id=?',(node,)).fetchone()
        if entity:new_directory[node]=(entity[0],entity[1],json.loads(entity[2]))
        else:
            candidates=[e for e in current if e['payload'].get('institution')==node and e['payload'].get('institution_name')]
            if candidates:
                first=min(candidates,key=lambda e:e['id'])['payload']
                new_directory[node]=(first['institution_name'],first.get('country'),{'identity_status':'reported-label-unresolved'})
            elif node not in raw_affected:
                raise ValueError('new aliased institution lacks authoritative entity')
        if node in base_scores['scores']:regions[node]=(new_directory.get(node,(None,None,{}))[2].get('region') or 'unknown')
    new_ids=sorted(affected-seen)
    scenarios={};expanded={}
    def scenario(label,edges,alpha=.85,personalization=None):
        report('sensitivity',scenario=label)
        fixed,extra=scenario_result(store,nodes,edges,alpha,personalization);scenarios[label]=fixed
        if extra is not None:expanded[label]=extra
        if not fixed['converged'] or abs(fixed['probability_mass']-1)>1e-10:raise ValueError('incremental scenario failed')
    scenario('alpha_0.65',graph,.65)
    scenarios['alpha_0.85']=_scenario_summary(nodes,base_scores['scores'],base_scores['convergence']['converged'],
        edges=len(graph),available_edges=len(graph),new_endpoints=[],boundary='fixed-reference-universe')
    scenario('alpha_0.95',graph,.95)
    for stage in DEFAULT_CONFIG['stage_weights']:
        weights=dict(sorted((pair,values[stage]) for pair,values in state['stages'].items() if values.get(stage,0)>0))
        scenario('stage_only_'+stage,AdditiveGraph(weights,dict(stage_add[stage])))
        del weights
    scenario('delete_source_orcid-2026-summary',dict(sorted(add.items())))
    scenario('delete_source_orcid-public-api',AdditiveGraph(state['edges'],{}))
    scenario('employment_since_2016',AdditiveGraph(state['recent_edges'],dict(recent_add)))
    groups=Counter(regions.values())
    scenario('region_balanced_prior',graph,personalization={n:1/groups[regions[n]] for n in nodes})
    del regions,groups
    report('intensity')
    intensity,conv=_rank_intensity(store,nodes,graph,denominators)
    if not conv['converged'] or abs(math.fsum(intensity.values())-1)>1e-10:raise ValueError('incremental intensity failed')
    def node_rows():
        missing=iter(new_ids);next_missing=next(missing,None)
        counts=Counter(state['counts']);counts.update(new_counts)
        sources={n:set(v) for n,v in state['source_counts'].items()}
        for n,values in new_sources.items():sources.setdefault(n,set()).update(values)
        def fresh(node):
            return _institution_node_row(node,new_directory,base_scores['scores'],base_scores['ranks'],intensity,
                denominators,counts,sources,Counter(state['corroborated']),state['independent_origins'],scenarios)
        for row in load('nodes'):
            while next_missing is not None and next_missing<row['institution_id']:
                yield fresh(next_missing);next_missing=next(missing,None)
            n=row['institution_id'];row.update(network_score=base_scores['scores'].get(n),
                observed_rank=base_scores['ranks'].get(n),network_intensity_score=intensity.get(n),
                observed_training_denominator=denominators[n] or None,
                status='observed_only' if n in base_scores['scores'] else 'unranked_no_qualifying_relations')
            if n in new_counts:
                row['relations']+=new_counts[n];row['source_classes']=sorted(set(row['source_classes'])|new_sources[n]);row['traceability']=1.0
            if n in label_updates:row['name'],row['country']=label_updates[n]
            row['rank_range']=([min(s['ranks'][n] for s in scenarios.values() if not s['prior_only']),
                                max(s['ranks'][n] for s in scenarios.values() if not s['prior_only'])] if n in base_scores['scores'] else None)
            yield row
        while next_missing is not None:
            yield fresh(next_missing);next_missing=next(missing,None)
    def edge_rows():
        remaining=dict(add)
        for row in load('edges'):
            pair=(row['source'],row['target'])
            if pair in remaining:
                row['weight']+=remaining.pop(pair)
                for stage,values in stage_add.items():
                    if pair in values:row['stages'][stage]=row['stages'].get(stage,0)+values[pair]
            yield row
        for pair,w in sorted(remaining.items()):yield {'source':pair[0],'target':pair[1],'weight':w,
            'stages':{stage:values[pair] for stage,values in stage_add.items() if pair in values}}
    coverage=metadata['coverage'];coverage['events']+=delta['event_delta'];coverage['people']+=delta['people_delta']
    coverage['member_status']['observed']+=delta['member_delta']
    for status,n in delta['observed_event_delta'].items():coverage['event_status'][status]=coverage['event_status'].get(status,0)+n
    coverage['countries']=sorted(set(coverage['countries'])|{e['payload']['country'] for e in current if e['payload'].get('country') is not None})
    metadata.update(nodes=AggregateRows(json.loads((Path(previous_directory)/'manifest.json').read_bytes())['nodes']+len(new_ids),node_rows),
                    edges=AggregateRows(len(graph),edge_rows),sensitivity=scenarios,expanded_universe_sensitivity=expanded,
                    convergence=base_scores['convergence'],acquisition_progress=store.candidate_progress())
    report('complete',nodes=len(metadata['nodes']),edges=len(graph))
    return metadata


def rank_global(store, *, public=False, publication_policy=None, checkpoint=False, progress=None, stream_rows=False):
    if type(stream_rows) is not bool:raise ValueError('stream_rows must be boolean')
    started=time.monotonic()
    def report(phase, **counts):
        if progress is not None:
            progress({'phase':phase,'elapsed_seconds':round(time.monotonic()-started,3),**counts})
    # This calculation reads SQLite and builds bounded in-memory graphs. It
    # does not create a 64 MiB disk workspace. Admit actual exports only once
    # their byte size is known; keep disk reserve and memory checks separate.
    store.task.check()
    store.task.check_memory()
    allowed = public_sources(store.task,publication_policy) if public else None
    edges, stages, counts, source_counts = defaultdict(float), defaultdict(Counter), Counter(), defaultdict(set)
    corroborated = Counter()
    independent_origins = defaultdict(set)
    recent_edges = defaultdict(float)
    relation_count = 0
    resume = None
    if checkpoint:
        report('checkpoint_load')
        from .analysis_checkpoint import GraphCheckpoint
        resume = GraphCheckpoint(store,allowed)
        saved = resume.load()
        if saved is not None:
            if hasattr(saved['edges'],'packed_rows'):
                edges=saved.pop('edges'); stages=saved.pop('stages'); recent_edges=saved.pop('recent_edges')
                counts.update(saved.pop('counts'))
                source_counts.update((key,set(value)) for key,value in saved.pop('source_counts').items())
                corroborated.update(saved.pop('corroborated'))
                independent_origins.update((key,set(value)) for key,value in saved.pop('independent_origins').items())
            else:
                edges.update((tuple(key),value) for key,value in saved.pop('edges'))
                stages.update((tuple(key),Counter(value)) for key,value in saved.pop('stages'))
                counts.update(saved.pop('counts'))
                source_counts.update((key,set(value)) for key,value in saved.pop('source_counts').items())
                corroborated.update(saved.pop('corroborated'))
                independent_origins.update((key,set(value)) for key,value in saved.pop('independent_origins').items())
                recent_edges.update((tuple(key),value) for key,value in saved.pop('recent_edges'))
            relation_count = saved['relation_count']
            del saved
            store.task.check_memory()
    def graph_state():
        return {'edges':list(edges.items()),'stages':list(stages.items()),'counts':dict(counts),
                'source_counts':{key:sorted(value) for key,value in source_counts.items()},
                'corroborated':dict(corroborated),
                'independent_origins':{key:sorted(value) for key,value in independent_origins.items()},
                'recent_edges':list(recent_edges.items()),'relation_count':relation_count}
    def person_complete(person):
        if resume is not None:
            resume.boundary(person,graph_state)
    options = ({'after_person':resume.after_person,'person_complete':person_complete}
               if resume is not None else {})
    relations = (() if resume is not None and resume.complete else
                 relation_rows(store,allowed_sources=allowed,**options))
    report('relations',resumed_complete=bool(resume is not None and resume.complete))
    for relation in relations:
        relation_count += 1
        _accumulate_relation(relation,edges,stages,counts,source_counts,corroborated,independent_origins,recent_edges)
        if relation_count % 1000 == 0:
            store.task.check_memory()
    if resume is not None:
        resume.finish(graph_state)
    from .analysis_checkpoint import TrainingDenominatorCheckpoint
    denominator_checkpoint=TrainingDenominatorCheckpoint(store,allowed) if checkpoint else None
    denominators=denominator_checkpoint.load() if denominator_checkpoint else None
    if denominators is None:
        denominators = _training_denominators(store,allowed,report)
        if denominator_checkpoint:denominator_checkpoint.save(denominators)
    else:report('training_denominators',resumed_complete=True,institutions=len(denominators))
    nodes = sorted({endpoint for edge in edges for endpoint in edge})
    report('base_pagerank',nodes=len(nodes),edges=len(edges),relations=relation_count)
    network, convergence = _rank_graph(store, nodes, edges, .85, 1e-12, 200)
    from .analysis_checkpoint import InstitutionDirectoryCheckpoint
    directory_checkpoint=InstitutionDirectoryCheckpoint(store,allowed) if checkpoint else None
    saved_directory=directory_checkpoint.load() if directory_checkpoint else None
    if saved_directory is None:
        directory,directory_ids=_institution_directory(store,allowed,report)
        if directory_checkpoint:directory_checkpoint.save(directory,directory_ids)
    else:
        directory,directory_ids=saved_directory
        report('institution_directory',resumed_complete=True,institutions=len(directory))
    ordering = sorted(network, key=lambda node: (-network[node], node))
    order = {node: i+1 for i, node in enumerate(ordering)}
    scenarios, expanded_scenarios = {}, {}
    def scenario(label, changed_edges, alpha=.85, personalization=None):
        report('sensitivity',scenario=label,edges=len(changed_edges))
        fixed,expanded=scenario_result(store,nodes,changed_edges,alpha,personalization)
        scenarios[label]=fixed
        if expanded is not None:expanded_scenarios[label]=expanded
    for alpha in (.65, .85, .95):
        scenario(f"alpha_{alpha}",edges,alpha)
    for stage in DEFAULT_CONFIG["stage_weights"]:
        scenario("stage_only_"+stage,{edge:values[stage] for edge,values in stages.items() if values[stage]>0})
    available_sources={row[0] for row in store.db.execute('SELECT DISTINCT source FROM events')
                       if allowed is None or row[0] in allowed}
    # A deduplicated relation may have alternative evidence from another source.
    # Subtracting the selected evidence's contribution would erase that support.
    # Replay source filtering BEFORE joining and deduplicating the person's facts.
    for source in sorted(available_sources):
        report('source_deletion_relations',source=source)
        remaining_edges=defaultdict(float)
        for (employer,training,stage),_train,_job in relation_rows(store,allowed_sources=available_sources-{source}):
            remaining_edges[(employer,training)]+=DEFAULT_CONFIG['stage_weights'][stage]
            if len(remaining_edges) % 1000 == 0:
                store.task.check_memory()
        scenario('delete_source_'+source,remaining_edges)
    scenario("employment_since_2016", recent_edges)
    groups = Counter(directory.get(node,(None,None,{}))[2].get("region") or "unknown" for node in nodes)
    if nodes:
        scenario("region_balanced_prior",edges,personalization={node:1/groups[directory.get(node,(None,None,{}))[2].get("region") or "unknown"] for node in nodes})
    report('intensity_pagerank')
    intensity, _ = _rank_intensity(store,nodes,edges,denominators)
    row_ids=sorted(directory_ids | set(nodes))
    def node_rows():
        for number,node in enumerate(row_ids,1):
            if number%1000==0:store.task.check_memory()
            yield _institution_node_row(node,directory,network,order,intensity,denominators,
                counts,source_counts,corroborated,independent_origins,scenarios)
    def edge_rows():
        if hasattr(edges,'packed_rows') and hasattr(stages,'packed_rows'):
            for number,(((source,target),weight),(pair,stage_weights)) in enumerate(zip(edges.items(),stages.items()),1):
                if (source,target)!=pair:raise ValueError('compact edge/stage order mismatch')
                if number%1000==0:store.task.check_memory()
                yield {'source':source,'target':target,'weight':weight,'stages':dict(stage_weights)}
            return
        for number,((source,target),weight) in enumerate(sorted(edges.items()),1):
            if number%1000==0:store.task.check_memory()
            yield {'source':source,'target':target,'weight':weight,'stages':dict(stages[(source,target)])}
    if stream_rows:
        from .aggregate_chunks import AggregateRows
        rows=AggregateRows(len(row_ids),node_rows)
        output_edges=AggregateRows(len(edges),edge_rows)
    else:
        rows=list(node_rows());output_edges=list(edge_rows())
    # Ranking consumes only these aggregate fields. Do not invoke acquisition's
    # full report (which also rebuilds candidate and archive progress) here.
    report('coverage_counts')
    from .analysis_checkpoint import CoverageCheckpoint
    coverage_checkpoint=CoverageCheckpoint(store,allowed) if checkpoint else None
    coverage=coverage_checkpoint.load() if coverage_checkpoint else None
    if coverage is None:
        coverage = _coverage_counts(store,allowed,report)
        if coverage_checkpoint:coverage_checkpoint.save(coverage)
    else:report('coverage_counts',resumed_complete=True)
    result = {"schema_version":"gapi-global-observed/v1","dataset_kind":"real/global-public-observations",
        "publication_status":"public-observations-pending-external-review" if public else "local-only-source-license-review",
        "scope":{"complete_global_census":False,"enumeration_complete":False,"source_scope":"committed, individually hashed records; annual archive excluded until verified"},
        "coverage":coverage,"nodes":rows,
        "edges":output_edges,
        "algorithm":{"method_version":"gapi-observed-network/4", "direction":"employer -> training",
            "config":DEFAULT_CONFIG,"network_universe":nodes,
            "version_boundary":"New method outputs supersede predecessor calculations; historical bytes require their frozen inputs and pinned predecessor code."},
        "convergence":convergence,"sensitivity":scenarios,"expanded_universe_sensitivity":expanded_scenarios,
        "uncertainty":{"kind":"parameter-scenarios-only","unknown_global_missingness_covered":False,
            "rank_range_scope":"fixed-reference-universe; excludes prior-only and expanded-universe scenarios",
            "not_run":["discipline","bootstrap","english_source_deletion","official_profile_loss"],
            "intensity_denominator":"observed training people; not all graduates or job seekers"},
        "limitations":["Observed scores are not employment rates or causal training effects.","Formal ranking and award/composite data are unavailable.","No completed independent official-profile corroboration."]}
    report('evidence_metadata')
    if hasattr(store,'candidate_progress'):
        result['acquisition_progress']=store.candidate_progress(allowed_sources=allowed)
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE name='institution_bridge'").fetchone():
        from .institution_bridge import VERSION as BRIDGE_VERSION, SPEC as BRIDGE_SPEC
        bridge_hash=hashlib.sha256()
        for bridge_row in store.db.execute('SELECT * FROM institution_bridge ORDER BY alias,version'):
            bridge_hash.update(canonical(bridge_row))
        result['algorithm']['institution_identity_projection'] = {
            'version':BRIDGE_VERSION, 'spec_sha256':digest(BRIDGE_SPEC),
            'index_sha256':bridge_hash.hexdigest(),
            'totals':dict(store.db.execute('SELECT status,count(*) FROM institution_bridge WHERE version=? GROUP BY status',(BRIDGE_VERSION,))),
            'scope':'observed Ringgold identifiers only; reciprocal ROR and revision-pinned Wikidata; no personal fact corroboration'}
    from .nobel import public_inventory
    from .corroboration import decisions, VERSION as EVIDENCE_VERSION, POLICY_SHA256 as EVIDENCE_POLICY
    verified = decisions(store, allowed_sources=allowed)
    result['evidence_adjudication'] = {'version': EVIDENCE_VERSION, 'policy_sha256': EVIDENCE_POLICY,
        'corroborated_events': len(verified), 'canonical_events_modified': 0,
        'scope': 'exact supported official profiles plus same-owner ORCID self assertions'}
    if verified:
        result['limitations'][2] = 'Official corroboration is limited to explicitly supported profile layouts and exact self assertions.'
    awards = public_inventory(store)
    if awards is not None:
        result['official_award_inventory'] = awards
        result['limitations'][1] = 'Formal ranking, complete canonical award scoring and composite data are unavailable.'
    from .award_projection import public_projection
    projection = public_projection(store)
    if projection is not None:
        result['official_award_attribution'] = projection
    from .career_normalization import has_interpretations, VERSION, POLICY_SHA256
    if has_interpretations(store):
        count_sql = '''SELECT count(*) FROM event_interpretations i JOIN events e ON e.id=i.event_id
            WHERE i.rule_version=? AND i.policy_sha256=?'''
        count_params = (VERSION, POLICY_SHA256)
        if allowed is not None:
            count_sql += ' AND e.source IN (' + ','.join('?' for _ in allowed) + ')'
            count_params += tuple(sorted(allowed))
        interpreted = store.db.execute(count_sql,count_params).fetchone()[0]
        if interpreted or allowed is None or 'orcid-public-api' in allowed:
            result['algorithm']['normalization_policy'] = {'version': VERSION, 'policy_sha256': POLICY_SHA256,
                'interpreted_events': interpreted, 'canonical_events_modified': 0,
                'confidence': 'observed-only; deterministic interpretations do not corroborate facts'}
    report('aggregate_ready',nodes=len(result['nodes']),edges=len(result['edges']))
    return result


def _write_aggregate(task, target: Path, content: bytes):
    """Publish one aggregate atomically; identical reruns retain bytes and mtime.

    The task writer serializes callers. Exclusive staging never takes ownership
    of a predecessor's unfinished file; failures leave the published file intact.
    """
    if target.is_symlink() or (target.exists() and not target.is_file()):
        raise ValueError('aggregate target must be a regular file')
    if target.exists() and target.stat().st_size == len(content):
        with target.open('rb') as source:
            if hashlib.file_digest(source, 'sha256').digest() == hashlib.sha256(content).digest():
                return
    # used() already includes the published predecessor. The only additional
    # filesystem allocation is this exclusive temporary file, then replacement.
    task.check(len(content) + 65536)
    temporary = target.with_name(target.name + '.tmp')
    created = False
    try:
        with temporary.open('xb') as destination:
            created = True
            destination.write(content)
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, target)
    finally:
        if created and temporary.exists():
            temporary.unlink()


def _write_public_snapshot(task, out, filename, content, *, compress=False):
    """Store a bounded aggregate; web builds still serve its exact decoded JSON."""
    if type(compress) is not bool:raise ValueError('snapshot compression must be boolean')
    if not compress:
        _write_aggregate(task,out/filename,content)
        return None
    if not 0<len(content)<=32*1024**2:raise ValueError('public snapshot exceeds 32 MiB bound')
    buffer=io.BytesIO()
    with gzip.GzipFile(filename='',mode='wb',fileobj=buffer,mtime=0,compresslevel=9) as stream:
        stream.write(content)
    packed=buffer.getvalue()
    task.check_memory()
    metadata={'encoding':'gzip','filename':filename+'.gz','sha256':hashlib.sha256(packed).hexdigest(),
        'bytes':len(packed),'decoded_bytes':len(content)}
    _write_aggregate(task,out/metadata['filename'],packed)
    return metadata


def export_release(store, out: Path, *, publication_policy=None, frozen_input=None, compress_snapshot=False, compact_nodes=False):
    """Public, licensed institution aggregates only; no individual trajectories."""
    if out.resolve() != (project_root()/"outputs/global").resolve():
        raise ValueError("public release output must use the budget-accounted outputs/global directory")
    store.task.check()
    store.task.check_memory()
    result = rank_global(store, public=True,publication_policy=publication_policy)
    # Failing this gate prevents a misleading blank real-score release.
    if not result["edges"]:
        raise ValueError("no licensed, validated real relations available for public score release")
    if not result["convergence"]["converged"]:
        raise ValueError("non-converged scoring output cannot be released")
    out.mkdir(parents=True, exist_ok=True)
    if type(compact_nodes) is not bool:
        raise ValueError('compact_nodes must be boolean')
    logical_checksum = hashlib.sha256(canonical(result)).hexdigest()
    if compact_nodes:
        from .public_transport import pack
        result = pack(result)
    content = canonical(result)
    store.task.check_memory()
    checksum = hashlib.sha256(content).hexdigest()
    filename = f"global-{checksum[:16]}.json"
    repository_snapshot=_write_public_snapshot(store.task,out,filename,content,compress=compress_snapshot)
    allowed = public_sources(store.task,publication_policy)
    inputs = {row[0]:row[1] for row in store.db.execute("SELECT source,identity FROM runs WHERE members>0 ORDER BY source") if row[0] in allowed}
    member_hash = hashlib.sha256()
    for row in store.db.execute("SELECT source,locator,sha256,status FROM members ORDER BY source,locator"):
        if row[0] in allowed:
            member_hash.update(canonical(row))
    events_hash = hashlib.sha256()
    for row in store.db.execute('SELECT id,institution,status,payload,source FROM events ORDER BY id'):
        if row[-1] in allowed:
            events_hash.update(canonical(row))
    aliases_hash=hashlib.sha256()
    for alias_row in store.db.execute('SELECT alias,institution,provenance FROM institution_aliases ORDER BY alias'):
        aliases_hash.update(canonical(alias_row))
    alias_issues_hash=hashlib.sha256()
    for issue_row in store.db.execute('SELECT * FROM institution_alias_issues ORDER BY alias,policy'):
        alias_issues_hash.update(canonical(issue_row))
    manifest = {"schema_version":"gapi-public-release/v1", "snapshot":filename, "output_sha256":checksum,
        "sources":inputs,"member_index_sha256":member_hash.hexdigest(),"normalized_event_index_sha256":events_hash.hexdigest(),"config_sha256":digest(store.task.config),
        "institution_alias_index_sha256":aliases_hash.hexdigest(),
        "institution_alias_issue_index_sha256":alias_issues_hash.hexdigest(),
        "code_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=project_root(),text=True).strip(),
        "code_tree_sha256":digest({str(p.relative_to(project_root())).replace('\\','/'):hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
            for p in sorted((project_root()/'src/gapi').glob('*.py'))}),
        "environment":{"python":platform.python_version()},"review_status":"pending-independent-dual-track",
        "publication_status":result.get('payload',result)["publication_status"]}
    if compact_nodes:
        manifest['logical_output_sha256'] = logical_checksum
        manifest['transport'] = result['schema_version']
    if publication_policy is not None:
        manifest['publication_policy']=publication_policy
        manifest['publication_policy_sha256']=digest(publication_policy)
    if frozen_input is not None:
        manifest['private_normalized_input']=frozen_input
    if repository_snapshot is not None:
        manifest['repository_snapshot']=repository_snapshot
    logical_result = result.get('payload',result)
    if 'official_award_inventory' in logical_result:
        awards = logical_result['official_award_inventory']
        manifest['official_award_source_sha256'] = awards['source_sha256']
        manifest['normalized_award_index_sha256'] = awards['normalized_award_index_sha256']
        manifest['official_award_page_index_sha256'] = awards['page_index_sha256']
    # Repository checksums name the bytes actually retained on disk. The
    # decoded browser checksum remains output_sha256 in the manifest.
    retained_sha=repository_snapshot['sha256'] if repository_snapshot else checksum
    retained_name=repository_snapshot['filename'] if repository_snapshot else filename
    _write_aggregate(store.task, out/"SHA256SUMS", f"{retained_sha}  {retained_name}\n".encode('ascii'))
    # The manifest is the final reader-visible pointer to an already settled file.
    _write_aggregate(store.task, out/"manifest.json", canonical(manifest))
    return manifest


def export_local(store, *, compress_snapshot=False, chunked=False, progress=None):
    """Local aggregate view can include noncommercial API data, never public by default."""
    if type(chunked) is not bool:
        raise ValueError('chunked must be boolean')
    result = rank_global(store,checkpoint=chunked,progress=progress,stream_rows=chunked)
    if chunked:
        if progress is not None:progress({'phase':'chunk_write'})
        from .aggregate_chunks import write
        manifest=write(store.task,result,store.task.root/'global-local-chunks')
        return {'manifest':manifest,'nodes':len(result['nodes']),
                'relations':len(result['edges']),'convergence':result['convergence']}
    content = canonical(result)
    store.task.check_memory()
    checksum = hashlib.sha256(content).hexdigest()
    path = store.task.root / "global-local.json"
    storage = _write_public_snapshot(store.task, path.parent, path.name, content, compress=compress_snapshot)
    manifest = {"schema_version":"gapi-local-view/v1", "snapshot":"global-local.json", "output_sha256":checksum,
        "publication_status":"local-only-source-license-review", "config_sha256":digest(store.task.config)}
    if storage is not None:
        manifest['repository_snapshot'] = storage
    _write_aggregate(store.task, path.with_suffix(".json.manifest.json"), canonical(manifest))
    return {"manifest":manifest,"nodes":len(result["nodes"]),"relations":len(result["edges"]),"convergence":result["convergence"]}


def export_parquet(store, target: Path):
    """Institution-level Parquet using DuckDB with memory cap and disabled spill."""
    import duckdb
    store.task.check(64*1024**2)
    result = rank_global(store, public=True)
    if target.resolve().parent != store.task.root:
        raise ValueError("Parquet output must remain in the accounted external work directory")
    with duckdb.connect(":memory:") as db:
        db.execute("SET memory_limit='128MB'")
        db.execute("SET threads=1")
        db.execute("SET max_temp_directory_size='0B'")
        db.execute("CREATE TABLE scores(institution_id VARCHAR,network_score DOUBLE,award_score DOUBLE,composite_score DOUBLE)")
        batch = []
        for row in result["nodes"]:
            batch.append((row["institution_id"],row["network_score"],None,None))
            if len(batch) == 1000:
                db.executemany("INSERT INTO scores VALUES(?,?,?,?)",batch)
                batch.clear()
        if batch: db.executemany("INSERT INTO scores VALUES(?,?,?,?)",batch)
        db.execute("COPY scores TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",[str(target)])
    return {"bytes":target.stat().st_size}
