"""Bounded institution-level analysis; privacy and release decisions live here."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import platform
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

from .ranking import DEFAULT_CONFIG, pagerank
from .career_rules import classify
from .storage import project_root
from .task import BudgetStop, canonical, digest
from .publication import public_sources


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


def resolve_institutions(store):
    wanted = set()
    for row in store.db.execute("SELECT DISTINCT institution FROM events WHERE institution IS NOT NULL"):
        wanted.add(row[0])
        if len(wanted) > 200000:
            raise BudgetStop("institution crosswalk exceeds bounded in-memory set")
    directory = project_root() / "data/real/institutions.json.gz"
    resolved, ambiguous = {}, set()
    if directory.exists() and wanted:
        # Verify the preserved input identity before using any crosswalk.
        expected = "959747b1ee6075d5598cc658d5599051714d4e054e93a706e422c43071f07db6"
        with directory.open("rb") as f:
            if hashlib.file_digest(f, "sha256").hexdigest() != expected:
                raise ValueError("ROR normalized directory identity changed")
        for record in ror_records(directory):
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
    def commit_batch(rows):
        with store.member("identity", "ror-crosswalk"):
            for alias, (target, record) in rows:
                store.db.execute("INSERT OR REPLACE INTO institution_aliases VALUES(?,?,?)",(alias,target,expected))
                store.db.execute("INSERT OR REPLACE INTO entities VALUES(?,?,?,?,?)", (target, record["name"], record.get("country_code"), 1,
                    canonical({"region": record.get("geo", {}).get("continent_name"), "type": record.get("type"), "ror": record["ror"]}).decode()))
    for item in resolved.items():
        batch.append(item)
        if len(batch) == 1000:
            commit_batch(batch)
            batch.clear()
    if batch:
        commit_batch(batch)
    return {"resolved_identifiers": len(resolved), "unresolved_identifiers": len(wanted - resolved.keys()),
            "ambiguous_identifiers": sorted(ambiguous), "name_similarity_merges": 0}


def corroborate(store):
    from .corroboration import decisions, VERSION, POLICY_SHA256
    resolution = resolve_institutions(store)
    candidates = store.db.execute('''SELECT count(*) FROM (
        SELECT person,institution,kind,stage,year_start,year_end FROM events
        WHERE institution IS NOT NULL AND stage IS NOT NULL
        GROUP BY person,institution,kind,stage,year_start,year_end HAVING count(DISTINCT source)>1)''').fetchone()[0]
    verified = decisions(store)
    return {"identity": resolution, "cross_source_candidates": candidates,
            "independent_official_confirmations": len(verified),
            "rule_version": VERSION, "policy_sha256": POLICY_SHA256,
            "decision_index_sha256": digest(verified), "canonical_events_modified": 0,
            "policy": "exact official profile and same-ORCID self assertion; copied official reference counts once"}


def relation_rows(store, *, allowed_sources=None):
    """Read one person's bounded events at a time. No population-sized JSON/list."""
    from .career_normalization import has_interpretations, VERSION, POLICY_SHA256
    enriched = has_interpretations(store)
    from .corroboration import decisions
    verified = decisions(store, allowed_sources=allowed_sources)
    statuses = {r[0]: r[1] for r in store.db.execute("SELECT source,status FROM runs")}
    columns = ',i.kind,i.stage,i.status' if enriched else ',NULL,NULL,NULL'
    join = " LEFT JOIN event_interpretations i ON i.event_id=e.id AND i.rule_version=? AND i.policy_sha256=?" if enriched else ''
    cursor = store.db.execute("""SELECT e.person,e.payload,COALESCE(a.institution,e.institution),e.status,e.source,e.id""" + columns + """
        FROM events e LEFT JOIN institution_aliases a ON a.alias=e.institution""" + join + """
        WHERE EXISTS(SELECT 1 FROM members m WHERE m.source=e.source AND m.status='observed'
          AND (m.locator=e.locator OR e.locator LIKE m.locator||'#%'))
        ORDER BY e.person,e.id""", (VERSION, POLICY_SHA256) if enriched else ())
    person, events = None, []
    def relations(items):
        trainings = [e for e in items if e["kind"] in ("education", "training") and e.get("year_end") and e.get("stage") in DEFAULT_CONFIG["stage_weights"]]
        jobs = [e for e in items if e["kind"] == "employment" and e.get("stage") == "academic" and e.get("year_start")]
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
    for pid, payload, institution, status, source, event_id, interpreted_kind, interpreted_stage, interpreted_status in cursor:
        if person is not None and pid != person:
            yield from relations(events)
            events = []
        person = pid
        if len(events) >= 4096:
            raise BudgetStop("person event fanout exceeds bounded relation analysis")
        if interpreted_status is not None:
            status = interpreted_status
        if status not in ("observed", "corroborated") or not institution:
            continue
        if allowed_sources is not None and source not in allowed_sources:
            continue
        if source == "orcid-2026-summary" and statuses.get(source) != "verified":
            continue
        row = json.loads(payload)
        row["institution"], row["status"] = institution, status
        if source.startswith('orcid-'):
            row['kind'], row['stage'] = classify(row['kind'],row.get('role'),completed_only=True)
        if interpreted_status is not None:
            row['kind'], row['stage'] = interpreted_kind, interpreted_stage
        if event_id in verified:
            row['status'] = 'corroborated'
            row['evidence_origins'] = verified[event_id]['origins']
        events.append(row)
    if events:
        yield from relations(events)


def rank_global(store, *, public=False, publication_policy=None):
    store.task.check(64*1024**2)
    allowed = public_sources(store.task,publication_policy) if public else None
    edges, stages, counts, source_counts = defaultdict(float), defaultdict(Counter), Counter(), defaultdict(set)
    corroborated = Counter()
    independent_origins = defaultdict(set)
    recent_edges = defaultdict(float)
    relation_count = 0
    for (employer, training, stage), train, job in relation_rows(store, allowed_sources=allowed):
        relation_count += 1
        key = employer, training
        counts[training] += 1
        source_counts[training].update(train['supporting_sources'])
        source_counts[training].update(job['supporting_sources'])
        if train["status"] == job["status"] == "corroborated":
            corroborated[training] += 1
            independent_origins[training].update(set(train.get('evidence_origins', ())) & set(job.get('evidence_origins', ())))
        weight = DEFAULT_CONFIG["stage_weights"][stage]
        edges[key] += weight
        stages[key][stage] += weight
        if job["year_start"] >= 2016:
            recent_edges[key] += weight
        # Bound Python sparse graph size; do not silently sample beyond this limit.
        if len(edges) > 200000:
            raise BudgetStop("sparse graph exceeds bounded in-memory analysis; use partitioned graph backend")
        if relation_count % 1000 == 0:
            store.task.check_memory()
    nodes = sorted({endpoint for edge in edges for endpoint in edge})
    network, convergence = pagerank(nodes, dict(sorted(edges.items())), .85, 1e-12, 200)
    directory = {r[0]: (r[1], r[2], json.loads(r[3])) for r in store.db.execute("SELECT id,name,country,payload FROM entities")}
    # A reported label is presentation only, never a name-based identity merge.
    for institution, payload, country, source in store.db.execute("SELECT institution,payload,country,source FROM events WHERE institution IS NOT NULL ORDER BY institution,id"):
        if allowed is not None and source not in allowed:
            continue
        reported = json.loads(payload).get("institution_name")
        if institution not in directory and reported:
            directory[institution] = (reported, country, {"identity_status":"reported-label-unresolved"})
    # Observed event endpoints belong in directory even when they have no qualifying edge.
    directory_ids = {r[0] for r in store.db.execute("SELECT DISTINCT COALESCE(a.institution,e.institution),e.source FROM events e LEFT JOIN institution_aliases a ON a.alias=e.institution WHERE e.institution IS NOT NULL")
                     if allowed is None or r[1] in allowed}
    ordering = sorted(network, key=lambda node: (-network[node], node))
    order = {node: i+1 for i, node in enumerate(ordering)}
    scenarios, expanded_scenarios = {}, {}
    reference_nodes = set(nodes)
    def scenario(label, changed_edges, alpha=.85, personalization=None):
        if len(changed_edges) > 200000:
            raise BudgetStop('sensitivity graph exceeds bounded sparse analysis')
        store.task.check_memory()
        positive_edges = {edge:weight for edge,weight in changed_edges.items() if weight > 0}
        new_endpoints = sorted({endpoint for edge in positive_edges for endpoint in edge} - reference_nodes)
        fixed_edges = {edge:weight for edge,weight in positive_edges.items()
                       if edge[0] in reference_nodes and edge[1] in reference_nodes}
        def evaluate(universe, valid_edges, boundary):
            scores, conv = pagerank(universe, dict(sorted(valid_edges.items())), alpha, 1e-12, 1000, personalization)
            return {"converged":conv["converged"], "edges":len(valid_edges),
                "available_edges":len(positive_edges), "filtered_boundary_edges":len(positive_edges)-len(valid_edges),
                "new_endpoints":new_endpoints, "universe_kind":boundary, "node_count":len(universe),
                "probability_mass":math.fsum(scores.values()), "prior_only":not bool(valid_edges),
                "ranks":{node:i+1 for i,node in enumerate(sorted(scores,key=lambda n:(-scores[n],n)))}}
        scenarios[label] = evaluate(nodes, fixed_edges, 'fixed-reference-universe')
        if new_endpoints:
            # Removing conflicting evidence can reveal previously excluded jobs.
            # Keep this expanded comparison separate from the fixed-universe rank range.
            expanded_scenarios[label] = evaluate(sorted(reference_nodes | set(new_endpoints)),
                                                  positive_edges, 'reference-plus-new-endpoints')
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
        remaining_edges=defaultdict(float)
        for (employer,training,stage),_train,_job in relation_rows(store,allowed_sources=available_sources-{source}):
            remaining_edges[(employer,training)]+=DEFAULT_CONFIG['stage_weights'][stage]
        scenario('delete_source_'+source,remaining_edges)
    scenario("employment_since_2016", recent_edges)
    groups = Counter(directory.get(node,(None,None,{}))[2].get("region") or "unknown" for node in nodes)
    if nodes:
        scenario("region_balanced_prior",edges,personalization={node:1/groups[directory.get(node,(None,None,{}))[2].get("region") or "unknown"] for node in nodes})
    # Conditional intensity: denominator is observed training people, never graduates at risk.
    from .career_normalization import has_interpretations, VERSION, POLICY_SHA256
    denominators = Counter()
    denominator_sql = """SELECT COALESCE(a.institution,e.institution),count(DISTINCT e.person)
        FROM events e LEFT JOIN institution_aliases a ON a.alias=e.institution
        WHERE e.kind IN ('education','training') AND e.status IN ('observed','corroborated')"""
    params = ()
    if has_interpretations(store):
        # The explicit v2 view counts its interpreted training observations in
        # the same denominator; old frozen inputs retain their predecessor view.
        denominator_sql = """SELECT COALESCE(a.institution,e.institution),count(DISTINCT e.person)
            FROM events e LEFT JOIN institution_aliases a ON a.alias=e.institution
            LEFT JOIN event_interpretations i ON i.event_id=e.id AND i.rule_version=? AND i.policy_sha256=?
            WHERE COALESCE(i.kind,e.kind) IN ('education','training')
            AND COALESCE(i.status,e.status) IN ('observed','corroborated')
            AND EXISTS(SELECT 1 FROM members m WHERE m.source=e.source AND m.status='observed'
                AND (m.locator=e.locator OR e.locator LIKE m.locator||'#%'))
            AND (e.source!='orcid-2026-summary' OR EXISTS(
                SELECT 1 FROM runs r WHERE r.source=e.source AND r.status='verified'))"""
        params = (VERSION, POLICY_SHA256)
    if allowed is not None:
        denominator_sql += " AND e.source IN (" + ','.join('?' for _ in allowed) + ")"
        params += tuple(sorted(allowed))
    for institution, count in store.db.execute(denominator_sql + " GROUP BY COALESCE(a.institution,e.institution)",params):
        if institution:
            denominators[institution] = count
    intensity, _ = pagerank(nodes,{(s,t):w/max(1,denominators[t]) for (s,t),w in sorted(edges.items())},.85,1e-12,200)
    rows = []
    for node in sorted(directory_ids | set(nodes)):
        name, country, meta = directory.get(node, (node, None, {}))
        reasons = []
        if corroborated[node] < 10: reasons.append("insufficient_independently_corroborated_relations")
        if len(independent_origins[node]) < 2: reasons.append("insufficient_independent_source_classes")
        if node not in directory or directory[node][2].get("identity_status") == 'reported-label-unresolved': reasons.append("institution_crosswalk_pending")
        reasons.append("external_dual_review_pending")
        rows.append({"institution_id":node,"name":name,"country":country,"region":meta.get("region"),
            "institution_type":meta.get("type"),"observed_rank":order.get(node),"formal_rank":None,
            "network_score":network.get(node),"award_score":None,"composite_score":None,
            "network_intensity_score":intensity.get(node),"observed_training_denominator":denominators[node] or None,
            "award_status":"missing_official_award_data", "status":"observed_only" if node in network else "unranked_no_qualifying_relations",
            "unranked_reasons":reasons,"relations":counts[node],"source_classes":sorted(source_counts[node]),
            "independent_source_classes":sorted(independent_origins[node]),
            "corroborated_relations":corroborated[node],"traceability":1.0 if counts[node] else None,
            "rank_range":[min(s["ranks"][node] for s in scenarios.values() if not s["prior_only"]),max(s["ranks"][node] for s in scenarios.values() if not s["prior_only"])] if node in network else None})
    report = store.report()
    # Operational counters/exception strings are private and nondeterministic; export an allowlist.
    coverage = {k:report[k] for k in ("people","events","countries","event_status","member_status")}
    if public:
        placeholders = ','.join('?' for _ in allowed)
        query = f"SELECT person,country,status FROM events WHERE source IN ({placeholders})"
        people, countries, statuses = set(), set(), Counter()
        for person, country, status in store.db.execute(query, tuple(sorted(allowed))):
            people.add(person)
            if country: countries.add(country)
            statuses[status] += 1
        coverage = {"people":len(people), "events":sum(statuses.values()), "countries":sorted(countries),
                    "event_status":dict(statuses), "member_status":dict(store.db.execute(
                        f"SELECT status,count(*) FROM members WHERE source IN ({placeholders}) GROUP BY status", tuple(sorted(allowed))))}
    result = {"schema_version":"gapi-global-observed/v1","dataset_kind":"real/global-public-observations",
        "publication_status":"public-observations-pending-external-review" if public else "local-only-source-license-review",
        "scope":{"complete_global_census":False,"enumeration_complete":False,"source_scope":"committed, individually hashed records; annual archive excluded until verified"},
        "coverage":coverage,"nodes":rows,
        "edges":[{"source":s,"target":t,"weight":w,"stages":dict(stages[(s,t)])} for (s,t),w in sorted(edges.items())],
        "algorithm":{"method_version":"gapi-observed-network/3", "direction":"employer -> training",
            "config":DEFAULT_CONFIG,"network_universe":nodes,
            "version_boundary":"New method outputs supersede predecessor calculations; historical bytes require their frozen inputs and pinned predecessor code."},
        "convergence":convergence,"sensitivity":scenarios,"expanded_universe_sensitivity":expanded_scenarios,
        "uncertainty":{"kind":"parameter-scenarios-only","unknown_global_missingness_covered":False,
            "rank_range_scope":"fixed-reference-universe; excludes prior-only and expanded-universe scenarios",
            "not_run":["discipline","bootstrap","english_source_deletion","official_profile_loss"],
            "intensity_denominator":"observed training people; not all graduates or job seekers"},
        "limitations":["Observed scores are not employment rates or causal training effects.","Formal ranking and award/composite data are unavailable.","No completed independent official-profile corroboration."]}
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
    if has_interpretations(store) and (allowed is None or 'orcid-public-api' in allowed):
        interpreted = store.db.execute('SELECT count(*) FROM event_interpretations WHERE rule_version=? AND policy_sha256=?', (VERSION, POLICY_SHA256)).fetchone()[0]
        result['algorithm']['normalization_policy'] = {'version': VERSION, 'policy_sha256': POLICY_SHA256,
            'interpreted_events': interpreted, 'canonical_events_modified': 0,
            'confidence': 'observed-only; lexical rules do not corroborate facts'}
    return result


def export_release(store, out: Path, *, publication_policy=None, frozen_input=None):
    """Public, licensed institution aggregates only; no individual trajectories."""
    if out.resolve() != (project_root()/"outputs/global").resolve():
        raise ValueError("public release output must use the budget-accounted outputs/global directory")
    store.task.check(64*1024**2)
    result = rank_global(store, public=True,publication_policy=publication_policy)
    # Failing this gate prevents a misleading blank real-score release.
    if not result["edges"]:
        raise ValueError("no licensed, validated real relations available for public score release")
    if not result["convergence"]["converged"]:
        raise ValueError("non-converged scoring output cannot be released")
    out.mkdir(parents=True, exist_ok=True)
    content = canonical(result)
    store.task.check(4*len(content))
    checksum = hashlib.sha256(content).hexdigest()
    filename = f"global-{checksum[:16]}.json"
    target = out/filename
    tmp = out/(filename+".tmp")
    tmp.write_bytes(content)
    os.replace(tmp,target)
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
    manifest = {"schema_version":"gapi-public-release/v1", "snapshot":filename, "output_sha256":checksum,
        "sources":inputs,"member_index_sha256":member_hash.hexdigest(),"normalized_event_index_sha256":events_hash.hexdigest(),"config_sha256":digest(store.task.config),
        "code_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=project_root(),text=True).strip(),
        "code_tree_sha256":digest({str(p.relative_to(project_root())).replace('\\','/'):hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
            for p in sorted((project_root()/'src/gapi').glob('*.py'))}),
        "environment":{"python":platform.python_version()},"review_status":"pending-independent-dual-track",
        "publication_status":result["publication_status"]}
    if publication_policy is not None:
        manifest['publication_policy']=publication_policy
        manifest['publication_policy_sha256']=digest(publication_policy)
    if frozen_input is not None:
        manifest['private_normalized_input']=frozen_input
    if 'official_award_inventory' in result:
        awards = result['official_award_inventory']
        manifest['official_award_source_sha256'] = awards['source_sha256']
        manifest['normalized_award_index_sha256'] = awards['normalized_award_index_sha256']
        manifest['official_award_page_index_sha256'] = awards['page_index_sha256']
    (out/"manifest.json").write_bytes(canonical(manifest))
    (out/"SHA256SUMS").write_text(f"{checksum}  {filename}\n",encoding="ascii")
    return manifest


def export_local(store):
    """Local aggregate view can include noncommercial API data, never public by default."""
    result = rank_global(store)
    content = canonical(result)
    store.task.check(4 * len(content))
    checksum = hashlib.sha256(content).hexdigest()
    path = store.task.root / "global-local.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_bytes(content)
    os.replace(temporary, path)
    manifest = {"schema_version":"gapi-local-view/v1", "snapshot":"global-local.json", "output_sha256":checksum,
        "publication_status":"local-only-source-license-review", "config_sha256":digest(store.task.config)}
    path.with_suffix(".json.manifest.json").write_bytes(canonical(manifest))
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
