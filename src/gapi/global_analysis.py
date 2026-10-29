"""Bounded institution-level analysis; privacy and release decisions live here."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

from .ranking import DEFAULT_CONFIG, pagerank
from .career_rules import classify
from .storage import project_root
from .task import BudgetStop, canonical, digest


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
    resolution = resolve_institutions(store)
    candidates = store.db.execute('''SELECT count(*) FROM (
        SELECT person,institution,kind,stage,year_start,year_end FROM events
        WHERE institution IS NOT NULL AND stage IS NOT NULL
        GROUP BY person,institution,kind,stage,year_start,year_end HAVING count(DISTINCT source)>1)''').fetchone()[0]
    return {"identity": resolution, "cross_source_candidates": candidates,
            "independent_official_confirmations": 0,
            "policy": "matching statements alone do not establish independent provenance; official adapter review pending"}


def relation_rows(store, *, allowed_sources=None):
    """Read one person's bounded events at a time. No population-sized JSON/list."""
    statuses = {r[0]: r[1] for r in store.db.execute("SELECT source,status FROM runs")}
    cursor = store.db.execute("""SELECT e.person,e.payload,COALESCE(a.institution,e.institution),e.status,e.source
        FROM events e LEFT JOIN institution_aliases a ON a.alias=e.institution
        WHERE EXISTS(SELECT 1 FROM members m WHERE m.source=e.source AND m.status='observed'
          AND (m.locator=e.locator OR e.locator LIKE m.locator||'#%'))
        ORDER BY e.person,e.id""")
    person, events = None, []
    def relations(items):
        trainings = [e for e in items if e["kind"] in ("education", "training") and e.get("year_end") and e.get("stage") in DEFAULT_CONFIG["stage_weights"]]
        jobs = [e for e in items if e["kind"] == "employment" and e.get("stage") == "academic" and e.get("year_start")]
        seen = set()
        for train in trainings:
            for job in jobs:
                key = (job["institution"], train["institution"], train["stage"])
                if key in seen or key[0] == key[1] or job["year_start"] < train["year_end"]:
                    continue
                # Detect overlapping jobs at different institutions. Missing end means ongoing.
                if any(other["institution"] != job["institution"] and
                       other["year_start"] <= (job.get("year_end") or 2100) and
                       job["year_start"] <= (other.get("year_end") or 2100) for other in jobs):
                    continue
                seen.add(key)
                yield key, train, job
    for pid, payload, institution, status, source in cursor:
        if person is not None and pid != person:
            yield from relations(events)
            events = []
        person = pid
        if len(events) >= 4096:
            raise BudgetStop("person event fanout exceeds bounded relation analysis")
        if status not in ("observed", "corroborated") or not institution:
            continue
        if allowed_sources is not None and source not in allowed_sources:
            continue
        if source == "orcid-2026-summary" and statuses.get(source) != "verified":
            continue
        row = json.loads(payload)
        row["institution"], row["status"] = institution, status
        if source.startswith('orcid-'):
            row['kind'], row['stage'] = classify(row['kind'],row.get('role'))
        events.append(row)
    if events:
        yield from relations(events)


def rank_global(store, *, public=False):
    store.task.check(64*1024**2)
    allowed = {name for name, spec in store.task.config["sources"].items() if spec.get("allow_derivatives")} if public else None
    edges, stages, counts, source_counts = defaultdict(float), defaultdict(Counter), Counter(), defaultdict(set)
    corroborated = Counter()
    by_source = defaultdict(lambda: defaultdict(float))
    recent_edges = defaultdict(float)
    relation_count = 0
    for (employer, training, stage), train, job in relation_rows(store, allowed_sources=allowed):
        relation_count += 1
        key = employer, training
        counts[training] += 1
        source_counts[training].update((train["source"], job["source"]))
        if train["status"] == job["status"] == "corroborated":
            corroborated[training] += 1
        weight = DEFAULT_CONFIG["stage_weights"][stage]
        edges[key] += weight
        stages[key][stage] += weight
        for source in {train["source"],job["source"]}:
            by_source[source][key] += weight
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
    scenarios = {}
    def scenario(label, changed_edges, alpha=.85, personalization=None):
        scores, conv = pagerank(nodes, dict(sorted(changed_edges.items())), alpha, 1e-12, 1000, personalization)
        scenarios[label] = {"converged":conv["converged"], "edges":len(changed_edges),
            "prior_only":not any(changed_edges.values()),
            "ranks":{node:i+1 for i,node in enumerate(sorted(scores,key=lambda n:(-scores[n],n)))}}
    for alpha in (.65, .85, .95):
        scenario(f"alpha_{alpha}",edges,alpha)
    for stage in DEFAULT_CONFIG["stage_weights"]:
        scenario("stage_only_"+stage,{edge:values[stage] for edge,values in stages.items() if values[stage]>0})
    for source, contributions in by_source.items():
        scenario("delete_source_"+source,{edge:weight-contributions.get(edge,0) for edge,weight in edges.items() if weight-contributions.get(edge,0)>1e-15})
    scenario("employment_since_2016", recent_edges)
    groups = Counter(directory.get(node,(None,None,{}))[2].get("region") or "unknown" for node in nodes)
    if nodes:
        scenario("region_balanced_prior",edges,personalization={node:1/groups[directory.get(node,(None,None,{}))[2].get("region") or "unknown"] for node in nodes})
    # Conditional intensity: denominator is observed training people, never graduates at risk.
    denominators = Counter()
    denominator_sql = """SELECT COALESCE(a.institution,e.institution),count(DISTINCT e.person)
        FROM events e LEFT JOIN institution_aliases a ON a.alias=e.institution
        WHERE e.kind IN ('education','training') AND e.status IN ('observed','corroborated')"""
    params = ()
    if allowed is not None:
        denominator_sql += " AND e.source IN (" + ','.join('?' for _ in allowed) + ")"
        params = tuple(sorted(allowed))
    for institution, count in store.db.execute(denominator_sql + " GROUP BY COALESCE(a.institution,e.institution)",params):
        if institution:
            denominators[institution] = count
    intensity, _ = pagerank(nodes,{(s,t):w/max(1,denominators[t]) for (s,t),w in sorted(edges.items())},.85,1e-12,200)
    rows = []
    for node in sorted(directory_ids | set(nodes)):
        name, country, meta = directory.get(node, (node, None, {}))
        reasons = []
        if corroborated[node] < 10: reasons.append("insufficient_independently_corroborated_relations")
        if len(source_counts[node]) < 2: reasons.append("insufficient_independent_source_classes")
        if node not in directory or directory[node][2].get("identity_status") == 'reported-label-unresolved': reasons.append("institution_crosswalk_pending")
        reasons.append("external_dual_review_pending")
        rows.append({"institution_id":node,"name":name,"country":country,"region":meta.get("region"),
            "institution_type":meta.get("type"),"observed_rank":order.get(node),"formal_rank":None,
            "network_score":network.get(node),"award_score":None,"composite_score":None,
            "network_intensity_score":intensity.get(node),"observed_training_denominator":denominators[node] or None,
            "award_status":"missing_official_award_data", "status":"observed_only" if node in network else "unranked_no_qualifying_relations",
            "unranked_reasons":reasons,"relations":counts[node],"source_classes":sorted(source_counts[node]),
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
    return {"schema_version":"gapi-global-observed/v1","dataset_kind":"real/global-public-observations",
        "publication_status":"public-observations-pending-external-review" if public else "local-only-source-license-review",
        "scope":{"complete_global_census":False,"enumeration_complete":False,"source_scope":"committed, individually hashed records; annual archive excluded until verified"},
        "coverage":coverage,"nodes":rows,
        "edges":[{"source":s,"target":t,"weight":w,"stages":dict(stages[(s,t)])} for (s,t),w in sorted(edges.items())],
        "algorithm":{"direction":"employer -> training","config":DEFAULT_CONFIG,"network_universe":nodes},
        "convergence":convergence,"sensitivity":scenarios,
        "uncertainty":{"kind":"parameter-scenarios-only","unknown_global_missingness_covered":False,
            "not_run":["discipline","bootstrap","english_source_deletion","official_profile_loss"],
            "intensity_denominator":"observed training people; not all graduates or job seekers"},
        "limitations":["Observed scores are not employment rates or causal training effects.","Formal ranking and award/composite data are unavailable.","No completed independent official-profile corroboration."]}


def export_release(store, out: Path):
    """Public, licensed institution aggregates only; no individual trajectories."""
    if out.resolve() != (project_root()/"outputs/global").resolve():
        raise ValueError("public release output must use the budget-accounted outputs/global directory")
    store.task.check(64*1024**2)
    result = rank_global(store, public=True)
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
    allowed = {n for n,s in store.task.config["sources"].items() if s.get("allow_derivatives")}
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
