"""Independent structured sources; copied claims never become corroboration."""
from __future__ import annotations

import json
import re
import time
from xml.parsers.expat import ExpatError
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.error import HTTPError

from .career_rules import classify, year, valid_orcid
from .orcid_stream import private_person, SummaryParser, CHUNK
from .task import digest, BudgetStop
from .source_http import SourcePause, SourceRedirect, ORCID_ANONYMOUS_DAILY_READS

DEGREES = {"Q752297": "doctorate", "Q849697": "doctorate", "Q177232": "master", "Q163727": "bachelor"}


def access_refused(exc):
    return isinstance(exc,(PermissionError,SourcePause)) or isinstance(exc,HTTPError) and exc.code in (401,403,429)


def wd_value(snak):
    if not isinstance(snak,dict) or not isinstance(snak.get('datavalue',{}),dict):
        raise ValueError('invalid Wikidata snak/value container')
    return snak.get("datavalue", {}).get("value")


def qualifier_year(claim, prop):
    values = claim.get("qualifiers", {}).get(prop, [])
    value = wd_value(values[0]) if len(values) == 1 else None
    return year(value["time"][1:5]) if isinstance(value, dict) and value.get("precision", 0) >= 9 else None


def wikidata_events(entity, content_hash):
    if not isinstance(entity,dict) or not isinstance(entity.get('id'),str) or not re.fullmatch(r'Q[1-9]\d*',entity['id']):
        raise ValueError('invalid Wikidata entity identity')
    if type(entity.get('lastrevid')) is not int or entity['lastrevid']<=0 or not isinstance(entity.get('claims',{}),dict):
        raise ValueError('invalid Wikidata revision/claims')
    qid, revision = entity["id"], entity["lastrevid"]
    claims = entity.get("claims", {})
    for prop in ('P69','P108','P496'):
        statements=claims.get(prop,[])
        if not isinstance(statements,list):raise ValueError('invalid Wikidata statement collection')
        for statement in statements:
            if not isinstance(statement,dict) or not isinstance(statement.get('mainsnak'),dict):
                raise ValueError('invalid Wikidata statement')
            qualifiers=statement.get('qualifiers',{})
            if not isinstance(qualifiers,dict) or any(not isinstance(value,list) for value in qualifiers.values()):
                raise ValueError('invalid Wikidata qualifiers')
            if not isinstance(statement.get('references',[]),list):raise ValueError('invalid Wikidata references')
    identifiers = [wd_value(c["mainsnak"]) for c in claims.get("P496", []) if c.get("rank") != "deprecated"]
    identifiers = sorted({x for x in identifiers if valid_orcid(x)})
    # A conflicting ORCID never triggers automatic cross-person merging.
    person = private_person(identifiers[0]) if len(identifiers) == 1 else "wd:" + qid
    for prop, kind in (("P69", "education"), ("P108", "employment")):
        for claim in claims.get(prop, []):
            if claim.get("rank") == "deprecated":
                continue
            value = wd_value(claim.get("mainsnak", {}))
            if not isinstance(value, dict) or not re.fullmatch(r"Q[1-9]\d*", value.get("id", "")):
                continue
            qualifications = [wd_value(s) for s in claim.get("qualifiers", {}).get("P512", [])]
            stages = {DEGREES[v["id"]] for v in qualifications if isinstance(v, dict) and v.get("id") in DEGREES}
            # Educated-at does not imply any degree. Employer does not imply academic role.
            stage = next(iter(stages)) if kind == "education" and len(stages) == 1 else None
            event = {"person": person, "institution": "wd:" + value["id"], "kind": kind,
                "stage": stage, "year_start": qualifier_year(claim, "P580"),
                "year_end": qualifier_year(claim, "P582"), "role": None, "country": None,
                "source": "wikidata", "locator": f"{qid}@{revision}#{claim['id']}",
                "url": f"https://www.wikidata.org/w/index.php?title={qid}&oldid={revision}",
                "statement_id": claim["id"], "revision": revision, "content_sha256": content_hash,
                "references": claim.get("references", []), "qualifiers": claim.get("qualifiers", {}),
                "assertion_origin": "wikidata-community", "status": "unresolved"}
            # Roles need explicit qualifiers; no job inference from being a named person.
            roles = [wd_value(s) for s in claim.get("qualifiers", {}).get("P39", [])]
            academic = {"Q121594", "Q1622272", "Q3400985"}  # professor, university teacher, academic
            if kind == "employment" and any(isinstance(v, dict) and v.get("id") in academic for v in roles):
                event["stage"], event["role"] = "academic", "explicit academic position qualifier"
            if event["stage"] and (event["year_end"] if kind == "education" else event["year_start"]):
                event["status"] = "observed"
            yield event


def ingest_wikidata(store, http, qids, *, max_seconds=None, progress=None):
    source = "wikidata"
    if not store.source_ready(source):return store.report()
    store.start(source)
    failures = []
    started = time.monotonic()
    previous_bytes=store.db.execute('SELECT bytes FROM runs WHERE source=?',(source,)).fetchone()[0]
    start_bytes=getattr(http,'bytes_received',0)
    blocked=False
    for qid in qids:
        store.task.check_memory()
        if max_seconds is not None and time.monotonic() - started >= max_seconds:
            break
        if not re.fullmatch(r"Q[1-9]\d*", qid):
            raise ValueError("invalid Wikidata ID")
        if store.db.execute('SELECT 1 FROM members WHERE source=? AND locator LIKE ? LIMIT 1',(source,qid+'@%')).fetchone():
            continue
        url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
        try:
            data, checksum = http.json(url)
            if not isinstance(data,dict) or not isinstance(data.get('entities'),dict):
                raise ValueError('invalid Wikidata EntityData envelope')
            entity = data["entities"][qid]
            if not isinstance(entity,dict) or entity.get('id')!=qid:
                raise ValueError('Wikidata response does not match requested entity')
            # Validate revision before using it as a member locator.
            if type(entity.get('lastrevid')) is not int or entity['lastrevid']<=0:
                raise ValueError('invalid Wikidata entity revision')
            locator = f"{qid}@{entity['lastrevid']}"
            if store.seen(source, locator):
                continue
            with store.member(source, locator):
                for event_index, event in enumerate(wikidata_events(entity, checksum), 1):
                    if event_index > store.task.config["batch_events"]:
                        raise ValueError("entity exceeds bounded event transaction; unresolved oversized member")
                    event['retrieved_at'] = datetime.now(timezone.utc).isoformat()
                    store.event(event)
                # Revision, references and member hash remain internal.
                store.finish_member(source, locator, checksum)
                # Follow a single explicit stable ID into a genuinely separate
                # public source. Discovery remains a lead, not corroboration.
                ids = {wd_value(claim['mainsnak']) for claim in entity.get('claims',{}).get('P496',[])
                       if claim.get('rank')!='deprecated' and valid_orcid(wd_value(claim['mainsnak']))}
                if len(ids)==1:
                    query_hash=digest({'rule':'wikidata-explicit-orcid-followup/1','locator':locator})
                    store.db.execute('INSERT OR IGNORE INTO discovery VALUES(?,?,?,?)',
                        ('ingest-orcid-api',next(iter(ids)),query_hash,checksum))
            if progress:
                progress({"source":source,"committed_entity":qid,"events":store.db.execute("SELECT count(*) FROM events").fetchone()[0]})
        except (KeyError, ValueError, OSError, RuntimeError) as exc:
            store.failure(source,qid,f"{type(exc).__name__}: {exc}")
            store.defer('ingest-wikidata',qid,str(exc))
            failures.append({"entity": qid, "reason": f"{type(exc).__name__}: {exc}"[:256]})
            if access_refused(exc):
                if isinstance(exc,SourcePause):store.pause_source(source,exc.retry_at,str(exc))
                blocked=True
                break
    store.status(source, "access_failed" if blocked else "partial", byte_count=previous_bytes+getattr(http,'bytes_received',0)-start_bytes,
                 detail=json.dumps({"requested": len(qids), "failures": failures[:20]}))
    return store.report()


def orcid_api_events(record, checksum, orcid):
    source = "orcid-public-api"
    person = private_person(orcid)
    activities = record.get("activities-summary", {})
    for plural, singular in (("educations", "education"), ("employments", "employment")):
        section = activities.get(plural) or {}
        # API v3 groups can wrap summaries, while older records expose a direct list.
        summaries = list(section.get(singular + "-summary", []) or [])
        for group in section.get("affiliation-group", []) or []:
            for item in group.get("summaries", []) or []:
                if singular + "-summary" in item:
                    summaries.append(item[singular + "-summary"])
        for summary in summaries:
            org = summary.get("organization", {})
            dis = org.get("disambiguated-organization") or {}
            from .career_rules import institution_id
            inst = institution_id(dis.get("disambiguated-organization-identifier"), dis.get("disambiguation-source"))
            role = summary.get("role-title") or ""
            kind, stage = classify(singular, role)
            def date(key):
                return year(((summary.get(key) or {}).get("year") or {}).get("value"))
            assertion = summary.get("source") or {}
            assertion_id = (assertion.get("source-orcid") or assertion.get("source-client-id") or {}).get("path", "unknown")
            yield {"person": person, "institution": inst, "kind": kind, "stage": stage,
                "year_start": date("start-date"), "year_end": date("end-date"), "role": role,
                "institution_name": org.get("name"), "country": (org.get("address") or {}).get("country"),
                "source": source, "locator": orcid, "put_code": str(summary.get("put-code", "")),
                "url": f"https://pub.orcid.org/v3.0/{orcid}/record", "content_sha256": checksum,
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "assertion_origin": "self" if assertion_id == orcid else assertion_id,
                "status": "observed" if inst and stage and (date("start-date") if kind == "employment" else date("end-date")) else "unresolved"}


def queue_orcid_redirect(store, redirect, requested_url):
    """A permanent official redirect discovers a candidate, never merges people.

    The existing discovery page owns the immutable normalized header evidence.
    Its hash is explicitly a metadata hash, not a raw response-body checksum.
    Old candidates stay pending; only a fresh verified response can add events.
    """
    pattern = r'https://pub\.orcid\.org/v3\.0/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])/(educations|employments)'
    origin = re.fullmatch(pattern, redirect.source_url)
    target = re.fullmatch(pattern, redirect.target_url)
    if (redirect.code not in (301, 308) or redirect.source_url != requested_url
            or not origin or not target or origin[2] != target[2]
            or origin[1] == target[1] or not valid_orcid(origin[1]) or not valid_orcid(target[1])):
        return False
    metadata = {'representation': 'http-permanent-redirect-metadata/v1',
                'from': redirect.source_url, 'to': redirect.target_url, 'status': redirect.code}
    checksum = digest(metadata)
    feed = 'orcid-api-permanent-redirect-candidates/v1'
    locator = redirect.source_url + '#' + str(redirect.code) + '@' + checksum
    if not store.db.execute('SELECT 1 FROM discovery_pages WHERE source=? AND cursor_before=?',
                            (feed, locator)).fetchone():
        store.queue_page(feed, 'ingest-orcid-api', locator, redirect.target_url,
                         digest({'policy': feed, 'purpose': 'candidate-only-no-person-merge'}),
                         checksum, [target[1]], 1)
    return True


def ingest_orcid_api(store, http, ids, *, max_seconds=None, progress=None):
    source = "orcid-public-api"
    if not store.source_ready(source):return store.report()
    store.start(source)
    failures = []
    byte_count = store.db.execute("SELECT bytes FROM runs WHERE source=?",(source,)).fetchone()[0]
    started = time.monotonic()
    blocked=False
    for orcid in ids:
        store.task.check_memory()
        if max_seconds is not None and time.monotonic() - started >= max_seconds:
            break
        if not valid_orcid(orcid):
            store.failure(source,digest(orcid),'invalid ORCID format/checksum; not requested')
            store.defer('ingest-orcid-api',orcid,'invalid ORCID format/checksum; not requested')
            continue
        # This is a fixed record snapshot once committed, not a mirror of the annual dump.
        if store.seen(source, orcid):
            continue
        try:
            with store.request_budget(source,2*(store.task.config['max_retries']+1),ORCID_ANONYMOUS_DAILY_READS,http),store.member(source, orcid):
                parts={}
                event_count=member_bytes=0
                for section in ('educations','employments'):
                    checksum = __import__('hashlib').sha256()
                    retrieved = datetime.now(timezone.utc).isoformat()
                    url=f"https://pub.orcid.org/v3.0/{orcid}/{section}"
                    def emit(event):
                        nonlocal event_count
                        event_count+=1
                        if event_count>min(store.task.config['max_events_per_member'],store.task.config['batch_events']):
                            raise ValueError('affiliation pair exceeds bounded event transaction')
                        event.update(retrieved_at=retrieved,transport='incremental-affiliation-xml/v1',
                                     locator=orcid+'#'+section,url=url)
                        store.event(event)
                    parser = SummaryParser(private_person(orcid), source, orcid, emit,
                        store.task.config['batch_events'],section=section)
                    section_bytes=0
                    with http.open(url, "application/vnd.orcid+xml") as response:
                        if "xml" not in response.headers.get("Content-Type", ""):
                            raise ValueError("ORCID must return XML for the strict streaming adapter")
                        while chunk := response.read(CHUNK):
                            member_bytes += len(chunk)
                            section_bytes += len(chunk)
                            byte_count += len(chunk)
                            store.task.check_memory()
                            if member_bytes > store.task.config["max_member_bytes"]:
                                raise ValueError("API XML affiliation pair exceeds size limit")
                            if max_seconds is not None and time.monotonic()-started >= max_seconds:
                                raise BudgetStop("bounded XML API batch time reached; current member rolled back")
                            checksum.update(chunk)
                            parser.feed(chunk)
                        parser.feed(b"", final=True)
                    parts[section]=checksum.hexdigest()
                    store.db.execute('INSERT INTO member_parts VALUES(?,?,?,?,?,?,?)',
                        (source,orcid,section,url,parts[section],section_bytes,retrieved))
                store.finish_member(source, orcid, digest({'transport':'incremental-affiliation-xml/v1','parts':parts}))
            if progress:
                progress({"source":source,"committed_members":store.db.execute("SELECT count(*) FROM members WHERE source=?",(source,)).fetchone()[0]})
        except BudgetStop as exc:
            store.status(source, "budget_stopped", byte_count=byte_count, detail=str(exc))
            return store.report()
        except SourceRedirect as exc:
            queued = queue_orcid_redirect(store, exc, url)
            reason = ('permanent official redirect; target queued separately; original identity unresolved'
                      if queued else 'unverified or temporary identity redirect; no target queued')
            store.failure(source, private_person(orcid), reason)
            store.defer('ingest-orcid-api', orcid, reason)
            failures.append({'id_hash': private_person(orcid), 'reason': reason})
        except (ValueError, KeyError, OSError, RuntimeError, ExpatError) as exc:
            store.failure(source,private_person(orcid),f"{type(exc).__name__}: {exc}")
            store.defer('ingest-orcid-api',orcid,str(exc))
            failures.append({"id_hash": private_person(orcid), "reason": str(exc)[:128]})
            if access_refused(exc):
                if isinstance(exc,SourcePause):store.pause_source(source,exc.retry_at,str(exc))
                blocked=True
                break
    store.status(source, "access_failed" if blocked else "partial", byte_count=byte_count, detail=json.dumps({"requested": len(ids), "failures": failures[:20]}))
    return store.report()


def discover_wikidata(http, *, after="", limit=100, degree_employer=False, academic=False):
    """Ordered live keyset selection; changes upstream can still cause omissions."""
    if after and not re.fullmatch(r"Q[1-9]\d*", after):
        raise ValueError("invalid cursor")
    if not 1 <= limit <= 200:
        raise ValueError("enumeration page bound is 200")
    query = ('''SELECT DISTINCT ?person WHERE { ?person p:P69 ?s; wdt:P108 ?employer .
        ?s pq:P512 ?degree . VALUES ?degree { wd:Q752297 wd:Q849697 }'''
        if degree_employer else '''SELECT ?person (GROUP_CONCAT(DISTINCT STR(?id); separator="|") AS ?orcids) WHERE { ?person wdt:P496 ?id; wdt:P69 ?school; wdt:P108 ?employer''')
    if academic:
        # Candidate selection only. P106 never creates a job/degree event.
        query = 'SELECT DISTINCT ?person WHERE { ?person wdt:P106 wd:Q1622272'
    if after:
        query += f' FILTER(STR(?person) > "http://www.wikidata.org/entity/{after}")'
    query += ' }' + ('' if degree_employer or academic else ' GROUP BY ?person') + ' ORDER BY STR(?person)' + f' LIMIT {limit}'
    data, checksum = http.json("https://query.wikidata.org/sparql?format=json&query=" + quote(query))
    if not isinstance(data,dict) or not isinstance(data.get('results'),dict):
        raise ValueError('invalid Wikidata discovery envelope')
    rows = data['results'].get('bindings')
    if not isinstance(rows,list) or len(rows)>limit:
        raise ValueError('invalid Wikidata discovery bindings')
    for row in rows:
        if not isinstance(row,dict) or not isinstance(row.get('person'),dict) or not isinstance(row['person'].get('value'),str):
            raise ValueError('invalid Wikidata discovery person')
        if 'orcids' in row and (not isinstance(row['orcids'],dict) or not isinstance(row['orcids'].get('value'),str)):
            raise ValueError('invalid Wikidata discovery identifier')
    result = [{"qid": r["person"]["value"].rsplit("/", 1)[-1],
               "orcid": r.get("orcids", {}).get("value") if '|' not in r.get("orcids", {}).get("value", '') else None} for r in rows]
    if len(result) > limit or any(not re.fullmatch(r"Q[1-9]\d*", r['qid']) or after and r['qid'] <= after for r in result):
        raise ValueError('invalid or non-advancing discovery page')
    if [r['qid'] for r in result] != sorted(r['qid'] for r in result):
        raise ValueError('discovery response is not ordered')
    return {"query_sha256": digest(query), "response_sha256": checksum, "rows": result,
            "enumeration": "ordered-live-selection-not-frozen-global-enumeration"}


def enqueue_discovery(store, http, command, *, limit=200, scope='academic'):
    """Atomically persist a page and its queue before advancing its live cursor."""
    if command not in ('ingest-wikidata', 'ingest-orcid-api'):
        raise ValueError('unsupported discovery source')
    if scope not in ('academic', 'degree-employer'):
        raise ValueError('unsupported discovery scope')
    if not store.source_ready('wikidata-discovery'):
        return {'status':'server-backoff-pending','rows':0,'queued':0}
    page_source = command+':'+scope if command == 'ingest-wikidata' else command+':orcid'
    previous = store.db.execute('SELECT cursor_after,row_count FROM discovery_pages WHERE source=? ORDER BY rowid DESC LIMIT 1', (page_source,)).fetchone()
    if previous and previous[1] == 0:
        return {'status': 'live-query-exhausted-not-global-complete', 'cursor': previous[0]}
    cursor = previous[0] if previous else ''
    try:
        page = discover_wikidata(http, after=cursor, limit=limit, degree_employer=command == 'ingest-wikidata',
            academic=command == 'ingest-wikidata' and scope == 'academic')
    except SourcePause as exc:
        store.pause_source('wikidata-discovery',exc.retry_at,str(exc))
        raise
    ids = set()
    for row in page['rows']:
        value = row['qid'] if command == 'ingest-wikidata' else row['orcid']
        if value and (command == 'ingest-wikidata' or valid_orcid(value)):
            ids.add(value)
    end = page['rows'][-1]['qid'] if page['rows'] else cursor
    store.queue_page(page_source,command,cursor,end,page['query_sha256'],page['response_sha256'],ids,len(page['rows']))
    return {'status': page['enumeration'], 'cursor': end, 'rows': len(page['rows']), 'queued': len(ids)}
