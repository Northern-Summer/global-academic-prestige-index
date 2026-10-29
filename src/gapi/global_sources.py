"""Independent structured sources; copied claims never become corroboration."""
from __future__ import annotations

import json
import re
import time
from xml.parsers.expat import ExpatError
from datetime import datetime, timezone
from urllib.parse import quote

from .career_rules import classify, year
from .orcid_stream import ORCID, private_person, SummaryParser, CHUNK
from .task import digest, BudgetStop

DEGREES = {"Q752297": "doctorate", "Q849697": "doctorate", "Q177232": "master", "Q163727": "bachelor"}


def wd_value(snak):
    return snak.get("datavalue", {}).get("value")


def qualifier_year(claim, prop):
    values = claim.get("qualifiers", {}).get(prop, [])
    value = wd_value(values[0]) if len(values) == 1 else None
    return year(value["time"][1:5]) if isinstance(value, dict) and value.get("precision", 0) >= 9 else None


def wikidata_events(entity, content_hash):
    qid, revision = entity["id"], entity["lastrevid"]
    claims = entity.get("claims", {})
    identifiers = [wd_value(c["mainsnak"]) for c in claims.get("P496", []) if c.get("rank") != "deprecated"]
    identifiers = sorted({x for x in identifiers if isinstance(x, str) and re.fullmatch(ORCID, x)})
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
    store.start(source)
    failures = []
    started = time.monotonic()
    previous_bytes=store.db.execute('SELECT bytes FROM runs WHERE source=?',(source,)).fetchone()[0]
    start_bytes=getattr(http,'bytes_received',0)
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
            entity = data["entities"][qid]
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
            if progress:
                progress({"source":source,"committed_entity":qid,"events":store.db.execute("SELECT count(*) FROM events").fetchone()[0]})
        except (KeyError, ValueError, OSError, RuntimeError) as exc:
            store.failure(source,qid,f"{type(exc).__name__}: {exc}")
            failures.append({"entity": qid, "reason": f"{type(exc).__name__}: {exc}"[:256]})
    store.status(source, "partial", byte_count=previous_bytes+getattr(http,'bytes_received',0)-start_bytes,
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


def ingest_orcid_api(store, http, ids, *, max_seconds=None, progress=None):
    source = "orcid-public-api"
    store.start(source)
    failures = []
    byte_count = store.db.execute("SELECT bytes FROM runs WHERE source=?",(source,)).fetchone()[0]
    started = time.monotonic()
    for orcid in ids:
        store.task.check_memory()
        if max_seconds is not None and time.monotonic() - started >= max_seconds:
            break
        if not re.fullmatch(ORCID, orcid):
            raise ValueError("invalid ORCID")
        # This is a fixed record snapshot once committed, not a mirror of the annual dump.
        if store.seen(source, orcid):
            continue
        try:
            with store.member(source, orcid):
                checksum = __import__('hashlib').sha256()
                retrieved = datetime.now(timezone.utc).isoformat()
                def emit(event):
                    event["retrieved_at"] = retrieved
                    event["transport"] = "incremental-xml-no-tree"
                    store.event(event)
                parser = SummaryParser(private_person(orcid), source, orcid, emit,
                    min(store.task.config["max_events_per_member"], store.task.config["batch_events"]))
                member_bytes = 0
                with http.open(f"https://pub.orcid.org/v3.0/{orcid}/record", "application/vnd.orcid+xml") as response:
                    if "xml" not in response.headers.get("Content-Type", ""):
                        raise ValueError("ORCID must return XML for the strict streaming adapter")
                    while chunk := response.read(CHUNK):
                        member_bytes += len(chunk)
                        byte_count += len(chunk)
                        store.task.check_memory()
                        if member_bytes > store.task.config["max_member_bytes"]:
                            raise ValueError("API XML member exceeds size limit")
                        if max_seconds is not None and time.monotonic()-started >= max_seconds:
                            raise BudgetStop("bounded XML API batch time reached; current member rolled back")
                        checksum.update(chunk)
                        parser.feed(chunk)
                    parser.feed(b"", final=True)
                store.finish_member(source, orcid, checksum.hexdigest())
            if progress:
                progress({"source":source,"committed_members":store.db.execute("SELECT count(*) FROM members WHERE source=?",(source,)).fetchone()[0]})
        except BudgetStop as exc:
            store.status(source, "budget_stopped", byte_count=byte_count, detail=str(exc))
            return store.report()
        except (ValueError, KeyError, OSError, RuntimeError, ExpatError) as exc:
            store.failure(source,private_person(orcid),f"{type(exc).__name__}: {exc}")
            failures.append({"id_hash": private_person(orcid), "reason": str(exc)[:128]})
    store.status(source, "partial", byte_count=byte_count, detail=json.dumps({"requested": len(ids), "failures": failures[:20]}))
    return store.report()


def discover_wikidata(http, *, after="", limit=100, degree_employer=False):
    """Keyset enumeration, never claimed as a frozen global census of people."""
    if after and not re.fullmatch(r"Q[1-9]\d*", after):
        raise ValueError("invalid cursor")
    if not 1 <= limit <= 200:
        raise ValueError("enumeration page bound is 200")
    if after:
        raise ValueError("live query discovery has no stable cursor; use persisted discovery queue")
    query = ('''SELECT DISTINCT ?person WHERE { ?person p:P69 ?s; wdt:P108 ?employer .
        ?s pq:P512 ?degree . VALUES ?degree { wd:Q752297 wd:Q849697 } }'''
        if degree_employer else '''SELECT DISTINCT ?person ?orcid WHERE { ?person wdt:P496 ?orcid; wdt:P69 ?school; wdt:P108 ?employer }''') + f" LIMIT {limit}"
    data, checksum = http.json("https://query.wikidata.org/sparql?format=json&query=" + quote(query))
    rows = data["results"]["bindings"]
    return {"query_sha256": digest(query), "response_sha256": checksum,
            "rows": [{"qid": r["person"]["value"].rsplit("/", 1)[-1], "orcid": r.get("orcid",{}).get("value")} for r in rows],
            "enumeration": "bounded-live-query-selection-not-global-enumeration"}
