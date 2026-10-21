from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


VALID_STAGES = {"bachelor", "master", "doctorate", "postdoc"}
VALID_EVIDENCE = {"official_cv", "official_profile", "orcid_public", "openalex_inferred"}


@dataclass(frozen=True)
class Institution:
    id: str
    name: str
    country: str
    region: str
    institution_type: str
    aliases: tuple[str, ...] = ()
    stable_id: str | None = None


@dataclass(frozen=True)
class EducationEvent:
    person_id: str
    institution_id: str
    stage: str
    year: int | None
    evidence_id: str
    source_class: str
    accepted: bool = True

    def __post_init__(self) -> None:
        if self.stage not in VALID_STAGES:
            raise ValueError(f"invalid stage: {self.stage}")


@dataclass(frozen=True)
class EmploymentEvent:
    person_id: str
    institution_id: str
    role_type: str
    year_start: int | None
    evidence_id: str
    source_class: str
    accepted: bool = True
    joint_fraction: float | None = None
    is_joint_appointment: bool = False


@dataclass(frozen=True)
class AwardEvent:
    award_id: str
    year: int
    recipient_id: str | None
    category: str
    institution_id: str | None
    producing_institution_id: str | None
    training_institution_id: str | None
    evidence_id: str
    accepted: bool = True


@dataclass(frozen=True)
class Evidence:
    id: str
    url: str
    retrieved_at: str
    source_class: str
    evidence_level: str
    license_status: str
    content_sha256: str


def parse_dataset(payload: dict[str, Any]) -> tuple[list[Institution], list[EducationEvent], list[EmploymentEvent], list[AwardEvent], list[Evidence]]:
    institutions = [Institution(**row) for row in payload.get("institutions", [])]
    education = [EducationEvent(**row) for row in payload.get("education_events", [])]
    employment = [EmploymentEvent(**row) for row in payload.get("employment_events", [])]
    awards = [AwardEvent(**row) for row in payload.get("awards", [])]
    evidence = [Evidence(**row) for row in payload.get("evidence", [])]
    evidence_by_id = {item.id: item for item in evidence}
    if len(evidence_by_id) != len(evidence):
        raise ValueError("evidence IDs must be unique")
    for item in evidence:
        if not item.url.startswith(("https://", "http://")):
            raise ValueError(f"evidence {item.id} must have an HTTP(S) source URL")
        if not re.fullmatch(r"[0-9a-f]{64}", item.content_sha256):
            raise ValueError(f"evidence {item.id} must have a lowercase SHA-256 content hash")
    institution_ids = {item.id for item in institutions}
    if len(institution_ids) != len(institutions):
        raise ValueError("institution IDs must be unique")
    if any(event.institution_id not in institution_ids for event in education):
        raise ValueError("education event references an unknown institution")
    if any(event.institution_id not in institution_ids for event in employment):
        raise ValueError("employment event references an unknown institution")
    for event in [*education, *employment, *awards]:
        if event.evidence_id not in evidence_by_id:
            raise ValueError(f"event references missing evidence: {event.evidence_id}")
        if event.accepted and evidence_by_id[event.evidence_id].license_status not in {"approved", "synthetic_demo"}:
            raise ValueError(f"accepted event has non-approved evidence license: {event.evidence_id}")
        if hasattr(event, "source_class") and event.source_class != evidence_by_id[event.evidence_id].source_class:
            raise ValueError(f"event source class does not match evidence: {event.evidence_id}")
    for award in awards:
        for institution_id in (award.institution_id, award.producing_institution_id, award.training_institution_id):
            if institution_id is not None and institution_id not in institution_ids:
                raise ValueError(f"award references an unknown institution: {institution_id}")
    return institutions, education, employment, awards, evidence
