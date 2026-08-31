"""Pure LAUS annual-processing policy and immutable satisfaction primitives.

Collection/parsing of BLS publications and hosted execution intentionally remain
outside this C1 module.  Callers must supply normalized evidence whose provenance
is validated here; numeric changes and observation footnotes are never readiness.
"""
from __future__ import annotations

import hashlib
import json
import base64
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping
from urllib.parse import quote, urlparse

from core.source_artifacts.github_release import GitHubAPI
from core.source_artifacts.hashing import canonical_json_bytes
from core.source_artifacts.publication import (IdentityCollisionError, PublicationError,
                                               TransientPublicationError)

POLICY_SCHEMA = "laus_annual_processing_policy_v1"
EVIDENCE_SCHEMA = "laus_annual_processing_evidence_v1"
SATISFACTION_SCHEMA = "laus_annual_deep_satisfaction_v1"
SATISFACTION_ROOT = "config/laus_annual_deep_satisfactions"


class AnnualState(str, Enum):
    NOT_EXPECTED = "NOT_EXPECTED"
    WATCHING = "WATCHING"
    READY_FOR_ANNUAL_DEEP = "READY_FOR_ANNUAL_DEEP"
    ANNUAL_DEEP_SATISFIED = "ANNUAL_DEEP_SATISFIED"


@dataclass(frozen=True)
class AnnualDecision:
    state: AnnualState
    acquisition_mode: str
    annual_vintage_id: str | None
    annual_reference_year: int | None
    evidence: dict[str, Any]


def classify_governed_processing_classes(
    registry: Iterable[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, tuple[str, ...]]:
    """Classify exact BLS area-code structures, never geography display names."""
    mapping = policy["series_prefix_processing_class"]
    grouped: dict[str, list[str]] = {}
    for row in registry:
        area = str(row["provider_area_code"])
        prefix = area[:2]
        if prefix not in mapping:
            raise ValueError(f"unclassified governed LAUS area-code prefix: {prefix}")
        grouped.setdefault(mapping[prefix], []).append(str(row["series_id"]))
    actual = set(grouped)
    required = set(policy["required_processing_classes"])
    if actual != required:
        raise ValueError(f"governed processing-class mismatch: {sorted(actual)}")
    return {key: tuple(sorted(value)) for key, value in sorted(grouped.items())}


def annual_vintage_id(annual_reference_year: int) -> str:
    year = int(annual_reference_year)
    if year < 1976:
        raise ValueError("invalid LAUS annual reference year")
    return f"bls-laus-annual-processing-v1:{year}"


def _official_url(url: str, allowed_hosts: set[str]) -> bool:
    parsed = urlparse(url)
    return parsed.scheme == "https" and parsed.hostname in allowed_hosts


def _normalize_evidence(evidence: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    if evidence.get("schema_version") != EVIDENCE_SCHEMA:
        raise ValueError("unsupported LAUS annual evidence schema")
    year = int(evidence["annual_reference_year"])
    event = evidence.get("release_event")
    classes = evidence.get("processing_classes")
    if not isinstance(event, Mapping) or not isinstance(classes, list):
        raise ValueError("malformed LAUS annual evidence")
    allowed = set(policy["authoritative_provider_hosts"])
    expected_type = policy["authoritative_evidence_type"]
    normalized_event = dict(event)
    if normalized_event.get("expected") is not True:
        raise ValueError("release event evidence must explicitly be expected")
    for item in [normalized_event, *classes]:
        if item.get("evidence_type") != expected_type or not item.get("provider_release_id"):
            raise ValueError("annual evidence is not authoritative BLS publication evidence")
        if not _official_url(str(item.get("authoritative_url", "")), allowed):
            raise ValueError("annual evidence URL is not an allowed authoritative BLS URL")
        if int(item.get("annual_reference_year", -1)) != year:
            raise ValueError("contradictory annual reference year")
    by_class: dict[str, dict[str, Any]] = {}
    for raw in classes:
        item = dict(raw); name = item.get("processing_class")
        if name in by_class:
            raise ValueError("duplicate annual processing-class evidence")
        if name not in policy["required_processing_classes"]:
            raise ValueError("unexpected annual processing class")
        if item.get("status") not in {"underway", "complete"}:
            raise ValueError("invalid annual processing-class status")
        by_class[name] = item
    return {"annual_reference_year": year, "release_event": normalized_event,
            "processing_classes": [by_class[key] for key in sorted(by_class)]}


def evaluate(
    *, policy: Mapping[str, Any], evidence: Mapping[str, Any] | None,
    satisfactions: Iterable[Mapping[str, Any]] = (), numeric_changes: bool | None = None,
    observation_footnote_codes: Iterable[str] = (),
) -> AnnualDecision:
    """Select scope. Numeric values and API footnotes are diagnostic-only inputs."""
    if policy.get("schema_version") != POLICY_SCHEMA or policy.get("source_id") != "laus":
        raise ValueError("unsupported LAUS annual-processing policy")
    diagnostic = {"numeric_changes_observed": numeric_changes,
                  "observation_footnote_codes": sorted(set(observation_footnote_codes))}
    if not policy.get("enabled") or evidence is None:
        return AnnualDecision(AnnualState.NOT_EXPECTED, policy["ordinary_mode"], None, None,
                              {**diagnostic, "authoritative_release_event": False})
    normalized = _normalize_evidence(evidence, policy)
    year = normalized["annual_reference_year"]; vintage = annual_vintage_id(year)
    matches = [record for record in satisfactions if record.get("annual_vintage_id") == vintage]
    if len(matches) > 1:
        first = matches[0]
        if any(_semantic_satisfaction(item) != _semantic_satisfaction(first) for item in matches[1:]):
            raise ValueError("conflicting durable annual satisfaction records")
        matches = [first]
    complete = {item["processing_class"] for item in normalized["processing_classes"]
                if item["status"] == "complete"}
    required = set(policy["required_processing_classes"])
    detail = {**diagnostic, **normalized, "annual_vintage_id": vintage,
              "complete_processing_classes": sorted(complete),
              "missing_processing_classes": sorted(required - complete)}
    if matches:
        validate_satisfaction(matches[0])
        return AnnualDecision(AnnualState.ANNUAL_DEEP_SATISFIED, policy["ordinary_mode"], vintage, year, detail)
    if complete == required:
        return AnnualDecision(AnnualState.READY_FOR_ANNUAL_DEEP, policy["annual_deep_mode"], vintage, year, detail)
    return AnnualDecision(AnnualState.WATCHING, policy["ordinary_mode"], vintage, year, detail)


def satisfaction_record(*, decision: AnnualDecision, result: Mapping[str, Any],
                        cycle_id: str) -> dict[str, Any]:
    """Create satisfaction only from a successful, immutable governed deep result."""
    if decision.state != AnnualState.READY_FOR_ANNUAL_DEEP or decision.acquisition_mode != "annual_deep":
        raise ValueError("annual vintage is not ready for satisfaction")
    required = {"status": "succeeded", "validation_status": "passed",
                "publication_state": "published_verified", "source_id": "laus",
                "acquisition_mode": "annual_deep"}
    for field, expected in required.items():
        if result.get(field) != expected:
            raise ValueError(f"annual deep result is not satisfiable: {field}")
    for field in ("candidate_artifact_id", "artifact_content_hash", "package_sha256", "provider_release_id"):
        if not result.get(field):
            raise ValueError(f"annual deep result missing {field}")
    record = {"schema_version": SATISFACTION_SCHEMA, "source_id": "laus",
              "annual_vintage_id": decision.annual_vintage_id,
              "annual_reference_year": decision.annual_reference_year,
              "detector_evidence": decision.evidence,
              "status": "satisfied", "satisfied_artifact_id": result["candidate_artifact_id"],
              "satisfied_artifact_content_hash": result["artifact_content_hash"],
              "satisfied_package_sha256": result["package_sha256"],
              "provider_release_id": result["provider_release_id"], "cycle_id": cycle_id}
    validate_satisfaction(record)
    return record


def validate_satisfaction(record: Mapping[str, Any]) -> None:
    required = {"schema_version", "source_id", "annual_vintage_id", "annual_reference_year",
                "detector_evidence", "status", "satisfied_artifact_id",
                "satisfied_artifact_content_hash", "satisfied_package_sha256",
                "provider_release_id", "cycle_id"}
    if set(record) != required or record.get("schema_version") != SATISFACTION_SCHEMA:
        raise ValueError("annual satisfaction schema mismatch")
    if record.get("source_id") != "laus" or record.get("status") != "satisfied":
        raise ValueError("annual satisfaction invariant mismatch")
    if record.get("annual_vintage_id") != annual_vintage_id(int(record["annual_reference_year"])):
        raise ValueError("annual satisfaction vintage contradiction")
    if any(not record.get(field) for field in ("satisfied_artifact_id", "satisfied_artifact_content_hash",
                                                "satisfied_package_sha256", "provider_release_id", "cycle_id")):
        raise ValueError("annual satisfaction identity is incomplete")


def _semantic_satisfaction(record: Mapping[str, Any]) -> str:
    validate_satisfaction(record)
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def add_satisfaction(existing: Mapping[str, Any] | None,
                     proposed: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    """Exact-idempotent, conflict-fail primitive for a CAS-backed durable store."""
    proposed_value = dict(proposed); proposed_hash = _semantic_satisfaction(proposed_value)
    if existing is None:
        return proposed_value, True
    existing_value = dict(existing)
    if existing_value.get("annual_vintage_id") != proposed_value.get("annual_vintage_id"):
        raise ValueError("annual satisfaction key mismatch")
    if _semantic_satisfaction(existing_value) == proposed_hash:
        return existing_value, False
    raise IdentityCollisionError(f"annual satisfaction collision for {proposed_value['annual_vintage_id']}")


def satisfaction_path(annual_reference_year: int) -> str:
    year = int(annual_reference_year)
    annual_vintage_id(year)  # validates the semantic key
    return f"{SATISFACTION_ROOT}/{year}.json"


class GitHubAnnualSatisfactionStore:
    """One immutable Contents/CAS object per annual vintage."""

    def __init__(self, api: GitHubAPI, branch: str, *, attempts: int = 4):
        self.api, self.branch, self.attempts = api, branch, attempts

    def _read(self, path: str) -> tuple[dict[str, Any] | None, str | None]:
        item, _ = self.api.request("GET", f"/contents/{quote(path, safe='/')}?ref={quote(self.branch)}",
                                   expected=(200, 404))
        if item is None:
            return None, None
        return json.loads(base64.b64decode(item["content"])), item["sha"]

    def put(self, proposed: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        validate_satisfaction(proposed)
        path = satisfaction_path(int(proposed["annual_reference_year"]))
        for attempt in range(self.attempts):
            existing, oid = self._read(path)
            value, changed = add_satisfaction(existing, proposed)
            if not changed:
                return value, False
            payload = {"message": f"Satisfy LAUS annual processing {proposed['annual_reference_year']}",
                       "content": base64.b64encode(canonical_json_bytes(value)).decode(),
                       "branch": self.branch}
            if oid is not None:
                payload["sha"] = oid
            try:
                self.api.request("PUT", f"/contents/{quote(path, safe='/')}", payload=payload,
                                 expected=(200, 201))
                return value, True
            except TransientPublicationError:
                if attempt + 1 == self.attempts:
                    raise
            except PublicationError:
                if attempt + 1 == self.attempts:
                    raise PublicationError("LAUS annual satisfaction CAS retries exhausted")
            time.sleep(0.1 * (attempt + 1))
        raise AssertionError("unreachable")
