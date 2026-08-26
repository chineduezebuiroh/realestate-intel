"""Durable Redfin catalyst control-plane contract."""
from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path
from typing import Any

from core.source_artifacts.catalog import validate_catalog
from core.source_artifacts.hashing import sha256_file
from jobs.monthly_refresh.production import cycle_id

SCHEMA = "monthly_refresh_readiness_v1"
RECORD_SCHEMA = "redfin_candidate_readiness_v1"


def readiness_id(cycle: str) -> str:
    return f"redfin_readiness__{cycle}"


def empty_readiness() -> dict[str, Any]:
    return {"schema_version": SCHEMA, "records": []}


def validate_record(record: dict[str, Any], *, catalog: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    required = {"schema_version", "readiness_id", "source_id", "drop_id", "drop_content_hash",
        "target_month", "cycle_id", "candidate_artifact_id", "artifact_content_hash", "package_sha256",
        "publication_state", "validation_status", "remote_repository", "release_id", "asset_id", "consumed"}
    if set(record) != required or record.get("schema_version") != RECORD_SCHEMA:
        raise ValueError("Redfin readiness schema mismatch")
    if record["source_id"] != "redfin" or record["publication_state"] != "published_immutable_verified" \
            or record["validation_status"] != "passed" or type(record["consumed"]) is not bool:
        raise ValueError("Redfin readiness is not validated and durably verified")
    expected_cycle = cycle_id(redfin_drop_id=record["drop_id"], redfin_drop_hash=record["drop_content_hash"],
        target_month=record["target_month"], policy_sha256=sha256_file(policy_path))
    if record["cycle_id"] != expected_cycle or record["readiness_id"] != readiness_id(expected_cycle):
        raise ValueError("Redfin readiness cycle identity mismatch")
    validate_catalog(catalog)
    matches = [r for r in catalog["immutable_records"] if r["object_type"] == "source"
        and r["object_id"] == record["candidate_artifact_id"]]
    if len(matches) != 1: raise ValueError("Redfin readiness candidate is missing from catalog")
    artifact = matches[0]
    pairs = {"artifact_content_hash":"artifact_content_hash", "package_sha256":"package_sha256",
        "remote_repository":"remote_repository", "release_id":"release_id", "asset_id":"asset_id"}
    if artifact["metadata"].get("source_id") != "redfin" or artifact["metadata"].get("provider_release_id") != record["drop_id"]:
        raise ValueError("Redfin readiness provider release mismatch")
    if artifact["publication_state"] != record["publication_state"]:
        raise ValueError("Redfin readiness publication state mismatch")
    for left, right in pairs.items():
        if record[left] != artifact[right]: raise ValueError(f"Redfin readiness {left} mismatch")
    return record


def validate_readiness(state: dict[str, Any], *, catalog: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    if set(state) != {"schema_version", "records"} or state.get("schema_version") != SCHEMA:
        raise ValueError("monthly readiness control-plane schema mismatch")
    identities: dict[str, dict[str, Any]] = {}
    for record in state["records"]:
        validate_record(record, catalog=catalog, policy_path=policy_path)
        old = identities.get(record["readiness_id"])
        if old is not None and old != record: raise ValueError("conflicting Redfin readiness identity")
        if old is not None: raise ValueError("duplicate Redfin readiness identity")
        identities[record["readiness_id"]] = record
    if state["records"] != sorted(state["records"], key=lambda r:r["readiness_id"]):
        raise ValueError("Redfin readiness records are not canonically sorted")
    return state


def add_readiness(state: dict[str, Any], record: dict[str, Any], *, catalog: dict[str, Any], policy_path: Path) -> tuple[dict[str, Any], bool]:
    validate_readiness(state, catalog=catalog, policy_path=policy_path)
    validate_record(record, catalog=catalog, policy_path=policy_path)
    out = deepcopy(state)
    for old in out["records"]:
        if old["readiness_id"] == record["readiness_id"]:
            if old == record: return out, False
            raise ValueError("conflicting Redfin readiness identity")
    out["records"].append(deepcopy(record)); out["records"].sort(key=lambda r:r["readiness_id"])
    return validate_readiness(out, catalog=catalog, policy_path=policy_path), True


def eligible_record(state: dict[str, Any], *, catalog: dict[str, Any], policy_path: Path,
                    requested_cycle_id: str | None = None) -> dict[str, Any] | None:
    validate_readiness(state, catalog=catalog, policy_path=policy_path)
    candidates = [r for r in state["records"] if not r["consumed"]]
    if requested_cycle_id is not None:
        candidates = [r for r in candidates if r["cycle_id"] == requested_cycle_id]
    if not candidates: return None
    if len(candidates) != 1: raise ValueError("ambiguous eligible Redfin catalysts")
    return deepcopy(candidates[0])


def make_record(*, drop_id: str, drop_content_hash: str, target_month: str, cycle: str,
                artifact: dict[str, Any]) -> dict[str, Any]:
    return {"schema_version":RECORD_SCHEMA, "readiness_id":readiness_id(cycle), "source_id":"redfin",
        "drop_id":drop_id, "drop_content_hash":drop_content_hash, "target_month":target_month,
        "cycle_id":cycle, "candidate_artifact_id":artifact["object_id"],
        "artifact_content_hash":artifact["artifact_content_hash"], "package_sha256":artifact["package_sha256"],
        "publication_state":artifact["publication_state"], "validation_status":"passed",
        "remote_repository":artifact["remote_repository"], "release_id":artifact["release_id"],
        "asset_id":artifact["asset_id"], "consumed":False}
