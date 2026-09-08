"""Pure prepared/recoverable cohort-promotion primitives.

The functions deliberately perform no remote writes.  A hosted adapter commits
one returned state with its existing blob CAS, rereads, and calls ``recover``
again.  Consequently interruption at every operation boundary is safe.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from .catalog import activate_object, activate_source, validate_catalog
from .hashing import sha256_json
from .publication import IdentityCollisionError, PublicationError
import re

VERSION = "cohort_promotion_record_v1"
OPERATION_ORDER = ("accept_source_set", "accept_canonical_market",
                   "accept_sources", "consume_redfin")
SHA = re.compile(r"[0-9a-f]{64}")


def create_promotion_record(*, cycle_id: str, source_set_id: str,
        source_set_semantic_sha256: str, canonical_artifact_id: str,
        canonical_artifact_hash: str, expected_source_pointers: dict[str, str | None],
        target_source_pointers: dict[str, str], expected_source_set: str | None,
        expected_canonical: str | None, readiness_id: str, resolution_id: str) -> dict[str, Any]:
    if set(expected_source_pointers) != set(target_source_pointers):
        raise PublicationError("promotion source pointer inventories differ")
    if any(s in target_source_pointers for s in ("census_bps", "census_bps_provisional")):
        raise PublicationError("physical BPS pointers cannot participate in promotion")
    semantic = {"schema_version": VERSION, "cycle_id": cycle_id,
        "source_set_id": source_set_id, "source_set_semantic_sha256": source_set_semantic_sha256,
        "canonical_artifact_id": canonical_artifact_id,
        "canonical_artifact_hash": canonical_artifact_hash,
        "expected_source_pointers": dict(sorted(expected_source_pointers.items())),
        "target_source_pointers": dict(sorted(target_source_pointers.items())),
        "expected_source_set": expected_source_set, "expected_canonical": expected_canonical,
        "readiness_id": readiness_id, "resolution_id": resolution_id,
        "operation_order": list(OPERATION_ORDER)}
    record = {**semantic, "promotion_id": "cohort_promotion__" + sha256_json(semantic)[:24]}
    return validate_promotion_record(record)


def validate_promotion_record(record: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "promotion_id", "cycle_id", "source_set_id",
        "source_set_semantic_sha256", "canonical_artifact_id", "canonical_artifact_hash",
        "expected_source_pointers", "target_source_pointers", "expected_source_set",
        "expected_canonical", "readiness_id", "resolution_id", "operation_order"}
    if set(record) != required or record.get("schema_version") != VERSION \
            or tuple(record.get("operation_order", ())) != OPERATION_ORDER:
        raise PublicationError("promotion record schema/order mismatch")
    if not SHA.fullmatch(record["source_set_semantic_sha256"]) or not SHA.fullmatch(record["canonical_artifact_hash"]):
        raise PublicationError("promotion content hash invalid")
    semantic = {k: record[k] for k in required - {"promotion_id"}}
    if "cohort_promotion__" + sha256_json(semantic)[:24] != record["promotion_id"]:
        raise PublicationError("promotion identity mismatch")
    return record


def add_promotion_record(existing: dict[str, Any] | None, proposed: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    validate_promotion_record(proposed)
    if existing is None: return deepcopy(proposed), True
    if existing == proposed: return deepcopy(existing), False
    raise IdentityCollisionError("contradictory cohort promotion record")


def _pointer(current: Any, expected: Any, target: Any, label: str) -> bool:
    if current == target: return False
    if current != expected: raise IdentityCollisionError(f"{label} expected-old pointer contradiction")
    return True


def promotion_progress(record: dict[str, Any], catalog: dict[str, Any], readiness: dict[str, Any]) -> dict[str, Any]:
    validate_promotion_record(record); validate_catalog(catalog)
    accepted = catalog["accepted"]
    records = {(r["object_type"], r["object_id"]): r for r in catalog["immutable_records"]}
    if ("source_set", record["source_set_id"]) not in records or ("canonical_market", record["canonical_artifact_id"]) not in records:
        raise PublicationError("promotion target object is not cataloged")
    for source, target in record["target_source_pointers"].items():
        if ("source", target) not in records or records[("source", target)]["metadata"].get("source_id") != source:
            raise PublicationError(f"promotion source target is not cataloged: {source}")
    match = [r for r in readiness.get("records", []) if r.get("readiness_id") == record["readiness_id"]]
    if len(match) != 1 or match[0].get("cycle_id") != record["cycle_id"]:
        raise PublicationError("promotion Redfin readiness identity mismatch")
    if (match[0].get("source_id") != "redfin"
            or match[0].get("candidate_artifact_id") != record["target_source_pointers"].get("redfin")
            or type(match[0].get("consumed")) is not bool):
        raise PublicationError("promotion Redfin readiness state contradiction")
    source_done = {s: accepted["source"].get(s) == t for s, t in record["target_source_pointers"].items()}
    prior_done = (accepted.get("source_set") == record["source_set_id"]
                  and accepted.get("canonical_market") == record["canonical_artifact_id"]
                  and all(source_done.values()))
    if match[0]["consumed"] and not prior_done:
        raise PublicationError("Redfin readiness was consumed before prior promotion targets")
    return {"source_set_accepted": accepted.get("source_set") == record["source_set_id"],
        "canonical_accepted": accepted.get("canonical_market") == record["canonical_artifact_id"],
        "source_pointers": source_done, "redfin_consumed": match[0]["consumed"],
        "complete": prior_done and match[0]["consumed"]}


def recover_promotion(record: dict[str, Any], catalog: dict[str, Any], readiness: dict[str, Any],
                      *, max_operations: int | None = None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Apply at most ``max_operations`` atomic boundaries to fixture state.

    Production calls this with one operation, commits that document by blob CAS,
    then rereads both authorities.  Exact targets are treated as completed;
    values other than expected-old or target fail closed.
    """
    validate_promotion_record(record); out = deepcopy(catalog); ready = deepcopy(readiness)
    operations = 0
    def allowed() -> bool: return max_operations is None or operations < max_operations
    if _pointer(out["accepted"].get("source_set"), record["expected_source_set"], record["source_set_id"], "source-set") and allowed():
        out = activate_object(out, "source_set", record["source_set_id"]); operations += 1
    if out["accepted"].get("source_set") != record["source_set_id"]:
        return out, ready, promotion_progress(record, out, ready)
    if _pointer(out["accepted"].get("canonical_market"), record["expected_canonical"], record["canonical_artifact_id"], "canonical") and allowed():
        out = activate_object(out, "canonical_market", record["canonical_artifact_id"]); operations += 1
    if out["accepted"].get("canonical_market") != record["canonical_artifact_id"]:
        return out, ready, promotion_progress(record, out, ready)
    for source, target in record["target_source_pointers"].items():
        if _pointer(out["accepted"]["source"].get(source), record["expected_source_pointers"][source], target, source) and allowed():
            out = activate_source(out, source, target); operations += 1
        if out["accepted"]["source"].get(source) != target:
            return out, ready, promotion_progress(record, out, ready)
    matches = [r for r in ready["records"] if r.get("readiness_id") == record["readiness_id"]]
    if len(matches) != 1: raise PublicationError("promotion readiness identity does not resolve once")
    if not matches[0]["consumed"] and allowed():
        matches[0]["consumed"] = True; operations += 1
    return out, ready, promotion_progress(record, out, ready)
