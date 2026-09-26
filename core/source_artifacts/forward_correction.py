"""Pure, fail-closed primitives for an authorized accepted-cohort correction.

This is deliberately not an ordinary cohort promotion: consumed Redfin
readiness is evidence, never a mutable operation.  Remote publication and CAS
storage are adapters around these pure primitives and are not performed here.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping
import re

import duckdb

from .catalog import activate_object, activate_source, validate_catalog
from .hashing import sha256_json
from .promotion import (LEGACY_SOURCE_TRANSITION_ORDER, LEGACY_VERSION,
                        promotion_progress, validate_promotion_record)
from .publication import IdentityCollisionError, PublicationError
from .source_set_v2 import create_source_set_v2, validate_source_set_v2

VERSION = "accepted_cohort_forward_correction_v1"
OPERATION_ORDER = ("accept_corrected_source_set", "accept_corrected_canonical",
                   "accept_fred_unemp")
SOURCES = ("fred_macro", "fred_unemp", "ces", "laus", "redfin", "bps", "acs",
           "bea_gdp_qtr", "bea_gdp_ann", "census_nrc")
UNCHANGED = tuple(source for source in SOURCES if source != "fred_unemp")
AUTHORIZATION_PREFIX = "AUTHORIZE_ACCEPTED_COHORT_FORWARD_CORRECTION__"
FRED_METRIC = "fred_unemployment_rate_sa"
FRED_GEOS = frozenset({"united_states__nation", "california__state",
    "district_of_columbia__state", "maryland__state", "new_jersey__state",
    "virginia__state"})


def build_corrected_source_set(output: Path, *, accepted_source_set: dict[str, Any],
        fred_unemp_entry: dict[str, Any], created_at: str, builder_git_sha: str) -> dict[str, Any]:
    """Create a v2 successor by adding one entry to an immutable nine-member v2 set."""
    old = validate_source_set_v2(deepcopy(accepted_source_set))
    if set(old["included_source_inventory"]) != set(UNCHANGED):
        raise PublicationError("correction requires exact historical nine-source Source Set")
    if fred_unemp_entry.get("source_id") != "fred_unemp":
        raise PublicationError("correction addition must be fred_unemp")
    corrected = create_source_set_v2(output, target_month=old["target_month"],
        created_at=created_at, builder_git_sha=builder_git_sha,
        entries=[*deepcopy(old["sources"]), deepcopy(fred_unemp_entry)],
        config_hashes=deepcopy(old["config_hashes"]),
        contract_versions=deepcopy(old["contract_versions"]),
        family_resolution=deepcopy(old["family_resolution"]))
    old_ids = {item["source_id"]: item["artifact_id"] for item in old["sources"]}
    new_ids = {item["source_id"]: item["artifact_id"] for item in corrected["sources"]}
    if any(new_ids[source] != artifact_id for source, artifact_id in old_ids.items()):
        raise AssertionError("correction changed an existing Source Set member")
    return corrected


def validate_canonical_factual_addition(*, accepted_database: Path,
        corrected_database: Path) -> dict[str, Any]:
    """Prove that corrected facts equal old facts plus governed FRED unemployment."""
    connection = duckdb.connect()
    try:
        old_path = str(accepted_database).replace("'", "''")
        corrected_path = str(corrected_database).replace("'", "''")
        connection.execute(f"ATTACH '{old_path}' AS old (READ_ONLY)")
        connection.execute(f"ATTACH '{corrected_path}' AS corrected (READ_ONLY)")
        columns = "geo_id,metric_id,date,property_type_id,value,source_id,property_type"
        removed = connection.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM old.fact_timeseries EXCEPT "
            f"SELECT {columns} FROM corrected.fact_timeseries)").fetchone()[0]
        added = connection.execute(
            f"SELECT {columns} FROM corrected.fact_timeseries EXCEPT "
            f"SELECT {columns} FROM old.fact_timeseries").fetchdf()
        duplicates = connection.execute("SELECT count(*) FROM (SELECT geo_id,metric_id,date,"
            "property_type_id,count(*) n FROM corrected.fact_timeseries GROUP BY ALL HAVING n>1)").fetchone()[0]
    finally:
        connection.close()
    if removed or added.empty:
        raise PublicationError("corrected canonical did not preserve all existing facts")
    if (set(added.source_id) != {"fred_unemp"} or set(added.metric_id) != {FRED_METRIC}
            or set(added.geo_id) != FRED_GEOS or duplicates):
        raise PublicationError("corrected canonical factual delta is not exact governed fred_unemp")
    return {"existing_fact_removals": 0, "added_fact_count": len(added),
            "added_source": "fred_unemp", "added_metric": FRED_METRIC,
            "added_geographies": sorted(FRED_GEOS), "duplicate_key_count": 0}


def _semantic(record: Mapping[str, Any]) -> dict[str, Any]:
    return {key: deepcopy(value) for key, value in record.items()
            if key not in {"correction_id", "authorization"}}


def authorization_token(record: Mapping[str, Any]) -> str:
    validate_correction_record(dict(record))
    return AUTHORIZATION_PREFIX + sha256_json(_semantic(record))


def create_correction_record(*, cycle_id: str, expected_source_set: str,
        expected_canonical: str, original_promotion_id: str,
        expected_source_pointers: Mapping[str, str | None],
        target_source_pointers: Mapping[str, str], corrected_source_set: str,
        corrected_source_set_hash: str, corrected_canonical: str,
        corrected_canonical_hash: str, readiness_evidence: Mapping[str, Any],
        family_resolution_lineage: Mapping[str, Any],
        governed_hashes: Mapping[str, str]) -> dict[str, Any]:
    expected = dict(expected_source_pointers)
    target = dict(target_source_pointers)
    semantic = {
        "schema_version": VERSION, "cycle_id": cycle_id,
        "expected_source_set": expected_source_set,
        "expected_canonical": expected_canonical,
        "original_promotion_id": original_promotion_id,
        "expected_source_pointers": dict(sorted(expected.items())),
        "target_source_pointers": dict(sorted(target.items())),
        "corrected_source_set": corrected_source_set,
        "corrected_source_set_hash": corrected_source_set_hash,
        "corrected_canonical": corrected_canonical,
        "corrected_canonical_hash": corrected_canonical_hash,
        "supersession": {"source_set": expected_source_set,
                          "canonical_market": expected_canonical},
        "family_resolution_lineage": deepcopy(family_resolution_lineage),
        "governed_hashes": dict(sorted(governed_hashes.items())),
        "readiness_evidence": deepcopy(readiness_evidence),
        "operation_order": list(OPERATION_ORDER),
        "recovery_contract": "expected-old-or-target-cas_v1",
    }
    correction_id = "cohort_correction__" + sha256_json(semantic)[:24]
    record = {**semantic, "correction_id": correction_id,
              "authorization": {"scheme": AUTHORIZATION_PREFIX.rstrip("_"),
                                "plan_sha256": sha256_json(semantic)}}
    return validate_correction_record(record)


def validate_correction_record(record: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "correction_id", "cycle_id", "expected_source_set",
        "expected_canonical", "original_promotion_id", "expected_source_pointers",
        "target_source_pointers", "corrected_source_set", "corrected_source_set_hash",
        "corrected_canonical", "corrected_canonical_hash", "supersession",
        "family_resolution_lineage", "governed_hashes", "readiness_evidence",
        "operation_order", "recovery_contract", "authorization"}
    if set(record) != required or record.get("schema_version") != VERSION:
        raise PublicationError("correction record schema mismatch")
    if tuple(record["operation_order"]) != OPERATION_ORDER:
        raise PublicationError("correction transition order mismatch")
    expected, target = record["expected_source_pointers"], record["target_source_pointers"]
    if set(expected) != set(SOURCES) or set(target) != set(SOURCES):
        raise PublicationError("correction requires exact ten-source pointer inventory")
    if expected["fred_unemp"] is not None or not target["fred_unemp"]:
        raise PublicationError("correction requires absent-to-target fred_unemp")
    if any(expected[source] != target[source] for source in UNCHANGED):
        raise PublicationError("correction may only add fred_unemp")
    if record["supersession"] != {"source_set": record["expected_source_set"],
                                  "canonical_market": record["expected_canonical"]}:
        raise PublicationError("correction supersession mismatch")
    if (set(record["family_resolution_lineage"]) != {"bps", "acs"} or
            not all(record["family_resolution_lineage"].values())):
        raise PublicationError("correction requires exact BPS/ACS resolution lineage")
    if (not record["governed_hashes"] or
            any(not re.fullmatch(r"[0-9a-f]{64}", value)
                for value in record["governed_hashes"].values())):
        raise PublicationError("correction governed hashes invalid")
    evidence = record["readiness_evidence"]
    if (set(evidence) != {"readiness_id", "record_sha256", "consumed_by_promotion_id"} or
            not re.fullmatch(r"[0-9a-f]{64}", evidence["record_sha256"]) or
            evidence["consumed_by_promotion_id"] != record["original_promotion_id"]):
        raise PublicationError("correction readiness evidence contract mismatch")
    semantic = _semantic(record)
    if record["correction_id"] != "cohort_correction__" + sha256_json(semantic)[:24]:
        raise PublicationError("correction identity mismatch")
    if record["authorization"] != {"scheme": AUTHORIZATION_PREFIX.rstrip("_"),
                                    "plan_sha256": sha256_json(semantic)}:
        raise PublicationError("correction authorization identity mismatch")
    return record


def add_correction_record(existing: dict[str, Any] | None,
                          proposed: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    validate_correction_record(proposed)
    if existing is None:
        return deepcopy(proposed), True
    if existing == proposed:
        return deepcopy(existing), False
    raise IdentityCollisionError("contradictory immutable correction record")


def _readiness(record: Mapping[str, Any], readiness: Mapping[str, Any]) -> dict[str, Any]:
    evidence = record["readiness_evidence"]
    matches = [item for item in readiness.get("records", [])
               if item.get("readiness_id") == evidence.get("readiness_id")]
    if len(matches) != 1:
        raise PublicationError("correction Redfin readiness identity mismatch")
    item = matches[0]
    if (item.get("consumed") is not True or item.get("cycle_id") != record["cycle_id"] or
            item.get("candidate_artifact_id") != record["target_source_pointers"]["redfin"] or
            sha256_json(item) != evidence.get("record_sha256")):
        raise PublicationError("correction requires exact consumed Redfin readiness evidence")
    return item


def correction_progress(record: dict[str, Any], catalog: dict[str, Any],
                        readiness: dict[str, Any]) -> dict[str, Any]:
    validate_correction_record(record); validate_catalog(catalog); _readiness(record, readiness)
    accepted = catalog["accepted"]
    records = {(item["object_type"], item["object_id"]): item
               for item in catalog["immutable_records"]}
    for kind, object_id in (("source_set", record["expected_source_set"]),
                            ("canonical_market", record["expected_canonical"]),
                            ("source_set", record["corrected_source_set"]),
                            ("canonical_market", record["corrected_canonical"]),
                            ("source", record["target_source_pointers"]["fred_unemp"])):
        if (kind, object_id) not in records:
            raise PublicationError(f"correction immutable object is not cataloged: {kind}")
    if (records[("source_set", record["corrected_source_set"])]["metadata"].get(
            "supersedes_artifact_id") != record["expected_source_set"] or
            records[("canonical_market", record["corrected_canonical"])]["metadata"].get(
            "supersedes_artifact_id") != record["expected_canonical"]):
        raise PublicationError("correction catalog supersession lineage mismatch")
    if (records[("source_set", record["corrected_source_set"])]["artifact_content_hash"] !=
            record["corrected_source_set_hash"] or
            records[("canonical_market", record["corrected_canonical"])]["artifact_content_hash"] !=
            record["corrected_canonical_hash"]):
        raise PublicationError("correction catalog target content hash mismatch")
    if records[("source", record["target_source_pointers"]["fred_unemp"])]["metadata"].get("source_id") != "fred_unemp":
        raise PublicationError("correction fred_unemp target ownership mismatch")
    for source in UNCHANGED:
        if accepted["source"].get(source) != record["expected_source_pointers"][source]:
            raise IdentityCollisionError(f"unchanged source pointer moved: {source}")
    if accepted.get("serving_market") is not None:
        raise IdentityCollisionError("serving pointer changed during correction")
    states = {"source_set": accepted.get("source_set") == record["corrected_source_set"],
              "canonical": accepted.get("canonical_market") == record["corrected_canonical"],
              "fred_unemp": accepted["source"].get("fred_unemp") == record["target_source_pointers"]["fred_unemp"]}
    return {**states, "complete": all(states.values()),
            "readiness_verified_consumed_unchanged": True,
            "next_operation": next((name for name, done in zip(OPERATION_ORDER, states.values())
                                    if not done), None)}


def preflight_correction(record: dict[str, Any], catalog: dict[str, Any],
        readiness: dict[str, Any], original_promotion: dict[str, Any]) -> dict[str, Any]:
    """Validate preparation without mutating either accepted authority."""
    before_catalog, before_readiness = sha256_json(catalog), sha256_json(readiness)
    validate_correction_record(record); validate_catalog(catalog); _readiness(record, readiness)
    if (catalog["accepted"].get("source_set") != record["expected_source_set"] or
            catalog["accepted"].get("canonical_market") != record["expected_canonical"] or
            catalog["accepted"]["source"].get("fred_unemp") is not None):
        raise PublicationError("preflight expected-old accepted state is stale")
    validate_promotion_record(original_promotion)
    if (original_promotion["schema_version"] != LEGACY_VERSION or
            tuple(original_promotion["target_source_pointers"]) != LEGACY_SOURCE_TRANSITION_ORDER or
            original_promotion["promotion_id"] != record["original_promotion_id"] or
            original_promotion["cycle_id"] != record["cycle_id"] or
            original_promotion["source_set_id"] != record["expected_source_set"] or
            original_promotion["canonical_artifact_id"] != record["expected_canonical"]):
        raise PublicationError("correction original historical promotion mismatch")
    old_progress = promotion_progress(original_promotion, catalog, readiness)
    if not old_progress["complete"]:
        raise PublicationError("original promotion is not the completed readiness consumer")
    progress = correction_progress(record, catalog, readiness)
    if any(progress[key] for key in ("source_set", "canonical", "fred_unemp")):
        raise PublicationError("preflight expected-old state is stale")
    assert before_catalog == sha256_json(catalog) and before_readiness == sha256_json(readiness)
    return {"correction_id": record["correction_id"], "preflight": "passed",
            "authorization_token": authorization_token(record), "progress": progress,
            "accepted_state_mutated": False, "readiness_mutated": False,
            "provider_acquisition_performed": False}


def recover_correction(record: dict[str, Any], catalog: dict[str, Any],
        readiness: dict[str, Any], *, supplied_authorization: str,
        max_operations: int | None = None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if supplied_authorization != authorization_token(record):
        raise PublicationError("exact correction authorization token required")
    original_readiness = deepcopy(readiness)
    out = deepcopy(catalog)
    correction_progress(record, out, readiness)
    operations = 0
    def allowed() -> bool:
        return max_operations is None or operations < max_operations
    transitions = (("source_set", record["expected_source_set"], record["corrected_source_set"]),
                   ("canonical_market", record["expected_canonical"], record["corrected_canonical"]),
                   ("fred_unemp", None, record["target_source_pointers"]["fred_unemp"]))
    for kind, expected, target in transitions:
        current = (out["accepted"]["source"].get(kind) if kind == "fred_unemp"
                   else out["accepted"].get(kind))
        if current == target:
            continue
        if current != expected:
            raise IdentityCollisionError(f"{kind} correction expected-old pointer contradiction")
        if not allowed():
            break
        out = (activate_source(out, kind, target) if kind == "fred_unemp"
               else activate_object(out, kind, target))
        operations += 1
    progress = correction_progress(record, out, original_readiness)
    return out, original_readiness, progress
