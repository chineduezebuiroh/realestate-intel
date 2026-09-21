"""Pure Phase 3 preparation/preflight and authorization contracts.

Provider acquisition is deliberately absent. Hosted adapters may publish the
prepared objects and apply one CAS operation at a time using these records.
"""
from __future__ import annotations

from typing import Any

from core.source_artifacts.catalog import validate_catalog
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.promotion import (SOURCE_TRANSITION_ORDER,
    create_promotion_record, promotion_progress)
from core.source_artifacts.publication import PublicationError
from core.source_artifacts.source_set_v2 import validate_source_set_v2

FORBIDDEN = {"census_bps", "census_bps_provisional", "census_acs1",
             "census_acs5", "census_nrc_fred"}
AUTHORIZATION_PREFIX = "AUTHORIZE_COHORT_PLAN__"


def validate_logical_plan(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if plan.get("schema_version") != "monthly_logical_cohort_plan_v1":
        raise PublicationError("logical cohort plan schema mismatch")
    sources = plan.get("sources", [])
    indexed = {item.get("source_id"): item for item in sources}
    if len(indexed) != len(sources) or tuple(plan.get("logical_source_inventory", ())) != SOURCE_TRANSITION_ORDER:
        raise PublicationError("logical cohort plan is duplicated or unordered")
    if set(indexed) != set(SOURCE_TRANSITION_ORDER) or set(indexed) & FORBIDDEN:
        raise PublicationError("logical cohort plan must contain exactly nine governed sources")
    for source, item in indexed.items():
        if not item.get("artifact_id") or not item.get("artifact_content_hash") or not item.get("package_sha256"):
            raise PublicationError(f"logical cohort identity incomplete: {source}")
    return indexed


def authorization_token(promotion_record: dict[str, Any]) -> str:
    """Human-visible token bound to every immutable plan field."""
    return AUTHORIZATION_PREFIX + sha256_json(promotion_record)


def preflight(*, logical_plan: dict[str, Any], source_set: dict[str, Any],
              canonical_manifest: dict[str, Any], catalog: dict[str, Any],
              readiness: dict[str, Any], resolution_id: str) -> dict[str, Any]:
    """Produce immutable promotion evidence with zero mutation."""
    validate_catalog(catalog); validate_source_set_v2(source_set)
    indexed = validate_logical_plan(logical_plan)
    selected = {entry["source_id"]: entry["artifact_id"] for entry in source_set["sources"]}
    planned = {source: item["artifact_id"] for source, item in indexed.items()}
    if selected != planned or source_set["target_month"] not in logical_plan["cycle_id"]:
        raise PublicationError("Source Set is not the exact cycle logical plan")
    if canonical_manifest.get("source_set_id") != source_set["source_set_id"]:
        raise PublicationError("canonical market did not consume exact Source Set")
    records = {(r["object_type"], r["object_id"]): r for r in catalog["immutable_records"]}
    for source, artifact_id in planned.items():
        record = records.get(("source", artifact_id))
        if record is None or record["metadata"].get("source_id") != source:
            raise PublicationError(f"source publication missing: {source}")
        item = indexed[source]
        if record["artifact_content_hash"] != item["artifact_content_hash"] or record["package_sha256"] != item["package_sha256"]:
            raise PublicationError(f"source publication identity drift: {source}")
    ready = [r for r in readiness.get("records", [])
             if r.get("cycle_id") == logical_plan["cycle_id"] and r.get("source_id") == "redfin"]
    if len(ready) != 1 or ready[0].get("consumed") is not False \
            or ready[0].get("candidate_artifact_id") != planned["redfin"]:
        raise PublicationError("eligible unconsumed Redfin readiness does not resolve once")
    record = create_promotion_record(cycle_id=logical_plan["cycle_id"],
        source_set_id=source_set["source_set_id"],
        source_set_semantic_sha256=canonical_manifest["source_set_semantic_sha256"],
        canonical_artifact_id=canonical_manifest["market_artifact_id"],
        canonical_artifact_hash=sha256_json(canonical_manifest),
        expected_source_pointers={s: catalog["accepted"]["source"].get(s) for s in SOURCE_TRANSITION_ORDER},
        target_source_pointers=planned, expected_source_set=catalog["accepted"].get("source_set"),
        expected_canonical=catalog["accepted"].get("canonical_market"),
        readiness_id=ready[0]["readiness_id"], resolution_id=resolution_id)
    # This validates target catalog/readiness consistency and is observational.
    progress = promotion_progress(record, catalog, readiness)
    return {"schema_version":"cohort_promotion_preflight_v1", "cycle_id":logical_plan["cycle_id"],
            "promotion_record":record, "authorization_token":authorization_token(record),
            "progress":progress, "accepted_state_mutated":False,
            "provider_discovery_performed":False, "redfin_acquisition_performed":False}


def authorize(preflight_record: dict[str, Any], supplied_token: str) -> dict[str, Any]:
    record = preflight_record["promotion_record"]
    expected = authorization_token(record)
    if supplied_token != expected or preflight_record.get("authorization_token") != expected:
        raise PublicationError("authorization is not bound to this exact promotion plan")
    return record
