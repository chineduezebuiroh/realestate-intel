"""Prepare the post-barrier logical cohort without performing live promotion."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.catalog import validate_catalog
from core.source_artifacts.hashing import sha256_file, sha256_json
from core.source_artifacts.source_set_v2 import (FAMILY_MAP_VERSION, create_source_set_v2,
    governed_config_hashes)
from jobs.monthly_refresh.production import validate_source_result
from jobs.monthly_refresh.readiness import eligible_record


def _catalog_source(catalog: Mapping[str, Any], source_id: str, artifact_id: str) -> dict[str, Any]:
    matches = [r for r in catalog["immutable_records"] if r["object_type"] == "source"
               and r["object_id"] == artifact_id and r["metadata"].get("source_id") == source_id]
    if len(matches) != 1: raise ValueError(f"exact catalog source does not resolve once: {source_id}")
    return matches[0]


def _entry(record: Mapping[str, Any], *, status: str, carried: bool) -> dict[str, Any]:
    return {"source_id": record["metadata"]["source_id"], "artifact_id": record["object_id"],
        "logical_artifact_uri": record["logical_artifact_uri"], "package_sha256": record["package_sha256"],
        "artifact_content_hash": record["artifact_content_hash"],
        "provider_release_id": record["metadata"]["provider_release_id"],
        "observation_max": record["metadata"]["observation_max"], "validation_status": "passed",
        "monthly_status": status, "release_tag": record["release_tag"], "asset_id": record["asset_id"],
        "publication_receipt_id": record["publication_receipt_id"], "cycle_check_succeeded": True,
        "carried_forward": carried, "carry_forward_policy_allowed": False}


def build_logical_source_set(*, output: Path, cycle_id: str, target_month: str,
        physical_results: list[dict[str, Any]], catalog: dict[str, Any], readiness: dict[str, Any],
        resolution: dict[str, Any], family_parent_republications: list[dict[str, Any]],
        created_at: str, builder_git_sha: str,
        repository_root: Path = Path(".")) -> dict[str, Any]:
    """Map complete physical results to exact logical assembly inputs."""
    validate_catalog(catalog)
    expected_physical = {"census_bps", "census_bps_provisional", "ces", "fred_macro", "laus", "redfin"}
    by_source = {}
    for result in physical_results:
        result = validate_source_result(result, expected_cycle_id=cycle_id)
        if result["status"] != "succeeded" or result["source_id"] in by_source:
            raise ValueError("physical barrier is incomplete or duplicated")
        by_source[result["source_id"]] = result
    if set(by_source) != expected_physical: raise ValueError("exact physical barrier inventory mismatch")
    ready = eligible_record(readiness, catalog=catalog,
        policy_path=repository_root / "config/monthly_refresh_policy.json", requested_cycle_id=cycle_id)
    if ready is None or ready["consumed"]: raise ValueError("exact Redfin readiness is absent or consumed")
    required_resolution = {"schema_version":"bps_family_resolution_record_v1", "cycle_id":cycle_id,
        "resolver_version":"bps_family_resolver_v1", "source_contract_version":"bps_governed_source_v1"}
    if any(resolution.get(k) != v for k, v in required_resolution.items()):
        raise ValueError("BPS family resolution contract/cycle mismatch")
    if any(resolution.get(field) is not False for field in ("accepted_pointer_changed",
            "source_set_created", "duckdb_mutated", "redfin_consumed", "provider_discovery_performed")):
        raise ValueError("BPS family resolution contains forbidden side effects")
    config_hashes = resolution.get("config_hashes", {})
    if not config_hashes or any(sha256_file(repository_root / path) != digest
                                for path, digest in config_hashes.items()):
        raise ValueError("BPS family resolution governed config drift")
    resolution_semantic = {key:resolution[key] for key in ("cycle_id", "resolver_version",
        "source_contract_version", "parents", "config_hashes", "output_artifact_id", "output_content_hash")}
    if resolution["resolution_id"] != "bps_family_resolution__" + sha256_json(resolution_semantic)[:24]:
        raise ValueError("BPS family resolution identity mismatch")
    parents = sorted(resolution["parents"], key=lambda p:p["source_id"])
    if {p["source_id"] for p in parents} != {"census_bps", "census_bps_provisional"}:
        raise ValueError("BPS family parent inventory mismatch")
    republications = {r.get("source_id"):r for r in family_parent_republications}
    if set(republications) != {"census_bps", "census_bps_provisional"}:
        raise ValueError("BPS family republication inventory mismatch")
    for parent in parents:
        result = by_source[parent["source_id"]]
        republication = republications[parent["source_id"]]
        expected_republication = {"schema_version":"bps_source_republication_v1",
            "parent_cycle_id":cycle_id, "source_contract_version":"bps_governed_source_v1",
            "parent_candidate_artifact_id":result["candidate_artifact_id"],
            "parent_candidate_content_hash":result["artifact_content_hash"],
            "parent_candidate_package_sha256":result["package_sha256"],
            "candidate_artifact_id":parent["artifact_id"],
            "candidate_content_hash":parent["artifact_content_hash"],
            "candidate_package_sha256":parent["package_sha256"],
            "publication_state":"published_immutable_verified", "revision":2}
        if any(republication.get(k) != v for k,v in expected_republication.items()) \
                or any(republication.get(k) is not False for k in
                    ("accepted_pointer_changed","source_set_created","family_resolution_created")):
            raise ValueError("BPS family parent is not the exact governed republication of its cycle result")
        record = _catalog_source(catalog, parent["source_id"], parent["artifact_id"])
        if record["artifact_content_hash"] != parent["artifact_content_hash"] or record["package_sha256"] != parent["package_sha256"]:
            raise ValueError("BPS family parent catalog identity mismatch")
    logical_bps = _catalog_source(catalog, "bps", resolution["output_artifact_id"])
    if logical_bps["artifact_content_hash"] != resolution["output_content_hash"] \
            or logical_bps["package_sha256"] != resolution["output_package_sha256"]:
        raise ValueError("BPS logical output identity mismatch")
    direct = []
    for source in sorted(expected_physical - {"census_bps", "census_bps_provisional"}):
        result = by_source[source]
        record = _catalog_source(catalog, source, result["candidate_artifact_id"])
        if record["artifact_content_hash"] != result["artifact_content_hash"] or record["package_sha256"] != result["package_sha256"]:
            raise ValueError(f"durable result/catalog mismatch: {source}")
        # Source Set status describes the selected immutable artifact, not only
        # row-value change.  A newly published revision is refreshed even when
        # its canonical data equals the prior artifact.
        status = "unchanged" if result["candidate_artifact_id"] == result["prior_artifact_id"] else "refreshed"
        direct.append(_entry(record, status=status, carried=status == "unchanged"))
    entries = [*direct, _entry(logical_bps, status="refreshed", carried=False)]
    physical_members = [{"source_id": p["source_id"], "artifact_id": p["artifact_id"],
        "artifact_content_hash": p["artifact_content_hash"], "package_sha256": p["package_sha256"]} for p in parents]
    family_map = {"schema_version": FAMILY_MAP_VERSION, "cycle_id": cycle_id,
        "physical_source_inventory": sorted(expected_physical),
        "logical_source_inventory": sorted(e["source_id"] for e in entries),
        "families": [{"logical_source_id":"bps", "resolution_id":resolution["resolution_id"],
            "output_artifact_id":resolution["output_artifact_id"],
            "output_content_hash":resolution["output_content_hash"],
            "output_package_sha256":resolution["output_package_sha256"],
            "physical_sources":physical_members}]}
    return create_source_set_v2(output, target_month=target_month, created_at=created_at,
        builder_git_sha=builder_git_sha, entries=entries,
        config_hashes=governed_config_hashes(repository_root), family_resolution=family_map)
