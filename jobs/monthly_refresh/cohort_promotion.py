"""Prepare the post-barrier logical cohort without performing live promotion."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.catalog import validate_catalog
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.source_set_v2 import (FAMILY_MAP_VERSION, create_source_set_v2,
    governed_config_hashes)
from jobs.monthly_refresh.production import validate_source_result
from jobs.monthly_refresh.readiness import eligible_record
from jobs.monthly_refresh.phase3 import validate_logical_plan
from sources.census_bps.artifact import family_resolution_config_hashes
from sources.census_acs.artifact import (CONTRACT_VERSION as ACS_CONTRACT_VERSION,
    governed_config_hashes as acs_governed_config_hashes)

NRC_CONTRACT_VERSION = "census_nrc_workbook_parser_v2_governed_geo_openpyxl_3.1.5"
NRC_METRICS = ("census_housing_starts_total_saar",
               "census_housing_completions_total_saar")
NRC_GOVERNED_CANDIDATE_GEOGRAPHIES = ("united_states__nation", "northeast_region__region",
                                      "south_region__region", "west_region__region")


LOGICAL_COHORT_SOURCES = frozenset({"acs", "bea_gdp_ann", "bea_gdp_qtr", "bps",
                                    "census_nrc", "ces", "fred_macro", "fred_unemp",
                                    "laus", "redfin"})
PHYSICAL_FAMILY_SOURCES = frozenset({"census_acs1", "census_acs5",
                                     "census_bps", "census_bps_provisional"})


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


def validate_nrc_cohort_contract(record: Mapping[str, Any]) -> None:
    """Fail closed unless the physical NRC catalog record carries its frozen contract."""
    metadata = record.get("metadata", {})
    expected = {
        "source_contract_version": NRC_CONTRACT_VERSION,
        "metric_inventory": sorted(NRC_METRICS),
        "geography_inventory": sorted(NRC_GOVERNED_CANDIDATE_GEOGRAPHIES),
        "unit": "thousands_of_housing_units_saar",
        "numeric_scale_factor": 1,
        "canonical_schema": "source_artifact_v1",
    }
    if any(metadata.get(key) != value for key, value in expected.items()):
        raise ValueError("census_nrc governed cohort contract evidence mismatch")


def build_logical_source_set(*, output: Path, cycle_id: str, target_month: str,
        logical_plan: dict[str, Any],
        physical_results: list[dict[str, Any]], catalog: dict[str, Any], readiness: dict[str, Any],
        resolution: dict[str, Any], acs_resolution: dict[str, Any],
        family_parent_republications: list[dict[str, Any]],
        created_at: str, builder_git_sha: str,
        repository_root: Path = Path(".")) -> dict[str, Any]:
    """Map complete physical results to exact logical assembly inputs."""
    validate_catalog(catalog)
    planned = validate_logical_plan(logical_plan)
    if logical_plan["cycle_id"] != cycle_id:
        raise ValueError("logical cohort plan cycle mismatch")
    expected_physical = {"bea_gdp_ann", "bea_gdp_qtr", "census_bps", "census_nrc",
                         "census_bps_provisional", "ces", "fred_macro", "laus", "redfin"}
    expected_physical.add("fred_unemp")
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
    family_hashes = family_resolution_config_hashes(repository_root)
    # Preserve the frozen July record's full identity, while checking drift
    # only for configuration actually consumed by logical family resolution.
    if not config_hashes or any(config_hashes.get(path) != digest
                                for path, digest in family_hashes.items()):
        raise ValueError("BPS family resolution governed config drift")
    resolution_semantic = {key:resolution[key] for key in ("cycle_id", "resolver_version",
        "source_contract_version", "parents", "config_hashes", "output_artifact_id", "output_content_hash")}
    if resolution["resolution_id"] != "bps_family_resolution__" + sha256_json(resolution_semantic)[:24]:
        raise ValueError("BPS family resolution identity mismatch")
    parents = sorted(resolution["parents"], key=lambda p:p["source_id"])
    if {p["source_id"] for p in parents} != {"census_bps", "census_bps_provisional"}:
        raise ValueError("BPS family parent inventory mismatch")
    republications = {r.get("source_id"):r for r in family_parent_republications}
    if not set(republications) <= {"census_bps", "census_bps_provisional"}:
        raise ValueError("BPS family republication inventory mismatch")
    for parent in parents:
        result = by_source[parent["source_id"]]
        direct_identity=(result["candidate_artifact_id"],result["artifact_content_hash"],result["package_sha256"])
        parent_identity=(parent["artifact_id"],parent["artifact_content_hash"],parent["package_sha256"])
        if direct_identity == parent_identity:
            record = _catalog_source(catalog, parent["source_id"], parent["artifact_id"])
            if record["artifact_content_hash"] != parent["artifact_content_hash"] or record["package_sha256"] != parent["package_sha256"]:
                raise ValueError("BPS family parent catalog identity mismatch")
            continue
        republication = republications.get(parent["source_id"])
        if republication is None:
            raise ValueError("BPS family parent does not match its cycle candidate or governed republication")
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
    if set(republications) != {p["source_id"] for p in parents
                               if p["artifact_id"] != by_source[p["source_id"]]["candidate_artifact_id"]}:
        raise ValueError("BPS family contains unused republication evidence")
    logical_bps = _catalog_source(catalog, "bps", resolution["output_artifact_id"])
    if logical_bps["artifact_content_hash"] != resolution["output_content_hash"] \
            or logical_bps["package_sha256"] != resolution["output_package_sha256"]:
        raise ValueError("BPS logical output identity mismatch")

    required_acs_resolution = {"schema_version":"acs_family_resolution_record_v1",
        "resolver_version":"acs_family_resolver_v1",
        "resolution_policy_version":"acs1_preferred_observation_key_v1",
        "source_contract_version":ACS_CONTRACT_VERSION}
    if any(acs_resolution.get(k) != v for k, v in required_acs_resolution.items()):
        raise ValueError("ACS family resolution contract mismatch")
    if any(acs_resolution.get(field) is not False for field in ("accepted_pointer_changed",
            "source_set_created", "duckdb_mutated", "serving_db_mutated",
            "provider_discovery_performed")):
        raise ValueError("ACS family resolution contains forbidden side effects")
    acs_hashes = acs_governed_config_hashes(repository_root)
    if acs_resolution.get("config_hashes") != acs_hashes:
        raise ValueError("ACS family resolution governed config drift")
    acs_semantic_fields = ("resolver_version", "resolution_policy_version",
        "source_contract_version", "parents", "config_hashes",
        "physical_to_logical_metric_mapping", "output_artifact_id", "output_content_hash")
    acs_semantic = {key:acs_resolution[key] for key in acs_semantic_fields}
    if acs_resolution.get("resolution_id") != "acs_family_resolution__" + sha256_json(acs_semantic)[:24]:
        raise ValueError("ACS family resolution identity mismatch")
    acs_parents = sorted(acs_resolution.get("parents", []), key=lambda p:p["source_id"])
    if {p.get("source_id") for p in acs_parents} != {"census_acs1", "census_acs5"}:
        raise ValueError("ACS family parent inventory mismatch")
    for parent in acs_parents:
        record = _catalog_source(catalog, parent["source_id"], parent["artifact_id"])
        if (record["artifact_content_hash"] != parent["artifact_content_hash"] or
                record["package_sha256"] != parent["package_sha256"]):
            raise ValueError("ACS family parent catalog identity mismatch")
    logical_acs = _catalog_source(catalog, "acs", acs_resolution["output_artifact_id"])
    if (logical_acs["artifact_content_hash"] != acs_resolution["output_content_hash"] or
            logical_acs["package_sha256"] != acs_resolution["output_package_sha256"]):
        raise ValueError("ACS logical output identity mismatch")
    direct = []
    for source in sorted(expected_physical - {"census_bps", "census_bps_provisional"}):
        result = by_source[source]
        record = _catalog_source(catalog, source, result["candidate_artifact_id"])
        if record["artifact_content_hash"] != result["artifact_content_hash"] or record["package_sha256"] != result["package_sha256"]:
            raise ValueError(f"durable result/catalog mismatch: {source}")
        if source == "census_nrc":
            validate_nrc_cohort_contract(record)
        # Source Set status describes the selected immutable artifact, not only
        # row-value change.  A newly published revision is refreshed even when
        # its canonical data equals the prior artifact.
        status = "unchanged" if result["candidate_artifact_id"] == result["prior_artifact_id"] else "refreshed"
        direct.append(_entry(record, status=status, carried=status == "unchanged"))
    entries = [*direct, _entry(logical_acs, status="refreshed", carried=False),
               _entry(logical_bps, status="refreshed", carried=False)]
    physical_members = [{"source_id": p["source_id"], "artifact_id": p["artifact_id"],
        "artifact_content_hash": p["artifact_content_hash"], "package_sha256": p["package_sha256"]} for p in parents]
    family_map = {"schema_version": FAMILY_MAP_VERSION, "cycle_id": cycle_id,
        "physical_source_inventory": sorted(expected_physical | {"census_acs1", "census_acs5"}),
        "logical_source_inventory": sorted(e["source_id"] for e in entries),
        "families": [{"logical_source_id":"bps", "resolution_id":resolution["resolution_id"],
            "output_artifact_id":resolution["output_artifact_id"],
            "output_content_hash":resolution["output_content_hash"],
            "output_package_sha256":resolution["output_package_sha256"],
            "physical_sources":physical_members},
            {"logical_source_id":"acs", "resolution_id":acs_resolution["resolution_id"],
            "output_artifact_id":acs_resolution["output_artifact_id"],
            "output_content_hash":acs_resolution["output_content_hash"],
            "output_package_sha256":acs_resolution["output_package_sha256"],
            "physical_sources":[{"source_id":p["source_id"], "artifact_id":p["artifact_id"],
                "artifact_content_hash":p["artifact_content_hash"], "package_sha256":p["package_sha256"]}
                for p in acs_parents]}]}
    if {entry["source_id"] for entry in entries} != LOGICAL_COHORT_SOURCES:
        raise AssertionError("logical cohort inventory drift")
    actual = {entry["source_id"]: (entry["artifact_id"], entry["artifact_content_hash"],
                                   entry["package_sha256"]) for entry in entries}
    expected = {source: (item["artifact_id"], item["artifact_content_hash"], item["package_sha256"])
                for source, item in planned.items()}
    if actual != expected:
        raise ValueError("Source Set inputs differ from the exact logical cohort plan")
    return create_source_set_v2(output, target_month=target_month, created_at=created_at,
        builder_git_sha=builder_git_sha, entries=entries,
        config_hashes=governed_config_hashes(repository_root), family_resolution=family_map)
