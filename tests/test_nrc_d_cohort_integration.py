from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

from core.source_artifacts.source_set_v2 import create_source_set_v2, governed_config_hashes
from jobs.monthly_refresh.cohort import barrier_evidence, required_sources
from jobs.monthly_refresh.cohort_promotion import (
    LOGICAL_COHORT_SOURCES,
    NRC_CONTRACT_VERSION,
    NRC_GEOGRAPHIES,
    NRC_METRICS,
    PHYSICAL_FAMILY_SOURCES,
    validate_nrc_cohort_contract,
)


def contract_metadata() -> dict:
    return {"source_id": "census_nrc", "source_contract_version": NRC_CONTRACT_VERSION,
        "metric_inventory": sorted(NRC_METRICS), "geography_inventory": sorted(NRC_GEOGRAPHIES),
        "unit": "thousands_of_housing_units_saar", "numeric_scale_factor": 1,
        "canonical_schema": "source_artifact_v1"}


def result() -> dict:
    return {"schema_version":"monthly_source_execution_result_v1", "source_id":"census_nrc",
        "cycle_id":"cycle", "status":"succeeded",
        "candidate_artifact_id":"src__census_nrc__2026-08__r1__94a8e9dc77b9063b",
        "artifact_content_hash":"94a8e9dc77b9063befc592de1f64678ac143bb4609307861ab075c37ff815982",
        "package_sha256":"b"*64, "publication_state":"published_verified",
        "validation_status":"passed",
        "provider_release_id":"nrc-workbooks:84f66e94466844bc93d40ffee06571e2026f09e9bbea9250eee19f89dfbb8676",
        "observation_max":"2026-08-31", "prior_artifact_id":None,
        "source_change_detected":True, "retryability":"not_applicable",
        "accepted_pointer_changed":False, "evidence_uri":"artifact://source/census_nrc/candidate"}


def test_nrc_is_one_direct_physical_and_logical_cohort_entry(tmp_path):
    assert "census_nrc" in LOGICAL_COHORT_SOURCES
    assert "census_nrc" not in PHYSICAL_FAMILY_SOURCES
    value = result()
    entry = {"source_id":"census_nrc", "artifact_id":value["candidate_artifact_id"],
        "logical_artifact_uri":value["evidence_uri"] + "/" + value["candidate_artifact_id"],
        "package_sha256":value["package_sha256"], "artifact_content_hash":value["artifact_content_hash"],
        "provider_release_id":value["provider_release_id"], "observation_max":value["observation_max"],
        "validation_status":"passed", "monthly_status":"refreshed", "release_tag":"fixture",
        "asset_id":1, "publication_receipt_id":"fixture", "cycle_check_succeeded":True,
        "carried_forward":False, "carry_forward_policy_allowed":False}
    source_set = create_source_set_v2(tmp_path/"set.json", target_month="2026-08",
        created_at="fixed", builder_git_sha="fixture", entries=[entry],
        config_hashes=governed_config_hashes(Path(".")))
    assert source_set["included_source_inventory"] == ["census_nrc"]
    assert source_set["family_resolution"] == {}


def test_nrc_contract_evidence_is_exact_and_fail_closed():
    record = {"metadata": contract_metadata()}
    validate_nrc_cohort_contract(record)
    for field in ("source_contract_version", "metric_inventory", "geography_inventory",
                  "unit", "numeric_scale_factor", "canonical_schema"):
        broken = copy.deepcopy(record); broken["metadata"].pop(field)
        with pytest.raises(ValueError, match="contract evidence mismatch"):
            validate_nrc_cohort_contract(broken)


def test_registry_barrier_and_hosted_plan_include_only_direct_census_nrc():
    registry = json.loads(Path("config/monthly_source_execution_registry.json").read_text())
    assert "census_nrc" in required_sources(registry)
    assert "census_nrc_fred" not in required_sources(registry)
    evidence = barrier_evidence(cycle={"cycle_id":"cycle", "invocation_mode":"normal"},
        results=[], pins=None, github={}, policy={"schema_version":"monthly_source_execution_registry_v1",
        "members":[{"source_id":"census_nrc", "required":True, "hosted_cohort_enabled":True}]})
    assert evidence["retry_source_ids"] == ["census_nrc"]
    workflow = yaml.safe_load(Path(".github/workflows/monthly-refresh-production.yml").read_text())
    assert workflow["jobs"]["census-nrc"]["uses"] == "./.github/workflows/nrc-monthly-source.yml"
