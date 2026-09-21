from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from jobs.monthly_refresh.cohort import (
    LOGICAL_DIRECT_SOURCES, REQUIRED_SOURCES, barrier_evidence, logical_cohort_plan,
    required_sources,
)


def result(source: str) -> dict:
    return {"schema_version":"monthly_source_execution_result_v1", "source_id":source,
        "cycle_id":"cycle", "status":"succeeded", "candidate_artifact_id":f"src__{source}__2026-08__r1__" + "a"*16,
        "artifact_content_hash":"b"*64, "package_sha256":"c"*64,
        "publication_state":"published_verified", "validation_status":"passed",
        "provider_release_id":"fixture", "observation_max":"2026-08-31", "prior_artifact_id":None,
        "source_change_detected":True, "retryability":"not_applicable", "accepted_pointer_changed":False,
        "evidence_uri":f"artifact://source/{source}"}


def resolution(logical: str, parents: set[str]) -> dict:
    return {"record":{"parents":[{"source_id":source,"artifact_id":result(source)["candidate_artifact_id"]}
        for source in sorted(parents)], "output_artifact_id":f"src__{logical}__2026-08__r1__"+"d"*16,
        "output_content_hash":"e"*64, "output_package_sha256":"f"*64,
        "resolution_id":logical+"-fixture", "accepted_pointer_changed":False, "source_set_created":False}}


def test_exact_physical_barrier_and_logical_plan_are_closed_and_non_mutating():
    registry=json.loads(Path("config/monthly_source_execution_registry.json").read_text())
    assert required_sources(registry) == REQUIRED_SOURCES
    assert len(REQUIRED_SOURCES) == 11
    evidence=barrier_evidence(cycle={"cycle_id":"cycle","invocation_mode":"resume"},
        results=[result(source) for source in REQUIRED_SOURCES], pins=None, github={})
    plan=logical_cohort_plan(physical_evidence=evidence,
        bps_resolution=resolution("bps", {"census_bps","census_bps_provisional"}),
        acs_resolution=resolution("acs", {"census_acs1","census_acs5"}))
    assert tuple(plan["logical_source_inventory"]) == LOGICAL_DIRECT_SOURCES
    assert len(plan["sources"]) == 9
    assert not {"census_bps","census_bps_provisional","census_acs1","census_acs5","census_nrc_fred"} & {x["source_id"] for x in plan["sources"]}
    assert "census_nrc" in {x["source_id"] for x in plan["sources"]}
    assert all(plan[key] is False for key in ("accepted_pointers_advanced","source_set_created","canonical_market_created","serving_market_created","redfin_consumption_committed"))


def test_logical_plan_rejects_missing_unexpected_and_stale_family_parent():
    evidence=barrier_evidence(cycle={"cycle_id":"cycle","invocation_mode":"replay"},
        results=[result(source) for source in REQUIRED_SOURCES], pins=None, github={})
    stale=resolution("acs", {"census_acs1","census_acs5"}); stale["record"]["parents"][0]["artifact_id"]="historical"
    with pytest.raises(ValueError, match="exact cohort parents"):
        logical_cohort_plan(physical_evidence=evidence,
            bps_resolution=resolution("bps", {"census_bps","census_bps_provisional"}), acs_resolution=stale)
    incomplete=dict(evidence, candidates=evidence["candidates"][:-1])
    with pytest.raises(ValueError, match="11-source"):
        logical_cohort_plan(physical_evidence=incomplete,
            bps_resolution=resolution("bps", {"census_bps","census_bps_provisional"}),
            acs_resolution=resolution("acs", {"census_acs1","census_acs5"}))


def test_master_orders_both_dynamic_resolvers_after_common_barrier():
    workflow=yaml.safe_load(Path(".github/workflows/monthly-refresh-production.yml").read_text())
    jobs=workflow["jobs"]
    assert jobs["census-acs1"]["uses"] == jobs["census-acs5"]["uses"] == "./.github/workflows/acs-monthly-source.yml"
    assert "barrier" in jobs["resolve-bps-family"]["needs"] and "barrier" in jobs["resolve-acs-family"]["needs"]
    assert jobs["logical-cohort-plan"]["needs"] == ["barrier","resolve-bps-family","resolve-acs-family"]
    text=Path(".github/workflows/monthly-refresh-production.yml").read_text()
    assert "needs.barrier.outputs.bps_artifact_id" in text
    assert "needs.barrier.outputs.acs1_artifact_id" in text
