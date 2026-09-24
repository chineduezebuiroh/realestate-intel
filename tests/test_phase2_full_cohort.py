from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from jobs.monthly_refresh.cohort import (
    LOGICAL_DIRECT_SOURCES, REQUIRED_SOURCES, barrier_evidence, logical_cohort_plan,
    required_sources, resume_plan,
)
from jobs.monthly_refresh.acs_family_resolution import resolution_record as acs_resolution_record
from jobs.monthly_refresh.bps_family_resolution import resolution_record as bps_resolution_record


AUTHORITATIVE_PHYSICAL_IDS = {
    "census_bps": "src__census_bps__2026-04__r2__993afaddb934ce4f",
    "census_bps_provisional": "src__census_bps_provisional__2026-07__r2__61c56540953237cb",
    "census_acs1": "src__census_acs1__2024-12__r1__ae2900e2b1367f0e",
    "census_acs5": "src__census_acs5__2024-12__r1__1aabeff9f408cbb5",
    "bea_gdp_qtr": "src__bea_gdp_qtr__2026-03__r1__9290d93e61b8e5dd",
    "bea_gdp_ann": "src__bea_gdp_ann__2024-12__r1__1c51a5b7a95bdc27",
    "census_nrc": "src__census_nrc__2026-08__r1__e02eda146c92a3d3",
}


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


def authoritative_result(source: str) -> dict:
    value = result(source)
    value["candidate_artifact_id"] = AUTHORITATIVE_PHYSICAL_IDS.get(
        source, value["candidate_artifact_id"])
    return value


def produced_resolution(logical: str, parents: tuple[str, str]) -> dict:
    parent_records = [{"source_id": source,
        "artifact_id": authoritative_result(source)["candidate_artifact_id"]}
        for source in parents]
    artifact_id = f"src__{logical}__2026-08__r99__" + ("d" if logical == "bps" else "e") * 16
    manifest = {"source_id": logical, "artifact_id": artifact_id,
        "artifact_content_hash": ("1" if logical == "bps" else "2") * 64,
        "config_hashes": {"fixture": "3" * 64},
        "family_resolution": {"parents": parent_records, "diagnostics": {"fixture": True}}}
    catalog_record = {"object_id": artifact_id,
        "artifact_content_hash": manifest["artifact_content_hash"],
        "package_sha256": ("4" if logical == "bps" else "5") * 64,
        "publication_state": "published_immutable_verified"}
    builder = bps_resolution_record if logical == "bps" else acs_resolution_record
    return {"record": builder(manifest=manifest, catalog_record=catalog_record)}


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
    assert plan["plan_id"].startswith("logical_cohort_plan__")
    assert len(plan["physical_results"]) == 11
    assert set(plan["family_resolutions"]) == {"bps", "acs"}
    assert not {"census_bps","census_bps_provisional","census_acs1","census_acs5","census_nrc_fred"} & {x["source_id"] for x in plan["sources"]}
    assert "census_nrc" in {x["source_id"] for x in plan["sources"]}
    assert all(plan[key] is False for key in ("accepted_pointers_advanced","source_set_created","canonical_market_created","serving_market_created","redfin_consumption_committed"))


def test_completed_exact_cycle_is_fully_reused_by_resume():
    completed = [result(source) for source in REQUIRED_SOURCES]

    resume = resume_plan(REQUIRED_SOURCES, completed, expected_cycle_id="cycle")
    evidence = barrier_evidence(
        cycle={"cycle_id": "cycle", "invocation_mode": "resume"},
        results=[], reused_results=completed, pins=resume["pins"], github={})

    assert resume["reuse"] == sorted(REQUIRED_SOURCES)
    assert resume["run"] == []
    assert evidence["barrier_status"] == "ready"
    assert evidence["reused_source_ids"] == sorted(REQUIRED_SOURCES)
    assert evidence["retry_source_ids"] == []


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
    assert "cohort_plan_store" in text and "--branch main" in text


def test_master_post_barrier_jobs_override_skips_and_fail_closed():
    workflow = yaml.safe_load(Path(".github/workflows/monthly-refresh-production.yml").read_text())
    triggers = workflow.get(True, workflow.get("on"))
    jobs = workflow["jobs"]

    assert set(triggers) == {"workflow_dispatch"}
    assert "push" not in triggers
    assert "schedule" not in triggers

    expected_family_condition = (
        "${{ !cancelled() && needs.resolve-cycle.result == 'success' && "
        "needs.barrier.result == 'success' }}")
    assert jobs["resolve-bps-family"]["if"] == expected_family_condition
    assert jobs["resolve-acs-family"]["if"] == expected_family_condition
    assert jobs["logical-cohort-plan"]["if"] == (
        "${{ !cancelled() && needs.barrier.result == 'success' && "
        "needs.resolve-bps-family.result == 'success' && "
        "needs.resolve-acs-family.result == 'success' }}")


def test_real_phase2_interfaces_produce_exact_preflight_plan_without_io(monkeypatch):
    """Exercise barrier -> real resolution records -> logical plan, entirely offline."""
    def network_forbidden(*_args, **_kwargs):
        raise AssertionError("Phase 2 integration proof attempted network/provider access")

    monkeypatch.setattr("socket.create_connection", network_forbidden)
    physical = [authoritative_result(source) for source in REQUIRED_SOURCES]
    evidence = barrier_evidence(cycle={"cycle_id":"cycle","invocation_mode":"resume"},
        results=physical, pins=None, github={})
    assert evidence["barrier_status"] == "ready"
    assert {item["source_id"] for item in evidence["candidates"]} == set(REQUIRED_SOURCES)
    assert "census_nrc_fred" not in {item["source_id"] for item in evidence["candidates"]}

    bps = produced_resolution("bps", ("census_bps", "census_bps_provisional"))
    acs = produced_resolution("acs", ("census_acs1", "census_acs5"))
    assert {p["source_id"] for p in bps["record"]["parents"]} == {
        "census_bps", "census_bps_provisional"}
    assert {p["source_id"] for p in acs["record"]["parents"]} == {
        "census_acs1", "census_acs5"}
    for record in (bps["record"], acs["record"]):
        assert record["accepted_pointer_changed"] is False
        assert record["source_set_created"] is False
        assert record["provider_discovery_performed"] is False

    plan = logical_cohort_plan(physical_evidence=evidence,
        bps_resolution=bps, acs_resolution=acs)
    final = [item["source_id"] for item in plan["sources"]]
    assert final == list(LOGICAL_DIRECT_SOURCES)
    assert final == ["fred_macro", "ces", "laus", "redfin", "bps", "acs",
                     "bea_gdp_qtr", "bea_gdp_ann", "census_nrc"]
    assert not ({"census_bps", "census_bps_provisional", "census_acs1", "census_acs5",
                 "census_nrc_fred"} & set(final))
    assert "census_nrc" in final and not any("nrc" in source and source != "census_nrc"
                                              for source in final)
    assert {"bea_gdp_qtr", "bea_gdp_ann"}.issubset(final)
    assert all(plan[key] is False for key in ("accepted_pointers_advanced", "source_set_created",
        "canonical_market_created", "serving_market_created", "redfin_consumption_committed"))
