from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from core.source_artifacts.publication import PublicationError
from core.source_artifacts.source_set_v2 import create_source_set_v2, governed_config_hashes
from jobs.monthly_refresh.cohort import barrier_evidence, durable_automated_results, required_sources
from jobs.monthly_refresh.cohort_promotion import LOGICAL_COHORT_SOURCES, PHYSICAL_FAMILY_SOURCES
from jobs.monthly_refresh.cohort_promotion_hosted import run


BEA_SOURCES = {"bea_gdp_qtr", "bea_gdp_ann"}


def result(source_id: str) -> dict:
    return {"schema_version":"monthly_source_execution_result_v1", "source_id":source_id,
        "cycle_id":"cycle", "status":"succeeded",
        "candidate_artifact_id":f"src__{source_id}__2026-03__r1__" + source_id[-3:] * 6,
        "artifact_content_hash":"a"*64, "package_sha256":"b"*64,
        "publication_state":"published_verified", "validation_status":"passed",
        "provider_release_id":source_id+":snapshot", "observation_max":"2026-03-31",
        "prior_artifact_id":None, "source_change_detected":True,
        "retryability":"not_applicable", "accepted_pointer_changed":False,
        "evidence_uri":f"artifact://source/{source_id}"}


def entry(source_id: str) -> dict:
    value = result(source_id)
    return {"source_id":source_id, "artifact_id":value["candidate_artifact_id"],
        "logical_artifact_uri":value["evidence_uri"] + "/" + value["candidate_artifact_id"],
        "package_sha256":value["package_sha256"],
        "artifact_content_hash":value["artifact_content_hash"],
        "provider_release_id":value["provider_release_id"],
        "observation_max":value["observation_max"], "validation_status":"passed",
        "monthly_status":"refreshed", "release_tag":"fixture", "asset_id":1,
        "publication_receipt_id":"fixture", "cycle_check_succeeded":True,
        "carried_forward":False, "carry_forward_policy_allowed":False}


def test_bea_sources_are_independent_governed_members_and_future_pointers():
    assert BEA_SOURCES.issubset(LOGICAL_COHORT_SOURCES)
    assert "bea" not in LOGICAL_COHORT_SOURCES
    assert BEA_SOURCES.isdisjoint(PHYSICAL_FAMILY_SOURCES)
    targets = {source: f"accepted.source.{source}" for source in BEA_SOURCES}
    assert targets == {"bea_gdp_qtr":"accepted.source.bea_gdp_qtr",
                       "bea_gdp_ann":"accepted.source.bea_gdp_ann"}


def test_source_set_can_pin_both_bea_sources_without_family_mapping(tmp_path):
    source_set = create_source_set_v2(tmp_path / "set.json", target_month="2026-03",
        created_at="fixed", builder_git_sha="fixture",
        entries=[entry(source) for source in sorted(BEA_SOURCES)],
        config_hashes=governed_config_hashes(Path(".")))
    assert source_set["included_source_inventory"] == ["bea_gdp_ann", "bea_gdp_qtr"]
    assert {item["source_id"] for item in source_set["sources"]} == BEA_SOURCES
    assert not source_set.get("family_resolution")


def test_barrier_requires_both_and_rejects_duplicate_identity():
    policy = {"schema_version":"monthly_source_execution_registry_v1", "members":[
        {"source_id":source, "required":True, "hosted_cohort_enabled":True}
        for source in sorted(BEA_SOURCES)]}
    cycle = {"cycle_id":"cycle", "invocation_mode":"normal"}
    evidence = barrier_evidence(cycle=cycle, results=[result("bea_gdp_qtr")], pins=None,
                                github={}, policy=policy)
    assert evidence["barrier_status"] == "incomplete_retryable"
    assert evidence["retry_source_ids"] == ["bea_gdp_ann"]
    with pytest.raises(ValueError, match="duplicate source result"):
        barrier_evidence(cycle=cycle, results=[result("bea_gdp_qtr")]*2,
                         pins=None, github={}, policy=policy)


def test_registry_and_hosted_dispatch_register_both_independently():
    registry = json.loads(Path("config/monthly_source_execution_registry.json").read_text())
    assert BEA_SOURCES.issubset(required_sources(registry))
    workflow = yaml.safe_load(Path(".github/workflows/monthly-refresh-production.yml").read_text())
    assert {"bea-gdp-qtr", "bea-gdp-ann"}.issubset(workflow["jobs"])
    reusable = yaml.safe_load(Path(".github/workflows/bea-monthly-source.yml").read_text())
    reusable["on"] = reusable.pop(True)
    assert reusable["on"]["workflow_dispatch"]["inputs"]["source_id"]["options"] == [
        "bea_gdp_qtr", "bea_gdp_ann"]


def test_resume_reuses_durable_bea_results_without_provider_discovery():
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    registry_policy = json.loads(Path("config/monthly_source_execution_registry.json").read_text())
    records, immutable = [], []
    for source in BEA_SOURCES:
        value = result(source)
        records.append({"source_id":source, "cycle_id":"cycle",
            "result_contract":"monthly_source_execution_result_v1",
            "policy_schema_version":policy["schema_version"], "result":value})
        immutable.append({"object_type":"source", "object_id":value["candidate_artifact_id"],
            "artifact_content_hash":value["artifact_content_hash"],
            "package_sha256":value["package_sha256"],
            "publication_state":"published_immutable_verified",
            "metadata":{"source_id":source, "provider_release_id":value["provider_release_id"]}})
    # Limit this contract-level proof to BEA; durable resolution itself has no provider client.
    reused = durable_automated_results(cycle={"cycle_id":"cycle"},
        catalog={"immutable_records":immutable},
        registry={"schema_version":"monthly_source_cycle_results_v1", "records":records},
        policy={**registry_policy, "source_execution_result_schema":"monthly_source_execution_result_v1",
                "sources":policy["sources"], "schema_version":policy["schema_version"]})
    assert {item["source_id"] for item in reused} == BEA_SOURCES


def test_deferred_promotion_guard_rejects_mutation_before_any_api_access(tmp_path):
        with pytest.raises(PublicationError, match="preflight authorization"):
            run(api=None, branch="main", cycle_id="monthly_cycle__2026-07__7cab1c5df177a1e4",
                workspace=tmp_path, git_sha="fixture", mutate=True)
