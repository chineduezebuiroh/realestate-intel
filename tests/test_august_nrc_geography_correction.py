from __future__ import annotations

import copy

import pytest

from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.august_nrc_geography_correction import (
    CYCLE_ID, R1_ID, authorization_token, build_reconciliation_plan)
from jobs.monthly_refresh.cycle_results import add_record


def _result(artifact=R1_ID, content="1" * 64, package="2" * 64):
    value = {"schema_version":"monthly_source_execution_result_v1", "source_id":"census_nrc",
        "cycle_id":CYCLE_ID, "status":"succeeded", "candidate_artifact_id":artifact,
        "artifact_content_hash":content, "package_sha256":package,
        "publication_state":"published_verified", "validation_status":"passed",
        "provider_release_id":"release", "observation_max":"2026-08-31",
        "prior_artifact_id":None, "source_change_detected":True,
        "retryability":"not_applicable", "accepted_pointer_changed":False,
        "evidence_uri":"artifact://source/census_nrc/" + artifact}
    return {"schema_version":"monthly_source_cycle_result_v1", "cycle_id":CYCLE_ID,
        "source_id":"census_nrc", "result_contract":"monthly_source_execution_result_v1",
        "policy_schema_version":"monthly_refresh_policy_v1", "result":value}


def _r2_manifest(r2: str, pin: dict) -> dict:
    provider = {"united_states__nation", "northeast_region__region",
        "midwest_region__region", "south_region__region", "west_region__region"}
    governed = provider - {"midwest_region__region"}
    metrics = {"census_housing_starts_total_saar", "census_housing_completions_total_saar"}
    manifest_hash = "6" * 64
    return {"artifact_id":r2, "artifact_uri":"artifact://source/census_nrc/"+r2,
        "artifact_content_hash":"3"*64, "data_sha256":"4"*64,
        "provider_release_id":"release", "observation_max":"2026-08-31",
        "revision_policy_id":"source_refresh_revision_v0_2:census_nrc",
        "supersedes_artifact_id":R1_ID,
        "config_hashes":{"config/geo_manifest.generated.csv":manifest_hash},
        "governed_contract":{"geography_inventory":sorted(governed)},
        "raw_source_lineage":{"provider_pin_id":"pin",
            "input_members":[{"kind":name,"sha256":member["sha256"]}
                             for name,member in sorted(pin["members"].items())],
            "provider_shape_validation_status":"passed",
            "provider_geography_inventory":sorted(provider),
            "provider_applicability_pairs":[{"geo_id":geo,"metric_id":metric}
                for geo in sorted(provider) for metric in sorted(metrics)],
            "governed_intersection_validation_status":"passed",
            "governed_geography_inventory":sorted(governed),
            "governed_applicability_pairs":[{"geo_id":geo,"metric_id":metric}
                for geo in sorted(governed) for metric in sorted(metrics)],
            "excluded_geographies":[{"canonical_geo_slug":"midwest_region__region",
                "classification":"OUT_OF_GOVERNANCE",
                "disposition":"EXCLUDED_FROM_CANONICAL_CANDIDATE",
                "metrics_present":sorted(metrics), "provider_row_count":2}],
            "excluded_row_count_by_metric":{metric:1 for metric in metrics},
            "geography_governance_manifest_sha256":manifest_hash}}


def test_ordinary_create_once_store_still_rejects_corrected_identity():
    existing = _result(); proposed = _result("src__census_nrc__2026-08__r2__new", "3"*64, "4"*64)
    with pytest.raises(IdentityCollisionError, match="cycle-result collision"):
        add_record(existing, proposed)


def test_reconciliation_plan_is_exact_and_preserves_authority(monkeypatch):
    import jobs.monthly_refresh.august_nrc_geography_correction as module
    monkeypatch.setattr(module, "validate_durable_plan", lambda value: value)
    pin = {"pin_id":"pin", "provider_release_id":"release", "members":{
        "starts":{"sha256":"a"*64}, "completions":{"sha256":"b"*64}}}
    r2 = "src__census_nrc__2026-08__r2__corrected"
    manifest = _r2_manifest(r2, pin)
    old_result = _result()
    old_plan = {"cycle_id":CYCLE_ID, "plan_id":"old-plan",
        "physical_candidates":[{"source_id":"census_nrc","artifact_id":R1_ID,
            "artifact_content_hash":"1"*64,"package_sha256":"2"*64}],
        "sources":[{"source_id":"census_nrc","artifact_id":R1_ID,
            "artifact_content_hash":"1"*64,"package_sha256":"2"*64}],
        "physical_results":[copy.deepcopy(old_result["result"])]}
    accepted = {"source":{"census_nrc":"accepted-old"}, "source_set":"set-old",
                "canonical_market":"market-old", "serving_market":"serving-old"}
    catalog = {"accepted":copy.deepcopy(accepted), "immutable_records":[{
        "object_type":"source", "object_id":R1_ID, "artifact_content_hash":"1"*64,
        "package_sha256":"2"*64, "metadata":{"source_id":"census_nrc"}}]}
    readiness = {"records":[{"readiness_id":"redfin", "consumed":False}]}
    policy = {"schema_version":"monthly_refresh_policy_v1", "sources":[],
              "slower_cadence_sources":["census_nrc"]}
    correction = build_reconciliation_plan(manifest=manifest, package_sha256="5"*64,
        pin=pin, existing_result=old_result, existing_plan=old_plan, catalog=catalog,
        readiness=readiness, policy=policy)
    assert correction["accepted_before"] == accepted
    assert readiness == {"records":[{"readiness_id":"redfin", "consumed":False}]}
    assert correction["corrected_cycle_result"]["result"]["candidate_artifact_id"] == r2
    assert correction["corrected_plan"]["sources"][0]["artifact_id"] == r2
    assert authorization_token(correction).startswith(
        "AUTHORIZE_AUGUST_NRC_GEOGRAPHY_CORRECTION__")
    reconciled_catalog = copy.deepcopy(catalog)
    reconciled_catalog["immutable_records"].append({"object_type":"source", "object_id":r2,
        "logical_artifact_uri":manifest["artifact_uri"], "artifact_content_hash":"3"*64,
        "package_sha256":"5"*64, "publication_state":"published_immutable_verified",
        "metadata":{"source_id":"census_nrc", "data_sha256":"4"*64,
                    "provider_release_id":"release", "observation_max":"2026-08-31"}})
    reentry = build_reconciliation_plan(manifest=manifest, package_sha256="5"*64,
        pin=pin, existing_result=correction["corrected_cycle_result"],
        existing_plan=correction["corrected_plan"], catalog=reconciled_catalog,
        readiness=readiness, policy=policy)
    assert reentry["corrected_cycle_result"] == correction["corrected_cycle_result"]
    assert reentry["corrected_plan"] == correction["corrected_plan"]


def test_reconciliation_rejects_missing_midwest_exclusion(monkeypatch):
    import jobs.monthly_refresh.august_nrc_geography_correction as module
    monkeypatch.setattr(module, "validate_durable_plan", lambda value: value)
    pin = {"pin_id":"pin", "provider_release_id":"release", "members":{
        "starts":{"sha256":"a"*64}, "completions":{"sha256":"b"*64}}}
    manifest = _r2_manifest("src__census_nrc__2026-08__r2__corrected", pin)
    manifest["raw_source_lineage"]["excluded_geographies"] = []
    with pytest.raises(PublicationError, match="Midwest exclusion"):
        module.validate_r2_geography_evidence(manifest)


def test_reconciliation_rejects_wrong_supersession(monkeypatch):
    import jobs.monthly_refresh.august_nrc_geography_correction as module
    monkeypatch.setattr(module, "validate_durable_plan", lambda value: value)
    with pytest.raises(PublicationError, match="does not supersede exact r1"):
        build_reconciliation_plan(manifest={"artifact_id":"src__census_nrc__2026-08__r2__x",
            "revision_policy_id":"x", "supersedes_artifact_id":"wrong"}, package_sha256="5"*64,
            pin={}, existing_result={}, existing_plan={}, catalog={}, readiness={}, policy={})
