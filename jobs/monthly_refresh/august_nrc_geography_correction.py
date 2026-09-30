"""Exact August NRC geography correction and authority reconciliation.

The ordinary cycle-result and logical-plan stores are create-once.  This module
is the deliberately narrow exception for the approved NRC r1 -> r2 correction.
It never discovers/acquires provider data and never changes accepted pointers or
Redfin readiness.  Live use requires an exact plan-bound authorization token.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import sha256_json, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import PublicationError
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.bps_hosted import CATALOG_PATH, publish_candidate
from jobs.monthly_refresh.cohort_plan_store import GitHubCohortPlanStore, validate_durable_plan
from jobs.monthly_refresh.cycle_results import governed_record, record_path
from jobs.monthly_refresh.source_inputs import GitHubPinStore, validate_pin

CYCLE_ID = "monthly_cycle__2026-08__a9e022a980d29cd7"
SOURCE_ID = "census_nrc"
R1_ID = "src__census_nrc__2026-08__r1__e02eda146c92a3d3"
AUTHORIZATION_PREFIX = "AUTHORIZE_AUGUST_NRC_GEOGRAPHY_CORRECTION__"
FIXED_CREATED_AT = "2026-08-01T00:00:00Z"
READINESS_PATH = "config/monthly_refresh_readiness.json"
PROVIDER_GEOGRAPHIES = {"united_states__nation", "northeast_region__region",
                        "midwest_region__region", "south_region__region", "west_region__region"}
GOVERNED_GEOGRAPHIES = PROVIDER_GEOGRAPHIES - {"midwest_region__region"}
METRICS = {"census_housing_starts_total_saar", "census_housing_completions_total_saar"}


def authorization_token(plan: Mapping[str, Any]) -> str:
    return AUTHORIZATION_PREFIX + sha256_json(dict(plan))


def _identity(result: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(result.get(key) for key in
                 ("candidate_artifact_id", "artifact_content_hash", "package_sha256",
                  "provider_release_id", "observation_max"))


def validate_r2_geography_evidence(manifest: Mapping[str, Any]) -> None:
    lineage = manifest.get("raw_source_lineage", {})
    provider_pairs = {(item.get("geo_id"), item.get("metric_id"))
                      for item in lineage.get("provider_applicability_pairs", [])}
    governed_pairs = {(item.get("geo_id"), item.get("metric_id"))
                      for item in lineage.get("governed_applicability_pairs", [])}
    excluded = lineage.get("excluded_geographies", [])
    expected_exclusion = [{"canonical_geo_slug":"midwest_region__region",
        "classification":"OUT_OF_GOVERNANCE",
        "disposition":"EXCLUDED_FROM_CANONICAL_CANDIDATE",
        "metrics_present":sorted(METRICS)}]
    normalized_exclusions = [{key:item.get(key) for key in expected_exclusion[0]} for item in excluded]
    if lineage.get("provider_shape_validation_status") != "passed" \
            or set(lineage.get("provider_geography_inventory", [])) != PROVIDER_GEOGRAPHIES \
            or provider_pairs != {(geo, metric) for geo in PROVIDER_GEOGRAPHIES for metric in METRICS}:
        raise PublicationError("corrected NRC provider geography evidence mismatch")
    if lineage.get("governed_intersection_validation_status") != "passed" \
            or set(lineage.get("governed_geography_inventory", [])) != GOVERNED_GEOGRAPHIES \
            or governed_pairs != {(geo, metric) for geo in GOVERNED_GEOGRAPHIES for metric in METRICS} \
            or set(manifest.get("governed_contract", {}).get("geography_inventory", [])) != GOVERNED_GEOGRAPHIES:
        raise PublicationError("corrected NRC governed geography evidence mismatch")
    if normalized_exclusions != expected_exclusion:
        raise PublicationError("corrected NRC Midwest exclusion evidence mismatch")
    manifest_hash = manifest.get("config_hashes", {}).get("config/geo_manifest.generated.csv")
    if not manifest_hash or lineage.get("geography_governance_manifest_sha256") != manifest_hash \
            or any(int(lineage.get("excluded_row_count_by_metric", {}).get(metric, 0)) <= 0
                   for metric in METRICS):
        raise PublicationError("corrected NRC geography governance identity mismatch")


def corrected_result(existing_record: Mapping[str, Any], r2_record: Mapping[str, Any],
                     policy: Mapping[str, Any], catalog: Mapping[str, Any]) -> dict[str, Any]:
    old = existing_record.get("result", {})
    if existing_record.get("cycle_id") != CYCLE_ID or existing_record.get("source_id") != SOURCE_ID:
        raise PublicationError("August NRC r1 cycle authority mismatch")
    meta = r2_record.get("metadata", {})
    proposed = deepcopy(old)
    proposed.update(candidate_artifact_id=r2_record["object_id"],
        artifact_content_hash=r2_record["artifact_content_hash"],
        package_sha256=r2_record["package_sha256"],
        provider_release_id=meta["provider_release_id"], observation_max=meta["observation_max"],
        prior_artifact_id=R1_ID, source_change_detected=True,
        evidence_uri=r2_record["logical_artifact_uri"])
    governed = governed_record(proposed, policy, catalog)
    if old.get("candidate_artifact_id") == R1_ID:
        return governed
    if existing_record == governed:
        return deepcopy(existing_record)
    raise PublicationError("August NRC cycle authority is neither exact r1 nor exact r2")


def corrected_logical_plan(existing: Mapping[str, Any], result_record: Mapping[str, Any]) -> dict[str, Any]:
    validate_durable_plan(dict(existing)); result = result_record["result"]
    if existing.get("cycle_id") != CYCLE_ID:
        raise PublicationError("August logical plan cycle mismatch")
    out = deepcopy(existing)
    physical = next(x for x in out["physical_candidates"] if x["source_id"] == SOURCE_ID)
    logical = next(x for x in out["sources"] if x["source_id"] == SOURCE_ID)
    prior_result = next(x for x in out["physical_results"] if x["source_id"] == SOURCE_ID)
    current = (physical["artifact_id"], logical["artifact_id"], prior_result["candidate_artifact_id"])
    target = (result["candidate_artifact_id"],) * 3
    if current == target:
        return deepcopy(existing)
    if current != (R1_ID, R1_ID, R1_ID):
        raise PublicationError("logical plan does not select exact NRC r1")
    identity = {"artifact_id":result["candidate_artifact_id"],
                "artifact_content_hash":result["artifact_content_hash"],
                "package_sha256":result["package_sha256"]}
    physical.update(identity); logical.update(identity)
    prior_result.clear(); prior_result.update(deepcopy(result))
    out["plan_id"] = ""
    out["plan_id"] = "logical_cohort_plan__" + sha256_json(
        {key:value for key,value in out.items() if key != "plan_id"})[:24]
    return validate_durable_plan(out)


def build_reconciliation_plan(*, manifest: Mapping[str, Any], package_sha256: str,
                              pin: Mapping[str, Any], existing_result: Mapping[str, Any],
                              existing_plan: Mapping[str, Any], catalog: Mapping[str, Any],
                              readiness: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    if manifest.get("revision_policy_id") is None or manifest.get("artifact_id", "").startswith(
            "src__census_nrc__2026-08__r2__") is False:
        raise PublicationError("corrected NRC artifact must be August r2")
    if manifest.get("supersedes_artifact_id") != R1_ID:
        raise PublicationError("corrected NRC artifact does not supersede exact r1")
    validate_r2_geography_evidence(manifest)
    lineage = manifest.get("raw_source_lineage", {})
    if lineage.get("provider_pin_id") != pin.get("pin_id"):
        raise PublicationError("corrected NRC artifact does not derive from durable pin")
    expected_members = {name:member["sha256"] for name,member in pin["members"].items()}
    actual_members = {item["kind"]:item["sha256"] for item in lineage.get("input_members", [])}
    if actual_members != expected_members or manifest.get("provider_release_id") != pin.get("provider_release_id"):
        raise PublicationError("corrected NRC pinned input identity mismatch")
    old = existing_result.get("result", {})
    r1_matches = [record for record in catalog.get("immutable_records", [])
        if record.get("object_type") == "source" and record.get("object_id") == R1_ID]
    if len(r1_matches) != 1 or r1_matches[0].get("metadata", {}).get("source_id") != SOURCE_ID:
        raise PublicationError("exact immutable NRC r1 catalog/result identity mismatch")
    if old.get("candidate_artifact_id") == R1_ID:
        if _identity(old)[:3] != (r1_matches[0].get("object_id"),
                r1_matches[0].get("artifact_content_hash"), r1_matches[0].get("package_sha256")):
            raise PublicationError("exact immutable NRC r1 catalog/result identity mismatch")
    elif _identity(old)[:3] != (manifest["artifact_id"], manifest["artifact_content_hash"], package_sha256):
        raise PublicationError("August NRC cycle authority is neither exact r1 nor exact r2")
    accepted_before = deepcopy(catalog["accepted"]); readiness_before = deepcopy(readiness)
    synthetic_record = {"object_type":"source", "object_id":manifest["artifact_id"],
        "logical_artifact_uri":manifest["artifact_uri"], "package_sha256":package_sha256,
        "artifact_content_hash":manifest["artifact_content_hash"],
        "publication_state":"published_immutable_verified",
        "metadata":{"source_id":SOURCE_ID,"data_sha256":manifest["data_sha256"],
                    "provider_release_id":manifest["provider_release_id"],
                    "observation_max":manifest["observation_max"]}}
    planning_catalog = deepcopy(catalog)
    if not any(r.get("object_id") == manifest["artifact_id"] for r in planning_catalog["immutable_records"]):
        # governed_record needs exact catalog evidence; dry-run uses the locally
        # verified package identity, while live publication supplies full receipt fields.
        planning_catalog["immutable_records"].append(synthetic_record)
    result = corrected_result(existing_result, synthetic_record, policy, planning_catalog)
    logical = corrected_logical_plan(existing_plan, result)
    payload = {"schema_version":"august_nrc_geography_correction_plan_v1",
        "cycle_id":CYCLE_ID, "r1_artifact_id":R1_ID, "r2_artifact_id":manifest["artifact_id"],
        "r2_artifact_content_hash":manifest["artifact_content_hash"],
        "r2_package_sha256":package_sha256, "provider_pin_id":pin["pin_id"],
        "existing_plan_id":existing_plan["plan_id"], "corrected_plan":logical,
        "corrected_cycle_result":result, "accepted_before":accepted_before,
        "readiness_sha256":sha256_json(readiness_before),
        "provider_discovery_performed":False, "provider_acquisition_performed":False,
        "accepted_pointers_changed":False, "redfin_readiness_consumed":False}
    return payload


def main() -> int:
    from jobs.monthly_refresh.nrc_monthly import candidate, recover_pinned_workbooks
    from jobs.monthly_refresh.cohort_promotion_hosted import GitHubJSONCAS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True); parser.add_argument("--branch", required=True)
    parser.add_argument("--workspace", type=Path, required=True); parser.add_argument("--git-sha", required=True)
    parser.add_argument("--policy", type=Path, default=Path("config/monthly_refresh_policy.json"))
    parser.add_argument("--output", type=Path, required=True); parser.add_argument("--live", action="store_true")
    parser.add_argument("--authorize", default=""); args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""), read_only=not args.live)
    pin = GitHubPinStore(api, args.branch).get(CYCLE_ID, SOURCE_ID)
    pin = validate_pin(pin, cycle_id=CYCLE_ID, source_id=SOURCE_ID,
                       required_members={"starts", "completions"})
    paths = recover_pinned_workbooks(pin, args.workspace / "pinned-input")
    artifact = args.workspace / "artifact"
    if artifact.exists(): shutil.rmtree(artifact)
    built = candidate(pin=pin, paths=paths, output=artifact, cycle_id=CYCLE_ID,
        git_sha=args.git_sha, revision=2, supersedes_artifact_id=R1_ID,
        artifact_created_at=FIXED_CREATED_AT)
    manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    package = args.workspace / f"{manifest['artifact_id']}.tar"
    package_info = build_publication_package(artifact, package)
    catalog_cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch, fixture=False)
    catalog, _ = catalog_cas.read()
    result_store = GitHubJSONCAS(api, record_path(CYCLE_ID, SOURCE_ID), args.branch)
    existing_result, result_oid = result_store.read()
    plan_store = GitHubJSONCAS(api, GitHubCohortPlanStore(api,args.branch).path(CYCLE_ID), args.branch)
    existing_plan, plan_oid = plan_store.read()
    readiness_store = GitHubJSONCAS(api, READINESS_PATH, args.branch)
    readiness, _ = readiness_store.read()
    policy = json.loads(args.policy.read_text())
    policy = {**policy, "slower_cadence_sources": [
        *policy.get("slower_cadence_sources", []), SOURCE_ID]}
    correction = build_reconciliation_plan(manifest=manifest,
        package_sha256=package_info["package_sha256"], pin=pin, existing_result=existing_result,
        existing_plan=existing_plan, catalog=catalog, readiness=readiness, policy=policy)
    token = authorization_token(correction)
    report = {"plan":correction, "authorization_token":token, "mode":"live" if args.live else "dry-run",
              "mutation_performed":False, "r2_manifest":built["manifest"]}
    if args.live:
        if args.authorize != token: raise PublicationError("live correction authorization mismatch")
        publication = publish_candidate(artifact=artifact, source_id=SOURCE_ID, api=api,
            cas=catalog_cas, workspace=args.workspace/"publication", git_sha=args.git_sha,
            logical_source_id=SOURCE_ID)
        durable_catalog, _ = catalog_cas.read()
        final_result = corrected_result(existing_result, publication["record"], policy, durable_catalog)
        final_plan = corrected_logical_plan(existing_plan, final_result)
        if existing_plan != final_plan:
            plan_store.write(final_plan, plan_oid, "Correct August logical cohort plan to NRC r2")
        if existing_result != final_result:
            result_store.write(final_result, result_oid, "Correct August NRC cycle authority to governed geography r2")
        final_catalog, _ = catalog_cas.read(); final_readiness, _ = readiness_store.read()
        if final_catalog["accepted"] != correction["accepted_before"] \
                or sha256_json(final_readiness) != correction["readiness_sha256"]:
            raise PublicationError("forbidden authority changed during NRC correction")
        report.update(mutation_performed=True, immutable_r2_published=True,
                      cycle_result_reconciled=True, logical_plan_reconciled=True)
    write_canonical_json(args.output, report); print(json.dumps(report, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
