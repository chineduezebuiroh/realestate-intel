"""Live GitHub adapter for the already-governed cohort promotion lifecycle.

This module intentionally has no provider clients.  It reads exact durable
control-plane objects, publishes the two derived immutable objects, persists a
prepared record, and advances a single recoverable operation per catalog CAS.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.parse
import tarfile
import hashlib
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.source_artifacts.assembly_v2 import assemble_source_set_v2
from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
    GitHubReleaseArtifactPublisher, GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import (canonical_json_bytes, sha256_file,
    sha256_json, write_canonical_json)
from core.source_artifacts.market_artifact import create_canonical_market_manifest
from core.source_artifacts.object_package import build_object_package, publish_object
from core.source_artifacts.object_package import validate_object_package
from core.source_artifacts.promotion import (add_promotion_record,
    create_promotion_record, promotion_progress, recover_promotion)
from core.source_artifacts.publication import PublicationError
from jobs.monthly_refresh.cohort_promotion import build_logical_source_set
from core.source_artifacts.source_set_v2 import source_set_semantic_sha256
from jobs.monthly_refresh.readiness import validate_readiness
from jobs.monthly_refresh.phase3 import authorization_token
from jobs.monthly_refresh.cohort_plan_store import GitHubCohortPlanStore, validate_durable_plan

CATALOG_PATH = "config/artifact_catalog.json"
READINESS_PATH = "config/monthly_refresh_readiness.json"
RECORD_ROOT = "config/cohort_promotion_records"
BUILD_PROVENANCE = "governed-monthly-phase3-adapter-v1"


class GitHubJSONCAS:
    """Exact tracked JSON object with a Git blob compare-and-swap precondition."""
    def __init__(self, api: GitHubAPI, path: str, branch: str):
        self.api, self.path, self.branch = api, path, branch

    def read(self) -> tuple[dict[str, Any] | None, str | None]:
        encoded = urllib.parse.quote(self.path, safe="/")
        item, _ = self.api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(self.branch)}",
                                   expected=(200, 404))
        if item is None: return None, None
        return json.loads(base64.b64decode(item["content"])), item["sha"]

    def write(self, value: dict[str, Any], expected_sha: str | None, message: str) -> None:
        payload = {"message": message,
                   "content": base64.b64encode(canonical_json_bytes(value)).decode(),
                   "branch": self.branch}
        if expected_sha is not None: payload["sha"] = expected_sha
        encoded = urllib.parse.quote(self.path, safe="/")
        self.api.request("PUT", f"/contents/{encoded}", payload=payload, expected=(200, 201))


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_required(store: GitHubJSONCAS) -> dict[str, Any]:
    value, _ = store.read()
    if value is None: raise PublicationError(f"required durable object is absent: {store.path}")
    return value


def _publish_object(*, api: GitHubAPI, cas: GitHubCatalogCAS, package: Path,
                    object_id: str, object_type: str, content_hash: str,
                    metadata: dict[str, Any], members: dict[str, str], git_sha: str) -> dict[str, Any]:
    """Publish/reuse exact bytes, catalog by blob CAS, then reread and verify."""
    uri = f"artifact://{object_type}/{object_id}"
    catalog, _ = cas.read()
    existing = [r for r in catalog["immutable_records"]
                if r["object_type"] == object_type and r["object_id"] == object_id]
    if existing:
        record = existing[0]
        if (record["logical_artifact_uri"] != uri or record["artifact_content_hash"] != content_hash
                or record["package_sha256"] != sha256_file(package)):
            raise PublicationError("existing immutable publication contradicts exact package")
        _verify_object_record(api, record, package.parent / (object_id + "-reuse"))
        return record
    publisher = GitHubReleaseArtifactPublisher(api)
    tag = {"source_set":"source-set", "canonical_market":"canonical-market",
           "serving_market":"serving-market"}[object_type] + "/" + object_id
    updated, receipt, _ = publish_object(publisher=publisher, catalog=catalog, package=package,
        logical_uri=uri, object_id=object_id, object_type=object_type,
        artifact_content_hash=content_hash, object_metadata=metadata, member_hashes=members,
        remote_repository=api.repository, release_tag=tag, release_id=1, asset_id=1,
        asset_filename=object_id+".tar", publisher_git_sha=git_sha, published_at=_utc(),
        contract_versions=[{"source_set":"source_set_manifest_v2",
                            "canonical_market":"canonical_market_artifact_v1",
                            "serving_market":"serving_market_artifact_v1"}[object_type]])
    record = next(r for r in updated["immutable_records"] if r["object_id"] == object_id)
    cas.add(record, receipt)
    durable, _ = cas.read()
    exact = [r for r in durable["immutable_records"] if r["object_type"] == object_type and r["object_id"] == object_id]
    if len(exact) != 1 or exact[0]["package_sha256"] != sha256_file(package):
        raise PublicationError("published object did not reread with exact identity")
    _verify_object_record(api, exact[0], package.parent / (object_id + "-proof"))
    return exact[0]


def _verify_object_record(api: GitHubAPI, record: dict[str, Any], directory: Path) -> None:
    """Reread exact numeric Release/asset and validate downloaded object bytes."""
    release, _ = api.request("GET", f"/releases/{record['release_id']}")
    assets = [a for a in release.get("assets", []) if a.get("id") == record["asset_id"]]
    if (release.get("draft") or release.get("tag_name") != record["release_tag"] or len(assets) != 1
            or assets[0].get("name") != record["asset_filename"]):
        raise PublicationError("durable object Release receipt contradiction")
    directory.mkdir(parents=True, exist_ok=True); downloaded = directory / record["asset_filename"]
    api.download_asset(record["asset_id"], downloaded)
    if sha256_file(downloaded) != record["package_sha256"]:
        raise PublicationError("durable object package receipt contradiction")
    with tarfile.open(downloaded, "r:") as archive:
        member_hashes = {}
        for member in archive.getmembers():
            stream = archive.extractfile(member)
            if stream is None: raise PublicationError("durable object package member unreadable")
            member_hashes[member.name] = hashlib.sha256(stream.read()).hexdigest()
    validate_object_package(downloaded, directory / "extracted", object_type=record["object_type"],
        expected={"object_id":record["object_id"], "artifact_content_hash":record["artifact_content_hash"],
                  "member_hashes":member_hashes})


def persist_prepared(store: GitHubJSONCAS, proposed: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    existing, oid = store.read(); value, changed = add_promotion_record(existing, proposed)
    if changed:
        store.write(value, oid, f"Prepare cohort promotion {value['promotion_id']}")
    durable, _ = store.read()
    if durable != value: raise PublicationError("prepared promotion record durable reread contradiction")
    return value, changed


def execute_one(record: dict[str, Any], catalog_cas: GitHubCatalogCAS,
                readiness_store: GitHubJSONCAS) -> dict[str, Any]:
    """Classify and commit at most one operation, then reread both authorities."""
    catalog, catalog_oid = catalog_cas.read(); readiness, readiness_oid = readiness_store.read()
    if readiness is None: raise PublicationError("durable readiness is absent")
    next_catalog, next_readiness, before = recover_promotion(record, catalog, readiness, max_operations=1)
    if next_catalog != catalog and next_readiness != readiness:
        raise AssertionError("one recovery boundary changed two durable authorities")
    if next_catalog != catalog:
        catalog_cas._write(next_catalog, catalog_oid, f"Advance cohort promotion {record['promotion_id']}")
    elif next_readiness != readiness:
        readiness_store.write(next_readiness, readiness_oid,
                              f"Consume readiness for {record['promotion_id']}")
    durable_catalog, _ = catalog_cas.read(); durable_readiness, _ = readiness_store.read()
    if durable_readiness is None: raise PublicationError("readiness disappeared after operation")
    progress = promotion_progress(record, durable_catalog, durable_readiness)
    return {"changed": next_catalog != catalog or next_readiness != readiness,
            "before": before, "progress": progress}


def execute_to_completion(record: dict[str, Any], catalog_cas: GitHubCatalogCAS,
                          readiness_store: GitHubJSONCAS) -> dict[str, Any]:
    operations = []
    while True:
        result = execute_one(record, catalog_cas, readiness_store); operations.append(result)
        if result["progress"]["complete"]: break
        if not result["changed"]: raise PublicationError("promotion made no progress before completion")
    # One exact rerun must classify as a no-op.
    repeat = execute_one(record, catalog_cas, readiness_store)
    if repeat["changed"] or not repeat["progress"]["complete"]:
        raise PublicationError("completed promotion is not idempotent")
    return {"operations": operations, "exact_rerun_noop": True, "progress": repeat["progress"]}


def _durable_inputs(api: GitHubAPI, branch: str, cycle_id: str) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    plan = GitHubCohortPlanStore(api, branch).get(cycle_id)
    results = list(plan["physical_results"])
    physical = {item["source_id"]:item for item in plan["physical_candidates"]}
    for result in results:
        expected=physical[result["source_id"]]
        if (result["candidate_artifact_id"],result["artifact_content_hash"],result["package_sha256"]) != \
                (expected["artifact_id"],expected["artifact_content_hash"],expected["package_sha256"]):
            raise PublicationError(f"durable cycle result drift from logical plan: {result['source_id']}")
    resolution, acs_resolution = plan["family_resolutions"]["bps"], plan["family_resolutions"]["acs"]
    republications=[]
    for source in ("census_bps","census_bps_provisional"):
        parent=next(p for p in resolution["parents"] if p["source_id"]==source)
        if parent["artifact_id"]==physical[source]["artifact_id"]:
            continue
        directory=f"config/monthly_source_republications/{cycle_id}/{source}"
        items,_=api.request("GET",f"/contents/{urllib.parse.quote(directory,safe='/')}?ref={urllib.parse.quote(branch)}")
        matches=[]
        for item in items:
            value=_read_required(GitHubJSONCAS(api,item["path"],branch))
            if value.get("candidate_artifact_id") == parent["artifact_id"]: matches.append(value)
        if len(matches)!=1: raise PublicationError(f"exact BPS parent republication does not resolve once: {source}")
        republications.append(matches[0])
    return plan, results, resolution, acs_resolution, republications


def run(*, api: GitHubAPI, branch: str, cycle_id: str, workspace: Path,
        git_sha: str, mutate: bool,
        supplied_authorization: str = "") -> dict[str, Any]:
    if mutate and not supplied_authorization:
        raise PublicationError("live promotion requires exact preflight authorization")
    workspace.mkdir(parents=True, exist_ok=True)
    catalog_cas = GitHubCatalogCAS(api, CATALOG_PATH, branch)
    readiness_store = GitHubJSONCAS(api, READINESS_PATH, branch)
    catalog, _ = catalog_cas.read(); readiness = _read_required(readiness_store)
    validate_readiness(readiness, catalog=catalog, policy_path=Path("config/monthly_refresh_policy.json"))
    serving_before = deepcopy(catalog["accepted"].get("serving_market"))
    if mutate:
        record=_read_required(GitHubJSONCAS(api,f"{RECORD_ROOT}/{cycle_id}.json",branch))
        if supplied_authorization != authorization_token(record):
            raise PublicationError("live authorization is not bound to the exact durable promotion plan")
        outcome=execute_to_completion(record,catalog_cas,readiness_store)
        final_catalog,_=catalog_cas.read()
        if final_catalog["accepted"].get("serving_market")!=serving_before:
            raise PublicationError("serving authority changed during cohort promotion")
        return {"cycle_id":cycle_id,"promotion_id":record["promotion_id"],
            "live_promotion_performed":True,"provider_discovery_performed":False,
            "redfin_acquisition_performed":False,**outcome}
    logical_plan, results, resolution, acs_resolution, republications = _durable_inputs(api,branch,cycle_id)
    redfin_matches=[r for r in readiness["records"] if r["cycle_id"] == cycle_id]
    if len(redfin_matches)!=1: raise PublicationError("exact cycle Redfin readiness does not resolve once")
    redfin=redfin_matches[0]
    redfin_records=[r for r in catalog["immutable_records"] if r["object_type"]=="source" and r["object_id"] == redfin["candidate_artifact_id"]]
    if len(redfin_records)!=1: raise PublicationError("Redfin readiness artifact does not resolve once")
    redfin_record=redfin_records[0]
    planned_redfin=next(r for r in results if r["source_id"]=="redfin")
    if planned_redfin["candidate_artifact_id"]!=redfin_record["object_id"]:
        raise PublicationError("Redfin readiness/result identity mismatch")
    # Construction remains deterministic on an exact rerun after consumption;
    # the durable value is never changed here and the recovery engine below
    # rejects consumption unless every preceding target is already complete.
    assembly_readiness = deepcopy(readiness)
    next(r for r in assembly_readiness["records"] if r["readiness_id"] == redfin["readiness_id"])["consumed"] = False
    validate_durable_plan(logical_plan)
    redfin_pin=next(p for p in logical_plan["physical_candidates"] if p["source_id"]=="redfin")
    if (redfin_record["object_id"],redfin_record["artifact_content_hash"],redfin_record["package_sha256"]) != \
            (redfin_pin["artifact_id"],redfin_pin["artifact_content_hash"],redfin_pin["package_sha256"]):
        raise PublicationError("Redfin readiness drift from durable logical plan")
    target_month=cycle_id.split("__")[1]
    build_time=target_month+"-01T00:00:00Z"
    source_set = build_logical_source_set(output=workspace/"source-set.json", cycle_id=cycle_id,
        logical_plan=logical_plan,
        target_month=target_month, physical_results=[r for r in results if r["source_id"] not in {"census_acs1","census_acs5"}], catalog=catalog, readiness=assembly_readiness,
        resolution=resolution, acs_resolution=acs_resolution,
        family_parent_republications=republications, created_at=build_time,
        builder_git_sha=BUILD_PROVENANCE)
    ss_package = build_object_package({"source-set.json":workspace/"source-set.json"}, workspace/"source-set.tar")
    resolver = GitHubReleaseArtifactResolver(catalog, api, workspace/"sources")
    validation = assemble_source_set_v2(source_set, workspace/"market.duckdb", resolver)
    manifest_values = dict(source_set_id=source_set["source_set_id"],
        source_set_semantic_sha256=source_set_semantic_sha256(source_set), source_set_package_sha256=ss_package["package_sha256"],
        canonical_assembly_contract_version="canonical_market_assembly_v1",
        canonical_schema_identity="canonical-timeseries-v1", config_hashes=source_set["config_hashes"],
        builder_contract_identity="core-source-artifacts-assembly-v1",
        dependency_lock_identity="requirements-sha256:"+sha256_file(Path("requirements.txt")), assembly_revision=1,
        compressed_package_sha256=sha256_file(workspace/"market.duckdb"), table_inventory=["fact_timeseries","source_artifact_metadata"],
        **{k:validation[k] for k in ("row_count","source_count","geography_count","metric_count","first_date","last_date","duplicate_key_count")},
        validation_status="passed", assembly_warnings=[], builder_git_sha=BUILD_PROVENANCE, built_at=build_time)
    market = create_canonical_market_manifest(workspace/"canonical-market.json",
                                               database_path=workspace/"market.duckdb", **manifest_values)
    market_package = build_object_package({"canonical-market.json":workspace/"canonical-market.json",
        "market.duckdb":workspace/"market.duckdb"}, workspace/"canonical-market.tar")
    targets = {e["source_id"]:e["artifact_id"] for e in source_set["sources"]}
    proposed_record = create_promotion_record(cycle_id=cycle_id,
        source_set_id=source_set["source_set_id"],
        source_set_semantic_sha256=source_set_semantic_sha256(source_set),
        canonical_artifact_id=market["market_artifact_id"],
        canonical_artifact_hash=sha256_json(market),
        expected_source_pointers={s:catalog["accepted"]["source"].get(s) for s in targets},
        target_source_pointers=targets,
        expected_source_set=catalog["accepted"].get("source_set"),
        expected_canonical=catalog["accepted"].get("canonical_market"),
        readiness_id=redfin["readiness_id"], resolution_id=resolution["resolution_id"])
    summary = {"cycle_id":cycle_id, "source_set_id":source_set["source_set_id"],
        "canonical_artifact_id":market["market_artifact_id"], "provider_discovery_performed":False,
        "promotion_id":proposed_record["promotion_id"], "promotion_plan_validated":True,
        "authorization_token":authorization_token(proposed_record),
        "live_promotion_performed":False, "mutate":mutate}
    ss_record = _publish_object(api=api, cas=catalog_cas, package=workspace/"source-set.tar",
        object_id=source_set["source_set_id"], object_type="source_set", content_hash=sha256_json(source_set),
        metadata={"cycle_id":cycle_id,"source_set_semantic_sha256":source_set_semantic_sha256(source_set)},
        members=ss_package["member_hashes"], git_sha=git_sha)
    market_record = _publish_object(api=api, cas=catalog_cas, package=workspace/"canonical-market.tar",
        object_id=market["market_artifact_id"], object_type="canonical_market", content_hash=sha256_json(market),
        metadata={"source_set_id":source_set["source_set_id"],"database_sha256":market["database_sha256"]},
        members=market_package["member_hashes"], git_sha=git_sha)
    prepared_catalog, _ = catalog_cas.read()
    record_store = GitHubJSONCAS(api, f"{RECORD_ROOT}/{cycle_id}.json", branch)
    existing_record, _ = record_store.read()
    frozen_expected_sources = (existing_record["expected_source_pointers"] if existing_record
                               else {s:prepared_catalog["accepted"]["source"].get(s) for s in targets})
    frozen_expected_set = existing_record["expected_source_set"] if existing_record else prepared_catalog["accepted"].get("source_set")
    frozen_expected_canonical = existing_record["expected_canonical"] if existing_record else prepared_catalog["accepted"].get("canonical_market")
    record = create_promotion_record(cycle_id=cycle_id, source_set_id=ss_record["object_id"],
        source_set_semantic_sha256=source_set_semantic_sha256(source_set), canonical_artifact_id=market_record["object_id"],
        canonical_artifact_hash=sha256_json(market),
        expected_source_pointers=frozen_expected_sources,
        target_source_pointers=targets, expected_source_set=frozen_expected_set,
        expected_canonical=frozen_expected_canonical, readiness_id=redfin["readiness_id"],
        resolution_id=resolution["resolution_id"])
    record, created = persist_prepared(record_store, record)
    summary.update(promotion_id=record["promotion_id"], authorization_token=authorization_token(record),
                   prepared_record_created=created, immutable_objects_published=True)
    return summary


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--repository",required=True)
    parser.add_argument("--branch",required=True); parser.add_argument("--cycle-id",required=True)
    parser.add_argument("--workspace",type=Path,required=True); parser.add_argument("--git-sha",required=True)
    parser.add_argument("--live",action="store_true"); parser.add_argument("--authorization-token",default="")
    parser.add_argument("--output",type=Path,required=True); args=parser.parse_args()
    api=GitHubAPI(args.repository,os.environ.get("GITHUB_TOKEN",""))
    report=run(api=api,branch=args.branch,cycle_id=args.cycle_id,workspace=args.workspace,
               git_sha=args.git_sha,mutate=args.live,
               supplied_authorization=args.authorization_token)
    write_canonical_json(args.output,report); print(json.dumps(report,sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
