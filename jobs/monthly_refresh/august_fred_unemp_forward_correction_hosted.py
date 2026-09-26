"""Hosted preparation and recovery for the governed August FRED correction.

Preparation may publish immutable objects and create the correction record on
the durable authority branch.  It never changes accepted pointers, serving, or
Redfin readiness.  Live mode reads the already prepared record and advances at
most one accepted-pointer transition per catalog compare-and-swap.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import tarfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.assembly_v2 import assemble_source_set_v2
from core.source_artifacts.forward_correction import (add_correction_record,
    authorization_token, build_corrected_source_set, correction_progress,
    preflight_correction, recover_correction, validate_canonical_factual_addition)
from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
    GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.market_artifact import create_canonical_market_manifest
from core.source_artifacts.object_package import build_object_package, validate_object_package
from core.source_artifacts.publication import PublicationError
from core.source_artifacts.source_set_v2 import source_set_semantic_sha256
from jobs.monthly_refresh.august_fred_unemp_forward_correction import (
    ACCEPTED_CANONICAL, ACCEPTED_SOURCE_SET, ACCEPTED_SOURCES, CYCLE_ID,
    ORIGINAL_PROMOTION, prepare)
from jobs.monthly_refresh.cohort_promotion_hosted import (BUILD_PROVENANCE,
    CATALOG_PATH, READINESS_PATH, RECORD_ROOT, GitHubJSONCAS, _publish_object,
    _read_required)
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, governed_record
from jobs.monthly_refresh.readiness import validate_readiness
from jobs.monthly_refresh.source_inputs import GitHubPinStore, validate_pin
from jobs.monthly_refresh.fred_unemp import MEMBER

CORRECTION_ROOT = "config/cohort_correction_records"
SOURCE_ID = "fred_unemp"
TARGET_MONTH = "2026-08"
BUILD_TIME = "2026-09-26T00:00:00Z"


def record_path() -> str:
    return f"{CORRECTION_ROOT}/{CYCLE_ID}.json"


def authority_snapshot(catalog: Mapping[str, Any], readiness: Mapping[str, Any]) -> dict[str, Any]:
    """Capture every mutable authority forbidden to change during preparation."""
    return {"accepted": deepcopy(catalog.get("accepted")),
            "readiness": deepcopy(dict(readiness)),
            "accepted_sha256": sha256_json(catalog.get("accepted")),
            "readiness_sha256": sha256_json(dict(readiness))}


def _assert_origin(catalog: Mapping[str, Any]) -> None:
    accepted = catalog.get("accepted", {})
    if (accepted.get("source_set") != ACCEPTED_SOURCE_SET or
            accepted.get("canonical_market") != ACCEPTED_CANONICAL or
            accepted.get("serving_market") is not None or
            accepted.get("source") != ACCEPTED_SOURCES):
        raise PublicationError("not the exact accepted August correction origin")


def _catalog_record(catalog: Mapping[str, Any], kind: str, object_id: str) -> dict[str, Any]:
    matches = [item for item in catalog.get("immutable_records", [])
               if item.get("object_type") == kind and item.get("object_id") == object_id]
    if len(matches) != 1:
        raise PublicationError(f"durable {kind} does not resolve exactly once: {object_id}")
    return matches[0]


def _resolve_object(api: GitHubAPI, record: Mapping[str, Any], workspace: Path) -> Path:
    """Download and verify an exact non-source catalog object."""
    release, _ = api.request("GET", f"/releases/{record['release_id']}")
    assets = [item for item in release.get("assets", [])
              if int(item.get("id", -1)) == int(record["asset_id"])]
    if release.get("draft") or release.get("tag_name") != record["release_tag"] \
            or len(assets) != 1 or assets[0].get("name") != record["asset_filename"]:
        raise PublicationError("durable object Release receipt contradiction")
    workspace.mkdir(parents=True, exist_ok=True)
    package = workspace / record["asset_filename"]
    api.download_asset(record["asset_id"], package)
    if sha256_file(package) != record["package_sha256"]:
        raise PublicationError("durable object package hash contradiction")
    with tarfile.open(package, "r:") as archive:
        member_hashes = {}
        for member in archive.getmembers():
            stream = archive.extractfile(member)
            if stream is None:
                raise PublicationError("durable object package member unreadable")
            import hashlib
            member_hashes[member.name] = hashlib.sha256(stream.read()).hexdigest()
    return validate_object_package(package, workspace / "extracted",
        object_type=record["object_type"], expected={"object_id": record["object_id"],
        "artifact_content_hash": record["artifact_content_hash"],
        "member_hashes": member_hashes})


def _fred_entry(record: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    metadata = record["metadata"]
    if (record["object_id"], record["artifact_content_hash"], record["package_sha256"]) != (
            result["candidate_artifact_id"], result["artifact_content_hash"], result["package_sha256"]):
        raise PublicationError("FRED result/catalog immutable identity mismatch")
    return {"source_id": SOURCE_ID, "artifact_id": record["object_id"],
        "logical_artifact_uri": record["logical_artifact_uri"],
        "package_sha256": record["package_sha256"],
        "artifact_content_hash": record["artifact_content_hash"],
        "provider_release_id": metadata["provider_release_id"],
        "observation_max": metadata["observation_max"], "validation_status": "passed",
        "monthly_status": "refreshed", "release_tag": record["release_tag"],
        "asset_id": record["asset_id"],
        "publication_receipt_id": record["publication_receipt_id"],
        "cycle_check_succeeded": True, "carried_forward": False,
        "carry_forward_policy_allowed": False}


def persist_record(store: GitHubJSONCAS, proposed: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    existing, oid = store.read()
    value, changed = add_correction_record(existing, proposed)
    if changed:
        store.write(value, oid, f"Prepare accepted-cohort correction {value['correction_id']}")
    durable, _ = store.read()
    if durable != value:
        raise PublicationError("correction record durable reread contradiction")
    return value, changed


def execute_one(record: dict[str, Any], catalog_cas: GitHubCatalogCAS,
                readiness_store: GitHubJSONCAS, *, supplied_authorization: str) -> dict[str, Any]:
    """Reread both authorities, commit at most one catalog CAS, and reread again."""
    catalog, oid = catalog_cas.read()
    readiness = _read_required(readiness_store)
    next_catalog, unchanged_readiness, before = recover_correction(record, catalog, readiness,
        supplied_authorization=supplied_authorization, max_operations=1)
    if unchanged_readiness != readiness:
        raise AssertionError("correction recovery attempted to mutate readiness")
    changed = next_catalog != catalog
    if changed:
        catalog_cas._write(next_catalog, oid, f"Advance accepted-cohort correction {record['correction_id']}")
    durable_catalog, _ = catalog_cas.read()
    durable_readiness = _read_required(readiness_store)
    if durable_readiness != readiness:
        raise PublicationError("Redfin readiness changed during correction CAS")
    return {"changed": changed, "before": before,
            "progress": correction_progress(record, durable_catalog, durable_readiness)}


def execute_to_completion(record: dict[str, Any], catalog_cas: GitHubCatalogCAS,
                          readiness_store: GitHubJSONCAS, *,
                          supplied_authorization: str) -> dict[str, Any]:
    operations = []
    while True:
        item = execute_one(record, catalog_cas, readiness_store,
            supplied_authorization=supplied_authorization)
        operations.append(item)
        if item["progress"]["complete"]:
            break
        if not item["changed"]:
            raise PublicationError("correction recovery made no progress")
    repeat = execute_one(record, catalog_cas, readiness_store,
        supplied_authorization=supplied_authorization)
    if repeat["changed"] or not repeat["progress"]["complete"]:
        raise PublicationError("completed correction is not idempotent")
    return {"operations": operations, "exact_rerun_noop": True,
            "progress": repeat["progress"]}


def _authority_commit(api: GitHubAPI, branch: str) -> str:
    value, _ = api.request("GET", f"/commits/{branch}")
    sha = value.get("sha") if isinstance(value, Mapping) else None
    if not sha:
        raise PublicationError("durable authority commit did not resolve")
    return str(sha)


def run(*, api: GitHubAPI, branch: str, workspace: Path, git_sha: str,
        live: bool, supplied_authorization: str = "") -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    catalog_cas = GitHubCatalogCAS(api, CATALOG_PATH, branch)
    readiness_store = GitHubJSONCAS(api, READINESS_PATH, branch)
    correction_store = GitHubJSONCAS(api, record_path(), branch)
    authority_commit_before = _authority_commit(api, branch)
    catalog, _ = catalog_cas.read()
    readiness = _read_required(readiness_store)
    validate_readiness(readiness, catalog=catalog,
                       policy_path=Path("config/monthly_refresh_policy.json"))
    before = authority_snapshot(catalog, readiness)
    if live:
        record = _read_required(correction_store)
        if supplied_authorization != authorization_token(record):
            raise PublicationError("exact correction authorization token required")
        outcome = execute_to_completion(record, catalog_cas, readiness_store,
            supplied_authorization=supplied_authorization)
        final_catalog, _ = catalog_cas.read(); final_readiness = _read_required(readiness_store)
        if final_readiness != readiness or final_catalog["accepted"].get("serving_market") is not None:
            raise PublicationError("forbidden readiness/serving mutation during live correction")
        return {"schema_version":"accepted_cohort_forward_correction_hosted_report_v1",
            "mode":"live", "cycle_id":CYCLE_ID, "correction_id":record["correction_id"],
            "authority_branch":branch, "authority_commit_before":authority_commit_before,
            "authorization_verified":True, "provider_acquisition_performed":False,
            "readiness_mutated":False, "serving_mutated":False, **outcome}

    _assert_origin(catalog)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    stored_result = GitHubCycleResultStore(api, branch).get(CYCLE_ID, SOURCE_ID)
    if stored_result is None:
        raise PublicationError("durable August fred_unemp cycle result is absent")
    stored_result = governed_record(stored_result["result"], policy, catalog)
    result = stored_result["result"]
    pin = validate_pin(GitHubPinStore(api, branch).get(CYCLE_ID, SOURCE_ID),
        cycle_id=CYCLE_ID, source_id=SOURCE_ID, required_members={MEMBER})
    fred_record = _catalog_record(catalog, "source", result["candidate_artifact_id"])
    old_set_record = _catalog_record(catalog, "source_set", ACCEPTED_SOURCE_SET)
    old_market_record = _catalog_record(catalog, "canonical_market", ACCEPTED_CANONICAL)
    old_set_dir = _resolve_object(api, old_set_record, workspace / "accepted-source-set")
    old_market_dir = _resolve_object(api, old_market_record, workspace / "accepted-canonical")
    accepted_set = json.loads((old_set_dir / "source-set.json").read_text())
    corrected_set = build_corrected_source_set(workspace / "source-set.json",
        accepted_source_set=accepted_set, fred_unemp_entry=_fred_entry(fred_record, result),
        created_at=BUILD_TIME, builder_git_sha=BUILD_PROVENANCE)
    ss_package = build_object_package({"source-set.json":workspace/"source-set.json"},
                                      workspace/"source-set.tar")
    source_resolver = GitHubReleaseArtifactResolver(catalog, api, workspace/"sources")
    validation = assemble_source_set_v2(corrected_set, workspace/"market.duckdb", source_resolver)
    delta = validate_canonical_factual_addition(
        accepted_database=old_market_dir/"market.duckdb", corrected_database=workspace/"market.duckdb")
    manifest = create_canonical_market_manifest(workspace/"canonical-market.json",
        database_path=workspace/"market.duckdb", supersedes_artifact_id=ACCEPTED_CANONICAL,
        source_set_id=corrected_set["source_set_id"],
        source_set_semantic_sha256=source_set_semantic_sha256(corrected_set),
        source_set_package_sha256=ss_package["package_sha256"],
        canonical_assembly_contract_version="canonical_market_assembly_v1",
        canonical_schema_identity="canonical-timeseries-v1",
        config_hashes=corrected_set["config_hashes"],
        builder_contract_identity="core-source-artifacts-assembly-v1",
        dependency_lock_identity="requirements-sha256:"+sha256_file(Path("requirements.txt")),
        assembly_revision=2, compressed_package_sha256=sha256_file(workspace/"market.duckdb"),
        table_inventory=["fact_timeseries","source_artifact_metadata"],
        **{key:validation[key] for key in ("row_count","source_count","geography_count",
            "metric_count","first_date","last_date","duplicate_key_count")},
        validation_status="passed", assembly_warnings=[], builder_git_sha=BUILD_PROVENANCE,
        built_at=BUILD_TIME)
    market_package = build_object_package({"canonical-market.json":workspace/"canonical-market.json",
        "market.duckdb":workspace/"market.duckdb"}, workspace/"canonical-market.tar")
    ss_record = _publish_object(api=api, cas=catalog_cas, package=workspace/"source-set.tar",
        object_id=corrected_set["source_set_id"], object_type="source_set",
        content_hash=sha256_json(corrected_set), metadata={"cycle_id":CYCLE_ID,
        "source_set_semantic_sha256":source_set_semantic_sha256(corrected_set),
        "supersedes_artifact_id":ACCEPTED_SOURCE_SET}, members=ss_package["member_hashes"],
        git_sha=git_sha)
    market_record = _publish_object(api=api, cas=catalog_cas, package=workspace/"canonical-market.tar",
        object_id=manifest["market_artifact_id"], object_type="canonical_market",
        content_hash=sha256_json(manifest), metadata={"source_set_id":corrected_set["source_set_id"],
        "database_sha256":manifest["database_sha256"],
        "supersedes_artifact_id":ACCEPTED_CANONICAL}, members=market_package["member_hashes"],
        git_sha=git_sha)
    prepared_catalog, _ = catalog_cas.read()
    if authority_snapshot(prepared_catalog, _read_required(readiness_store)) != before:
        raise PublicationError("accepted authority or Redfin readiness changed during preparation")
    promotion = _read_required(GitHubJSONCAS(api, f"{RECORD_ROOT}/{CYCLE_ID}.json", branch))
    family = {item["logical_source_id"]:item["resolution_id"]
              for item in corrected_set["family_resolution"]["families"]}
    governed_hashes = {**corrected_set["config_hashes"], "fred_unemp_input_pin":sha256_json(pin),
        "fred_unemp_cycle_result":sha256_json(stored_result)}
    record, _ = prepare(catalog=prepared_catalog, readiness=readiness,
        original_promotion=promotion, fred_unemp_artifact_id=fred_record["object_id"],
        corrected_source_set_id=ss_record["object_id"],
        corrected_source_set_hash=ss_record["artifact_content_hash"],
        corrected_canonical_id=market_record["object_id"],
        corrected_canonical_hash=market_record["artifact_content_hash"],
        family_resolution_lineage=family, governed_hashes=governed_hashes)
    record, record_created = persist_record(correction_store, record)
    final_catalog, _ = catalog_cas.read(); final_readiness = _read_required(readiness_store)
    after = authority_snapshot(final_catalog, final_readiness)
    if after != before:
        raise PublicationError("accepted authority or Redfin readiness changed after preparation")
    preflight = preflight_correction(record, final_catalog, final_readiness, promotion)
    return {"schema_version":"accepted_cohort_forward_correction_hosted_report_v1",
        "mode":"preflight", "cycle_id":CYCLE_ID, "authority_branch":branch,
        "authority_commit_before":authority_commit_before,
        "authority_commit_after":_authority_commit(api, branch),
        "migration_git_sha":git_sha, "correction_vintage":"2026-09 later governed correction; not August-time vintage",
        "fred_unemp":{"pin_id":pin["pin_id"], "pin_sha256":sha256_json(pin),
            "cycle_result_sha256":sha256_json(stored_result), **result,
            "publication_receipt_id":fred_record["publication_receipt_id"]},
        "corrected_source_set":{"object_id":ss_record["object_id"],
            "artifact_content_hash":ss_record["artifact_content_hash"],
            "package_sha256":ss_record["package_sha256"]},
        "corrected_canonical":{"object_id":market_record["object_id"],
            "artifact_content_hash":market_record["artifact_content_hash"],
            "package_sha256":market_record["package_sha256"],
            "database_sha256":manifest["database_sha256"]},
        "factual_delta":delta, "correction_id":record["correction_id"],
        "correction_record_sha256":sha256_json(record), "correction_record_created":record_created,
        "expected_old":{"source_set":record["expected_source_set"],
            "canonical_market":record["expected_canonical"],
            "source":record["expected_source_pointers"], "serving_market":None},
        "target":{"source_set":record["corrected_source_set"],
            "canonical_market":record["corrected_canonical"],
            "source":record["target_source_pointers"], "serving_market":None},
        "readiness_evidence":record["readiness_evidence"],
        "operation_order":record["operation_order"], "preflight":preflight,
        "authorization_token":authorization_token(record),
        "accepted_state_unchanged":True, "readiness_unchanged":True,
        "serving_unchanged":True, "provider_acquisition_performed":False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True); parser.add_argument("--branch", required=True)
    parser.add_argument("--workspace", type=Path, required=True); parser.add_argument("--git-sha", required=True)
    parser.add_argument("--output", type=Path, required=True); parser.add_argument("--live", action="store_true")
    parser.add_argument("--authorization-token", default=""); args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""))
    if args.workspace.exists(): shutil.rmtree(args.workspace)
    report = run(api=api, branch=args.branch, workspace=args.workspace, git_sha=args.git_sha,
        live=args.live, supplied_authorization=args.authorization_token)
    write_canonical_json(args.output, report); print(json.dumps(report, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
