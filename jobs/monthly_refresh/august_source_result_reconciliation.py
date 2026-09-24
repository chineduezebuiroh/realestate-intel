"""Narrow, create-once reconciliation for three August authority-boundary records.

This module copies no bytes between providers or Releases.  It verifies exact
already-published immutable packages and reconciles only their missing catalog
and cycle-result records from the migration branch to production authority.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import tempfile
import urllib.parse
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.catalog import immutable_record_identity, validate_catalog_namespace, validate_record
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactResolver
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, add_record, governed_record, record_path

CYCLE_ID = "monthly_cycle__2026-08__a9e022a980d29cd7"
SOURCE_BRANCH = "monthly-refresh-orchestration"
TARGET_BRANCH = "main"
CATALOG_PATH = "config/artifact_catalog.json"
EXPECTED_IDS = {
    "ces": "src__ces__2026-08__r1__c39c12b32234bd93",
    "fred_macro": "src__fred_macro__2026-09__r1__29a8ac35a657cfce",
    "laus": "src__laus__2026-07__r1__a1b60a2a16d2b99f",
}
EXPECTED_REMOTE = {
    "ces": {"artifact_content_hash":"c39c12b32234bd93b9c5d020e6aa3b9cd6ea9bc128908b655bb33a0271ac8b5d",
        "package_sha256":"587354777ad751c78112bdc9b9706a863088b7f1cde69949993085319adaf21d",
        "provider_release_id":"ordinary-current:1ac26c539265efb8553a8021ba2ea44b3b265024d611030f4176b7911fb05daa",
        "observation_max":"2026-08-31", "release_id":395239324, "asset_id":584961759},
    "fred_macro": {"artifact_content_hash":"29a8ac35a657cfce70d9b24c0a6635642c401f65644d3e369feb6f37d8ee748c",
        "package_sha256":"191eb91d69da76922ee8ea16c467417d27ef94c0fff3ae995f1d00cb5ca5dc86",
        "provider_release_id":"ordinary-current:bee23ae2ce8e773664398e0e85eb9b90718f51d8da05c767fcbd211ef99429bb",
        "observation_max":"2026-09-30", "release_id":394847182, "asset_id":584147302},
    "laus": {"artifact_content_hash":"a1b60a2a16d2b99f4e05b27d80fb57ca8dabeed2fba738dcca02d273598dde91",
        "package_sha256":"2eb48a926707caf7b43cda2dc427d6ce53f5b54bd664699a14e00b597e50af8f",
        "provider_release_id":"laus-ordinary_overlap-current:89852c3572362efc549378408f0f61a59d0707b3dd2fb26efe42a0a3dfe310f4",
        "observation_max":"2026-08-31", "release_id":395239429, "asset_id":584961909},
}
CONFIRMATION = "RECONCILE_AUGUST_CES_FRED_LAUS_TO_MAIN"
FORBIDDEN_ACCEPTED_KEYS = ("source_set", "canonical_market", "serving_market")


def _read_json(api: GitHubAPI, branch: str, path: str) -> dict[str, Any] | None:
    encoded = urllib.parse.quote(path, safe="/")
    item, _ = api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(branch)}",
                          expected=(200, 404))
    return None if item is None else json.loads(base64.b64decode(item["content"]))


def _one(items: list[dict[str, Any]], label: str) -> dict[str, Any]:
    if len(items) != 1:
        raise PublicationError(f"{label} must resolve exactly once")
    return items[0]


def _verify_remote(api: GitHubAPI, catalog: dict[str, Any], record: dict[str, Any]) -> None:
    release, _ = api.request("GET", f"/releases/{record['release_id']}", expected=(200, 404))
    if release is None or release.get("id") != record["release_id"] or release.get("draft") \
            or release.get("tag_name") != record["release_tag"]:
        raise PublicationError("exact immutable Release identity mismatch")
    asset = _one([item for item in release.get("assets", [])
                  if item.get("id") == record["asset_id"]], "exact immutable Release asset")
    if asset.get("name") != record["asset_filename"]:
        raise PublicationError("exact immutable asset filename mismatch")
    if asset.get("digest") not in (None, f"sha256:{record['package_sha256']}"):
        raise PublicationError("exact immutable asset digest mismatch")
    with tempfile.TemporaryDirectory() as directory:
        resolved = GitHubReleaseArtifactResolver(catalog, api, Path(directory)).resolve(
            record["logical_artifact_uri"])
        manifest = json.loads((resolved / "manifest.json").read_text())
    expected = {"artifact_id": record["object_id"],
                "artifact_content_hash": record["artifact_content_hash"],
                "source_id": record["metadata"]["source_id"],
                "provider_release_id": record["metadata"]["provider_release_id"],
                "observation_max": record["metadata"]["observation_max"]}
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise PublicationError("immutable artifact manifest identity mismatch")


def build_plan(*, source_catalog: dict[str, Any], target_catalog: dict[str, Any],
               source_records: Mapping[str, dict[str, Any]], policy: dict[str, Any],
               expected_ids: Mapping[str, str]) -> dict[str, Any]:
    if dict(expected_ids) != EXPECTED_IDS:
        raise PublicationError("reconciliation candidate identities are not the governed August set")
    source_catalog = validate_catalog_namespace(deepcopy(source_catalog), fixture=False)
    target_catalog = validate_catalog_namespace(deepcopy(target_catalog), fixture=False)
    accepted_before = deepcopy(target_catalog["accepted"])
    merged = deepcopy(target_catalog)
    catalog_actions: dict[str, str] = {}
    result_actions: dict[str, str] = {}
    records: dict[str, dict[str, Any]] = {}

    for source, artifact_id in EXPECTED_IDS.items():
        candidate = _one([item for item in source_catalog["immutable_records"]
                          if item.get("object_id") == artifact_id], f"source catalog {source}")
        validate_record(candidate)
        expected_remote = EXPECTED_REMOTE[source]
        exact = {"artifact_content_hash":candidate.get("artifact_content_hash"),
            "package_sha256":candidate.get("package_sha256"), "release_id":candidate.get("release_id"),
            "asset_id":candidate.get("asset_id"),
            "provider_release_id":candidate.get("metadata", {}).get("provider_release_id"),
            "observation_max":candidate.get("metadata", {}).get("observation_max")}
        if candidate["metadata"].get("source_id") != source or exact != expected_remote \
                or candidate.get("release_tag") != f"source-artifact/{source}/{artifact_id}" \
                or candidate.get("asset_filename") != f"{artifact_id}.tar":
            raise PublicationError(f"{source} exact immutable catalog identity mismatch")
        current = [item for item in merged["immutable_records"]
                   if item.get("object_id") == artifact_id or
                   item.get("logical_artifact_uri") == candidate["logical_artifact_uri"]]
        if current and (len(current) != 1 or
                        immutable_record_identity(current[0]) != immutable_record_identity(candidate)):
            raise IdentityCollisionError(f"contradictory {source} catalog record on authority")
        if not current:
            merged["immutable_records"].append(deepcopy(candidate))
            merged["immutable_records"].sort(key=lambda item: (item["object_type"], item["object_id"]))
            catalog_actions[source] = "add"
        else:
            catalog_actions[source] = "reuse"
        proposed = source_records.get(source)
        if proposed is None:
            raise PublicationError(f"preserved {source} cycle result is absent")
        governed = governed_record(proposed["result"], policy, merged,
            source_evidence=proposed.get("source_evidence"))
        if governed != proposed:
            raise PublicationError(f"preserved {source} governed result JSON mismatch")
        records[source] = governed
        result_actions[source] = "pending"

    merged = validate_catalog_namespace(merged, fixture=False)
    if merged["accepted"] != accepted_before:
        raise PublicationError("reconciliation would mutate accepted pointers")
    return {"catalog": merged, "accepted_before": accepted_before, "records": records,
            "catalog_actions": catalog_actions, "result_actions": result_actions}


def run(*, api: GitHubAPI, policy: dict[str, Any], cycle_id: str,
        expected_ids: Mapping[str, str], live: bool) -> dict[str, Any]:
    if cycle_id != CYCLE_ID:
        raise PublicationError("reconciliation cycle identity mismatch")
    source_cas = GitHubCatalogCAS(api, CATALOG_PATH, SOURCE_BRANCH, fixture=False)
    target_cas = GitHubCatalogCAS(api, CATALOG_PATH, TARGET_BRANCH, fixture=False)
    source_catalog, _ = source_cas.read(); target_catalog, target_oid = target_cas.read()
    source_records = {source: _read_json(api, SOURCE_BRANCH, record_path(cycle_id, source))
                      for source in EXPECTED_IDS}
    plan = build_plan(source_catalog=source_catalog, target_catalog=target_catalog,
        source_records=source_records, policy=policy, expected_ids=expected_ids)
    for source in EXPECTED_IDS:
        record = next(item for item in source_catalog["immutable_records"]
                      if item["object_id"] == EXPECTED_IDS[source])
        _verify_remote(api, source_catalog, record)
        existing = _read_json(api, TARGET_BRANCH, record_path(cycle_id, source))
        _, changed = add_record(existing, plan["records"][source])
        plan["result_actions"][source] = "add" if changed else "reuse"

    report = {"schema_version":"august_source_result_reconciliation_plan_v1",
        "mode":"live" if live else "preflight", "cycle_id":cycle_id,
        "source_branch":SOURCE_BRANCH, "authority_branch":TARGET_BRANCH,
        "catalog_actions":plan["catalog_actions"], "cycle_result_actions":plan["result_actions"],
        "provider_discovery_performed":False, "provider_acquisition_performed":False,
        "source_artifact_publication_performed":False, "accepted_pointers_changed":False,
        "redfin_readiness_consumed":False, "source_set_created":False,
        "canonical_market_created":False, "serving_market_created":False,
        "mutation_performed":False}
    if not live:
        return report

    if any(action == "add" for action in plan["catalog_actions"].values()):
        target_cas._write(plan["catalog"], target_oid, "Reconcile August CES/FRED/LAUS catalog evidence")
    durable_catalog, _ = target_cas.read()
    if durable_catalog["accepted"] != plan["accepted_before"]:
        raise PublicationError("accepted pointers changed during reconciliation")
    for source, expected in plan["records"].items():
        exact = _one([item for item in durable_catalog["immutable_records"]
                      if item.get("object_id") == EXPECTED_IDS[source]], f"durable {source} catalog record")
        source_exact = next(item for item in source_catalog["immutable_records"]
                            if item["object_id"] == EXPECTED_IDS[source])
        if immutable_record_identity(exact) != immutable_record_identity(source_exact):
            raise PublicationError(f"durable {source} catalog verification failed")
        GitHubCycleResultStore(api, TARGET_BRANCH).put(expected)
        if _read_json(api, TARGET_BRANCH, record_path(cycle_id, source)) != expected:
            raise PublicationError(f"durable {source} cycle-result verification failed")
    return {**report, "mutation_performed": any(
        action == "add" for action in (*plan["catalog_actions"].values(), *plan["result_actions"].values()))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True); parser.add_argument("--cycle-id", required=True)
    for source, artifact_id in EXPECTED_IDS.items():
        parser.add_argument(f"--{source.replace('_', '-')}-artifact-id", required=True)
    parser.add_argument("--policy", type=Path, default=Path("config/monthly_refresh_policy.json"))
    parser.add_argument("--output", type=Path, required=True); parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm", default=""); args = parser.parse_args()
    if args.live and args.confirm != CONFIRMATION:
        raise SystemExit(f"live reconciliation requires exact confirmation: {CONFIRMATION}")
    expected = {source:getattr(args, source + "_artifact_id") for source in EXPECTED_IDS}
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""), read_only=not args.live)
    report = run(api=api, policy=json.loads(args.policy.read_text()), cycle_id=args.cycle_id,
                 expected_ids=expected, live=args.live)
    args.output.write_text(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(report, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
