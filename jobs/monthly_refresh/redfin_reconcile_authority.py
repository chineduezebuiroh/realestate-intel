"""One-time reconciliation of the published August Redfin candidate to main.

This adapter can only copy the exact, already-published catalog identity and
derive its readiness catalyst.  It has no publication, activation, consumption,
provider-discovery, or serving boundary.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

from core.source_artifacts.catalog import immutable_record_identity, validate_catalog_namespace
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import sha256_file, write_canonical_json
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.cohort_promotion_hosted import GitHubJSONCAS
from jobs.monthly_refresh.readiness import add_readiness, make_record, validate_readiness

SOURCE_BRANCH = "monthly-refresh-orchestration"
TARGET_BRANCH = "main"
CATALOG_PATH = "config/artifact_catalog.json"
READINESS_PATH = "config/monthly_refresh_readiness.json"
POLICY_PATH = Path("config/monthly_refresh_policy.json")
ARTIFACT_ID = "src__redfin__2026-08__r1__f2ca39c3c36a9c2b"
ARTIFACT_CONTENT_HASH = "f2ca39c3c36a9c2baf528a0f6e174d3f766a416e268e784bd18db91959097138"
PACKAGE_SHA256 = "95f19c4c7f6f3101298fe3a09a5d8bcca4689d0f6af4bbe64ad0ec0675d382ed"
RELEASE_ID = 394846183
ASSET_ID = 584144598
CYCLE_ID = "monthly_cycle__2026-08__a9e022a980d29cd7"
JULY_ARTIFACT_ID = "src__redfin__2026-07__r1__cabe3a10e22e58a8"
CONFIRMATION = "RECONCILE_AUGUST_REDFIN_TO_MAIN"


def _one(items: list[dict[str, Any]], description: str) -> dict[str, Any]:
    if len(items) != 1:
        raise PublicationError(f"{description} must resolve exactly once")
    return items[0]


def _verify_record(record: dict[str, Any]) -> None:
    expected = {"object_id": ARTIFACT_ID, "artifact_content_hash": ARTIFACT_CONTENT_HASH,
                "package_sha256": PACKAGE_SHA256, "release_id": RELEASE_ID,
                "asset_id": ASSET_ID, "publication_state": "published_immutable_verified"}
    for key, value in expected.items():
        if record.get(key) != value:
            raise PublicationError(f"August Redfin source {key} identity mismatch")
    if record.get("object_type") != "source" or record.get("metadata", {}).get("source_id") != "redfin":
        raise PublicationError("August Redfin source identity mismatch")


def _verify_remote(api: GitHubAPI, record: dict[str, Any]) -> None:
    release, _ = api.request("GET", f"/releases/{RELEASE_ID}", expected=(200, 404))
    if release is None or release.get("id") != RELEASE_ID or release.get("draft") \
            or release.get("tag_name") != record["release_tag"]:
        raise PublicationError("exact published GitHub Release is absent or contradictory")
    assets = [a for a in release.get("assets", []) if a.get("id") == ASSET_ID]
    asset = _one(assets, "exact GitHub Release asset")
    if asset.get("name") != record["asset_filename"]:
        raise PublicationError("exact GitHub Release asset filename mismatch")
    digest = asset.get("digest")
    if digest is not None and digest != f"sha256:{PACKAGE_SHA256}":
        raise PublicationError("GitHub Release asset digest contradicts package SHA-256")
    with tempfile.TemporaryDirectory() as directory:
        package = Path(directory) / "asset.tar"
        api.download_asset(ASSET_ID, package)
        if sha256_file(package) != PACKAGE_SHA256:
            raise PublicationError("GitHub Release asset bytes contradict package SHA-256")


def _build_plan(source_catalog: dict[str, Any], source_readiness: dict[str, Any],
                target_catalog: dict[str, Any], target_readiness: dict[str, Any]) -> dict[str, Any]:
    source_catalog = validate_catalog_namespace(deepcopy(source_catalog), fixture=False)
    target_catalog = validate_catalog_namespace(deepcopy(target_catalog), fixture=False)
    source_record = _one([r for r in source_catalog["immutable_records"]
                          if r.get("object_id") == ARTIFACT_ID], "source August catalog record")
    _verify_record(source_record)
    source_ready = _one([r for r in source_readiness.get("records", [])
                         if r.get("cycle_id") == CYCLE_ID], "source August readiness")
    validate_readiness(source_readiness, catalog=source_catalog, policy_path=POLICY_PATH)
    if source_ready.get("candidate_artifact_id") != ARTIFACT_ID or source_ready.get("consumed") is not False:
        raise PublicationError("source August readiness identity is not exact and unconsumed")

    accepted_before = deepcopy(target_catalog["accepted"])
    if accepted_before.get("source", {}).get("redfin") != JULY_ARTIFACT_ID:
        raise PublicationError("main accepted.source.redfin is not the governed July identity")
    matches = [r for r in target_catalog["immutable_records"] if r.get("object_id") == ARTIFACT_ID]
    if matches and (len(matches) != 1 or immutable_record_identity(matches[0]) != immutable_record_identity(source_record)):
        raise IdentityCollisionError("main contains a conflicting August Redfin catalog record")
    merged = deepcopy(target_catalog)
    if not matches:
        merged["immutable_records"].append(deepcopy(source_record))
        merged["immutable_records"].sort(key=lambda r: (r["object_type"], r["object_id"]))
    merged = validate_catalog_namespace(merged, fixture=False)
    if merged["accepted"] != accepted_before:
        raise PublicationError("catalog reconciliation would change accepted mapping")

    readiness_record = make_record(drop_id=source_ready["drop_id"],
        drop_content_hash=source_ready["drop_content_hash"], target_month=source_ready["target_month"],
        cycle=CYCLE_ID, artifact=_one([r for r in merged["immutable_records"]
                                      if r.get("object_id") == ARTIFACT_ID], "merged August record"))
    next_readiness, readiness_changed = add_readiness(deepcopy(target_readiness), readiness_record,
        catalog=merged, policy_path=POLICY_PATH)
    return {"record": source_record, "merged_catalog": merged,
            "catalog_changed": not matches, "readiness_record": readiness_record,
            "next_readiness": next_readiness, "readiness_changed": readiness_changed,
            "accepted_before": accepted_before}


def run(*, api: GitHubAPI, live: bool) -> dict[str, Any]:
    source_cas = GitHubCatalogCAS(api, CATALOG_PATH, SOURCE_BRANCH)
    target_cas = GitHubCatalogCAS(api, CATALOG_PATH, TARGET_BRANCH)
    source_store = GitHubJSONCAS(api, READINESS_PATH, SOURCE_BRANCH)
    target_store = GitHubJSONCAS(api, READINESS_PATH, TARGET_BRANCH)
    source_catalog, _ = source_cas.read(); target_catalog, target_oid = target_cas.read()
    source_readiness, _ = source_store.read(); target_readiness, target_ready_oid = target_store.read()
    if source_readiness is None or target_readiness is None:
        raise PublicationError("source and target readiness objects must exist")
    plan = _build_plan(source_catalog, source_readiness, target_catalog, target_readiness)
    _verify_remote(api, plan["record"])
    report = {"mode":"live" if live else "preflight", "artifact_id":ARTIFACT_ID,
        "cycle_id":CYCLE_ID, "catalog_action":"add" if plan["catalog_changed"] else "reuse",
        "readiness_action":"add" if plan["readiness_changed"] else "reuse",
        "accepted_mapping_unchanged":True, "mutation_performed":False}
    if not live:
        return report
    if plan["catalog_changed"]:
        target_cas._write(deepcopy(plan["merged_catalog"]), target_oid,
                          f"Reconcile published August Redfin artifact {ARTIFACT_ID}")
    durable_catalog, _ = target_cas.read()
    durable_record = _one([r for r in durable_catalog["immutable_records"]
                           if r.get("object_id") == ARTIFACT_ID], "durable August catalog record")
    _verify_record(durable_record)
    if durable_catalog["accepted"] != plan["accepted_before"] \
            or durable_catalog["accepted"]["source"].get("redfin") != JULY_ARTIFACT_ID:
        raise PublicationError("main accepted mapping changed during catalog reconciliation")
    # Derive again from the authoritative reread, never from stale readiness JSON.
    authoritative_ready = make_record(drop_id=plan["readiness_record"]["drop_id"],
        drop_content_hash=plan["readiness_record"]["drop_content_hash"],
        target_month=plan["readiness_record"]["target_month"], cycle=CYCLE_ID, artifact=durable_record)
    current_readiness, current_oid = target_store.read()
    if current_readiness is None:
        raise PublicationError("main readiness disappeared during reconciliation")
    updated, changed = add_readiness(current_readiness, authoritative_ready,
        catalog=durable_catalog, policy_path=POLICY_PATH)
    if changed:
        if current_oid != target_ready_oid:
            raise PublicationError("main readiness changed after preflight; refusing stale write")
        target_store.write(updated, current_oid, f"Reconcile August Redfin readiness {CYCLE_ID}")
    durable_readiness, _ = target_store.read()
    if durable_readiness is None:
        raise PublicationError("main readiness is absent after reconciliation")
    validate_readiness(durable_readiness, catalog=durable_catalog, policy_path=POLICY_PATH)
    august = _one([r for r in durable_readiness["records"] if r.get("cycle_id") == CYCLE_ID],
                  "durable August readiness")
    if august != authoritative_ready or august["consumed"] is not False:
        raise PublicationError("durable August readiness verification failed")
    july = _one([r for r in durable_readiness["records"] if r.get("candidate_artifact_id") == JULY_ARTIFACT_ID],
                "preserved July readiness")
    if july != _one([r for r in target_readiness["records"]
                     if r.get("candidate_artifact_id") == JULY_ARTIFACT_ID], "pre-mutation July readiness"):
        raise PublicationError("July readiness changed during reconciliation")
    return {**report, "mutation_performed":plan["catalog_changed"] or changed}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args()
    if args.live and args.confirm != CONFIRMATION:
        raise SystemExit(f"live reconciliation requires exact confirmation: {CONFIRMATION}")
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""), read_only=not args.live)
    report = run(api=api, live=args.live)
    write_canonical_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
