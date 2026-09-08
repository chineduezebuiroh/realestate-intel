"""One-time governed July control-plane bootstrap into ``main``.

The adapter copies only already-durable JSON evidence and the catalog records
that evidence references.  It cannot discover providers, publish Releases, or
move an accepted pointer.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.parse
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.catalog import immutable_record_identity, validate_catalog_namespace
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import canonical_json_bytes, sha256_json, write_canonical_json
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.readiness import add_readiness, empty_readiness, validate_readiness

SCHEMA = "control_plane_migration_v1"
SOURCE_BRANCH = "monthly-refresh-orchestration"
TARGET_BRANCH = "main"
CYCLE_ID = "monthly_cycle__2026-07__7cab1c5df177a1e4"
RESOLUTION_ID = "bps_family_resolution__457b5a17a73da623cfcfea08"
CONFIRMATION = "MIGRATE_GOVERNED_JULY_CONTROL_PLANE"
CATALOG_PATH = "config/artifact_catalog.json"
READINESS_PATH = "config/monthly_refresh_readiness.json"
EVIDENCE_PATHS = (
    *(f"config/monthly_source_cycle_results/{CYCLE_ID}/{source}.json" for source in
      ("census_bps", "census_bps_provisional", "ces", "fred_macro", "laus")),
    f"config/bps_family_resolutions/{RESOLUTION_ID}.json",
    f"config/monthly_source_republications/{CYCLE_ID}/census_bps/"
    "source_republication__census_bps__c20f4259f4cae4c9802e.json",
    f"config/monthly_source_republications/{CYCLE_ID}/census_bps_provisional/"
    "source_republication__census_bps_provisional__bbcce0aab4e203176b14.json",
)


class JSONStore:
    def __init__(self, api: GitHubAPI, branch: str, path: str):
        self.api, self.branch, self.path = api, branch, path

    def read(self) -> tuple[dict[str, Any] | None, str | None]:
        encoded = urllib.parse.quote(self.path, safe="/")
        item, _ = self.api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(self.branch)}",
                                   expected=(200, 404))
        if item is None:
            return None, None
        return json.loads(base64.b64decode(item["content"])), item["sha"]

    def write(self, value: Mapping[str, Any], oid: str | None, message: str) -> None:
        payload = {"message": message, "branch": self.branch,
                   "content": base64.b64encode(canonical_json_bytes(value)).decode()}
        if oid is not None:
            payload["sha"] = oid
        encoded = urllib.parse.quote(self.path, safe="/")
        self.api.request("PUT", f"/contents/{encoded}", payload=payload, expected=(200, 201))


def _required(value: dict[str, Any] | None, path: str) -> dict[str, Any]:
    if value is None:
        raise PublicationError(f"required source authority object is absent: {path}")
    return value


def _artifact_ids(value: Any) -> set[str]:
    """Collect every exact source-artifact reference carried by frozen evidence."""
    if isinstance(value, Mapping):
        return set().union(*(_artifact_ids(v) for v in value.values()), set())
    if isinstance(value, list):
        return set().union(*(_artifact_ids(v) for v in value), set())
    return {value} if isinstance(value, str) and value.startswith("src__") else set()


def _validate_evidence(evidence: Mapping[str, Mapping[str, Any]], catalog: Mapping[str, Any]) -> None:
    records = {r["object_id"]:r for r in catalog["immutable_records"]}
    expected_sources = {"census_bps", "census_bps_provisional", "ces", "fred_macro", "laus"}
    seen = set()
    for path, value in evidence.items():
        if "/monthly_source_cycle_results/" in path:
            result = value.get("result", {})
            source = result.get("source_id"); seen.add(source)
            if value.get("schema_version") != "monthly_source_cycle_result_v1" \
                    or value.get("cycle_id") != CYCLE_ID or result.get("cycle_id") != CYCLE_ID \
                    or result.get("status") != "succeeded" or result.get("validation_status") != "passed" \
                    or result.get("publication_state") != "published_verified" \
                    or result.get("accepted_pointer_changed") is not False:
                raise PublicationError(f"invalid governed monthly result: {path}")
            record = records.get(result.get("candidate_artifact_id"))
            if record is None or record["metadata"].get("source_id") != source \
                    or record["artifact_content_hash"] != result.get("artifact_content_hash") \
                    or record["package_sha256"] != result.get("package_sha256"):
                raise PublicationError(f"monthly result/catalog contradiction: {path}")
        elif "/bps_family_resolutions/" in path:
            if value.get("resolution_id") != RESOLUTION_ID or value.get("cycle_id") != CYCLE_ID \
                    or value.get("schema_version") != "bps_family_resolution_record_v1":
                raise PublicationError("frozen BPS family resolution identity contradiction")
        elif "/monthly_source_republications/" in path:
            if value.get("parent_cycle_id") != CYCLE_ID or value.get("schema_version") != "bps_source_republication_v1" \
                    or value.get("source_id") not in {"census_bps", "census_bps_provisional"}:
                raise PublicationError(f"BPS republication identity contradiction: {path}")
    if seen != expected_sources:
        raise PublicationError("physical monthly result inventory is incomplete")


def migration_path(migration_id: str) -> str:
    return f"config/control_plane_migrations/{migration_id}.json"


def build_plan(*, source_catalog: Mapping[str, Any], target_catalog: Mapping[str, Any],
               source_objects: Mapping[str, Mapping[str, Any]], target_objects: Mapping[str, Mapping[str, Any] | None],
               source_catalog_sha: str, target_catalog_sha: str | None) -> dict[str, Any]:
    """Validate the exact closure and return a deterministic, non-mutating plan."""
    source_catalog = validate_catalog_namespace(deepcopy(dict(source_catalog)), fixture=False)
    target_catalog = validate_catalog_namespace(deepcopy(dict(target_catalog)), fixture=False)
    readiness = deepcopy(_required(source_objects.get(READINESS_PATH), READINESS_PATH))
    matches = [r for r in readiness.get("records", []) if r.get("cycle_id") == CYCLE_ID]
    if len(matches) != 1 or matches[0].get("consumed") is not False:
        raise PublicationError("source authority must contain one unconsumed governed July readiness")
    july_readiness = matches[0]
    validate_readiness({"schema_version": readiness.get("schema_version"), "records":[july_readiness]},
                       catalog=dict(source_catalog), policy_path=Path("config/monthly_refresh_policy.json"))
    evidence = {p: deepcopy(_required(source_objects.get(p), p)) for p in EVIDENCE_PATHS}
    _validate_evidence(evidence, source_catalog)
    referenced = _artifact_ids([july_readiness, *evidence.values()])
    records = {r["object_id"]: r for r in source_catalog["immutable_records"]}
    missing_source = sorted(referenced - set(records))
    if missing_source:
        raise PublicationError(f"source catalog lacks referenced immutable records: {missing_source}")
    required_records = [deepcopy(records[object_id]) for object_id in sorted(referenced)]
    target_by_key = {(r["object_type"], r["object_id"]): r for r in target_catalog["immutable_records"]}
    add = []
    for record in required_records:
        old = target_by_key.get((record["object_type"], record["object_id"]))
        if old is None:
            add.append(record)
        elif immutable_record_identity(old) != immutable_record_identity(record):
            raise IdentityCollisionError(f"target catalog contradicts {record['object_id']}")
    merged = deepcopy(dict(target_catalog)); merged["immutable_records"].extend(add)
    merged["immutable_records"].sort(key=lambda r:(r["object_type"], r["object_id"]))
    merged = validate_catalog_namespace(merged, fixture=False)
    if merged["accepted"] != target_catalog["accepted"]:
        raise AssertionError("catalog merge moved accepted pointers")
    object_inventory = []
    source_copy = {**evidence, READINESS_PATH:{"schema_version":readiness["schema_version"], "records":[july_readiness]}}
    for path, value in sorted(source_copy.items()):
        target = target_objects.get(path)
        if path == READINESS_PATH:
            target_state = empty_readiness() if target is None else deepcopy(dict(target))
            next_state, changed = add_readiness(target_state, deepcopy(july_readiness), catalog=merged,
                                                policy_path=Path("config/monthly_refresh_policy.json"))
            target_value = next_state
        else:
            if target is not None and dict(target) != dict(value):
                raise IdentityCollisionError(f"target evidence contradicts source: {path}")
            changed, target_value = target is None, deepcopy(dict(value))
        object_inventory.append({"path":path, "source_sha256":sha256_json(value),
                                 "target_pre_sha256":None if target is None else sha256_json(target),
                                 "action":"create" if changed else "reuse", "target_value":target_value})
    identity = {"schema_version":SCHEMA, "source_branch":SOURCE_BRANCH, "target_branch":TARGET_BRANCH,
                "cycle_id":CYCLE_ID, "objects":[{"path":x["path"],"source_sha256":x["source_sha256"]}
                                                  for x in object_inventory],
                "catalog_records":[{"object_type":r["object_type"],"object_id":r["object_id"],
                    "artifact_content_hash":r["artifact_content_hash"],"package_sha256":r["package_sha256"]}
                    for r in required_records]}
    migration_id = "control_plane_migration__" + sha256_json(identity)[:24]
    record = {**identity, "migration_id":migration_id, "source_catalog_sha":source_catalog_sha,
              "target_catalog_pre_sha":target_catalog_sha,
              "target_catalog_pre_content_sha256":sha256_json(target_catalog),
              "target_accepted_pre_state":deepcopy(target_catalog["accepted"]),
              "object_inventory":[{k:v for k,v in item.items() if k != "target_value"}
                                  for item in object_inventory],
              "catalog_records_added":[r["object_id"] for r in add], "completion_state":"prepared"}
    return {"migration_id":migration_id, "record":record, "merged_catalog":merged,
            "catalog_changed":bool(add), "objects":object_inventory,
            "required_artifact_ids":sorted(referenced)}


def execute_plan(*, plan: Mapping[str, Any], target_catalog_store: Any,
                 target_stores: Mapping[str, Any], migration_store: Any) -> dict[str, Any]:
    """Persist prepared intent, CAS catalog, create/reuse evidence, then complete."""
    proposed = deepcopy(plan["record"])
    catalog, oid = target_catalog_store.read()
    existing, migration_oid = migration_store.read()
    if existing is not None and existing.get("migration_id") != proposed["migration_id"]:
        raise IdentityCollisionError("migration record identity contradiction")
    record = deepcopy(existing) if existing is not None else proposed
    if catalog["accepted"] != record["target_accepted_pre_state"]:
        raise PublicationError("target accepted pointers changed after migration preflight")
    if plan["catalog_changed"] and oid != record["target_catalog_pre_sha"]:
        raise PublicationError("stale target catalog SHA; migration writes nothing")
    if existing is not None:
        if existing.get("completion_state") == "complete":
            return {"migration_id":proposed["migration_id"], "changed":False, "completion_state":"complete"}
        if existing.get("completion_state") != "prepared":
            raise IdentityCollisionError("migration record state contradiction")
    else:
        migration_store.write(proposed, None, f"Prepare {proposed['migration_id']}")
    if plan["catalog_changed"]:
        target_catalog_store._write(deepcopy(plan["merged_catalog"]), oid,
                                    f"Bootstrap July control plane {proposed['migration_id']}")
    for item in plan["objects"]:
        store = target_stores[item["path"]]; current, current_oid = store.read()
        if current is None:
            store.write(item["target_value"], None, f"Bootstrap July evidence {proposed['migration_id']}")
        elif current != item["target_value"]:
            raise IdentityCollisionError(f"target changed during migration: {item['path']}")
    durable_catalog, _ = target_catalog_store.read()
    if durable_catalog["accepted"] != record["target_accepted_pre_state"]:
        raise PublicationError("migration changed accepted pointers")
    record["completion_state"] = "complete"
    current, migration_oid = migration_store.read()
    migration_store.write(record, migration_oid, f"Complete {record['migration_id']}")
    return {"migration_id":record["migration_id"], "changed":True, "completion_state":"complete"}


def run(*, api: GitHubAPI, live: bool) -> dict[str, Any]:
    source_cas = GitHubCatalogCAS(api, CATALOG_PATH, SOURCE_BRANCH)
    target_cas = GitHubCatalogCAS(api, CATALOG_PATH, TARGET_BRANCH)
    source_catalog, source_sha = source_cas.read(); target_catalog, target_sha = target_cas.read()
    paths = (*EVIDENCE_PATHS, READINESS_PATH)
    source_stores = {p:JSONStore(api, SOURCE_BRANCH, p) for p in paths}
    target_stores = {p:JSONStore(api, TARGET_BRANCH, p) for p in paths}
    source_objects = {p:source_stores[p].read()[0] for p in paths}
    target_objects = {p:target_stores[p].read()[0] for p in paths}
    plan = build_plan(source_catalog=source_catalog, target_catalog=target_catalog,
        source_objects=source_objects, target_objects=target_objects,
        source_catalog_sha=source_sha or "", target_catalog_sha=target_sha)
    summary = {"schema_version":SCHEMA, "migration_id":plan["migration_id"], "cycle_id":CYCLE_ID,
        "source_branch":SOURCE_BRANCH, "target_branch":TARGET_BRANCH, "live_migration_performed":False,
        "provider_discovery_performed":False, "source_artifacts_published":False,
        "source_set_published":False, "canonical_market_published":False,
        "cohort_promotion_record_created":False, "accepted_pointers_changed":False,
        "redfin_consumed":False, "serving_mutated":False,
        "catalog_records_to_add":plan["record"]["catalog_records_added"],
        "object_plan":[{k:v for k,v in item.items() if k != "target_value"} for item in plan["objects"]]}
    if not live:
        return summary
    migration_store = JSONStore(api, TARGET_BRANCH, migration_path(plan["migration_id"]))
    outcome = execute_plan(plan=plan, target_catalog_store=target_cas,
                           target_stores=target_stores, migration_store=migration_store)
    return {**summary, **outcome, "live_migration_performed":True}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True); parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--live", action="store_true"); parser.add_argument("--confirm", default="")
    args = parser.parse_args()
    if args.live and args.confirm != CONFIRMATION:
        raise SystemExit(f"live migration requires exact confirmation: {CONFIRMATION}")
    report = run(api=GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", "")), live=args.live)
    write_canonical_json(args.output, report); print(json.dumps(report, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
