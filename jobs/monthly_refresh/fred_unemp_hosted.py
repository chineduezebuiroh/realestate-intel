"""Hosted durable-input lifecycle for the independent FRED unemployment source."""
from __future__ import annotations

import argparse
import json
import os
import shutil
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping

from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
                                                   GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import sha256_json, write_canonical_json
from jobs.monthly_refresh.bps_hosted import CATALOG_PATH, publish_candidate
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, governed_record
from jobs.monthly_refresh.fred_unemp import (MEMBER, candidate, discover_pin,
                                             recover_pinned_snapshot)
from jobs.monthly_refresh.source_inputs import GitHubPinStore, discover_persist_execute
from sources.fred_unemp.artifact import SOURCE_ID


def execute_source(*, mode: str, cycle_id: str, workspace: Path, pin_store: Any,
                   existing_result: Mapping[str, Any] | None,
                   discover: Callable[[], tuple[dict, dict[str, Path]]],
                   build: Callable[..., dict], publish: Callable[[Path, str], Mapping[str, Any]],
                   record: Callable[[Mapping[str, Any], Mapping[str, Any]], Any],
                   prior_artifact: Path | None = None) -> dict[str, Any]:
    if existing_result is not None:
        result = existing_result.get("result", existing_result)
        if result.get("source_id") != SOURCE_ID or result.get("cycle_id") != cycle_id \
                or result.get("status") != "succeeded" or result.get("accepted_pointer_changed") is not False:
            raise ValueError("existing FRED unemployment cycle result is invalid")
        return {"result": deepcopy(result), "reused": True}
    cached: dict[str, Path] = {}
    def discovery() -> dict:
        pin, paths = discover(); cached.update(paths); return pin
    def execution(pin: Mapping[str, Any]) -> dict[str, Any]:
        paths = cached
        if not paths:
            path = workspace / "pinned-input" / "fred_unemp.normalized.json"
            recover_pinned_snapshot(pin, path); paths = {MEMBER: path}
        artifact = workspace / "artifact"
        if artifact.exists(): shutil.rmtree(artifact)
        built = build(pin=pin, paths=paths, output=artifact, cycle_id=cycle_id,
                      prior_artifact=prior_artifact)
        publication = dict(publish(artifact, SOURCE_ID)); item = publication["record"]
        result = {"schema_version": "monthly_source_execution_result_v1", "source_id": SOURCE_ID,
            "cycle_id": cycle_id, "status": "succeeded", "candidate_artifact_id": item["object_id"],
            "artifact_content_hash": item["artifact_content_hash"], "package_sha256": item["package_sha256"],
            "publication_state": "published_verified", "validation_status": "passed",
            "provider_release_id": item["metadata"]["provider_release_id"],
            "observation_max": item["metadata"]["observation_max"],
            "prior_artifact_id": built.get("prior_artifact_id"),
            "source_change_detected": built["source_change_detected"], "retryability": "not_applicable",
            "accepted_pointer_changed": False, "evidence_uri": item["logical_artifact_uri"]}
        record(result, publication["catalog"])
        return {"result": result, "pin": dict(pin), "candidate": built,
                "publication": publication, "reused": False}
    return discover_persist_execute(mode=mode, store=pin_store, cycle_id=cycle_id,
        source_id=SOURCE_ID, required_members={MEMBER}, discover_and_retrieve=discovery, execute=execution)


def authority_fingerprint(catalog: Mapping[str, Any], readiness: Mapping[str, Any]) -> str:
    return sha256_json({"accepted": catalog.get("accepted"), "readiness": readiness})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("normal", "resume", "replay"), required=True)
    parser.add_argument("--cycle-id", required=True); parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True); parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""))
    cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch); catalog, _ = cas.read()
    readiness_item, _ = api.request("GET", "/contents/config/monthly_refresh_readiness.json?ref=" + args.branch,
                                    expected=(200,))
    import base64
    readiness = json.loads(base64.b64decode(readiness_item["content"]))
    before = authority_fingerprint(catalog, readiness)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    results = GitHubCycleResultStore(api, args.branch)
    existing = results.get(args.cycle_id, SOURCE_ID)
    if existing is not None:
        # Revalidate durable result identity against the current catalog before
        # allowing resume/normal reuse.  A malformed or orphaned record is never
        # treated as successful merely because its path exists.
        validated = governed_record(existing["result"], policy, catalog)
        if validated != existing:
            raise RuntimeError("durable FRED unemployment cycle result evidence drift")
    prior_artifact = None
    accepted_id = catalog.get("accepted", {}).get("source", {}).get(SOURCE_ID)
    if accepted_id is not None:
        matches = [item for item in catalog.get("immutable_records", [])
                   if item.get("object_type") == "source" and item.get("object_id") == accepted_id
                   and item.get("metadata", {}).get("source_id") == SOURCE_ID]
        if len(matches) != 1:
            raise RuntimeError("accepted FRED unemployment prior does not resolve exactly once")
        prior_artifact = GitHubReleaseArtifactResolver(
            catalog, api, args.workspace / "prior").resolve(matches[0]["logical_artifact_uri"])
    def publish(path: Path, source: str) -> Mapping[str, Any]:
        return publish_candidate(artifact=path, source_id=source, api=api, cas=cas,
            workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"),
            logical_source_id=source)
    value = execute_source(mode=args.mode, cycle_id=args.cycle_id, workspace=args.workspace,
        pin_store=GitHubPinStore(api, args.branch), existing_result=existing,
        discover=lambda: discover_pin(cycle_id=args.cycle_id, workspace=args.workspace / "discovery"),
        build=candidate, publish=publish, prior_artifact=prior_artifact,
        record=lambda result, durable_catalog: results.put(governed_record(result, policy, durable_catalog)))
    after_catalog, _ = cas.read()
    after_item, _ = api.request("GET", "/contents/config/monthly_refresh_readiness.json?ref=" + args.branch,
                                expected=(200,))
    after_readiness = json.loads(base64.b64decode(after_item["content"]))
    if authority_fingerprint(after_catalog, after_readiness) != before:
        raise RuntimeError("FRED unemployment source execution changed accepted state or Redfin readiness")
    write_canonical_json(args.output, value["result"])
    print(json.dumps(value["result"], sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
