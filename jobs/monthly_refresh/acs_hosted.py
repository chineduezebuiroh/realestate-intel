"""Hosted common-lifecycle runner for one independently pinned ACS product."""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Mapping

from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.acs_monthly import (candidate, discover_pin, retrieve_pinned_snapshot)
from jobs.monthly_refresh.bps_hosted import CATALOG_PATH, publish_candidate
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, governed_record
from jobs.monthly_refresh.source_inputs import GitHubPinStore, discover_persist_execute
from sources.census_acs.artifact import PRODUCTS


def execute_product(*, source_id: str, mode: str, cycle_id: str, workspace: Path,
                    pin_store: Any, discover: Callable[[], tuple[dict, dict[str, Path]]],
                    retrieve: Callable[[Mapping[str, Any], Path], Any],
                    build: Callable[..., dict], publish: Callable[[Path, str], Mapping[str, Any]],
                    record: Callable[[Mapping[str, Any], Mapping[str, Any]], Any]) -> dict[str, Any]:
    cached: dict[str, Path] = {}
    def discovery() -> dict:
        pin, paths = discover(); cached.update(paths); return pin
    def execution(pin: Mapping[str, Any]) -> dict[str, Any]:
        paths = cached
        if not paths:
            path = workspace / "pinned-input" / "snapshot"
            path.parent.mkdir(parents=True, exist_ok=True); retrieve(pin, path); paths = {"snapshot": path}
        artifact = workspace / "artifact"
        if artifact.exists(): shutil.rmtree(artifact)
        built = build(pin=pin, paths=paths, output=artifact, cycle_id=cycle_id)
        publication = dict(publish(artifact, source_id)); item = publication["record"]
        result = {"schema_version": "monthly_source_execution_result_v1", "source_id": source_id,
            "cycle_id": cycle_id, "status": "succeeded", "candidate_artifact_id": item["object_id"],
            "artifact_content_hash": item["artifact_content_hash"], "package_sha256": item["package_sha256"],
            "publication_state": "published_verified", "validation_status": "passed",
            "provider_release_id": item["metadata"]["provider_release_id"],
            "observation_max": item["metadata"]["observation_max"], "prior_artifact_id": None,
            "source_change_detected": True, "retryability": "not_applicable",
            "accepted_pointer_changed": False, "evidence_uri": item["logical_artifact_uri"]}
        record(result, publication["catalog"])
        return {"result": result, "pin": dict(pin), "candidate": built, "publication": publication}
    return discover_persist_execute(mode=mode, store=pin_store, cycle_id=cycle_id,
        source_id=source_id, required_members={"snapshot"}, discover_and_retrieve=discovery, execute=execution)


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--source-id", choices=sorted(PRODUCTS), required=True)
    parser.add_argument("--mode", choices=("normal", "resume", "replay"), required=True)
    parser.add_argument("--cycle-id", required=True); parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True); parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", "")); cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    results = GitHubCycleResultStore(api, args.branch)
    def publish(path: Path, source: str) -> Mapping[str, Any]:
        return publish_candidate(artifact=path, source_id=source, api=api, cas=cas,
            workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"),
            logical_source_id=source)
    value = execute_product(source_id=args.source_id, mode=args.mode, cycle_id=args.cycle_id,
        workspace=args.workspace, pin_store=GitHubPinStore(api, args.branch),
        discover=lambda: discover_pin(cycle_id=args.cycle_id, source_id=args.source_id,
                                      workspace=args.workspace / "discovery"),
        retrieve=lambda pin, path: retrieve_pinned_snapshot(pin, path), build=candidate, publish=publish,
        record=lambda result, catalog: results.put(governed_record(result, policy, catalog)))
    write_canonical_json(args.output, value["result"]); print(json.dumps(value["result"], sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
