"""Common durable lifecycle entry point for the independent census_nrc candidate."""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Mapping

from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.bps_hosted import CATALOG_PATH, publish_candidate
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, governed_record
from jobs.monthly_refresh.nrc_monthly import (MEMBERS, candidate, discover_pin,
                                               recover_pinned_workbooks)
from jobs.monthly_refresh.source_inputs import GitHubPinStore, discover_persist_execute
from sources.census_nrc.parser import SOURCE_ID


def execute_source(*, mode: str, cycle_id: str, workspace: Path, pin_store: Any,
                   discover: Callable[[], tuple[dict, dict[str, Path]]], build: Callable[..., dict],
                   publish: Callable[[Path, str], Mapping[str, Any]],
                   record: Callable[[Mapping[str, Any], Mapping[str, Any]], Any]) -> dict[str, Any]:
    cached: dict[str, Path] = {}
    def discovery() -> dict:
        pin, paths = discover(); cached.update(paths); return pin
    def execution(pin: Mapping[str, Any]) -> dict[str, Any]:
        paths = cached or recover_pinned_workbooks(pin, workspace / "pinned-input")
        artifact = workspace / "artifact"
        if artifact.exists(): shutil.rmtree(artifact)
        built = build(pin=pin, paths=paths, output=artifact, cycle_id=cycle_id)
        publication = dict(publish(artifact, SOURCE_ID)); item = publication["record"]
        result = {"schema_version": "monthly_source_execution_result_v1", "source_id": SOURCE_ID,
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
        source_id=SOURCE_ID, required_members=set(MEMBERS), discover_and_retrieve=discovery, execute=execution)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("normal", "resume", "replay"), required=True)
    parser.add_argument("--cycle-id", required=True); parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True); parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", "")); cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    # Isolated NRC-C recording authorization only; the versioned cohort policy remains untouched until NRC-D.
    policy = {**policy, "slower_cadence_sources": [*policy.get("slower_cadence_sources", []), SOURCE_ID]}
    results = GitHubCycleResultStore(api, args.branch)
    def publish(path: Path, source: str) -> Mapping[str, Any]:
        return publish_candidate(artifact=path, source_id=source, api=api, cas=cas,
            workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"), logical_source_id=source)
    value = execute_source(mode=args.mode, cycle_id=args.cycle_id, workspace=args.workspace,
        pin_store=GitHubPinStore(api, args.branch),
        discover=lambda: discover_pin(cycle_id=args.cycle_id, workspace=args.workspace / "discovery"),
        build=candidate, publish=publish,
        record=lambda result, catalog: results.put(governed_record(result, policy, catalog)))
    write_canonical_json(args.output, value["result"]); print(json.dumps(value["result"], sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
