"""Thin hosted runner for the two independent physical BPS source members."""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Mapping

from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
    GitHubReleaseArtifactPublisher, GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.bps_bootstrap import acquire as acquire_compiled
from jobs.monthly_refresh.bps_monthly import (COMPILED_SOURCE_ID, PROVISIONAL_SOURCE_ID,
    compiled_candidate, discover_compiled_pin, discover_provisional_pin, provisional_candidate)
from jobs.monthly_refresh.bps_provisional_verification import LEVELS, acquire as acquire_provisional
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, governed_record
from jobs.monthly_refresh.source_inputs import GitHubPinStore, discover_persist_execute

CATALOG_PATH = "config/artifact_catalog.json"


def _retrieve_pin_members(pin: Mapping[str, Any], workspace: Path,
                          retrieve: Callable[[str, Path], Any]) -> dict[str, Path]:
    """Retrieve only the exact durable URLs; hashes are checked by the adapter."""
    workspace.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, member in pin["members"].items():
        path = workspace / name
        retrieve(str(member["url"]), path)
        paths[name] = path
    return paths


def publish_candidate(*, artifact: Path, source_id: str, api: GitHubAPI,
                      cas: GitHubCatalogCAS, workspace: Path, git_sha: str) -> dict[str, Any]:
    """Publish/reuse one immutable physical candidate without touching pointers."""
    manifest = validate_artifact(artifact, expected_source_id=source_id)["manifest"]
    catalog, _ = cas.read()
    matches = [r for r in catalog["immutable_records"] if r["object_type"] == "source"
               and r["object_id"] == manifest["artifact_id"]]
    if matches:
        if len(matches) != 1 or matches[0]["metadata"].get("source_id") != source_id \
                or matches[0]["artifact_content_hash"] != manifest["artifact_content_hash"]:
            raise RuntimeError("contradictory BPS candidate identity collision")
        record = matches[0]
        resolved = GitHubReleaseArtifactResolver(catalog, api, workspace / "existing").resolve(
            record["logical_artifact_uri"])
        remote = validate_artifact(resolved, expected_source_id=source_id)["manifest"]
        if remote["data_sha256"] != manifest["data_sha256"]:
            raise RuntimeError("published BPS candidate data identity collision")
        return {"record": record, "catalog": catalog, "reused": True}

    package = workspace / f"{manifest['artifact_id']}.tar"
    package_info = build_publication_package(artifact, package)
    metadata = {"logical_artifact_uri": manifest["artifact_uri"], "object_id": manifest["artifact_id"],
        "object_type": "source", "object_metadata": {"source_id": source_id},
        "artifact_content_hash": manifest["artifact_content_hash"],
        "member_hashes": {m["path"]: m["sha256"] for m in package_info["members"]},
        "publisher_git_sha": git_sha,
        "contract_versions": [manifest["artifact_contract_version"], package_info["package_contract_version"]]}
    publisher = GitHubReleaseArtifactPublisher(api)
    publisher.prepare(manifest["artifact_uri"], package.read_bytes(), metadata)
    publisher.upload(manifest["artifact_uri"]); publisher.verify(manifest["artifact_uri"])
    receipt = publisher.finalize(manifest["artifact_uri"])
    record = {"object_type": "source", "object_id": manifest["artifact_id"],
        "logical_artifact_uri": manifest["artifact_uri"], "remote_repository": receipt["remote_repository"],
        "release_tag": receipt["release_tag"], "release_id": receipt["release_id"],
        "asset_id": receipt["asset_id"], "asset_filename": receipt["asset_filename"],
        "package_sha256": receipt["package_sha256"], "artifact_content_hash": manifest["artifact_content_hash"],
        "publication_receipt_id": receipt["receipt_id"], "publication_state": receipt["publication_state"],
        "metadata": {"source_id": source_id, "logical_source_id": "bps",
            "data_sha256": manifest["data_sha256"], "provider_release_id": manifest["provider_release_id"],
            "observation_max": manifest["observation_max"]}}
    catalog, _ = cas.add(record, receipt)
    GitHubReleaseArtifactResolver(catalog, api, workspace / "proof").resolve(manifest["artifact_uri"])
    return {"record": record, "catalog": catalog, "reused": False}


def execute_member(*, source_id: str, mode: str, cycle_id: str, workspace: Path,
                   pin_store: Any, discover: Callable[[], tuple[dict, dict[str, Path]]],
                   retrieve: Callable[[str, Path], Any], build: Callable[..., dict],
                   publish: Callable[[Path, str], Mapping[str, Any]],
                   record: Callable[[Mapping[str, Any], Mapping[str, Any]], Any]) -> dict[str, Any]:
    """Common pin -> build -> publish -> durable-result path for either BPS member."""
    cached: dict[str, Path] = {}
    required = {"compiled_zip"} if source_id == COMPILED_SOURCE_ID else set(LEVELS)

    def discovery() -> dict:
        pin, paths = discover(); cached.update(paths); return pin

    def execution(pin: Mapping[str, Any]) -> dict[str, Any]:
        paths = cached or _retrieve_pin_members(pin, workspace / "pinned-input", retrieve)
        artifact = workspace / "artifact"
        if artifact.exists(): shutil.rmtree(artifact)
        built = build(pin=pin, paths=paths, output=artifact, cycle_id=cycle_id)
        publication = dict(publish(artifact, source_id))
        item = publication["record"]
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
        source_id=source_id, required_members=required, discover_and_retrieve=discovery, execute=execution)


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--source-id", choices=[COMPILED_SOURCE_ID, PROVISIONAL_SOURCE_ID], required=True)
    parser.add_argument("--mode", choices=["normal", "resume", "replay"], required=True)
    parser.add_argument("--cycle-id", required=True); parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True); parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", "")); cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch)
    pins = GitHubPinStore(api, args.branch); results = GitHubCycleResultStore(api, args.branch)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    if args.source_id == COMPILED_SOURCE_ID:
        discover = lambda: discover_compiled_pin(cycle_id=args.cycle_id, workspace=args.workspace / "discovery")
        retrieve, build = acquire_compiled, compiled_candidate
    else:
        discover = lambda: discover_provisional_pin(cycle_id=args.cycle_id, workspace=args.workspace / "discovery")
        retrieve, build = acquire_provisional, provisional_candidate
    def publish(path: Path, source: str) -> Mapping[str, Any]:
        return publish_candidate(artifact=path, source_id=source, api=api, cas=cas,
            workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"))
    def record(result: Mapping[str, Any], catalog: Mapping[str, Any]) -> None:
        results.put(governed_record(result, policy, catalog))
    value = execute_member(source_id=args.source_id, mode=args.mode, cycle_id=args.cycle_id,
        workspace=args.workspace, pin_store=pins, discover=discover, retrieve=retrieve,
        build=build, publish=publish, record=record)
    write_canonical_json(args.output, value["result"]); print(json.dumps(value["result"], sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
