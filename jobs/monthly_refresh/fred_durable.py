"""Phase 2B durable publication and exact prior resolution for FRED macro."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
    GitHubReleaseArtifactPublisher, GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import sha256_file, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import TransientPublicationError
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh import fred_prior_actions

SOURCE_ID = "fred_macro"
CATALOG_PATH = "config/artifact_catalog.json"
BRANCH = "monthly-refresh-orchestration"


def accepted_uri(catalog: dict[str, Any]) -> str | None:
    object_id = catalog["accepted"]["source"].get(SOURCE_ID)
    if object_id is None:
        return None
    matches = [r for r in catalog["immutable_records"] if r["object_type"] == "source"
               and r["object_id"] == object_id and r["metadata"].get("source_id") == SOURCE_ID]
    if len(matches) != 1:
        raise RuntimeError("accepted FRED pointer does not identify exactly one immutable record")
    return matches[0]["logical_artifact_uri"]


def resolve_prior(*, api: GitHubAPI, cas: GitHubCatalogCAS, workspace: Path,
                  repository: str, current_run_id: int, token: str,
                  explicit_path: Path | None = None) -> dict[str, Any]:
    """Prefer durable accepted state; Actions is bootstrap-only, never corruption fallback."""
    catalog, _ = cas.read()
    uri = accepted_uri(catalog)
    if uri is not None:
        path = GitHubReleaseArtifactResolver(catalog, api, workspace / "durable").resolve(uri)
        manifest = validate_artifact(path, expected_source_id=SOURCE_ID)["manifest"]
        return {"resolution": "durable_registry", "path": path, "artifact_id": manifest["artifact_id"]}
    legacy = fred_prior_actions.resolve(repository=repository, current_run_id=current_run_id,
        token=token, download_root=workspace / "actions", explicit_path=explicit_path)
    return legacy


def publication_metadata(artifact: Path, package_info: dict[str, Any], publisher_git_sha: str) -> dict[str, Any]:
    manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    return {"logical_artifact_uri": manifest["artifact_uri"], "object_id": manifest["artifact_id"],
        "object_type": "source", "object_metadata": {"source_id": SOURCE_ID},
        "artifact_content_hash": manifest["artifact_content_hash"],
        "member_hashes": {m["path"]: m["sha256"] for m in package_info["members"]},
        "publisher_git_sha": publisher_git_sha,
        "contract_versions": [manifest["artifact_contract_version"], package_info["package_contract_version"]]}


def catalog_record(manifest: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    return {"object_type": "source", "object_id": manifest["artifact_id"],
        "logical_artifact_uri": manifest["artifact_uri"], "remote_repository": receipt["remote_repository"],
        "release_tag": receipt["release_tag"], "release_id": receipt["release_id"],
        "asset_id": receipt["asset_id"], "asset_filename": receipt["asset_filename"],
        "package_sha256": receipt["package_sha256"], "artifact_content_hash": manifest["artifact_content_hash"],
        "publication_receipt_id": receipt["receipt_id"], "publication_state": receipt["publication_state"],
        "metadata": {"source_id": SOURCE_ID, "data_sha256": manifest["data_sha256"],
            "provider_release_id": manifest["provider_release_id"], "observation_max": manifest["observation_max"]}}


def publish(*, artifact: Path, api: GitHubAPI, cas: GitHubCatalogCAS, workspace: Path,
            publisher_git_sha: str, activate: bool = False) -> dict[str, Any]:
    manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    package = workspace / f"{manifest['artifact_id']}.tar"
    info = build_publication_package(artifact, package)
    metadata = publication_metadata(artifact, info, publisher_git_sha)
    publisher = GitHubReleaseArtifactPublisher(api)
    uri = manifest["artifact_uri"]
    publisher.prepare(uri, package.read_bytes(), metadata)
    publisher.upload(uri); publisher.verify(uri); receipt = publisher.finalize(uri)
    catalog, catalog_changed = cas.add(catalog_record(manifest, receipt), receipt)
    pointer_changed = False
    if activate:
        catalog, pointer_changed = cas.activate_source(SOURCE_ID, manifest["artifact_id"])
    resolved = GitHubReleaseArtifactResolver(catalog, api, workspace / "proof").resolve(uri)
    resolved_manifest = validate_artifact(resolved, expected_source_id=SOURCE_ID)["manifest"]
    if resolved_manifest["data_sha256"] != manifest["data_sha256"]:
        raise RuntimeError("durable resolver did not reproduce FRED data identity")
    return {"package_sha256": sha256_file(package), "release_tag": receipt["release_tag"],
        "release_id": receipt["release_id"], "asset_id": receipt["asset_id"],
        "asset_filename": receipt["asset_filename"], "publication_receipt_id": receipt["receipt_id"],
        "publication_state": receipt["publication_state"], "catalog_changed": catalog_changed,
        "accepted_pointer_changed": pointer_changed, "durable_resolution_passed": True,
        "publication_receipt": receipt}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); sub = parser.add_subparsers(dest="command", required=True)
    prior = sub.add_parser("resolve-prior"); prior.add_argument("--repository", required=True); prior.add_argument("--current-run-id", type=int, required=True)
    prior.add_argument("--workspace", type=Path, required=True); prior.add_argument("--explicit-path", type=Path); prior.add_argument("--output", type=Path, required=True); prior.add_argument("--github-output", type=Path, required=True)
    pub = sub.add_parser("publish"); pub.add_argument("--repository", required=True); pub.add_argument("--artifact", type=Path, required=True)
    pub.add_argument("--workspace", type=Path, required=True); pub.add_argument("--publisher-git-sha", required=True); pub.add_argument("--output", type=Path, required=True); pub.add_argument("--activate", action="store_true")
    args = parser.parse_args(); token = os.environ.get("GITHUB_TOKEN", "")
    api = GitHubAPI(args.repository, token); cas = GitHubCatalogCAS(api, CATALOG_PATH, BRANCH, fixture=False)
    if args.command == "resolve-prior":
        result = resolve_prior(api=api, cas=cas, workspace=args.workspace, repository=args.repository,
            current_run_id=args.current_run_id, token=token, explicit_path=args.explicit_path)
        serial = {**result, "path": str(result["path"]) if result.get("path") else None}; write_canonical_json(args.output, serial)
        with args.github_output.open("a") as stream: stream.write(f"prior_artifact={serial['path'] or ''}\nresolution={serial['resolution']}\n")
    else:
        args.workspace.mkdir(parents=True, exist_ok=True)
        try:
            serial = publish(artifact=args.artifact, api=api, cas=cas, workspace=args.workspace,
                publisher_git_sha=args.publisher_git_sha, activate=args.activate)
        except Exception as exc:
            # Identity/catalog/validation/governance defects are deterministic.
            # Only explicitly typed remote transport/API failures are retryable.
            write_canonical_json(args.output.with_name("publication_failure.json"), {
                "error": f"{type(exc).__name__}: {exc}",
                "retryability": "retryable" if isinstance(exc, TransientPublicationError) else "terminal",
                "schema_version": "fred_publication_failure_v1",
            })
            raise
        write_canonical_json(args.output, serial)
    print(json.dumps({k: v for k, v in serial.items() if k != "path"}, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
