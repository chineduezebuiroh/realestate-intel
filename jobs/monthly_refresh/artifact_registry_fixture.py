"""Real, fixture-only GitHub Release registry acceptance entry point."""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

import pandas as pd

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
    GitHubReleaseArtifactPublisher, GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import sha256_file, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import IdentityCollisionError
from core.source_artifacts.validation import validate_artifact


def _fixture(root: Path) -> tuple[Path, dict, dict]:
    frame = pd.DataFrame([{"geo_id": "US", "metric_id": "fixture_metric", "date": "2026-08-31",
        "property_type_id": "all", "value": 1.0, "source_id": "fixture_source", "property_type": "all"}])
    artifact = root / "artifact"
    manifest = create_artifact(artifact, frame, source_id="fixture_source", source_family="Fixture",
        source_type="fixture", provider="Deterministic Phase 2A fixture", distribution_channel="generated",
        provider_release_id="phase-2a-fixed-v1", provider_release_timestamp_or_date="2026-08-31",
        retrieved_at="2026-08-31T00:00:00Z", target_month="2026-08",
        source_request_identity="phase-2a-fixed-fixture", source_urls_or_endpoint_identity=["fixture://phase-2a"],
        git_sha="fixture-identity-v1", artifact_created_at="2026-08-31T00:00:00Z")
    package = root / f"{manifest['artifact_id']}.tar"; package_info = build_publication_package(artifact, package)
    return package, manifest, package_info


def _metadata(manifest: dict, package_info: dict, git_sha: str) -> dict:
    members = {item["path"]: item["sha256"] for item in package_info["members"]}
    return {"logical_artifact_uri": manifest["artifact_uri"], "object_id": manifest["artifact_id"],
        "object_type": "source", "object_metadata": {"source_id": "fixture_source"},
        "artifact_content_hash": manifest["artifact_content_hash"], "member_hashes": members,
        "publisher_git_sha": git_sha, "contract_versions": [manifest["artifact_contract_version"],
        package_info["package_contract_version"]]}


def _record(manifest: dict, receipt: dict) -> dict:
    return {"object_type": "source", "object_id": manifest["artifact_id"],
        "logical_artifact_uri": manifest["artifact_uri"], "remote_repository": receipt["remote_repository"],
        "release_tag": receipt["release_tag"], "release_id": receipt["release_id"],
        "asset_id": receipt["asset_id"], "asset_filename": receipt["asset_filename"],
        "package_sha256": receipt["package_sha256"], "artifact_content_hash": manifest["artifact_content_hash"],
        "publication_receipt_id": receipt["receipt_id"], "publication_state": receipt["publication_state"],
        "metadata": {"source_id": "fixture_source", "data_sha256": manifest["data_sha256"],
        "provider_release_id": manifest["provider_release_id"], "observation_max": manifest["observation_max"]}}


def run(repository: str, token: str, branch: str, catalog_path: str, evidence: Path, git_sha: str) -> dict:
    api = GitHubAPI(repository, token)
    with tempfile.TemporaryDirectory() as td:
        root = Path(td); package, manifest, package_info = _fixture(root)
        metadata = _metadata(manifest, package_info, git_sha); payload = package.read_bytes()
        publisher = GitHubReleaseArtifactPublisher(api, fixture=True)
        publisher.prepare(manifest["artifact_uri"], payload, metadata); publisher.upload(manifest["artifact_uri"])
        publisher.verify(manifest["artifact_uri"]); receipt = publisher.finalize(manifest["artifact_uri"])

        # A finalized Release is intentionally still an orphan until this
        # separate CAS operation succeeds.
        updater = GitHubCatalogCAS(api, catalog_path, branch); before, _ = updater.read()
        orphan_detected = not any(r["logical_artifact_uri"] == manifest["artifact_uri"] for r in before["immutable_records"])
        record = _record(manifest, receipt); catalog, catalog_changed = updater.add(record, receipt)
        resolved = GitHubReleaseArtifactResolver(catalog, api, root / "resolved").resolve(manifest["artifact_uri"])
        validate_artifact(resolved, expected_source_id="fixture_source")

        repeat = GitHubReleaseArtifactPublisher(api, fixture=True)
        repeat.prepare(manifest["artifact_uri"], payload, metadata); repeat.upload(manifest["artifact_uri"])
        repeat.verify(manifest["artifact_uri"]); repeat_receipt = repeat.finalize(manifest["artifact_uri"])
        if (repeat_receipt["release_id"], repeat_receipt["asset_id"], repeat_receipt["package_sha256"]) != (receipt["release_id"], receipt["asset_id"], receipt["package_sha256"]):
            raise RuntimeError("idempotent publication changed immutable remote identity")
        collision_rejected = False
        collision = GitHubReleaseArtifactPublisher(api, fixture=True)
        collision.prepare(manifest["artifact_uri"], payload + b"deliberate-collision", metadata)
        try: collision.upload(manifest["artifact_uri"])
        except IdentityCollisionError: collision_rejected = True
        if not collision_rejected: raise RuntimeError("conflicting remote bytes were not rejected")

        report = {"schema_version": "artifact_registry_fixture_acceptance_v1", "status": "passed",
            "fixture_only": True, "logical_artifact_uri": manifest["artifact_uri"], "release_tag": receipt["release_tag"],
            "release_id": receipt["release_id"], "asset_id": receipt["asset_id"],
            "asset_filename": receipt["asset_filename"], "package_sha256": sha256_file(package),
            "receipt_id": receipt["receipt_id"], "catalog_path": catalog_path, "catalog_changed": catalog_changed,
            "orphan_before_catalog": orphan_detected, "idempotent_repeat": True, "collision_rejected": True}
        evidence.mkdir(parents=True, exist_ok=True)
        write_canonical_json(evidence / "publication_receipt.json", receipt)
        write_canonical_json(evidence / "catalog_snapshot.json", catalog)
        write_canonical_json(evidence / "acceptance_report.json", report)
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True); parser.add_argument("--branch", default="monthly-refresh-orchestration")
    parser.add_argument("--catalog-path", default="artifacts/fixture_registry/catalog.json")
    parser.add_argument("--evidence", type=Path, required=True); args = parser.parse_args()
    result = run(args.repository, os.environ.get("GITHUB_TOKEN", ""), args.branch, args.catalog_path,
        args.evidence, os.environ.get("GITHUB_SHA", "unknown"))
    print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
