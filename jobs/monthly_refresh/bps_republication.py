"""Explicit BPS r2 republication from an immutable historical provider pin.

This lifecycle is deliberately separate from monthly source execution results.
It has no discovery, barrier, acceptance, family-resolution, Source Set, Redfin,
or database API.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import time
import urllib.parse
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping

from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import canonical_json_bytes, sha256_json, write_canonical_json
from core.source_artifacts.publication import IdentityCollisionError, PublicationError, TransientPublicationError
from jobs.monthly_refresh.bps_bootstrap import acquire as acquire_compiled
from jobs.monthly_refresh.bps_hosted import CATALOG_PATH, _retrieve_pin_members, publish_candidate
from jobs.monthly_refresh.bps_monthly import (
    COMPILED_SOURCE_ID, PROVISIONAL_SOURCE_ID, compiled_candidate, provisional_candidate,
)
from jobs.monthly_refresh.bps_provisional_verification import LEVELS, acquire as acquire_provisional
from jobs.monthly_refresh.cycle_results import _validate_record_invariants, record_path, semantic_identity
from jobs.monthly_refresh.source_inputs import GitHubPinStore, validate_pin, verify_member_bytes
from sources.census_bps.artifact import ADAPTER_CONTRACT_VERSION, governed_config_hashes

REPUBLICATION_VERSION = "bps_source_republication_v1"
REPUBLICATION_ROOT = "config/monthly_source_republications"
REVISION = 2
REQUEST_VERSION = "bps_source_republication_request_v1"
RECORD_FIELDS = {
    "schema_version", "republication_id", "parent_cycle_id", "source_id",
    "parent_monthly_result_identity", "parent_candidate_artifact_id",
    "parent_candidate_content_hash", "parent_candidate_package_sha256",
    "provider_pin_id", "provider_release_id", "provider_members",
    "source_contract_version", "governed_config_hashes", "revision",
    "candidate_artifact_id", "candidate_content_hash", "candidate_package_sha256",
    "supersedes_artifact_id", "prior_artifact_id", "publication_state",
    "publication_receipt_id", "accepted_pointer_changed", "source_set_created",
    "family_resolution_created",
}
REQUEST_IDENTITY_FIELDS = (
    "parent_cycle_id", "source_id", "parent_monthly_result_identity",
    "parent_candidate_artifact_id", "parent_candidate_content_hash",
    "parent_candidate_package_sha256", "provider_pin_id", "provider_release_id",
    "provider_members", "source_contract_version", "governed_config_hashes", "revision",
)


def republication_path(parent_cycle_id: str, source_id: str, republication_id: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    for label, value in (("parent cycle", parent_cycle_id), ("source", source_id),
                         ("republication", republication_id)):
        if not value or set(value) - allowed:
            raise ValueError(f"invalid {label} identity")
    return f"{REPUBLICATION_ROOT}/{parent_cycle_id}/{source_id}/{republication_id}.json"


def _raw_release(provider_release_id: str, source_id: str) -> str:
    prefix = "bps-compiled:" if source_id == COMPILED_SOURCE_ID else "bps-provisional:"
    if not provider_release_id.startswith(prefix):
        raise ValueError("parent candidate provider release is not a BPS physical release")
    return provider_release_id.removeprefix(prefix)


def _artifact_parts(artifact_id: str, source_id: str, revision: int) -> tuple[str, str]:
    pattern = rf"src__{re.escape(source_id)}__(\d{{4}}-\d{{2}})__r{revision}__([0-9a-f]{{16}})"
    match = re.fullmatch(pattern, artifact_id)
    if match is None:
        raise ValueError(f"candidate is not a canonical {source_id} r{revision} artifact ID")
    return match.group(1), match.group(2)


def _republication_id(value: Mapping[str, Any]) -> str:
    identity = {field: value[field] for field in REQUEST_IDENTITY_FIELDS}
    return f"source_republication__{value['source_id']}__{sha256_json(identity)[:20]}"


def republication_request(*, parent_record: Mapping[str, Any], parent_catalog: Mapping[str, Any],
                          pin: Mapping[str, Any], expected_parent_artifact_id: str,
                          expected_pin_id: str, source_contract_version: str,
                          config_hashes: Mapping[str, str], revision: int = REVISION) -> dict[str, Any]:
    """Validate all parents and return the deterministic immutable request."""
    if not isinstance(parent_record, Mapping):
        raise ValueError("parent durable monthly result is missing")
    if not isinstance(pin, Mapping):
        raise ValueError("parent durable provider pin is missing")
    _validate_record_invariants(parent_record)
    source_id, cycle_id = str(parent_record["source_id"]), str(parent_record["cycle_id"])
    if source_id not in {COMPILED_SOURCE_ID, PROVISIONAL_SOURCE_ID}:
        raise ValueError("republication is restricted to BPS physical sources")
    result = parent_record["result"]
    if result["candidate_artifact_id"] != expected_parent_artifact_id:
        raise ValueError("expected parent candidate does not match durable monthly result")
    if revision != REVISION:
        raise ValueError("BPS republication requires revision 2 with an r1 parent")
    _artifact_parts(expected_parent_artifact_id, source_id, 1)
    matches = [item for item in parent_catalog.get("immutable_records", [])
               if item.get("object_type") == "source" and item.get("object_id") == expected_parent_artifact_id]
    if len(matches) != 1:
        raise ValueError("parent candidate does not resolve exactly once in immutable catalog")
    parent = matches[0]
    checks = {"artifact_content_hash": parent.get("artifact_content_hash"),
              "package_sha256": parent.get("package_sha256"),
              "provider_release_id": parent.get("metadata", {}).get("provider_release_id")}
    if parent.get("metadata", {}).get("source_id") != source_id:
        raise ValueError("parent candidate source identity mismatch")
    if parent.get("publication_state") != "published_immutable_verified":
        raise ValueError("parent candidate publication is not immutable and verified")
    for field, expected in checks.items():
        if result.get(field) != expected:
            raise ValueError(f"parent candidate identity mismatch: {field}")

    required = {"compiled_zip"} if source_id == COMPILED_SOURCE_ID else set(LEVELS)
    validated_pin = validate_pin(pin, cycle_id=cycle_id, source_id=source_id,
                                 required_members=required)
    if validated_pin["pin_id"] != expected_pin_id:
        raise ValueError("expected provider pin does not match durable pin")
    if validated_pin["provider_release_id"] != _raw_release(result["provider_release_id"], source_id):
        raise ValueError("provider pin release does not match parent candidate release")
    if source_contract_version != ADAPTER_CONTRACT_VERSION:
        raise ValueError("target BPS source contract is not the active adapter contract")

    members = {name: {"url": item["url"], "sha256": item["sha256"]}
               for name, item in sorted(validated_pin["members"].items())}
    identity = {"parent_cycle_id": cycle_id, "source_id": source_id,
        "parent_monthly_result_identity": list(semantic_identity(parent_record)),
        "parent_candidate_artifact_id": expected_parent_artifact_id,
        "parent_candidate_content_hash": result["artifact_content_hash"],
        "parent_candidate_package_sha256": result["package_sha256"],
        "provider_pin_id": validated_pin["pin_id"],
        "provider_release_id": validated_pin["provider_release_id"],
        "provider_members": members, "source_contract_version": source_contract_version,
        "governed_config_hashes": dict(sorted(config_hashes.items())), "revision": revision}
    request = {"schema_version": REQUEST_VERSION, **identity}
    return {**request, "republication_id": _republication_id(request)}


def build_record(request: Mapping[str, Any], publication: Mapping[str, Any]) -> dict[str, Any]:
    item = publication["record"]
    if item.get("publication_state") != "published_immutable_verified":
        raise ValueError("republication candidate publication is not immutable and verified")
    if item.get("metadata", {}).get("source_id") != request["source_id"]:
        raise ValueError("republication candidate source identity mismatch")
    expected_release = ("bps-compiled:" if request["source_id"] == COMPILED_SOURCE_ID
                        else "bps-provisional:") + request["provider_release_id"]
    if item.get("metadata", {}).get("provider_release_id") != expected_release:
        raise ValueError("republication candidate provider release mismatch")
    return {"schema_version": REPUBLICATION_VERSION,
        "republication_id": request["republication_id"],
        "parent_cycle_id": request["parent_cycle_id"], "source_id": request["source_id"],
        "parent_monthly_result_identity": request["parent_monthly_result_identity"],
        "parent_candidate_artifact_id": request["parent_candidate_artifact_id"],
        "parent_candidate_content_hash": request["parent_candidate_content_hash"],
        "parent_candidate_package_sha256": request["parent_candidate_package_sha256"],
        "provider_pin_id": request["provider_pin_id"],
        "provider_release_id": request["provider_release_id"],
        "provider_members": request["provider_members"],
        "source_contract_version": request["source_contract_version"],
        "governed_config_hashes": request["governed_config_hashes"], "revision": REVISION,
        "candidate_artifact_id": item["object_id"],
        "candidate_content_hash": item["artifact_content_hash"],
        "candidate_package_sha256": item["package_sha256"],
        "supersedes_artifact_id": request["parent_candidate_artifact_id"],
        "prior_artifact_id": request["parent_candidate_artifact_id"],
        "publication_state": item["publication_state"],
        "publication_receipt_id": item["publication_receipt_id"],
        "accepted_pointer_changed": False, "source_set_created": False,
        "family_resolution_created": False}


def validate_record(record: Mapping[str, Any], request: Mapping[str, Any],
                    catalog: Mapping[str, Any]) -> dict[str, Any]:
    if set(record) != RECORD_FIELDS or record.get("schema_version") != REPUBLICATION_VERSION:
        raise ValueError("republication record schema mismatch")
    if record.get("republication_id") != _republication_id(record):
        raise ValueError("republication record identity mismatch")
    for field in ("republication_id", "parent_cycle_id", "source_id",
                  "parent_monthly_result_identity", "parent_candidate_artifact_id",
                  "parent_candidate_content_hash", "parent_candidate_package_sha256",
                  "provider_pin_id", "provider_release_id", "provider_members",
                  "source_contract_version", "governed_config_hashes", "revision"):
        if record.get(field) != request.get(field):
            raise IdentityCollisionError(f"republication request contradiction: {field}")
    if record.get("revision") != REVISION or record.get("supersedes_artifact_id") != request["parent_candidate_artifact_id"] \
            or record.get("prior_artifact_id") != request["parent_candidate_artifact_id"]:
        raise ValueError("republication predecessor lineage mismatch")
    for field in ("accepted_pointer_changed", "source_set_created", "family_resolution_created"):
        if record.get(field) is not False:
            raise ValueError(f"republication side-effect invariant violated: {field}")
    matches = [item for item in catalog.get("immutable_records", [])
               if item.get("object_type") == "source" and item.get("object_id") == record.get("candidate_artifact_id")]
    if len(matches) != 1:
        raise ValueError("republication candidate does not resolve exactly once")
    item = matches[0]
    expected = {"candidate_content_hash": item.get("artifact_content_hash"),
                "candidate_package_sha256": item.get("package_sha256"),
                "publication_state": item.get("publication_state"),
                "publication_receipt_id": item.get("publication_receipt_id")}
    if item.get("metadata", {}).get("source_id") != request["source_id"]:
        raise ValueError("republication catalog source mismatch")
    expected_release = ("bps-compiled:" if request["source_id"] == COMPILED_SOURCE_ID
                        else "bps-provisional:") + request["provider_release_id"]
    if item.get("metadata", {}).get("provider_release_id") != expected_release:
        raise ValueError("republication catalog provider release mismatch")
    for field, value in expected.items():
        if record.get(field) != value:
            raise ValueError(f"republication candidate identity mismatch: {field}")
    return dict(record)


def add_record(existing: Mapping[str, Any] | None, proposed: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    if set(proposed) != RECORD_FIELDS or proposed.get("schema_version") != REPUBLICATION_VERSION:
        raise ValueError("republication record schema mismatch")
    if proposed.get("republication_id") != _republication_id(proposed):
        raise ValueError("republication record identity mismatch")
    if proposed.get("revision") != REVISION:
        raise ValueError("republication record revision mismatch")
    if proposed.get("prior_artifact_id") != proposed.get("supersedes_artifact_id") \
            or proposed.get("prior_artifact_id") != proposed.get("parent_candidate_artifact_id"):
        raise ValueError("republication record predecessor mismatch")
    for field in ("accepted_pointer_changed", "source_set_created", "family_resolution_created"):
        if proposed.get(field) is not False:
            raise ValueError(f"republication side-effect invariant violated: {field}")
    if existing is None:
        return dict(proposed), True
    if dict(existing) == dict(proposed):
        return dict(existing), False
    raise IdentityCollisionError(f"republication record collision: {proposed.get('republication_id')}")


class GitHubRepublicationStore:
    """Create-once Contents store outside the ordinary monthly-result namespace."""
    def __init__(self, api: GitHubAPI, branch: str, *, attempts: int = 4):
        self.api, self.branch, self.attempts = api, branch, attempts

    def get(self, cycle_id: str, source_id: str, republication_id: str) -> dict[str, Any] | None:
        path = republication_path(cycle_id, source_id, republication_id)
        item, _ = self.api.request("GET", f"/contents/{urllib.parse.quote(path, safe='/')}?ref={urllib.parse.quote(self.branch)}",
                                   expected=(200, 404))
        return None if item is None else json.loads(base64.b64decode(item["content"]))

    def put(self, record: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        path = republication_path(record["parent_cycle_id"], record["source_id"], record["republication_id"])
        for attempt in range(self.attempts):
            existing = self.get(record["parent_cycle_id"], record["source_id"], record["republication_id"])
            value, changed = add_record(existing, record)
            if not changed:
                return value, False
            payload = {"message": f"Record {record['source_id']} candidate republication",
                "content": base64.b64encode(canonical_json_bytes(value)).decode(), "branch": self.branch}
            try:
                self.api.request("PUT", f"/contents/{urllib.parse.quote(path, safe='/')}",
                                 payload=payload, expected=(200, 201))
                return value, True
            except TransientPublicationError:
                if attempt + 1 == self.attempts: raise
            except PublicationError:
                if attempt + 1 == self.attempts:
                    raise PublicationError("republication compare-and-swap retries exhausted")
            time.sleep(0.1 * (attempt + 1))
        raise AssertionError("unreachable")


def execute_republication(*, request: Mapping[str, Any], pin: Mapping[str, Any],
                          catalog: Mapping[str, Any], workspace: Path,
                          retrieve: Callable[[str, Path], Any], build: Callable[..., dict],
                          publish: Callable[[Path, str], Mapping[str, Any]], store: Any,
                          repository_root: Path = Path(".")) -> dict[str, Any]:
    """Retrieve the selected pin and publish r2; provider discovery is impossible here."""
    existing = store.get(request["parent_cycle_id"], request["source_id"], request["republication_id"])
    if existing is not None:
        return {"record": validate_record(existing, request, catalog), "record_changed": False,
                "candidate_reused": True}
    paths = _retrieve_pin_members(pin, workspace / "pinned-input", retrieve)
    verify_member_bytes(pin, paths)  # Must precede any adapter/canonicalization call.
    artifact = workspace / "artifact"
    if artifact.exists(): shutil.rmtree(artifact)
    built = build(pin=pin, paths=paths, output=artifact,
        cycle_id=request["parent_cycle_id"], revision=REVISION,
        prior_artifact_id=request["parent_candidate_artifact_id"],
        prior_artifact_sha256=request["parent_candidate_content_hash"],
        republication_id=request["republication_id"],
        source_contract_version=request["source_contract_version"],
        repository_root=repository_root)
    manifest = built["manifest"]
    parent_target, _ = _artifact_parts(request["parent_candidate_artifact_id"], request["source_id"], 1)
    candidate_target, _ = _artifact_parts(manifest.get("artifact_id", ""), request["source_id"], REVISION)
    expected_release = ("bps-compiled:" if request["source_id"] == COMPILED_SOURCE_ID
                        else "bps-provisional:") + request["provider_release_id"]
    if manifest.get("supersedes_artifact_id") != request["parent_candidate_artifact_id"] \
            or manifest.get("prior_artifact_sha256") != request["parent_candidate_content_hash"] \
            or manifest.get("prior_artifact_id") != request["parent_candidate_artifact_id"] \
            or manifest.get("republication_id") != request["republication_id"] \
            or manifest.get("source_contract_version") != request["source_contract_version"] \
            or manifest.get("config_hashes") != request["governed_config_hashes"] \
            or manifest.get("provider_release_id") != expected_release \
            or candidate_target != parent_target:
        raise ValueError("built candidate does not carry required r2 supersession lineage")
    before_accepted = deepcopy(catalog.get("accepted", {}))
    publication = dict(publish(artifact, request["source_id"]))
    if publication["catalog"].get("accepted", {}) != before_accepted:
        raise ValueError("candidate republication changed an accepted pointer")
    proposed = build_record(request, publication)
    stored, changed = store.put(proposed)
    validate_record(stored, request, publication["catalog"])
    return {"record": stored, "record_changed": changed,
            "candidate_reused": bool(publication.get("reused")), "manifest": manifest}


def _read_contents_json(api: GitHubAPI, branch: str, path: str) -> dict[str, Any] | None:
    item, _ = api.request("GET", f"/contents/{urllib.parse.quote(path, safe='/')}?ref={urllib.parse.quote(branch)}",
                          expected=(200, 404))
    return None if item is None else json.loads(base64.b64decode(item["content"]))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", choices=[COMPILED_SOURCE_ID, PROVISIONAL_SOURCE_ID], required=True)
    parser.add_argument("--parent-cycle-id", required=True)
    parser.add_argument("--expected-parent-artifact-id", required=True)
    parser.add_argument("--expected-pin-id", required=True)
    parser.add_argument("--revision", type=int, required=True)
    parser.add_argument("--target-source-contract", required=True)
    parser.add_argument("--repository", required=True); parser.add_argument("--branch", required=True)
    parser.add_argument("--workspace", type=Path, required=True); parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.revision != REVISION: raise ValueError("only explicit BPS revision 2 republication is supported")
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""))
    catalog, _ = GitHubCatalogCAS(api, CATALOG_PATH, args.branch).read()
    parent = _read_contents_json(api, args.branch, record_path(args.parent_cycle_id, args.source_id))
    if parent is None: raise ValueError("parent durable monthly result is missing")
    pin = GitHubPinStore(api, args.branch).get(args.parent_cycle_id, args.source_id)
    if pin is None: raise ValueError("parent durable provider pin is missing")
    request = republication_request(parent_record=parent, parent_catalog=catalog, pin=pin,
        expected_parent_artifact_id=args.expected_parent_artifact_id, expected_pin_id=args.expected_pin_id,
        source_contract_version=args.target_source_contract,
        config_hashes=governed_config_hashes(Path(".")), revision=args.revision)
    retrieve, build = ((acquire_compiled, compiled_candidate) if args.source_id == COMPILED_SOURCE_ID
                       else (acquire_provisional, provisional_candidate))
    cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch)
    def publish(path: Path, source: str) -> Mapping[str, Any]:
        return publish_candidate(artifact=path, source_id=source, api=api, cas=cas,
            workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"))
    value = execute_republication(request=request, pin=pin, catalog=catalog,
        workspace=args.workspace, retrieve=retrieve, build=build, publish=publish,
        store=GitHubRepublicationStore(api, args.branch))
    write_canonical_json(args.output, value); print(json.dumps(value, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
