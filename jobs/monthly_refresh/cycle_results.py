"""Durable, immutable successful result records for automated monthly sources."""
from __future__ import annotations

import argparse
import base64
import json
import time
import urllib.parse
from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import canonical_json_bytes, write_canonical_json
from core.source_artifacts.publication import (IdentityCollisionError, PublicationError,
                                               TransientPublicationError)
from jobs.monthly_refresh.cohort import RESULT_CONTRACT, RESULT_REGISTRY_VERSION
from jobs.monthly_refresh.production import validate_source_result

RECORD_ROOT = "config/monthly_source_cycle_results"
IDENTITY_FIELDS = ("cycle_id", "source_id", "candidate_artifact_id", "artifact_content_hash",
                   "package_sha256", "provider_release_id", "prior_artifact_id")
SUCCESS_INVARIANTS = {"schema_version": RESULT_CONTRACT, "status": "succeeded",
                      "validation_status": "passed", "publication_state": "published_verified",
                      "accepted_pointer_changed": False}


def _component(value: str, label: str) -> str:
    if not value or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in value):
        raise ValueError(f"invalid {label} for durable cycle-result path")
    return value


def record_path(cycle_id: str, source_id: str) -> str:
    return f"{RECORD_ROOT}/{_component(cycle_id, 'cycle_id')}/{_component(source_id, 'source_id')}.json"


def governed_record(result: Mapping[str, Any], policy: Mapping[str, Any],
                    catalog: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a published result and bind it to its exact durable catalog entry."""
    value = validate_source_result(dict(result), expected_cycle_id=result.get("cycle_id"))
    source_id = value["source_id"]
    automated = {s["source_id"] for s in policy["sources"] if s.get("acquisition_mode") == "automated"}
    if source_id not in automated:
        raise ValueError("only policy-declared automated sources may record cycle results")
    required = {"status": "succeeded", "validation_status": "passed",
                "publication_state": "published_verified", "accepted_pointer_changed": False}
    for field, expected in required.items():
        if value.get(field) != expected:
            raise ValueError(f"cycle result is not recordable: {field}")
    matches = [r for r in catalog.get("immutable_records", [])
               if r.get("object_type") == "source" and r.get("object_id") == value["candidate_artifact_id"]]
    if len(matches) != 1:
        raise ValueError("cycle result candidate does not resolve exactly once in durable catalog")
    item = matches[0]
    checks = {"artifact_content_hash": item.get("artifact_content_hash"),
              "package_sha256": item.get("package_sha256"),
              "provider_release_id": item.get("metadata", {}).get("provider_release_id")}
    if item.get("publication_state") != "published_immutable_verified":
        raise ValueError("cycle result candidate publication is not remotely verified")
    if item.get("metadata", {}).get("source_id") != source_id:
        raise ValueError("cycle result catalog source identity mismatch")
    for field, expected in checks.items():
        if value.get(field) != expected:
            raise ValueError(f"cycle result catalog identity mismatch: {field}")
    return {"schema_version": "monthly_source_cycle_result_v1",
            "cycle_id": value["cycle_id"], "source_id": source_id,
            "result_contract": RESULT_CONTRACT,
            "policy_schema_version": policy["schema_version"], "result": value}


def semantic_identity(record: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return immutable candidate identity, excluding execution diagnostics."""
    return (record.get("cycle_id"), record.get("source_id"), record.get("result_contract"),
            record.get("policy_schema_version"),
            *(record.get("result", {}).get(field) for field in IDENTITY_FIELDS))


def _validate_record_invariants(record: Mapping[str, Any]) -> None:
    """Reject malformed or contradictory success records before identity reuse."""
    if record.get("schema_version") != "monthly_source_cycle_result_v1":
        raise ValueError("incompatible durable cycle-result schema")
    if record.get("result_contract") != RESULT_CONTRACT or not record.get("policy_schema_version"):
        raise ValueError("incompatible durable cycle-result contract")
    result = record.get("result")
    if not isinstance(result, Mapping):
        raise ValueError("durable cycle-result is missing its result")
    if result.get("cycle_id") != record.get("cycle_id") or result.get("source_id") != record.get("source_id"):
        raise ValueError("durable cycle-result key contradiction")
    for field, expected in SUCCESS_INVARIANTS.items():
        if result.get(field) != expected:
            raise ValueError(f"durable cycle-result success invariant contradiction: {field}")
    for field in (value for value in IDENTITY_FIELDS if value != "prior_artifact_id"):
        if result.get(field) in (None, ""):
            raise ValueError(f"durable cycle-result missing identity: {field}")


def add_record(existing: Mapping[str, Any] | None, proposed: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    _validate_record_invariants(proposed)
    if existing is None:
        return dict(proposed), True
    _validate_record_invariants(existing)
    if semantic_identity(existing) == semantic_identity(proposed):
        return dict(existing), False
    raise IdentityCollisionError(
        f"durable cycle-result collision for ({proposed.get('cycle_id')}, {proposed.get('source_id')})")


class GitHubCycleResultStore:
    """One immutable Contents object per semantic key; sibling sources never share a write."""
    def __init__(self, api: GitHubAPI, branch: str, *, attempts: int = 4):
        self.api, self.branch, self.attempts = api, branch, attempts

    def _read(self, path: str) -> tuple[dict[str, Any] | None, str | None]:
        encoded = urllib.parse.quote(path, safe="/")
        item, _ = self.api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(self.branch)}",
                                   expected=(200, 404))
        if item is None:
            return None, None
        return json.loads(base64.b64decode(item["content"])), item["sha"]

    def put(self, record: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        path = record_path(record["cycle_id"], record["source_id"])
        for attempt in range(self.attempts):
            existing, oid = self._read(path)
            value, changed = add_record(existing, record)
            if not changed:
                return value, False
            payload = {"message": f"Record {record['source_id']} result for {record['cycle_id']}",
                       "content": base64.b64encode(canonical_json_bytes(value)).decode(),
                       "branch": self.branch}
            if oid is not None:
                payload["sha"] = oid
            try:
                self.api.request("PUT", f"/contents/{urllib.parse.quote(path, safe='/')}",
                                 payload=payload, expected=(200, 201))
                return value, True
            except TransientPublicationError:
                if attempt + 1 == self.attempts:
                    raise
            except PublicationError:
                # A concurrent create/ref advance is resolved by re-reading. An
                # exact record becomes a no-op; a contradiction fails closed.
                if attempt + 1 == self.attempts:
                    raise PublicationError("cycle-result compare-and-swap retries exhausted")
            time.sleep(0.1 * (attempt + 1))
        raise AssertionError("unreachable")


def load_registry(index_path: Path, records_root: Path | None = None) -> dict[str, Any]:
    index = json.loads(index_path.read_text())
    if index.get("schema_version") != RESULT_REGISTRY_VERSION:
        raise ValueError("unsupported monthly source cycle-result registry")
    records = list(index.get("records", []))
    root = records_root or index_path.with_suffix("")
    if root.exists():
        records.extend(json.loads(path.read_text()) for path in sorted(root.glob("*/*.json")))
    keys = [(r.get("cycle_id"), r.get("source_id")) for r in records]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate durable source result for cycle")
    return {"schema_version": RESULT_REGISTRY_VERSION, "records": records}


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True); parser.add_argument("--token", required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=Path("config/monthly_refresh_policy.json"))
    parser.add_argument("--catalog-path", default="config/artifact_catalog.json")
    parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    api = GitHubAPI(args.repository, args.token)
    catalog, _ = GitHubCatalogCAS(api, args.catalog_path, args.branch).read()
    record = governed_record(json.loads(args.result.read_text()), json.loads(args.policy.read_text()), catalog)
    stored, changed = GitHubCycleResultStore(api, args.branch).put(record)
    receipt = {"schema_version": "monthly_source_cycle_result_recording_receipt_v1",
               "cycle_id": stored["cycle_id"], "source_id": stored["source_id"],
               "record_path": record_path(stored["cycle_id"], stored["source_id"]),
               "record_changed": changed, "semantic_identity": list(semantic_identity(stored))}
    write_canonical_json(args.output, receipt); print(json.dumps(receipt, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
