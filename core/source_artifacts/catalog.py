from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from .hashing import sha256_json
from .publication import IdentityCollisionError, PublicationError, validate_receipt

CATALOG_VERSION = "artifact_catalog_v1"
OBJECT_TYPES = {"source", "source_set", "canonical_market", "serving_market"}
SHA = re.compile(r"[0-9a-f]{64}")


def empty_catalog(*, expected_git_blob_sha: str | None = None) -> dict[str, Any]:
    return {"schema_version": CATALOG_VERSION, "compare_and_swap": {"expected_git_blob_sha": expected_git_blob_sha},
            "immutable_records": [], "accepted": {"source": {}, "canonical_market": None, "serving_market": None}}


def record_key(record: dict[str, Any]) -> tuple[str, str]:
    return record["object_type"], record["object_id"]


def validate_record(record: dict[str, Any], receipt: dict[str, Any] | None = None) -> None:
    required = {"object_type", "object_id", "logical_artifact_uri", "remote_repository", "release_tag",
                "release_id", "asset_id", "asset_filename", "package_sha256", "artifact_content_hash",
                "publication_receipt_id", "publication_state", "metadata"}
    if set(record) != required or record["object_type"] not in OBJECT_TYPES:
        raise PublicationError("catalog record schema mismatch")
    if not record["logical_artifact_uri"].startswith("artifact://") or "/latest" in record["logical_artifact_uri"]:
        raise PublicationError("catalog requires an exact logical URI")
    if not isinstance(record["release_id"], int) or not isinstance(record["asset_id"], int):
        raise PublicationError("catalog remote IDs must be numeric")
    for key in ("package_sha256", "artifact_content_hash"):
        if not isinstance(record[key], str) or not SHA.fullmatch(record[key]): raise PublicationError("catalog SHA-256 invalid")
    if record["publication_state"] != "published_immutable_verified":
        raise PublicationError("catalog record is not finalized")
    if receipt is not None:
        validate_receipt(receipt, require_eligible=True)
        pairs = (("receipt_id", "publication_receipt_id"), ("logical_artifact_uri", "logical_artifact_uri"),
                 ("object_id", "object_id"), ("object_type", "object_type"), ("package_sha256", "package_sha256"),
                 ("artifact_content_hash", "artifact_content_hash"), ("release_id", "release_id"), ("asset_id", "asset_id"))
        if any(receipt[a] != record[b] for a, b in pairs): raise PublicationError("catalog record/receipt mismatch")
    meta = record["metadata"]
    if record["object_type"] == "source":
        needed = {"source_id", "data_sha256", "provider_release_id", "observation_max"}
        if not needed <= set(meta) or not SHA.fullmatch(meta["data_sha256"]): raise PublicationError("source catalog metadata incomplete")
    if record["object_type"] == "serving_market" and "canonical_market_artifact_id" not in meta:
        raise PublicationError("serving record must reference canonical market")


def validate_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    if set(catalog) != {"schema_version", "compare_and_swap", "immutable_records", "accepted"} or catalog["schema_version"] != CATALOG_VERSION:
        raise PublicationError("catalog schema mismatch")
    cas = catalog["compare_and_swap"]
    if set(cas) != {"expected_git_blob_sha"} or (cas["expected_git_blob_sha"] is not None and not SHA.fullmatch(cas["expected_git_blob_sha"])):
        raise PublicationError("catalog compare-and-swap metadata invalid")
    records = catalog["immutable_records"]
    keys, uris, assets = set(), set(), set()
    for record in records:
        validate_record(record)
        key, uri, asset = record_key(record), record["logical_artifact_uri"], (record["remote_repository"], record["asset_id"])
        if key in keys or uri in uris or asset in assets: raise IdentityCollisionError("duplicate catalog identity or remote asset")
        keys.add(key); uris.add(uri); assets.add(asset)
    if records != sorted(records, key=record_key): raise PublicationError("catalog records are not canonically sorted")
    accepted = catalog["accepted"]
    if set(accepted) != {"source", "canonical_market", "serving_market"} or not isinstance(accepted["source"], dict):
        raise PublicationError("catalog accepted pointer schema mismatch")
    for source_id, object_id in accepted["source"].items():
        if ("source", object_id) not in keys or not any(r["object_id"] == object_id and r["metadata"].get("source_id") == source_id for r in records):
            raise PublicationError("dangling source accepted pointer")
    for kind in ("canonical_market", "serving_market"):
        if accepted[kind] is not None and (kind, accepted[kind]) not in keys: raise PublicationError(f"dangling {kind} accepted pointer")
    return catalog


def add_record(catalog: dict[str, Any], record: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    validate_catalog(catalog); validate_record(record)
    out = deepcopy(catalog)
    for old in out["immutable_records"]:
        if record_key(old) == record_key(record) or old["logical_artifact_uri"] == record["logical_artifact_uri"]:
            if old == record: return out
            raise IdentityCollisionError("conflicting immutable catalog record")
        if (old["remote_repository"], old["asset_id"]) == (record["remote_repository"], record["asset_id"]):
            raise IdentityCollisionError("remote asset already belongs to another object")
    validate_record(record, receipt)
    out["immutable_records"].append(deepcopy(record)); out["immutable_records"].sort(key=record_key)
    return validate_catalog(out)


def catalog_semantic_sha256(catalog: dict[str, Any]) -> str:
    validate_catalog(catalog); return sha256_json(catalog)
