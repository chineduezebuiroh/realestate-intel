from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .hashing import sha256_json

RECEIPT_VERSION = "governed_artifact_publication_receipt_v1"
STATES = ("prepared", "uploaded", "remotely_verified", "published_immutable_verified", "failed")
TRANSITIONS = {
    "prepared": {"uploaded", "failed"},
    "uploaded": {"remotely_verified", "failed"},
    "remotely_verified": {"published_immutable_verified", "failed"},
    "published_immutable_verified": set(),
    "failed": set(),
}
SHA = re.compile(r"[0-9a-f]{64}")


class PublicationError(RuntimeError):
    pass


class TransientPublicationError(PublicationError):
    """A remote publication operation may succeed when safely retried."""


class IdentityCollisionError(PublicationError):
    pass


def transition(current: str, new: str) -> str:
    if current not in TRANSITIONS or new not in TRANSITIONS[current]:
        raise PublicationError(f"illegal publication transition: {current} -> {new}")
    return new


def receipt_identity(receipt: dict[str, Any]) -> str:
    semantic = {k: v for k, v in receipt.items() if k not in {"receipt_id", "published_at", "verified_at"}}
    return "publication_receipt__" + sha256_json(semantic)[:24]


def validate_receipt(receipt: dict[str, Any], *, require_eligible: bool = False) -> dict[str, Any]:
    required = {"receipt_schema_version", "receipt_id", "logical_artifact_uri", "object_id", "object_type",
                "object_metadata", "artifact_content_hash", "package_sha256", "member_hashes", "remote_backend",
                "remote_repository", "release_tag", "release_id", "asset_id", "asset_filename",
                "published_at", "verified_at", "publication_state", "publisher_git_sha", "contract_versions"}
    if set(receipt) != required or receipt["receipt_schema_version"] != RECEIPT_VERSION:
        raise PublicationError("publication receipt schema mismatch")
    if receipt["object_type"] not in {"source", "source_set", "canonical_market", "serving_market"}:
        raise PublicationError("invalid publication object type")
    if not isinstance(receipt["object_metadata"], dict) or (receipt["object_type"] == "source" and not receipt["object_metadata"].get("source_id")):
        raise PublicationError("publication receipt object metadata incomplete")
    if receipt["publication_state"] not in STATES:
        raise PublicationError("invalid publication state")
    if require_eligible and receipt["publication_state"] != "published_immutable_verified":
        raise PublicationError("publication receipt is not catalog-eligible")
    for value in (receipt["artifact_content_hash"], receipt["package_sha256"], *receipt["member_hashes"].values()):
        if not isinstance(value, str) or not SHA.fullmatch(value):
            raise PublicationError("invalid SHA-256 in publication receipt")
    if not receipt["logical_artifact_uri"].startswith("artifact://") or not receipt["release_tag"] or not receipt["asset_filename"]:
        raise PublicationError("invalid remote publication identity")
    if not isinstance(receipt["release_id"], int) or not isinstance(receipt["asset_id"], int):
        raise PublicationError("release and asset IDs must be numeric (offline fixtures use deterministic integers)")
    if receipt["receipt_id"] != receipt_identity(receipt):
        raise PublicationError("publication receipt identity mismatch")
    return receipt


def create_receipt(**values: Any) -> dict[str, Any]:
    receipt = {"receipt_schema_version": RECEIPT_VERSION, "receipt_id": "", **values}
    receipt["receipt_id"] = receipt_identity(receipt)
    return validate_receipt(receipt)


@dataclass(frozen=True)
class RemoteInspection:
    state: str
    package_sha256: str | None = None


class ArtifactPublisher:
    """Backend-neutral explicit publication state machine."""

    def inspect(self, logical_uri: str) -> RemoteInspection: raise NotImplementedError
    def prepare(self, logical_uri: str, package: bytes, metadata: dict[str, Any]) -> None: raise NotImplementedError
    def upload(self, logical_uri: str) -> None: raise NotImplementedError
    def verify(self, logical_uri: str) -> None: raise NotImplementedError
    def finalize(self, logical_uri: str) -> dict[str, Any]: raise NotImplementedError
