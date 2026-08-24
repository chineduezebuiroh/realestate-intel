from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path
from typing import Any

from .catalog import validate_catalog, validate_record
from .hashing import sha256_file
from .package import extract_publication_package
from .publication import (ArtifactPublisher, IdentityCollisionError, PublicationError,
                          RemoteInspection, create_receipt, transition)
from .storage import ArtifactResolver


class OfflineArtifactPublisher(ArtifactPublisher):
    def __init__(self, *, fail_upload: bool = False, fail_verify: bool = False):
        self.objects: dict[str, dict[str, Any]] = {}; self.fail_upload = fail_upload; self.fail_verify = fail_verify

    def inspect(self, logical_uri: str) -> RemoteInspection:
        obj = self.objects.get(logical_uri)
        return RemoteInspection("absent" if obj is None else obj["state"], None if obj is None else obj["package_sha256"])

    def prepare(self, logical_uri: str, package: bytes, metadata: dict[str, Any]) -> None:
        digest = hashlib.sha256(package).hexdigest(); old = self.objects.get(logical_uri)
        if old:
            if old["package_sha256"] != digest: raise IdentityCollisionError("same logical identity has different package bytes")
            return
        self.objects[logical_uri] = {"state": "prepared", "package": package, "package_sha256": digest, "metadata": deepcopy(metadata)}

    def upload(self, logical_uri: str) -> None:
        obj = self.objects[logical_uri]
        if self.fail_upload: obj["state"] = transition(obj["state"], "failed"); raise PublicationError("simulated upload interruption")
        if obj["state"] == "prepared": obj["state"] = transition(obj["state"], "uploaded")

    def verify(self, logical_uri: str) -> None:
        obj = self.objects[logical_uri]
        if self.fail_verify: obj["state"] = transition(obj["state"], "failed"); raise PublicationError("simulated verification failure")
        if hashlib.sha256(obj["package"]).hexdigest() != obj["package_sha256"]: raise PublicationError("remote package mismatch")
        if obj["state"] == "uploaded": obj["state"] = transition(obj["state"], "remotely_verified")

    def finalize(self, logical_uri: str) -> dict[str, Any]:
        obj = self.objects[logical_uri]
        if obj["state"] == "published_immutable_verified": return obj["receipt"]
        obj["state"] = transition(obj["state"], "published_immutable_verified")
        meta = obj["metadata"]
        receipt = create_receipt(package_sha256=obj["package_sha256"], publication_state=obj["state"], **meta)
        obj["receipt"] = receipt
        return receipt


class CatalogPackageResolver(ArtifactResolver):
    def __init__(self, catalog: dict[str, Any], packages_by_asset_id: dict[int, Path],
                 receipts_by_id: dict[str, dict[str, Any]], workspace: Path):
        self.catalog = validate_catalog(catalog); self.packages = packages_by_asset_id
        self.receipts = receipts_by_id; self.workspace = workspace

    def resolve(self, uri: str) -> Path:
        matches = [r for r in self.catalog["immutable_records"] if r["logical_artifact_uri"] == uri]
        if len(matches) != 1: raise FileNotFoundError(f"uncataloged exact artifact URI: {uri}")
        record = matches[0]
        receipt = self.receipts.get(record["publication_receipt_id"])
        if receipt is None: raise FileNotFoundError("verified publication receipt unavailable")
        validate_record(record, receipt)
        package = self.packages.get(record["asset_id"])
        if package is None: raise FileNotFoundError("cataloged remote asset unavailable")
        if sha256_file(package) != record["package_sha256"]: raise PublicationError("catalog package hash mismatch")
        target = self.workspace / record["package_sha256"]
        if target.exists(): return target
        return extract_publication_package(package, target, expected_sha256=record["package_sha256"])
