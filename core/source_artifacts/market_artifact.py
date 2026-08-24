from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .hashing import sha256_file, sha256_json, write_canonical_json
from .publication import PublicationError

VERSION = "canonical_market_artifact_v1"
SHA = re.compile(r"[0-9a-f]{64}")


def _identity(manifest: dict[str, Any]) -> dict[str, Any]:
    keys = ("source_set_id", "source_set_semantic_sha256", "canonical_assembly_contract_version",
            "canonical_schema_identity", "config_hashes", "builder_contract_identity",
            "dependency_lock_identity", "assembly_revision", "database_sha256")
    return {k: manifest[k] for k in keys}


def validate_canonical_market_artifact(manifest: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "market_artifact_id", "source_set_id", "source_set_semantic_sha256",
                "source_set_package_sha256", "canonical_assembly_contract_version", "canonical_schema_identity",
                "config_hashes", "builder_contract_identity", "dependency_lock_identity", "assembly_revision",
                "database_filename", "database_sha256", "compressed_package_sha256", "table_inventory", "row_count",
                "source_count", "geography_count", "metric_count", "first_date", "last_date", "duplicate_key_count",
                "validation_status", "assembly_warnings", "builder_git_sha", "built_at"}
    if set(manifest) != required or manifest["schema_version"] != VERSION: raise PublicationError("canonical market artifact schema mismatch")
    for key in ("source_set_semantic_sha256", "source_set_package_sha256", "database_sha256", "compressed_package_sha256"):
        if not SHA.fullmatch(manifest[key]): raise PublicationError(f"invalid canonical market hash: {key}")
    if not manifest["source_set_id"].startswith("source_set__") or manifest["validation_status"] != "passed" or manifest["duplicate_key_count"] != 0:
        raise PublicationError("canonical market validation or source-set reference invalid")
    if not manifest["table_inventory"] or manifest["row_count"] < 0 or manifest["source_count"] < 1:
        raise PublicationError("canonical market inventory invalid")
    expected = "market__" + manifest["source_set_id"].split("__")[1] + "__r" + str(manifest["assembly_revision"]) + "__" + sha256_json(_identity(manifest))[:16]
    if manifest["market_artifact_id"] != expected: raise PublicationError("canonical market semantic identity mismatch")
    return manifest


def create_canonical_market_manifest(output: Path, *, database_path: Path, **values: Any) -> dict[str, Any]:
    manifest = {"schema_version": VERSION, "market_artifact_id": "", "database_filename": "market.duckdb",
                "database_sha256": sha256_file(database_path), **values}
    manifest["market_artifact_id"] = "market__" + manifest["source_set_id"].split("__")[1] + "__r" + str(manifest["assembly_revision"]) + "__" + sha256_json(_identity(manifest))[:16]
    validate_canonical_market_artifact(manifest); write_canonical_json(output, manifest); return manifest
