"""Immutable serving-market identity and guarded promotion primitives."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from .catalog import activate_object, validate_catalog
from .hashing import sha256_file, sha256_json, write_canonical_json
from .publication import IdentityCollisionError, PublicationError

VERSION = "serving_market_artifact_v1"


def validate_serving_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    required={"schema_version","serving_artifact_id","canonical_market_artifact_id",
        "canonical_database_sha256","database_sha256","validation","database_filename",
        "built_at","builder_git_sha"}
    if set(payload)!=required or payload.get("schema_version")!=VERSION:
        raise PublicationError("serving manifest schema mismatch")
    identity={key:payload[key] for key in ("canonical_market_artifact_id",
        "canonical_database_sha256","database_sha256","validation")}
    expected="serving__"+payload["canonical_market_artifact_id"]+"__"+sha256_json(identity)[:16]
    if payload["serving_artifact_id"]!=expected or payload["validation"].get("status")!="passed":
        raise PublicationError("serving manifest identity or validation mismatch")
    return payload


def create_serving_manifest(output: Path, *, database_path: Path,
                            canonical_market_artifact_id: str,
                            canonical_database_sha256: str,
                            validation: dict[str, Any], built_at: str,
                            builder_git_sha: str) -> dict[str, Any]:
    if validation.get("status") != "passed" or validation.get("row_count", 0) < 1:
        raise PublicationError("serving validation did not pass")
    identity = {"canonical_market_artifact_id": canonical_market_artifact_id,
                "canonical_database_sha256": canonical_database_sha256,
                "database_sha256": sha256_file(database_path),
                "validation": validation}
    payload = {"schema_version": VERSION,
               "serving_artifact_id": "serving__" + canonical_market_artifact_id + "__" + sha256_json(identity)[:16],
               **identity, "database_filename": "market_serving.duckdb",
               "built_at": built_at, "builder_git_sha": builder_git_sha}
    validate_serving_manifest(payload); write_canonical_json(output, payload); return payload


def promote_serving(catalog: dict[str, Any], *, expected_canonical: str,
                    expected_serving: str | None, serving_artifact_id: str) -> tuple[dict[str, Any], bool]:
    """CAS the serving pointer without coupling it to cohort promotion."""
    validate_catalog(catalog)
    if catalog["accepted"].get("canonical_market") != expected_canonical:
        raise IdentityCollisionError("accepted canonical changed before serving promotion")
    current = catalog["accepted"].get("serving_market")
    if current == serving_artifact_id:
        return deepcopy(catalog), False
    if current != expected_serving:
        raise IdentityCollisionError("accepted serving expected-old pointer contradiction")
    records = [r for r in catalog["immutable_records"]
               if r["object_type"] == "serving_market" and r["object_id"] == serving_artifact_id]
    if len(records) != 1 or records[0]["metadata"].get("canonical_market_artifact_id") != expected_canonical:
        raise PublicationError("serving artifact lineage does not match accepted canonical")
    return activate_object(catalog, "serving_market", serving_artifact_id), True
