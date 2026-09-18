"""BEA physical acquisition, normalized input pinning, and candidate construction."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import requests

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file
from jobs.monthly_refresh.source_inputs import provider_pin, verify_member_bytes
from sources.bea.artifact import (CONTRACT_VERSION, ENDPOINT, SOURCES, build_snapshot,
    canonicalize_snapshot, governed_config_hashes, snapshot_bytes)

MEMBER = "normalized_snapshot"


def _credential(explicit: str | None = None) -> str:
    value = (explicit or os.environ.get("BEA_API_KEY") or os.environ.get("BEA_API_USER_ID") or "").strip()
    if not value: raise RuntimeError("BEA_API_KEY is required for governed BEA acquisition")
    return value


def _result_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    api = payload.get("BEAAPI", {})
    if not isinstance(api, Mapping): raise ValueError("invalid BEA response envelope")
    if "Error" in api: raise RuntimeError("BEA API returned an error")
    results = api.get("Results", {})
    if isinstance(results, list): results = results[0] if results else {}
    rows = results.get("Data", []) if isinstance(results, Mapping) else []
    if not isinstance(rows, list): raise ValueError("invalid BEA response data")
    return [dict(row) for row in rows]


def acquire_provider(source_id: str, *, key: str | None = None, session: Any = None,
                     root: Path = Path(".")) -> tuple[dict[str, Any], dict[str, Any]]:
    """Acquire once; credentials and mutable envelope evidence never enter the snapshot."""
    from sources.bea.artifact import request_plan
    credential = _credential(key); client = session or requests.Session()
    response = client.post(ENDPOINT, data={"UserID": credential, "ResultFormat": "JSON",
                                           **request_plan(source_id, root)}, timeout=180)
    response.raise_for_status()
    raw = bytes(response.content)
    payload = response.json()
    snapshot = build_snapshot(source_id, _result_rows(payload), root)
    lineage = {"schema_version": "bea_acquisition_lineage_v1", "endpoint": ENDPOINT,
        "transport": "HTTPS POST", "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
        "transport_status_code": int(response.status_code)}
    return snapshot, lineage


def discover_pin(*, cycle_id: str, source_id: str, workspace: Path,
                 acquire: Callable[..., tuple[dict[str, Any], dict[str, Any]]] = acquire_provider,
                 retrieved_at: str | None = None, root: Path = Path("."), **acquire_kwargs: Any
                 ) -> tuple[dict[str, Any], dict[str, Path], dict[str, Any]]:
    snapshot, lineage = acquire(source_id, root=root, **acquire_kwargs)
    payload = snapshot_bytes(snapshot); workspace.mkdir(parents=True, exist_ok=True)
    path = workspace / f"{source_id}.normalized.json"; path.write_bytes(payload)
    digest = sha256_file(path)
    member = {"url": f"pin-embedded://bea/{source_id}/{digest}",
        "retrieved_at": retrieved_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sha256": digest, "size_bytes": len(payload),
        # Embedding the exact compact snapshot in the durable pin makes replay independent
        # of the mutable BEA endpoint.  It is deterministic governed input, not raw lineage.
        "content_base64": base64.b64encode(payload).decode("ascii"),
        "evidence": {"snapshot_contract_version": CONTRACT_VERSION,
            "normalized_governed_content_sha256": snapshot["normalized_governed_content_sha256"],
            "canonical_key_inventory_sha256": snapshot["canonical_key_inventory_sha256"]}}
    release = f"{source_id}:{snapshot['normalized_governed_content_sha256']}"
    pin = provider_pin(cycle_id=cycle_id, source_id=source_id,
                       provider_release_id=release, members={MEMBER: member})
    return pin, {MEMBER: path}, {**lineage, "retrieved_at": member["retrieved_at"]}


def recover_pinned_snapshot(pin: Mapping[str, Any], output: Path) -> None:
    """Recover exact durable bytes from the pin; never contacts BEA."""
    member = pin["members"][MEMBER]
    if not str(member.get("url", "")).startswith("pin-embedded://bea/"):
        raise ValueError("BEA pin does not reference a durable embedded snapshot")
    try: payload = base64.b64decode(member["content_base64"], validate=True)
    except Exception as exc: raise ValueError("BEA pinned snapshot encoding is invalid") from exc
    if len(payload) != member.get("size_bytes") or hashlib.sha256(payload).hexdigest() != member["sha256"]:
        raise ValueError("BEA durable normalized snapshot hash mismatch")
    output.parent.mkdir(parents=True, exist_ok=True); output.write_bytes(payload)


def candidate(*, pin: Mapping[str, Any], paths: Mapping[str, Path], output: Path,
              cycle_id: str, acquisition_lineage: Mapping[str, Any] | None = None,
              git_sha: str = "unknown", repository_root: Path = Path(".")) -> dict[str, Any]:
    verify_member_bytes(pin, paths); source_id = str(pin["source_id"])
    snapshot = json.loads(paths[MEMBER].read_text(encoding="utf-8"))
    frame, diagnostics = canonicalize_snapshot(snapshot, source_id, repository_root)
    expected_release = f"{source_id}:{snapshot['normalized_governed_content_sha256']}"
    if pin["provider_release_id"] != expected_release:
        raise ValueError("BEA semantic release identity differs from pinned snapshot")
    target = str(frame["date"].max())[:7]
    evidence = {"schema_version": "bea_candidate_evidence_v1", "cycle_id": cycle_id,
        "physical_source_id": source_id, "provider_pin_id": pin["pin_id"],
        "coverage": diagnostics, "acquisition_lineage": dict(acquisition_lineage or {})}
    manifest = create_artifact(output, frame, source_id=source_id, source_family=source_id,
        source_type="government_statistics", provider="U.S. Bureau of Economic Analysis",
        distribution_channel="bea_regional_api_normalized_snapshot",
        provider_release_id=expected_release, provider_release_timestamp_or_date=None,
        retrieved_at=pin["members"][MEMBER]["retrieved_at"], target_month=target,
        source_request_identity=snapshot["normalized_governed_content_sha256"],
        source_urls_or_endpoint_identity=[ENDPOINT], raw_source_lineage=evidence,
        config_hashes=governed_config_hashes(repository_root), git_sha=git_sha,
        source_contract_version=CONTRACT_VERSION)
    return {"manifest": manifest, "evidence": evidence}
