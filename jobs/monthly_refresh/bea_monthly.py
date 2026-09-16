"""BEA GDP discovery, content-addressed pinning, and candidate construction."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import requests

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import canonical_json_bytes, sha256_json, write_canonical_json
from jobs.monthly_refresh.source_inputs import provider_pin, verify_member_bytes
from sources.bea.artifact import (CONTRACT_VERSION, PARSER_CONTRACT_VERSION, PRODUCTS,
    canonicalize, geography_plan, governed_config_hashes, request_params)

API = "https://apps.bea.gov/api/data"
ROW_FIELDS = ("GeoFips", "TimePeriod", "DataValue", "CL_UNIT", "UNIT_MULT", "LineCode", "LineDescription", "TableName")


def _credential(key: str | None = None) -> str:
    value = (key or os.environ.get("BEA_API_KEY") or os.environ.get("BEA_API_USER_ID") or "").strip()
    if not value: raise RuntimeError("BEA_API_KEY is required for governed BEA acquisition")
    return value


def _results(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    api = payload.get("BEAAPI", {})
    if "Error" in api: raise RuntimeError(f"BEA API error code {api['Error'].get('APIErrorCode', 'unknown')}")
    result = api.get("Results", {})
    return result[0] if isinstance(result, list) and result else result


def acquire_snapshot(*, source_id: str, output: Path, session: Any = None, key: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Acquire once; persist only normalized governed content, never credentials/envelope noise."""
    plan = geography_plan(source_id); params = request_params(source_id, plan)
    transport = {"UserID": _credential(key), "ResultFormat": "JSON", **params}
    client = session or requests.Session()
    response = client.post(API, data=transport, timeout=180); response.raise_for_status()
    raw = response.content
    result = _results(response.json()); provider_rows = result.get("Data", [])
    if not isinstance(provider_rows, list): raise ValueError("BEA Data payload is not a list")
    rows = [{field: str(row.get(field, "")).strip() for field in ROW_FIELDS} for row in provider_rows]
    rows.sort(key=lambda x: (x["GeoFips"], x["TimePeriod"], x["LineCode"]))
    snapshot = {"contract_version": CONTRACT_VERSION, "source_id": source_id,
        "request": params, "applicability": plan, "rows": rows,
        "provider_metadata": {"dataset": "Regional", "table": PRODUCTS[source_id]["table"],
            "line_code": "1", "frequency": PRODUCTS[source_id]["frequency"], "unit_contract": PRODUCTS[source_id]["unit"]},
        "parser_contract_version": PARSER_CONTRACT_VERSION}
    # Validate membership, mapping, periods, sentinels, duplicates before it can become a pin.
    frame, diagnostics = canonicalize(source_id, snapshot)
    output.parent.mkdir(parents=True, exist_ok=True); write_canonical_json(output, snapshot)
    lineage = {"retrieved_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "raw_response_sha256": hashlib.sha256(raw).hexdigest(), "transport": "HTTPS POST",
        "raw_hash_semantic": False, "row_count": len(frame), "coverage": diagnostics}
    return snapshot, lineage


def semantic_identity(source_id: str, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    frame, coverage = canonicalize(source_id, snapshot)
    content = frame.assign(date=frame.date.astype(str)).to_dict(orient="records")
    keys = frame[["geo_id", "metric_id", "date", "property_type_id"]].assign(date=lambda x: x.date.astype(str)).to_dict(orient="records")
    return {"schema_version": "bea_content_addressed_input_v1", "adapter_contract_version": CONTRACT_VERSION,
        "physical_source_id": source_id, "endpoint": API, "sanitized_request_plan": snapshot["request"],
        "provider_metadata": snapshot["provider_metadata"], "parser_contract_version": PARSER_CONTRACT_VERSION,
        "governed_applicability": snapshot["applicability"], "availability": coverage,
        "normalized_governed_content_sha256": sha256_json(content), "canonical_key_inventory_sha256": sha256_json(keys)}


def discover_pin(*, cycle_id: str, source_id: str, workspace: Path, acquire=acquire_snapshot,
                 retrieved_at: str | None = None) -> tuple[dict[str, Any], dict[str, Path]]:
    path = workspace / f"{source_id}.json"
    acquired = acquire(source_id=source_id, output=path)
    snapshot, lineage = acquired if isinstance(acquired, tuple) else (acquired, {})
    identity = semantic_identity(source_id, snapshot); semantic_hash = sha256_json(identity)
    # The normalized snapshot is embedded so resume/replay consume the exact pin without BEA discovery.
    evidence = {"semantic_identity": identity, "semantic_input_sha256": semantic_hash,
        "embedded_snapshot": snapshot, "lineage_nonsemantic": {k: v for k, v in lineage.items() if k not in {"retrieved_at", "raw_response_sha256"}}}
    member = {"url": API, "retrieved_at": retrieved_at or lineage.get("retrieved_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sha256": hashlib.sha256(canonical_json_bytes(snapshot)).hexdigest(), "size_bytes": path.stat().st_size, "evidence": evidence}
    pin = provider_pin(cycle_id=cycle_id, source_id=source_id,
        provider_release_id=f"bea-content:{semantic_hash}", members={"snapshot": member})
    return pin, {"snapshot": path}


def retrieve_pinned_snapshot(pin: Mapping[str, Any], output: Path) -> None:
    """Materialize content carried by an immutable pin; performs no provider call."""
    snapshot = pin["members"]["snapshot"]["evidence"]["embedded_snapshot"]
    output.parent.mkdir(parents=True, exist_ok=True); write_canonical_json(output, snapshot)


def candidate(*, pin: Mapping[str, Any], paths: Mapping[str, Path], output: Path,
              cycle_id: str, git_sha: str = "unknown", repository_root: Path = Path(".")) -> dict[str, Any]:
    verify_member_bytes(pin, paths); source_id = str(pin["source_id"])
    snapshot = json.loads(paths["snapshot"].read_text()); identity = semantic_identity(source_id, snapshot)
    if identity != pin["members"]["snapshot"]["evidence"]["semantic_identity"]: raise ValueError("BEA snapshot contradicts semantic pin identity")
    frame, coverage = canonicalize(source_id, snapshot)
    latest = str(frame.date.max()); target_month = latest[:7]
    evidence = {"schema_version": "bea_candidate_evidence_v1", "cycle_id": cycle_id,
        "physical_source_id": source_id, "semantic_input": identity, "coverage": coverage,
        "credential_participates": False, "accepted_pointer_changed": False}
    manifest = create_artifact(output, frame, source_id=source_id, source_family=source_id,
        source_type="government_statistics", provider="U.S. Bureau of Economic Analysis",
        distribution_channel="bea_regional_api", provider_release_id=str(pin["provider_release_id"]),
        provider_release_timestamp_or_date=None, retrieved_at=pin["members"]["snapshot"]["retrieved_at"],
        target_month=target_month, source_request_identity=identity["normalized_governed_content_sha256"],
        source_urls_or_endpoint_identity=[API], raw_source_lineage=evidence,
        config_hashes=governed_config_hashes(repository_root), git_sha=git_sha,
        source_contract_version=CONTRACT_VERSION, identity_context={"semantic_input_sha256": pin["members"]["snapshot"]["evidence"]["semantic_input_sha256"]})
    return {"manifest": manifest, "evidence": evidence}
