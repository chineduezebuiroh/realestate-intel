"""Physical ACS1/ACS5 discovery, exact pinning, and candidate construction."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlencode

import requests

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh.source_inputs import provider_pin, verify_member_bytes
from sources.census_acs.artifact import (CONTRACT_VERSION, PRODUCTS, canonicalize,
    geography_plan, governed_config_hashes, request_params)

API = "https://api.census.gov/data"


def discover_vintages(source_id: str, *, get: Callable[..., Any] = requests.get) -> list[int]:
    """Discover provider-advertised vintages independently for one exact product."""
    product = PRODUCTS[source_id]
    response = get(f"{API}.json", timeout=60); response.raise_for_status()
    values = []
    for dataset in response.json().get("dataset", []):
        vintage = str(dataset.get("c_vintage", ""))
        components = dataset.get("c_dataset", [])
        identifier = str(dataset.get("identifier", ""))
        exact = components == product.split("/") or f"/{vintage}/{product}" in identifier
        if vintage.isdigit() and exact: values.append(int(vintage))
    if not values: raise RuntimeError(f"Census discovery found no vintages for {product}")
    return sorted(set(values), reverse=True)


def _safe_url(vintage: int, product: str, params: Mapping[str, str]) -> str:
    return f"{API}/{vintage}/{product}?{urlencode(sorted(params.items()))}"


def acquire_snapshot(*, source_id: str, vintage: int, output: Path,
                     session: Any = None, key: str | None = None) -> dict[str, Any]:
    """Acquire one product snapshot; the key exists only in transport parameters."""
    credential = (key if key is not None else os.environ.get("CENSUS_API_KEY", "")).strip()
    if not credential: raise RuntimeError("CENSUS_API_KEY is required for governed ACS acquisition")
    client = session or requests.Session(); plan, excluded = geography_plan(); members = []
    for item in plan:
        params = request_params(item); transport = {**params, "key": credential}
        response = client.get(f"{API}/{vintage}/{PRODUCTS[source_id]}", params=transport, timeout=60)
        if response.status_code == 204:
            members.append({**item, "request_url": _safe_url(vintage, PRODUCTS[source_id], params),
                            "status": "provider_ineligible_no_content", "payload": None})
            continue
        response.raise_for_status(); payload = response.json()
        if not isinstance(payload, list) or len(payload) != 2 or len(payload[0]) != len(payload[1]):
            raise ValueError(f"invalid ACS payload for {source_id}/{item['geo_id']}")
        members.append({**item, "request_url": _safe_url(vintage, PRODUCTS[source_id], params),
                        "status": "available", "payload": payload})
    snapshot = {"contract_version": CONTRACT_VERSION, "source_id": source_id,
        "product": PRODUCTS[source_id], "vintage": vintage, "members": members,
        "excluded_geographies": excluded}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n")
    return snapshot


def discover_pin(*, cycle_id: str, source_id: str, workspace: Path,
                 vintages: Callable[[str], list[int]] = discover_vintages,
                 acquire: Callable[..., Mapping[str, Any]] = acquire_snapshot,
                 retrieved_at: str | None = None) -> tuple[dict[str, Any], dict[str, Path]]:
    """Resolve the latest eligible complete snapshot and freeze exact content/membership."""
    errors = []
    for vintage in vintages(source_id):
        path = workspace / f"{source_id}-{vintage}.json"
        try: snapshot = dict(acquire(source_id=source_id, vintage=vintage, output=path))
        except (requests.RequestException, ValueError) as exc:
            errors.append(f"{vintage}:{type(exc).__name__}"); continue
        available = [m for m in snapshot["members"] if m["status"] == "available"]
        if not available: errors.append(f"{vintage}:no_available_members"); continue
        evidence = {"product": PRODUCTS[source_id], "vintage": vintage,
            "request_identity": sha256_json([{k: m[k] for k in ("geo_id", "level", "census_code", "request_url", "status")}
                                              for m in snapshot["members"]]),
            "available_membership": sorted(m["geo_id"] for m in available),
            "provider_ineligible": sorted(m["geo_id"] for m in snapshot["members"] if m["status"] != "available"),
            "excluded_geographies": snapshot["excluded_geographies"]}
        member = {"url": f"{API}/{vintage}/{PRODUCTS[source_id]}",
            "retrieved_at": retrieved_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "sha256": sha256_file(path), "size_bytes": path.stat().st_size, "evidence": evidence}
        return provider_pin(cycle_id=cycle_id, source_id=source_id,
            provider_release_id=f"{PRODUCTS[source_id]}:{vintage}", members={"snapshot": member}), {"snapshot": path}
    raise RuntimeError(f"no eligible ACS vintage for {source_id}; {errors}")


def retrieve_pinned_snapshot(pin: Mapping[str, Any], output: Path, **kwargs: Any) -> None:
    vintage = int(str(pin["provider_release_id"]).rsplit(":", 1)[1])
    acquire_snapshot(source_id=str(pin["source_id"]), vintage=vintage, output=output, **kwargs)


def candidate(*, pin: Mapping[str, Any], paths: Mapping[str, Path], output: Path,
              cycle_id: str, git_sha: str = "unknown", repository_root: Path = Path(".")) -> dict[str, Any]:
    verify_member_bytes(pin, paths); source_id = str(pin["source_id"])
    snapshot = json.loads(paths["snapshot"].read_text())
    vintage = int(str(pin["provider_release_id"]).rsplit(":", 1)[1])
    frame, diagnostics = canonicalize(source_id, vintage, snapshot)
    if diagnostics["available_membership"] != pin["members"]["snapshot"]["evidence"]["available_membership"]:
        raise ValueError("ACS pinned membership differs from candidate snapshot")
    evidence = {"schema_version": "census_acs_candidate_evidence_v1", "cycle_id": cycle_id,
        "physical_source_id": source_id, "provider_pin": dict(pin), "coverage": diagnostics}
    manifest = create_artifact(output, frame, source_id=source_id, source_family=source_id,
        source_type="government_survey", provider="U.S. Census Bureau", distribution_channel="census_data_api",
        provider_release_id=str(pin["provider_release_id"]), provider_release_timestamp_or_date=str(vintage),
        retrieved_at=pin["members"]["snapshot"]["retrieved_at"], target_month=f"{vintage}-12",
        source_request_identity=pin["members"]["snapshot"]["evidence"]["request_identity"],
        source_urls_or_endpoint_identity=[pin["members"]["snapshot"]["url"]], raw_source_lineage=evidence,
        config_hashes=governed_config_hashes(repository_root), git_sha=git_sha)
    return {"manifest": manifest, "evidence": evidence}
