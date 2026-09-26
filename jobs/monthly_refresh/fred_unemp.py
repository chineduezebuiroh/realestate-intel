"""Pinned FRED-unemployment acquisition, reconciliation, and candidate creation."""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd

from core.dates import to_month_end_date
from core.source_artifacts.artifact import canonicalize, create_artifact
from core.source_artifacts.hashing import canonical_json_bytes, sha256_file, sha256_json
from core.source_artifacts.models import CANONICAL_COLUMNS, CANONICAL_KEY
from core.source_artifacts.reconciliation import preserve_prior
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.source_inputs import provider_pin, verify_member_bytes
from sources.fred_unemp.artifact import (CONTRACT_VERSION, METRIC_ID, SOURCE_ID,
                                          governed_config_hashes)

MEMBER = "normalized_snapshot"
ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
EXPECTED_TARGETS = {
    "united_states__nation": "UNRATE",
    "california__state": "CAUR",
    "district_of_columbia__state": "DCUR",
    "maryland__state": "MDUR",
    "new_jersey__state": "NJUR",
    "virginia__state": "VAUR",
}


def resolve_targets(repository_root: Path = Path(".")) -> dict[str, str]:
    path = repository_root / "config/geo_manifest.generated.csv"
    frame = pd.read_csv(path, dtype=str).fillna("")
    required = {"geo_slug", "include_fred_unemp", "fred_unemp_series_id"}
    if not required.issubset(frame.columns):
        raise ValueError("generated geography manifest lacks FRED unemployment contract columns")
    enabled = frame[frame.include_fred_unemp.str.strip().str.lower().isin({"1", "true", "yes", "y"})]
    if enabled.geo_slug.duplicated().any() or enabled.fred_unemp_series_id.duplicated().any():
        raise ValueError("duplicate governed FRED unemployment geography or series")
    targets = dict(zip(enabled.geo_slug, enabled.fred_unemp_series_id))
    if targets != EXPECTED_TARGETS:
        raise ValueError(f"governed FRED unemployment applicability drift: {targets}")
    return dict(sorted(targets.items()))


def _credential(explicit: str | None = None) -> str:
    value = (explicit if explicit is not None else os.environ.get("FRED_API_KEY", "")).strip()
    if not value:
        raise RuntimeError("FRED_API_KEY is required for governed FRED unemployment acquisition")
    return value


def acquire_histories(targets: Mapping[str, str], *, key: str | None = None,
                      client: Any = None) -> dict[str, list[dict[str, Any]]]:
    """Fetch every exact governed history once; partial provider success is fatal."""
    credential = _credential(key)
    if client is None:
        from fredapi import Fred
        client = Fred(api_key=credential)
    histories: dict[str, list[dict[str, Any]]] = {}
    for geo_id, series_id in sorted(targets.items()):
        try:
            values = client.get_series(series_id)
        except Exception as exc:
            raise RuntimeError(f"FRED unemployment acquisition failed for {series_id}") from exc
        if values is None or len(values) == 0:
            raise RuntimeError(f"FRED unemployment returned no history for {series_id}")
        rows = []
        for raw_date, raw_value in values.items():
            if pd.isna(raw_value):
                continue
            value = float(raw_value)
            if not math.isfinite(value):
                raise ValueError(f"non-finite FRED unemployment value for {series_id}")
            rows.append({"date": str(pd.Timestamp(raw_date).date()), "value": value})
        if not rows:
            raise RuntimeError(f"FRED unemployment returned no finite history for {series_id}")
        histories[series_id] = sorted(rows, key=lambda row: row["date"])
    if set(histories) != set(targets.values()):
        raise RuntimeError("partial FRED unemployment provider success")
    return histories


def normalized_snapshot(targets: Mapping[str, str],
                        histories: Mapping[str, list[Mapping[str, Any]]], *,
                        config_hashes: Mapping[str, str]) -> dict[str, Any]:
    if dict(targets) != EXPECTED_TARGETS or set(histories) != set(EXPECTED_TARGETS.values()):
        raise ValueError("FRED unemployment snapshot target/series inventory mismatch")
    members = []
    for geo_id, series_id in sorted(targets.items()):
        rows = [dict(row) for row in histories[series_id]]
        dates = [str(row.get("date", "")) for row in rows]
        if not rows or len(dates) != len(set(dates)):
            raise ValueError(f"missing or duplicate FRED unemployment observations: {series_id}")
        normalized = []
        for row in rows:
            date = str(pd.Timestamp(row["date"]).date())
            value = float(row["value"])
            if not math.isfinite(value):
                raise ValueError(f"non-finite FRED unemployment value: {series_id}")
            normalized.append({"date": date, "value": value})
        members.append({"geo_id": geo_id, "series_id": series_id,
                        "observations": sorted(normalized, key=lambda row: row["date"])})
    return {"schema_version": "fred_unemp_normalized_input_v1",
            "source_contract_version": CONTRACT_VERSION, "source_id": SOURCE_ID,
            "metric_id": METRIC_ID, "seasonal_adjustment": "SA",
            "members": members, "config_hashes": dict(sorted(config_hashes.items()))}


def discover_pin(*, cycle_id: str, workspace: Path, repository_root: Path = Path("."),
                 acquire: Callable[..., dict[str, list[dict[str, Any]]]] = acquire_histories,
                 retrieved_at: str | None = None, **acquire_kwargs: Any
                 ) -> tuple[dict[str, Any], dict[str, Path]]:
    targets = resolve_targets(repository_root)
    configs = governed_config_hashes(repository_root)
    snapshot = normalized_snapshot(targets, acquire(targets, **acquire_kwargs), config_hashes=configs)
    payload = canonical_json_bytes(snapshot); workspace.mkdir(parents=True, exist_ok=True)
    path = workspace / "fred_unemp.normalized.json"; path.write_bytes(payload)
    digest = sha256_file(path)
    request_identity = sha256_json({"targets": targets,
        "geo_manifest_sha256": configs["config/geo_manifest.generated.csv"]})
    member = {"url": f"pin-embedded://fred_unemp/{digest}",
        "retrieved_at": retrieved_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sha256": digest, "size_bytes": len(payload),
        "content_base64": base64.b64encode(payload).decode("ascii"),
        "evidence": {"request_identity": request_identity, "targets": targets,
                     "config_hashes": configs}}
    pin = provider_pin(cycle_id=cycle_id, source_id=SOURCE_ID,
        provider_release_id=f"ordinary-current:{digest}", members={MEMBER: member})
    return pin, {MEMBER: path}


def recover_pinned_snapshot(pin: Mapping[str, Any], output: Path) -> None:
    member = pin["members"][MEMBER]
    if not str(member.get("url", "")).startswith("pin-embedded://fred_unemp/"):
        raise ValueError("FRED unemployment pin lacks immutable embedded input")
    try:
        payload = base64.b64decode(member["content_base64"], validate=True)
    except Exception as exc:
        raise ValueError("FRED unemployment pinned input encoding is invalid") from exc
    if len(payload) != member.get("size_bytes") or hashlib.sha256(payload).hexdigest() != member["sha256"]:
        raise ValueError("FRED unemployment durable input hash mismatch")
    output.parent.mkdir(parents=True, exist_ok=True); output.write_bytes(payload)


def snapshot_frame(snapshot: Mapping[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    expected_header = {"schema_version": "fred_unemp_normalized_input_v1",
        "source_contract_version": CONTRACT_VERSION, "source_id": SOURCE_ID,
        "metric_id": METRIC_ID, "seasonal_adjustment": "SA"}
    if any(snapshot.get(key) != value for key, value in expected_header.items()):
        raise ValueError("FRED unemployment normalized input contract mismatch")
    members = snapshot.get("members", [])
    identities = [(item.get("geo_id"), item.get("series_id")) for item in members]
    if len(identities) != len(set(identities)) or dict(identities) != EXPECTED_TARGETS:
        raise ValueError("missing, duplicate, or unexpected FRED unemployment series")
    frames, coverage = [], []
    for member in members:
        observations = pd.DataFrame(member.get("observations", []))
        if observations.empty or set(observations.columns) != {"date", "value"}:
            raise ValueError(f"missing FRED unemployment history: {member['series_id']}")
        raw_dates = pd.to_datetime(observations.date, errors="raise")
        if raw_dates.duplicated().any():
            raise ValueError(f"duplicate FRED unemployment observation: {member['series_id']}")
        values = pd.to_numeric(observations.value, errors="raise").astype(float)
        if not values.map(math.isfinite).all():
            raise ValueError(f"non-finite FRED unemployment observation: {member['series_id']}")
        dates = to_month_end_date(raw_dates)
        frame = pd.DataFrame({"geo_id": member["geo_id"], "metric_id": METRIC_ID,
            "date": dates, "property_type_id": "all", "value": values,
            "source_id": SOURCE_ID, "property_type": "all"})
        frames.append(frame); coverage.append({"geo_id": member["geo_id"],
            "series_id": member["series_id"], "row_count": len(frame),
            "observation_min": str(pd.Timestamp(frame.date.min()).date()),
            "observation_max": str(pd.Timestamp(frame.date.max()).date())})
    result = canonicalize(pd.concat(frames, ignore_index=True))
    if result[CANONICAL_KEY].duplicated().any():
        raise ValueError("duplicate canonical FRED unemployment key")
    return result, {"expected_series_count": 6, "present_series_count": 6,
                    "series": coverage}


def candidate(*, pin: Mapping[str, Any], paths: Mapping[str, Path], output: Path,
              cycle_id: str, prior_artifact: Path | None = None,
              git_sha: str = "unknown", repository_root: Path = Path("."),
              artifact_created_at: str | None = None) -> dict[str, Any]:
    verify_member_bytes(pin, paths)
    snapshot = json.loads(paths[MEMBER].read_text(encoding="utf-8"))
    configs = governed_config_hashes(repository_root)
    if snapshot.get("config_hashes") != configs \
            or pin["members"][MEMBER].get("evidence", {}).get("config_hashes") != configs:
        raise ValueError("FRED unemployment pinned governed configuration drift")
    expected_request = sha256_json({"targets": EXPECTED_TARGETS,
        "geo_manifest_sha256": configs["config/geo_manifest.generated.csv"]})
    if pin["members"][MEMBER].get("evidence", {}).get("targets") != EXPECTED_TARGETS \
            or pin["members"][MEMBER].get("evidence", {}).get("request_identity") != expected_request:
        raise ValueError("FRED unemployment pinned request identity drift")
    current, coverage = snapshot_frame(snapshot)
    prior = None; prior_manifest = None
    if prior_artifact is not None:
        prior_manifest = validate_artifact(prior_artifact, expected_source_id=SOURCE_ID)["manifest"]
        prior = pd.read_parquet(prior_artifact / prior_manifest["data_filename"])
    reconciled = canonicalize(preserve_prior(prior, current))
    changed = prior is None or not canonicalize(prior).equals(reconciled)
    if output.exists(): shutil.rmtree(output)
    if not changed:
        shutil.copytree(prior_artifact, output)
        return {"manifest": validate_artifact(output, expected_source_id=SOURCE_ID)["manifest"],
                "evidence": {"coverage": coverage, "provider_pin_id": pin["pin_id"]},
                "source_change_detected": False, "prior_artifact_id": prior_manifest["artifact_id"]}
    target = str(pd.Timestamp(current.date.max()))[:7]
    evidence = {"schema_version": "fred_unemp_candidate_evidence_v1", "cycle_id": cycle_id,
        "physical_source_id": SOURCE_ID, "metric_inventory": [METRIC_ID],
        "seasonal_adjustment": "SA", "distinct_from_laus_nsa": True,
        "provider_pin_id": pin["pin_id"], "provider_input_sha256": pin["members"][MEMBER]["sha256"],
        "request_identity": pin["members"][MEMBER]["evidence"]["request_identity"],
        "coverage": coverage, "reconciliation": {"mode": "complete_history_current_truth",
            "prior_only_preserved": 0 if prior is None else len(canonicalize(prior).merge(
                current[CANONICAL_KEY], on=CANONICAL_KEY, how="left", indicator=True).query("_merge == 'left_only'"))}}
    manifest = create_artifact(output, reconciled, source_id=SOURCE_ID,
        source_family="FRED unemployment", source_type="revisionary_current_truth",
        provider="Federal Reserve Bank of St. Louis", distribution_channel="FRED API normalized immutable snapshot",
        provider_release_id=str(pin["provider_release_id"]), provider_release_timestamp_or_date=None,
        retrieved_at=pin["members"][MEMBER]["retrieved_at"], target_month=target,
        source_request_identity=pin["members"][MEMBER]["evidence"]["request_identity"],
        source_urls_or_endpoint_identity=[ENDPOINT], prior_artifact_id=prior_manifest["artifact_id"] if prior_manifest else None,
        prior_artifact_sha256=prior_manifest["artifact_content_hash"] if prior_manifest else None,
        raw_source_lineage=evidence, config_hashes=configs, git_sha=git_sha,
        source_contract_version=CONTRACT_VERSION,
        artifact_created_at=artifact_created_at or pin["members"][MEMBER]["retrieved_at"])
    return {"manifest": manifest, "evidence": evidence, "source_change_detected": True,
            "prior_artifact_id": prior_manifest["artifact_id"] if prior_manifest else None}
