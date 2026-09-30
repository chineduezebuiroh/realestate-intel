"""Pure governed BLS CES planning, acquisition, and canonicalization.

This module deliberately has no DuckDB dependency.  Legacy CES ingestion remains
in :mod:`sources.bls_ces.ingest` until the governed migration is accepted.
"""
from __future__ import annotations

import csv
import hashlib
import math
import time
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd
import requests

from core.source_artifacts.hashing import sha256_json

SOURCE_ID = "ces"
ADAPTER_CONTRACT_VERSION = "ces_governed_source_v1"
BLS_API_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
UNIT = "thousands_of_jobs"
SCALE_TRANSFORM = "none"
MAX_SERIES_PER_REQUEST = 50
MAX_YEARS_PER_REQUEST = 20
DEEP_RECONCILIATION_START_YEAR = 1960
ACQUISITION_MAX_ATTEMPTS = 3
ACQUISITION_BACKOFF_SECONDS = (2.0, 5.0)
CANONICAL_COLUMNS = (
    "geo_id", "metric_id", "date", "property_type_id", "value", "source_id",
    "property_type",
)

GOVERNED_METRICS = (
    "ces_construction_sa",
    "ces_total_nonfarm_sa",
    "ces_total_private_sa",
)
MANDATORY_TARGET_METRIC = "ces_total_nonfarm_sa"
METRIC_BASE_TO_ID = {
    "ces_construction": "ces_construction_sa",
    "ces_total_nonfarm": "ces_total_nonfarm_sa",
    "ces_total_private": "ces_total_private_sa",
}
GOVERNED_CONFIG_PATHS = (
    "config/ces_series.generated.csv",
    "config/geo_manifest.generated.csv",
    "config/source_metric_registry.csv",
    "config/source_refresh_revision_policy_v0_2.json",
)


class TransientCESAcquisitionError(RuntimeError):
    """A bounded provider/transport failure that is safe to retry later."""


def governed_config_hashes(repository_root: Path = Path(".")) -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in GOVERNED_CONFIG_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise FileNotFoundError(f"missing governed CES configuration: {relative}")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(result.items()))


def load_series_spec(path: Path = Path("config/ces_series.generated.csv")) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return [dict(row) for row in csv.DictReader(stream)]


def _governed_series(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for raw in rows:
        base = str(raw.get("metric_base") or "").strip()
        seasonal = str(raw.get("seasonal") or "").strip().upper()
        if base not in METRIC_BASE_TO_ID or seasonal != "S":
            continue
        series_id = str(raw.get("series_id") or "").strip()
        geo_id = str(raw.get("geo_id") or "").strip()
        if not series_id or not geo_id:
            raise ValueError("governed CES series metadata is incomplete")
        if not series_id.startswith("SMS"):
            raise ValueError(f"contradictory CES seasonality for {series_id}")
        selected.append({
            "series_id": series_id,
            "geo_id": geo_id,
            "metric_id": METRIC_BASE_TO_ID[base],
            "seasonal_adjustment": "SA",
            "mandatory_for_target": METRIC_BASE_TO_ID[base] == MANDATORY_TARGET_METRIC,
        })
    selected.sort(key=lambda item: item["series_id"])
    ids = [item["series_id"] for item in selected]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate governed CES series ID in specification")
    identities = [(item["geo_id"], item["metric_id"]) for item in selected]
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate governed CES geography/metric mapping")
    if not selected or not any(item["mandatory_for_target"] for item in selected):
        raise ValueError("governed CES specification has no mandatory target series")
    return selected


def _year_windows(start_year: int, end_year: int) -> list[dict[str, int]]:
    if start_year > end_year:
        raise ValueError("CES start_year must not exceed end_year")
    result = []
    cursor = start_year
    while cursor <= end_year:
        window_end = min(cursor + MAX_YEARS_PER_REQUEST - 1, end_year)
        result.append({"start_year": cursor, "end_year": window_end})
        cursor = window_end + 1
    return result


def build_request_plan(
    rows: Iterable[Mapping[str, Any]], *, start_year: int, end_year: int,
    acquisition_mode: str, config_hashes: Mapping[str, str],
) -> dict[str, Any]:
    """Build an ordering-independent semantic request plan.

    ``ordinary_overlap`` requires at least three inclusive years and
    ``deep_reconciliation`` requires at least five.  Callers choose explicit
    bounds; the planner never consults the wall clock.
    """
    minimum = {"ordinary_overlap": 3, "deep_reconciliation": 5}
    if acquisition_mode not in minimum:
        raise ValueError("invalid governed CES acquisition mode")
    if end_year - start_year + 1 < minimum[acquisition_mode]:
        raise ValueError(f"{acquisition_mode} requires at least {minimum[acquisition_mode]} years")
    if acquisition_mode == "deep_reconciliation" and start_year > DEEP_RECONCILIATION_START_YEAR:
        raise ValueError(
            f"deep_reconciliation must start no later than {DEEP_RECONCILIATION_START_YEAR}"
        )
    series = _governed_series(rows)
    windows = _year_windows(int(start_year), int(end_year))
    batches = [series[index:index + MAX_SERIES_PER_REQUEST]
               for index in range(0, len(series), MAX_SERIES_PER_REQUEST)]
    requests_plan = []
    for window in windows:
        for batch_index, batch in enumerate(batches):
            requests_plan.append({
                **window,
                "batch_index": batch_index,
                "series_ids": [item["series_id"] for item in batch],
            })
    semantic = {
        "adapter_contract_version": ADAPTER_CONTRACT_VERSION,
        "acquisition_mode": acquisition_mode,
        "annualaverage": False,
        "config_hashes": dict(sorted(config_hashes.items())),
        "endpoint_identity": BLS_API_ENDPOINT,
        "end_year": int(end_year),
        "max_series_per_request": MAX_SERIES_PER_REQUEST,
        "max_years_per_request": MAX_YEARS_PER_REQUEST,
        "requests": requests_plan,
        "scale_transform": SCALE_TRANSFORM,
        "series": series,
        "start_year": int(start_year),
        "unit": UNIT,
        "year_windows": windows,
    }
    return {**semantic, "source_request_identity": f"bls-ces-v2:{sha256_json(semantic)}"}


def is_transient_acquisition_error(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return status in {408, 429} or (status is not None and 500 <= status <= 599)
    return isinstance(exc, (requests.Timeout, requests.ConnectionError, TimeoutError, ConnectionError))


def _request_with_retry(
    operation: Callable[[], Any], *, max_attempts: int = ACQUISITION_MAX_ATTEMPTS,
    backoff_seconds: tuple[float, ...] = ACQUISITION_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    if max_attempts < 1 or len(backoff_seconds) < max_attempts - 1:
        raise ValueError("invalid CES acquisition retry policy")
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if not is_transient_acquisition_error(exc):
                raise
            if attempt == max_attempts:
                raise TransientCESAcquisitionError(
                    f"CES acquisition exhausted {max_attempts} transient attempts: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            sleep(backoff_seconds[attempt - 1])
    raise AssertionError("unreachable")


def _decode_response(response: Any) -> dict[str, Any]:
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
    value = response.json() if hasattr(response, "json") else response
    if not isinstance(value, dict):
        raise ValueError("BLS response is not a JSON object")
    if value.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError(f"BLS request failed deterministically: {value.get('message')!r}")
    results = value.get("Results")
    if not isinstance(results, dict) or not isinstance(results.get("series"), list):
        raise ValueError("BLS response schema mismatch")
    return value


def acquire(
    plan: Mapping[str, Any], *, api_key: str,
    transport: Callable[..., Any] = requests.post, timeout: float = 60,
    max_attempts: int = ACQUISITION_MAX_ATTEMPTS,
    backoff_seconds: tuple[float, ...] = ACQUISITION_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Execute every planned request or fail without returning partial truth."""
    if not api_key.strip():
        raise ValueError("BLS_API_KEY is required for governed CES acquisition")
    acquired: list[dict[str, Any]] = []
    for request_plan in plan["requests"]:
        payload = {
            "seriesid": list(request_plan["series_ids"]),
            "startyear": str(request_plan["start_year"]),
            "endyear": str(request_plan["end_year"]),
            "annualaverage": False,
            "registrationkey": api_key.strip(),
        }
        decoded = _request_with_retry(
            lambda: _decode_response(
                transport(plan["endpoint_identity"], json=payload, timeout=timeout)
            ),
            max_attempts=max_attempts, backoff_seconds=backoff_seconds, sleep=sleep,
        )
        # Malformed HTTP-success data raises ValueError and is intentionally not retried.
        acquired.append({"request": dict(request_plan), "response": decoded})
    return acquired


def canonicalize(
    plan: Mapping[str, Any], acquired: Sequence[Mapping[str, Any]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate BLS membership and return sorted source-artifact facts/diagnostics."""
    metadata = {item["series_id"]: item for item in plan["series"]}
    expected_request_keys = {
        (request["start_year"], request["end_year"], request["batch_index"]): set(request["series_ids"])
        for request in plan["requests"]
    }
    acquired_keys = []
    for item in acquired:
        request = item.get("request")
        if not isinstance(request, Mapping):
            raise ValueError("acquired CES response lacks request identity")
        acquired_keys.append((request.get("start_year"), request.get("end_year"),
                              request.get("batch_index")))
    if len(acquired_keys) != len(set(acquired_keys)):
        raise ValueError("duplicate acquired CES request identity")
    missing_requests = sorted(set(expected_request_keys) - set(acquired_keys))
    unexpected_requests = sorted(set(acquired_keys) - set(expected_request_keys))
    if missing_requests or unexpected_requests:
        raise ValueError(
            f"CES acquisition request coverage drift: missing={missing_requests}, "
            f"unexpected={unexpected_requests}"
        )
    returned_series: set[str] = set()
    missing_memberships: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    per_series_max: dict[str, date] = {}
    provider_block_count = 0
    for item in acquired:
        request = item.get("request")
        key = (request.get("start_year"), request.get("end_year"), request.get("batch_index"))
        expected = expected_request_keys.get(key)
        if expected is None or list(request.get("series_ids", [])) != next(
            (r["series_ids"] for r in plan["requests"]
             if (r["start_year"], r["end_year"], r["batch_index"]) == key), None
        ):
            raise ValueError("CES response/request membership identity drift")
        response = _decode_response(item.get("response"))
        blocks = response["Results"]["series"]
        block_ids = [str(block.get("seriesID") or "").strip() if isinstance(block, Mapping) else ""
                     for block in blocks]
        if any(not sid for sid in block_ids):
            raise ValueError("BLS series block missing seriesID")
        if len(block_ids) != len(set(block_ids)):
            raise ValueError("duplicate provider series block")
        unexpected = sorted(set(block_ids) - expected)
        if unexpected:
            raise ValueError(f"unexpected BLS series IDs: {unexpected}")
        missing = sorted(expected - set(block_ids))
        if missing:
            missing_memberships.append({"request": key, "series_ids": missing})
        provider_block_count += len(blocks)
        for block in blocks:
            sid = str(block["seriesID"]).strip()
            meta = metadata.get(sid)
            if meta is None:
                raise ValueError(f"unknown governed CES series: {sid}")
            if not isinstance(block.get("data"), list):
                raise ValueError(f"BLS data block malformed for {sid}")
            returned_series.add(sid)
            for observation in block["data"]:
                if not isinstance(observation, Mapping):
                    raise ValueError(f"BLS observation malformed for {sid}")
                period = str(observation.get("period") or "")
                if period == "M13":
                    continue
                if len(period) != 3 or not period.startswith("M") or not period[1:].isdigit():
                    raise ValueError(f"invalid BLS monthly period for {sid}: {period!r}")
                month = int(period[1:])
                if not 1 <= month <= 12:
                    raise ValueError(f"invalid BLS monthly period for {sid}: {period!r}")
                try:
                    year = int(observation["year"])
                    value = float(observation["value"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(f"invalid BLS observation for {sid}") from exc
                if not math.isfinite(value):
                    raise ValueError(f"non-finite BLS value for {sid}")
                observed = pd.Timestamp(year=year, month=month, day=1).to_period("M").end_time.date()
                if year < int(request["start_year"]) or year > int(request["end_year"]):
                    raise ValueError(f"BLS observation outside requested window for {sid}")
                per_series_max[sid] = max(observed, per_series_max.get(sid, observed))
                rows.append({"geo_id": meta["geo_id"], "metric_id": meta["metric_id"],
                    "date": observed, "property_type_id": "all", "value": value,
                    "source_id": SOURCE_ID, "property_type": "all", "series_id": sid})
    frame = pd.DataFrame(rows)
    if frame.empty:
        canonical = pd.DataFrame(columns=CANONICAL_COLUMNS)
    else:
        key_columns = ["geo_id", "metric_id", "date", "property_type_id"]
        duplicates = int(frame.duplicated(key_columns).sum())
        if duplicates:
            raise ValueError(f"duplicate CES canonical keys: {duplicates}")
        canonical = frame.sort_values(key_columns, kind="mergesort").reset_index(drop=True)
        canonical = canonical[list(CANONICAL_COLUMNS)]
    mandatory = [item["series_id"] for item in plan["series"] if item["mandatory_for_target"]]
    mandatory_maxes = {sid: per_series_max.get(sid) for sid in mandatory}
    common_max = min(mandatory_maxes.values()) if mandatory_maxes and all(mandatory_maxes.values()) else None
    optional_lag = []
    for item in plan["series"]:
        if item["mandatory_for_target"]:
            continue
        maximum = per_series_max.get(item["series_id"])
        lag = None
        if common_max is not None and maximum is not None:
            lag = (common_max.year - maximum.year) * 12 + common_max.month - maximum.month
        optional_lag.append({"series_id": item["series_id"], "observation_max": str(maximum) if maximum else None,
                             "lag_months": lag})
    requested = set(metadata)
    diagnostics = {
        "requested_series_count": len(requested),
        "returned_series_count": len(returned_series),
        "missing_requested_series": sorted({sid for missing in missing_memberships
                                             for sid in missing["series_ids"]}),
        "unexpected_returned_series": [],
        "missing_request_memberships": missing_memberships,
        "rows_by_metric": dict(sorted(Counter(canonical["metric_id"]).items())) if not canonical.empty else {},
        "rows_by_geography": dict(sorted(Counter(canonical["geo_id"]).items())) if not canonical.empty else {},
        "observation_min": str(canonical["date"].min()) if not canonical.empty else None,
        "observation_max": str(canonical["date"].max()) if not canonical.empty else None,
        "per_series_observation_max": {sid: str(per_series_max[sid]) for sid in sorted(per_series_max)},
        "mandatory_series": mandatory,
        "missing_mandatory_series": sorted(sid for sid, maximum in mandatory_maxes.items() if maximum is None),
        "mandatory_series_common_observation_max": str(common_max) if common_max else None,
        "target_month": common_max.strftime("%Y-%m") if common_max else None,
        "optional_series_lag": optional_lag,
        "duplicate_count": 0,
        "invalid_or_missing_observation_count": 0,
        "provider_series_block_count": provider_block_count,
        "configured_absence_rule": "exact_series_registry_not_metric_geography_cartesian_product",
        "unit": UNIT,
        "scale_transform": SCALE_TRANSFORM,
    }
    return canonical, diagnostics
