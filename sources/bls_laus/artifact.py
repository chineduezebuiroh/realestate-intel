"""Pure governed LAUS registry, request, acquisition, transform, and reconciliation."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import time
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd
import requests

from core.source_artifacts.hashing import sha256_json

SOURCE_ID = "laus"
ADAPTER_CONTRACT_VERSION = "laus_governed_source_v1"
BLS_API_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
REGISTRY_PATH = Path("config/laus_governed_series_v1.csv")
MAX_SERIES_PER_REQUEST = 50
MAX_YEARS_PER_REQUEST = 20
FULL_HISTORY_START_YEAR = 1976
ACQUISITION_MAX_ATTEMPTS = 3
ACQUISITION_BACKOFF_SECONDS = (2.0, 5.0)
CANONICAL_COLUMNS = ("geo_id", "metric_id", "date", "property_type_id", "value", "source_id", "property_type")
KEY = ["geo_id", "metric_id", "date", "property_type_id"]
REQUIRED_METRICS = {"laus_labor_force_nsa", "laus_employment_nsa", "laus_unemployment_rate_nsa"}
DIAGNOSTIC_METRICS = {"laus_unemployment_nsa"}
GOVERNED_CONFIG_PATHS = (
    "config/laus_governed_series_v1.csv", "config/geo_manifest.generated.csv",
    "config/source_metric_registry.csv", "config/source_refresh_revision_policy_v0_2.json",
)


class TransientLAUSAcquisitionError(RuntimeError):
    """A bounded provider/transport failure safe to retry later."""


def governed_config_hashes(repository_root: Path = Path(".")) -> dict[str, str]:
    result = {}
    for relative in GOVERNED_CONFIG_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise FileNotFoundError(f"missing governed LAUS configuration: {relative}")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(result.items()))


def load_registry(path: Path = REGISTRY_PATH) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as stream:
        raw = list(csv.DictReader(stream))
    rows = []
    for item in raw:
        row = {key: str(value or "").strip() for key, value in item.items()}
        if not all(row.get(key) for key in (
            "series_id", "geo_id", "provider_area_code", "metric_id", "seasonal_adjustment",
            "unit", "scale_transform", "classification", "target_controlling",
        )):
            raise ValueError("governed LAUS registry metadata is incomplete")
        if row["seasonal_adjustment"] != "NSA" or not row["series_id"].startswith("LAU"):
            raise ValueError(f"contradictory governed LAUS seasonality: {row['series_id']}")
        if row["classification"] not in {"GOVERNED_REQUIRED", "GOVERNED_DIAGNOSTIC"}:
            raise ValueError("LEGACY_ONLY/unknown series cannot enter governed LAUS registry")
        controlling = row["target_controlling"].lower() == "true"
        if controlling != (row["classification"] == "GOVERNED_REQUIRED"):
            raise ValueError("LAUS target-controlling classification contradiction")
        row["target_controlling"] = controlling
        rows.append(row)
    rows.sort(key=lambda value: value["series_id"])
    ids = [row["series_id"] for row in rows]
    bindings = [(row["geo_id"], row["metric_id"]) for row in rows]
    if len(ids) != len(set(ids)) or len(bindings) != len(set(bindings)):
        raise ValueError("duplicate governed LAUS series or canonical binding")
    counts = Counter(row["classification"] for row in rows)
    if (len(rows), counts["GOVERNED_REQUIRED"], counts["GOVERNED_DIAGNOSTIC"],
            len(set(row["geo_id"] for row in rows))) != (820, 615, 205, 205):
        raise ValueError("governed LAUS registry scope mismatch")
    if {row["metric_id"] for row in rows if row["target_controlling"]} != REQUIRED_METRICS \
            or {row["metric_id"] for row in rows if not row["target_controlling"]} != DIAGNOSTIC_METRICS:
        raise ValueError("governed LAUS metric classification mismatch")
    return rows


def revision_bounds(mode: str, end_year: int) -> tuple[int, int]:
    end = int(end_year)
    if mode == "ordinary_overlap":
        return end - 2, end
    if mode in {"annual_deep", "deep_reconciliation", "bootstrap"}:
        if end < FULL_HISTORY_START_YEAR:
            raise ValueError("LAUS full-history end_year precedes 1976")
        return FULL_HISTORY_START_YEAR, end
    raise ValueError(f"invalid governed LAUS acquisition mode: {mode}")


def _year_windows(start_year: int, end_year: int) -> list[dict[str, int]]:
    if start_year > end_year:
        raise ValueError("LAUS start_year must not exceed end_year")
    windows = []
    cursor = start_year
    while cursor <= end_year:
        window_end = min(cursor + MAX_YEARS_PER_REQUEST - 1, end_year)
        windows.append({"start_year": cursor, "end_year": window_end})
        cursor = window_end + 1
    return windows


def build_request_plan(*, acquisition_mode: str, end_year: int,
                       registry: Iterable[Mapping[str, Any]] | None = None,
                       config_hashes: Mapping[str, str] | None = None) -> dict[str, Any]:
    series = [dict(row) for row in (registry if registry is not None else load_registry())]
    # Revalidate caller-provided rows through canonical field serialization.
    if registry is not None:
        expected = load_registry()
        if series != expected:
            raise ValueError("caller LAUS registry differs from implementation-owned registry")
    start_year, resolved_end = revision_bounds(acquisition_mode, end_year)
    windows = _year_windows(start_year, resolved_end)
    batches = [series[i:i + MAX_SERIES_PER_REQUEST] for i in range(0, len(series), MAX_SERIES_PER_REQUEST)]
    requests_plan = [{**window, "batch_index": index,
                      "series_ids": [item["series_id"] for item in batch]}
                     for window in windows for index, batch in enumerate(batches)]
    semantic = {
        "adapter_contract_version": ADAPTER_CONTRACT_VERSION,
        "acquisition_mode": acquisition_mode,
        "annualaverage": False,
        "config_hashes": dict(sorted((config_hashes or governed_config_hashes()).items())),
        "endpoint_identity": BLS_API_ENDPOINT,
        "end_year": resolved_end,
        "max_series_per_request": MAX_SERIES_PER_REQUEST,
        "max_years_per_request": MAX_YEARS_PER_REQUEST,
        "requests": requests_plan,
        "series": series,
        "start_year": start_year,
        "year_windows": windows,
    }
    return {**semantic, "source_request_identity": f"bls-laus-v2:{sha256_json(semantic)}"}


def is_transient_acquisition_error(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return status in {408, 429} or (status is not None and 500 <= status <= 599)
    return isinstance(exc, (requests.Timeout, requests.ConnectionError, TimeoutError, ConnectionError))


def _request_with_retry(operation: Callable[[], Any], *, max_attempts: int = 3,
                        backoff_seconds: tuple[float, ...] = ACQUISITION_BACKOFF_SECONDS,
                        sleep: Callable[[float], None] = time.sleep) -> Any:
    if max_attempts < 1 or len(backoff_seconds) < max_attempts - 1:
        raise ValueError("invalid LAUS retry policy")
    for attempt in range(max_attempts):
        try:
            return operation()
        except Exception as exc:
            if not is_transient_acquisition_error(exc):
                raise
            if attempt + 1 == max_attempts:
                raise TransientLAUSAcquisitionError("LAUS acquisition exhausted transient attempts") from exc
            sleep(backoff_seconds[attempt])
    raise AssertionError("unreachable")


def _decode_response(response: Any) -> dict[str, Any]:
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
    value = response.json() if hasattr(response, "json") else response
    if not isinstance(value, dict) or value.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError("BLS response status/schema contradiction")
    results = value.get("Results")
    if not isinstance(results, dict) or not isinstance(results.get("series"), list):
        raise ValueError("BLS response schema mismatch")
    return value


def acquire(plan: Mapping[str, Any], *, api_key: str,
            transport: Callable[..., Any] = requests.post, timeout: float = 60,
            max_attempts: int = 3, backoff_seconds: tuple[float, ...] = ACQUISITION_BACKOFF_SECONDS,
            sleep: Callable[[float], None] = time.sleep) -> list[dict[str, Any]]:
    if not api_key.strip():
        raise ValueError("BLS_API_KEY is required for governed LAUS acquisition")
    acquired = []
    for request_plan in plan["requests"]:
        payload = {"seriesid": list(request_plan["series_ids"]),
                   "startyear": str(request_plan["start_year"]),
                   "endyear": str(request_plan["end_year"]), "annualaverage": False,
                   "registrationkey": api_key.strip()}
        decoded = _request_with_retry(
            lambda: _decode_response(transport(plan["endpoint_identity"], json=payload, timeout=timeout)),
            max_attempts=max_attempts, backoff_seconds=backoff_seconds, sleep=sleep)
        acquired.append({"request": dict(request_plan), "response": decoded})
    return acquired


def _numeric(value: Any) -> tuple[float, str]:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid BLS numeric value") from exc
    if not math.isfinite(number):
        raise ValueError("non-finite BLS numeric value")
    # JSON numeric rendering is deterministic, locale-independent, and round-trippable.
    return number, json.dumps(number, allow_nan=False, separators=(",", ":"))


def _footnote_codes(raw: Mapping[str, Any]) -> tuple[str, ...]:
    footnotes = raw.get("footnotes")
    if footnotes is None:
        return ()
    if not isinstance(footnotes, list):
        raise ValueError("invalid BLS LAUS footnotes")
    codes = []
    for footnote in footnotes:
        if not isinstance(footnote, Mapping):
            raise ValueError("invalid BLS LAUS footnote")
        code = str(footnote.get("code") or "").strip()
        if code:
            codes.append(code)
    return tuple(sorted(set(codes)))


def _classify_observation(raw: Mapping[str, Any]) -> tuple[str, float | None, str | None, tuple[str, ...]]:
    """Classify a datum before parsing; only BLS marker '-' plus stable code X is unavailable."""
    codes = _footnote_codes(raw)
    value = raw.get("value")
    if value == "-" and "X" in codes:
        return "provider_unavailable", None, None, codes
    number, rendered = _numeric(value)
    return "numeric", number, rendered, codes


def canonicalize(plan: Mapping[str, Any], acquired: Sequence[Mapping[str, Any]], *,
                 prior_target_month: str | None = None) -> tuple[pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    metadata = {item["series_id"]: item for item in plan["series"]}
    planned = {(r["start_year"], r["end_year"], r["batch_index"]): r for r in plan["requests"]}
    seen_requests = set(); returned = set(); observations = []; rows = []; m13_count = 0
    unavailable = []; periods_by_series: dict[str, set[tuple[int, int]]] = {}
    per_series_min: dict[str, date] = {}; per_series_max: dict[str, date] = {}
    for acquired_item in acquired:
        request = acquired_item.get("request")
        if not isinstance(request, Mapping):
            raise ValueError("acquired LAUS response lacks request identity")
        key = (request.get("start_year"), request.get("end_year"), request.get("batch_index"))
        if key in seen_requests or key not in planned or dict(request) != dict(planned[key]):
            raise ValueError("LAUS acquired request identity drift/duplication")
        seen_requests.add(key)
        expected = set(planned[key]["series_ids"])
        blocks = _decode_response(acquired_item.get("response"))["Results"]["series"]
        block_ids = [str(block.get("seriesID") or "").strip() if isinstance(block, Mapping) else "" for block in blocks]
        if any(not sid for sid in block_ids) or len(block_ids) != len(set(block_ids)):
            raise ValueError("missing or duplicate BLS LAUS series block identity")
        if set(block_ids) != expected:
            raise ValueError(f"BLS LAUS membership mismatch: missing={sorted(expected-set(block_ids))}, unexpected={sorted(set(block_ids)-expected)}")
        for block in blocks:
            sid = str(block["seriesID"]).strip(); meta = metadata.get(sid)
            if meta is None or not isinstance(block.get("data"), list):
                raise ValueError(f"unknown/malformed governed LAUS series: {sid}")
            returned.add(sid)
            for raw in block["data"]:
                if not isinstance(raw, Mapping):
                    raise ValueError(f"malformed BLS LAUS observation: {sid}")
                period = str(raw.get("period") or "")
                if period == "M13":
                    m13_count += 1; continue
                if len(period) != 3 or not period.startswith("M") or not period[1:].isdigit() or not 1 <= int(period[1:]) <= 12:
                    raise ValueError(f"invalid BLS LAUS period: {sid}/{period}")
                try: year = int(raw["year"])
                except (KeyError, TypeError, ValueError) as exc: raise ValueError("invalid BLS LAUS year") from exc
                if not int(request["start_year"]) <= year <= int(request["end_year"]):
                    raise ValueError(f"BLS LAUS observation outside request: {sid}")
                status, value, rendered, codes = _classify_observation(raw); month = int(period[1:])
                observed = pd.Timestamp(year=year, month=month, day=1).to_period("M").end_time.date()
                periods_by_series.setdefault(sid, set()).add((year, month))
                evidence = {"series_id": sid, "year": str(year), "period": period, "status": status}
                if status == "numeric":
                    per_series_min[sid] = min(observed, per_series_min.get(sid, observed)); per_series_max[sid] = max(observed, per_series_max.get(sid, observed))
                    rows.append({"geo_id": meta["geo_id"], "metric_id": meta["metric_id"], "date": observed,
                                 "property_type_id": "all", "value": value, "source_id": SOURCE_ID, "property_type": "all"})
                    evidence["value"] = rendered
                else:
                    evidence.update({"provider_marker": "-", "footnote_codes": list(codes)})
                    unavailable.append({"series_id": sid, "period": f"{year}-{month:02d}",
                                        "metric_id": meta["metric_id"], "classification": meta["classification"], "codes": codes})
                observations.append(evidence)
    if seen_requests != set(planned):
        raise ValueError("missing acquired LAUS request")
    frame = pd.DataFrame(rows, columns=CANONICAL_COLUMNS)
    if frame.duplicated(KEY).any():
        raise ValueError("duplicate LAUS canonical observation")
    frame = frame.sort_values(KEY, kind="mergesort").reset_index(drop=True)
    observations.sort(key=lambda item: (item["series_id"], item["year"], item["period"], item["status"]))
    required = [item["series_id"] for item in plan["series"] if item["target_controlling"]]
    missing_required = sorted(sid for sid in required if sid not in per_series_max)
    if missing_required:
        raise ValueError(f"missing/empty required LAUS series: {missing_required}")
    common = min(per_series_max[sid] for sid in required)
    present_at_target = {(obs["series_id"], obs["year"], obs["period"]) for obs in observations}
    target_period = f"M{common.month:02d}"
    target_missing = sorted(sid for sid in required if (sid, str(common.year), target_period) not in present_at_target)
    if target_missing:
        raise ValueError(f"required LAUS target-month rows missing: {target_missing}")
    target_month = common.strftime("%Y-%m")
    if prior_target_month and target_month < prior_target_month:
        raise ValueError(f"LAUS target regression: {target_month} < {prior_target_month}")
    diagnostic_lag = []
    for item in plan["series"]:
        if item["target_controlling"]: continue
        maximum = per_series_max.get(item["series_id"])
        lag = None if maximum is None else (common.year-maximum.year)*12 + common.month-maximum.month
        diagnostic_lag.append({"series_id": item["series_id"], "observation_max": str(maximum) if maximum else None, "lag_months": lag})
    identity = provider_release_identity(plan, observations)
    omitted_interior = {}
    for sid, present in periods_by_series.items():
        if len(present) < 2: continue
        first = min(y * 12 + m - 1 for y, m in present); last = max(y * 12 + m - 1 for y, m in present)
        missing = [f"{index // 12:04d}-{index % 12 + 1:02d}" for index in range(first, last + 1)
                   if (index // 12, index % 12 + 1) not in present]
        if missing: omitted_interior[sid] = missing
    diagnostics = {
        "requested_series_count": len(metadata), "returned_series_count": len(returned), "missing_series": sorted(set(metadata)-returned),
        "required_series_count": len(required), "diagnostic_series_count": len(metadata)-len(required),
        "rows_by_metric": dict(sorted(Counter(frame.metric_id).items())), "rows_by_geography": dict(sorted(Counter(frame.geo_id).items())),
        "series_observation_min": {sid: str(per_series_min[sid]) for sid in sorted(per_series_min)},
        "series_observation_max": {sid: str(per_series_max[sid]) for sid in sorted(per_series_max)},
        "observation_min": str(frame.date.min()), "observation_max": str(frame.date.max()),
        "required_common_max": str(common), "target_month": target_month, "diagnostic_lag": diagnostic_lag,
        "m13_discarded_count": m13_count, "invalid_count": 0, "duplicate_count": 0,
        "provider_unavailable_count": len(unavailable),
        "provider_unavailable_series_count": len({item["series_id"] for item in unavailable}),
        "provider_unavailable_by_period": dict(sorted(Counter(item["period"] for item in unavailable).items())),
        "provider_unavailable_by_metric": dict(sorted(Counter(item["metric_id"] for item in unavailable).items())),
        "provider_unavailable_by_classification": dict(sorted(Counter(item["classification"] for item in unavailable).items())),
        "recognized_unavailable_codes": sorted({code for item in unavailable for code in item["codes"]}),
        "omitted_interior_period_count": sum(map(len, omitted_interior.values())),
        "omitted_interior_series_count": len(omitted_interior),
        "omitted_interior_periods_by_series": dict(sorted(omitted_interior.items())),
        "source_request_identity": plan["source_request_identity"], "provider_release_id": identity,
        "unit_by_metric": dict(sorted({item["metric_id"]: item["unit"] for item in plan["series"]}.items())),
        "scale_transform": "none",
    }
    return frame, diagnostics, observations


def provider_release_identity(plan: Mapping[str, Any], observations: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(list(observations), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    plan_bytes = json.dumps({k: v for k, v in plan.items() if k != "source_request_identity"}, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    digest = hashlib.sha256(plan_bytes + b"\n" + payload).hexdigest()
    return f"laus-{plan['acquisition_mode']}-current:{digest}"


def reconcile(prior: pd.DataFrame, provider: pd.DataFrame) -> pd.DataFrame:
    """Provider keys win; prior-only facts persist without implicit deletion."""
    for name, frame in (("prior", prior), ("provider", provider)):
        if frame.duplicated(KEY).any(): raise ValueError(f"duplicate {name} LAUS canonical keys")
    prior_only = prior.merge(provider[KEY], on=KEY, how="left", indicator=True)
    prior_only = prior_only[prior_only["_merge"].eq("left_only")].drop(columns="_merge")
    result = pd.concat([prior_only[list(CANONICAL_COLUMNS)], provider[list(CANONICAL_COLUMNS)]], ignore_index=True)
    return result.sort_values(KEY, kind="mergesort").reset_index(drop=True)
