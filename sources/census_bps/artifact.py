"""Pure governed Census BPS contract, canonicalization, and reconciliation."""
from __future__ import annotations

import csv
import hashlib
import math
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.reconciliation import preserve_prior

SOURCE_ID = "bps"
ADAPTER_CONTRACT_VERSION = "bps_governed_source_v1"
MASTER_DIRECTORY = "https://www2.census.gov/econ/bps/Master%20Data%20Set/"
REGISTRY_PATH = Path("config/bps_governed_geographies_v1.csv")
CANONICAL_COLUMNS = ("geo_id", "metric_id", "date", "property_type_id", "value", "source_id", "property_type")
KEY = ["geo_id", "metric_id", "date", "property_type_id"]
REQUIRED_METRIC = "census_bp_total_units"
# No textual token is admitted until a pinned payload and Census documentation
# establish its meaning.  Actual nulls remain diagnosable unavailable values.
UNAVAILABLE_TOKENS: set[str] = set()
EXPECTED_GEOGRAPHIES = 221
GOVERNED_CONFIG_PATHS = ("config/bps_governed_geographies_v1.csv", "config/geo_manifest.generated.csv", "config/source_metric_registry.csv", "config/source_refresh_revision_policy_v0_2.json")


def governed_config_hashes(repository_root: Path = Path(".")) -> dict[str, str]:
    result = {}
    for relative in GOVERNED_CONFIG_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise FileNotFoundError(f"missing governed BPS configuration: {relative}")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(result.items()))


def load_registry(path: Path = REGISTRY_PATH) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = [{key: str(value or "").strip() for key, value in row.items()} for row in csv.DictReader(stream)]
    required = {"geo_id", "level", "provider_location_type", "provider_identifier", "metric_id", "provider_measure", "unit", "scale_transform", "seasonal_adjustment", "classification"}
    if any(set(row) != required or any(not row[key] for key in required) for row in rows):
        raise ValueError("governed BPS registry metadata is incomplete or structurally drifted")
    rows.sort(key=lambda row: (row["level"], row["provider_identifier"], row["geo_id"]))
    bindings = [(row["provider_location_type"], row["provider_identifier"]) for row in rows]
    if len(rows) != EXPECTED_GEOGRAPHIES or len(bindings) != len(set(bindings)) or len({row["geo_id"] for row in rows}) != EXPECTED_GEOGRAPHIES:
        raise ValueError("governed BPS geography cardinality or identity mismatch")
    if ({row["metric_id"] for row in rows} != {REQUIRED_METRIC} or
        {row["provider_measure"] for row in rows} != {"total_units"} or
        {row["unit"] for row in rows} != {"housing_units"} or
        {row["scale_transform"] for row in rows} != {"none"} or
        {row["seasonal_adjustment"] for row in rows} != {"NSA"} or
        {row["classification"] for row in rows} != {"GOVERNED_REQUIRED"} or
        {row["level"] for row in rows} != {"nation", "state", "county", "cbsa_metro"}):
        raise ValueError("governed BPS metric/geography contract contradiction")
    return rows


def build_request_plan(*, release_month: str, config_hashes: Mapping[str, str] | None = None, registry: Iterable[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    try:
        stamp = pd.Period(release_month, freq="M").strftime("%Y%m")
    except Exception as exc:
        raise ValueError("release_month must be YYYY-MM") from exc
    if stamp != release_month.replace("-", ""):
        raise ValueError("release_month must be canonical YYYY-MM")
    rows = [dict(row) for row in (registry if registry is not None else load_registry())]
    if registry is not None and rows != load_registry():
        raise ValueError("caller BPS registry differs from implementation-owned registry")
    filename = f"BPS_Compiled_File_{stamp}.zip"
    url = MASTER_DIRECTORY + filename
    semantic = {"adapter_contract_version": ADAPTER_CONTRACT_VERSION, "acquisition_mode": "complete_compiled_snapshot", "release_month": release_month, "provider_release_id": f"bps-compiled:{stamp}", "endpoint_identity": url, "config_hashes": dict(sorted((config_hashes or {}).items())), "geographies": rows, "metrics": [{"metric_id": REQUIRED_METRIC, "provider_measure": "total_units", "unit": "housing_units", "scale_transform": "none", "seasonal_adjustment": "NSA"}]}
    return {**semantic, "source_request_identity": f"census-bps-compiled:{sha256_json(semantic)}"}


def _provider_key(row: Mapping[str, Any]) -> tuple[str, str]:
    location = str(row.get("location_type") or "").strip()
    if location == "Country":
        return location, "00"
    field = {"State": "state_fips", "County": "county_fips", "Metro": "cbsa_code"}.get(location)
    if field is None:
        raise ValueError(f"unexpected BPS geography type: {location!r}")
    value = str(row.get(field) or "").strip()
    width = 2 if location == "State" else 5
    if not value.isdigit():
        raise ValueError(f"malformed BPS provider geography identifier: {value!r}")
    return location, value.zfill(width)


def _numeric(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if text.lower() in UNAVAILABLE_TOKENS:
        return None
    try:
        number = float(text.replace(",", ""))
    except ValueError as exc:
        raise ValueError(f"malformed BPS numeric value: {value!r}") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"invalid BPS permit count: {value!r}")
    return number


def canonicalize(plan: Mapping[str, Any], observations: Iterable[Mapping[str, Any]]) -> tuple[pd.DataFrame, dict[str, Any]]:
    if plan.get("adapter_contract_version") != ADAPTER_CONTRACT_VERSION:
        raise ValueError("BPS request-plan contract mismatch")
    lookup = {(row["provider_location_type"], row["provider_identifier"]): row for row in plan["geographies"]}
    output = []
    unavailable = []
    for raw in observations:
        row = dict(raw)
        if str(row.get("period") or "").strip().lower() != "monthly":
            raise ValueError("non-monthly BPS observation entered governed canonicalization")
        binding = lookup.get(_provider_key(row))
        if binding is None:
            raise ValueError("unexpected governed BPS provider geography")
        try:
            year, month = int(row["year"]), int(row["month"])
            normalized_date = date(year, month, 1)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("malformed BPS observation date") from exc
        value = _numeric(row.get("total_units"))
        identity = (binding["geo_id"], REQUIRED_METRIC, normalized_date, "all")
        if value is None:
            unavailable.append(identity)
            continue
        output.append({"geo_id": binding["geo_id"], "metric_id": REQUIRED_METRIC, "date": normalized_date, "property_type_id": "all", "value": value, "source_id": SOURCE_ID, "property_type": "all"})
    frame = pd.DataFrame(output, columns=CANONICAL_COLUMNS)
    if frame.duplicated(KEY).any():
        conflicts = frame.groupby(KEY, dropna=False).value.nunique().reset_index(name="values").query("values > 1")
        if not conflicts.empty:
            raise ValueError("conflicting duplicate governed BPS observation identity")
        frame = frame.sort_values(KEY, kind="mergesort").drop_duplicates(KEY).reset_index(drop=True)
    else:
        frame = frame.sort_values(KEY, kind="mergesort").reset_index(drop=True)
    observation_max = frame.date.max() if not frame.empty else None
    diagnostics = {"provider_release_id": plan["provider_release_id"], "row_count": len(frame), "geography_count": int(frame.geo_id.nunique()) if not frame.empty else 0, "metric_count": int(frame.metric_id.nunique()) if not frame.empty else 0, "unavailable_observation_count": len(unavailable), "unavailable_identities": [list(map(str, item)) for item in sorted(unavailable)], "observation_min": str(frame.date.min()) if not frame.empty else None, "observation_max": str(observation_max) if observation_max else None, "target_month": observation_max.strftime("%Y-%m") if observation_max else None, "unit": "housing_units", "seasonal_adjustment": "NSA"}
    return frame, diagnostics


def reconcile(prior: pd.DataFrame | None, provider: pd.DataFrame) -> pd.DataFrame:
    """Returned compiled values win; prior-only rows persist absent retraction evidence."""
    result = preserve_prior(prior, provider)
    if result.duplicated(KEY).any():
        raise ValueError("BPS reconciliation produced duplicate identities")
    return result[list(CANONICAL_COLUMNS)]
