"""Frozen BEA GDP contracts and deterministic provider-input snapshots."""
from __future__ import annotations

import csv
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from core.source_artifacts.hashing import sha256_file, sha256_json

CONTRACT_VERSION = "bea_gdp_provider_snapshot_v1"
PARSER_VERSION = "bea_b_observation_parser_v1"
ENDPOINT = "https://apps.bea.gov/api/data"
CONFIG = Path("config/bea_governed_geographies_v1.csv")
NUMERIC = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
COMMA_NUMERIC = re.compile(r"^[+-]?\d{1,3}(?:,\d{3})+(?:\.\d*)?$")

SOURCES: dict[str, dict[str, str]] = {
    "bea_gdp_qtr": {"table": "SQGDP9", "frequency": "quarterly",
        "metric_id": "bea_qgdp_real_total_chained2017_saar", "first": "2005Q1", "last": "2026Q1"},
    "bea_gdp_ann": {"table": "CAGDP9", "frequency": "annual",
        "metric_id": "bea_agdp_real_total_chained2017", "first": "2001", "last": "2024"},
}
STABLE_METADATA = {"line_code": "1", "line_description": "All industry total",
                   "unit": "Millions of chained 2017 dollars", "unit_multiplier": "6"}


def governed_geographies(source_id: str, root: Path = Path(".")) -> list[dict[str, str]]:
    if source_id not in SOURCES:
        raise ValueError(f"unsupported BEA physical source: {source_id}")
    with (root / CONFIG).open(newline="", encoding="utf-8") as stream:
        rows = [dict(row) for row in csv.DictReader(stream) if row["source_id"] == source_id]
    expected = 6 if source_id == "bea_gdp_qtr" else 169
    unavailable = [r for r in rows if r["availability"] == "PROVIDER_UNAVAILABLE"]
    if len(rows) != expected or (source_id == "bea_gdp_ann" and len(unavailable) != 40):
        raise ValueError("BEA governed geography configuration violates frozen membership")
    if any(r["availability"] not in {"AVAILABLE_DIRECT", "PROVIDER_UNAVAILABLE"} for r in rows):
        raise ValueError("unknown BEA provider availability classification")
    if any(r["availability"] == "PROVIDER_UNAVAILABLE" and
           (r["geo_class"] != "county" or not r["provider_geo_fips"].startswith("51")) for r in rows):
        raise ValueError("BEA unavailable membership must be Virginia county identities")
    return sorted(rows, key=lambda r: r["provider_geo_fips"])


def request_plan(source_id: str, root: Path = Path(".")) -> dict[str, str]:
    config = SOURCES[source_id]
    return {"method": "GetData", "DataSetName": "Regional", "TableName": config["table"],
            "LineCode": "1", "Year": "ALL",
            "GeoFips": ",".join(r["provider_geo_fips"] for r in governed_geographies(source_id, root))}


def expected_periods(source_id: str) -> list[str]:
    config = SOURCES[source_id]
    if config["frequency"] == "annual":
        return [str(year) for year in range(int(config["first"]), int(config["last"]) + 1)]
    first_y, first_q = int(config["first"][:4]), int(config["first"][-1])
    last_y, last_q = int(config["last"][:4]), int(config["last"][-1])
    return [f"{year}Q{quarter}" for year in range(first_y, last_y + 1) for quarter in range(1, 5)
            if (year, quarter) >= (first_y, first_q) and (year, quarter) <= (last_y, last_q)]


def canonical_date(period: str, frequency: str) -> str:
    if frequency == "annual":
        if not re.fullmatch(r"\d{4}", period): raise ValueError(f"invalid annual period: {period}")
        return f"{period}-12-31"
    match = re.fullmatch(r"(\d{4})Q([1-4])", period)
    if not match: raise ValueError(f"invalid quarterly period: {period}")
    month = int(match.group(2)) * 3
    return f"{match.group(1)}-{month:02d}-{'31' if month in (3, 12) else '30'}"


def canonical_number(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if not (NUMERIC.fullmatch(text) or COMMA_NUMERIC.fullmatch(text)):
        raise ValueError("BEA DataValue contains an unknown blank/nonnumeric sentinel")
    try: number = Decimal(text.replace(",", ""))
    except InvalidOperation as exc: raise ValueError("invalid BEA numeric value") from exc
    if not number.is_finite(): raise ValueError("non-finite BEA numeric value")
    normalized = format(number, "f").rstrip("0").rstrip(".") if "." in format(number, "f") else format(number, "f")
    return "0" if normalized in {"-0", ""} else normalized


def _field(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = str(row.get(name, "")).strip()
        if value: return value
    return ""


def build_snapshot(source_id: str, rows: Iterable[Mapping[str, Any]], root: Path = Path(".")) -> dict[str, Any]:
    """Validate provider truth and return a transport-independent deterministic snapshot."""
    config = SOURCES[source_id]; governed = governed_geographies(source_id, root)
    mapping = {r["provider_geo_fips"]: r for r in governed}
    expected_direct = {code for code, item in mapping.items() if item["availability"] == "AVAILABLE_DIRECT"}
    periods = expected_periods(source_id); observations: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set(); returned: set[str] = set()
    for raw in rows:
        code = _field(raw, "GeoFips")
        if code not in mapping: raise ValueError(f"unexpected BEA provider geography: {code}")
        if code not in expected_direct: raise ValueError(f"provider-unavailable BEA geography became available: {code}")
        period = _field(raw, "TimePeriod"); key = (code, period)
        if key in seen: raise ValueError(f"duplicate BEA provider key: {code}/{period}")
        seen.add(key); returned.add(code)
        if period not in periods: raise ValueError(f"unexpected BEA history period: {period}")
        # Regional GetData observation rows carry unit metadata, but table/line
        # identity is established by the governed request and table metadata.
        # In particular, SQGDP9 and CAGDP9 rows need not repeat LineCode or
        # LineDescription; requiring those fields would fabricate a row contract.
        unit = _field(raw, "CL_UNIT")
        multiplier = _field(raw, "UNIT_MULT")
        if unit != STABLE_METADATA["unit"] or multiplier != STABLE_METADATA["unit_multiplier"]:
            raise ValueError("BEA observation unit metadata changed")
        optional_table = _field(raw, "TableName")
        optional_line = _field(raw, "LineCode")
        optional_description = _field(raw, "LineDescription")
        if ((optional_table and optional_table != config["table"])
                or (optional_line and optional_line != STABLE_METADATA["line_code"])
                or (optional_description and optional_description != STABLE_METADATA["line_description"])):
            raise ValueError("BEA optional observation contract metadata changed")
        geo = mapping[code]
        observations.append({"provider_geo_fips": code, "geo_id": geo["geo_id"], "period": period,
            "date": canonical_date(period, config["frequency"]), "value": canonical_number(raw.get("DataValue"))})
    if returned != expected_direct:
        raise ValueError(f"BEA direct geography membership changed: missing={sorted(expected_direct-returned)}")
    expected_keys = {(code, period) for code in expected_direct for period in periods}
    if seen != expected_keys: raise ValueError("BEA governed history is incomplete")
    observations.sort(key=lambda r: (r["provider_geo_fips"], r["period"]))
    content_hash = sha256_json(observations)
    keys = [[r["geo_id"], config["metric_id"], r["date"], "all"] for r in observations]
    applicability = [{k: r[k] for k in ("geo_id", "provider_geo_fips", "geo_class", "availability")}
                     for r in governed]
    return {"schema_version": CONTRACT_VERSION, "source_id": source_id, "dataset": "Regional",
        "table": config["table"], "line_code": "1", "frequency": config["frequency"],
        "metric_id": config["metric_id"], "sanitized_request_plan": request_plan(source_id, root),
        "stable_provider_metadata": STABLE_METADATA, "parser_contract": {"version": PARSER_VERSION,
            "accepted_values": ["plain_numeric", "comma_formatted_numeric"], "unknown_sentinel": "FAIL_CLOSED"},
        "governed_applicability": applicability, "direct_provider_membership": sorted(expected_direct),
        "provider_unavailable_membership": sorted(set(mapping) - expected_direct),
        "normalized_observations": observations, "normalized_governed_content_sha256": content_hash,
        "canonical_key_inventory_sha256": sha256_json(keys)}


def snapshot_bytes(snapshot: Mapping[str, Any]) -> bytes:
    return (json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def validate_snapshot(snapshot: Mapping[str, Any], source_id: str, root: Path = Path(".")) -> dict[str, Any]:
    value = dict(snapshot)
    rebuilt_rows = [{"GeoFips": r["provider_geo_fips"], "TimePeriod": r["period"],
        "DataValue": r["value"], "CL_UNIT": STABLE_METADATA["unit"],
        "UNIT_MULT": STABLE_METADATA["unit_multiplier"]}
        for r in value.get("normalized_observations", [])]
    rebuilt = build_snapshot(source_id, rebuilt_rows, root)
    if value != rebuilt: raise ValueError("BEA normalized snapshot contract mismatch")
    return value


def canonicalize_snapshot(snapshot: Mapping[str, Any], source_id: str, root: Path = Path(".")) -> tuple[pd.DataFrame, dict[str, Any]]:
    value = validate_snapshot(snapshot, source_id, root); config = SOURCES[source_id]
    frame = pd.DataFrame([{"geo_id": r["geo_id"], "metric_id": config["metric_id"], "date": r["date"],
        "property_type_id": "all", "value": float(Decimal(r["value"])), "source_id": source_id,
        "property_type": "all"} for r in value["normalized_observations"]])
    diagnostics = {"applicability_count": len(value["governed_applicability"]),
        "direct_geography_count": len(value["direct_provider_membership"]),
        "provider_unavailable_count": len(value["provider_unavailable_membership"]),
        "provider_availability": value["governed_applicability"], "row_count": len(frame),
        "normalized_governed_content_sha256": value["normalized_governed_content_sha256"],
        "canonical_key_inventory_sha256": value["canonical_key_inventory_sha256"]}
    return frame, diagnostics


def governed_config_hashes(root: Path = Path(".")) -> dict[str, str]:
    return {str(CONFIG): sha256_file(root / CONFIG)}
