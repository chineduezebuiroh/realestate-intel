"""Frozen BEA-C contracts and canonical normalization."""
from __future__ import annotations

import csv
import hashlib
import math
import re
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

CONTRACT_VERSION = "bea_gdp_physical_source_v1"
PARSER_CONTRACT_VERSION = "bea-b-observation-parser-v1"
CANONICAL_COLUMNS = ("geo_id", "metric_id", "date", "property_type_id", "value", "source_id", "property_type")
PRODUCTS = {
    "bea_gdp_qtr": {"table": "SQGDP9", "flag": "include_bea_qgdp", "frequency": "quarterly", "metric_id": "bea_qgdp_real_total_chained2017_saar", "unit": "Millions of chained 2017 dollars, SAAR", "provider_unit": "Millions of chained 2017 dollars"},
    "bea_gdp_ann": {"table": "CAGDP9", "flag": "include_bea_agdp", "frequency": "annual", "metric_id": "bea_agdp_real_total_chained2017", "unit": "Millions of chained 2017 dollars", "provider_unit": "Millions of chained 2017 dollars"},
}
NUMERIC = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
COMMA_NUMERIC = re.compile(r"^[+-]?(?:\d{1,3}(?:,\d{3})+)(?:\.\d*)?$")
CONFIG_PATHS = ("config/geo_manifest.generated.csv", "config/bea_gdp_availability_v1.csv", "config/source_metric_registry.csv", "config/source_refresh_revision_policy_v0_2.json", "config/monthly_refresh_policy.json")


def governed_config_hashes(root: Path = Path(".")) -> dict[str, str]:
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in CONFIG_PATHS}


def geography_plan(source_id: str, manifest: Path = Path("config/geo_manifest.generated.csv"), availability: Path = Path("config/bea_gdp_availability_v1.csv")) -> list[dict[str, str]]:
    product = PRODUCTS[source_id]
    frozen = {r["geo_id"]: r["availability"] for r in csv.DictReader(availability.open(encoding="utf-8"))}
    plan = []
    for row in csv.DictReader(manifest.open(encoding="utf-8")):
        if row[product["flag"]].strip() != "1": continue
        item = {"geo_id": row["geo_slug"].strip(), "geo_level": row["level"].strip(), "provider_geo_fips": row["bea_geo_fips"].strip()}
        item["availability"] = "AVAILABLE_DIRECT" if source_id == "bea_gdp_qtr" else frozen[item["geo_id"]]
        plan.append(item)
    expected = 6 if source_id == "bea_gdp_qtr" else 169
    if len(plan) != expected or len({x["provider_geo_fips"] for x in plan}) != expected: raise ValueError("BEA governed geography contract mismatch")
    if source_id == "bea_gdp_ann" and sum(x["availability"] == "PROVIDER_UNAVAILABLE" for x in plan) != 40: raise ValueError("BEA annual availability contract mismatch")
    return sorted(plan, key=lambda x: x["provider_geo_fips"])


def request_params(source_id: str, plan: list[Mapping[str, str]]) -> dict[str, str]:
    return {"method": "GetData", "DataSetName": "Regional", "TableName": PRODUCTS[source_id]["table"], "LineCode": "1", "Year": "ALL", "GeoFips": ",".join(x["provider_geo_fips"] for x in plan)}


def _number(value: Any) -> float:
    text = str(value if value is not None else "").strip()
    if not (NUMERIC.fullmatch(text) or COMMA_NUMERIC.fullmatch(text)): raise ValueError(f"unknown BEA DataValue token at governed observation: {text!r}")
    value = float(text.replace(",", ""))
    if not math.isfinite(value): raise ValueError("non-finite BEA DataValue")
    return value


def _period(value: Any, frequency: str) -> date:
    text = str(value).strip()
    if frequency == "annual" and re.fullmatch(r"\d{4}", text): return date(int(text), 12, 31)
    if frequency == "quarterly" and re.fullmatch(r"\d{4}Q[1-4]", text):
        year, quarter = int(text[:4]), int(text[-1]); return date(year, quarter * 3, 31 if quarter in (1, 4) else 30)
    raise ValueError(f"unexpected BEA period: {text!r}")


def canonicalize(source_id: str, snapshot: Mapping[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    product = PRODUCTS[source_id]
    if snapshot.get("contract_version") != CONTRACT_VERSION or snapshot.get("source_id") != source_id: raise ValueError("BEA physical product contract mismatch")
    plan = snapshot["applicability"]; mapping = {x["provider_geo_fips"]: x for x in plan}
    returned = {str(row.get("GeoFips", "")).strip() for row in snapshot["rows"]}
    expected = {x["provider_geo_fips"] for x in plan if x["availability"] == "AVAILABLE_DIRECT"}
    unavailable = {x["provider_geo_fips"] for x in plan if x["availability"] == "PROVIDER_UNAVAILABLE"}
    if returned != expected or returned & unavailable: raise ValueError("BEA returned geography membership contradicts frozen contract")
    rows = []
    for raw in snapshot["rows"]:
        code = str(raw.get("GeoFips", "")).strip()
        if code not in mapping: raise ValueError(f"unmapped BEA geography: {code}")
        if str(raw.get("LineCode", "")).strip() != "1": raise ValueError("BEA line contract mismatch")
        if str(raw.get("TableName", "")).strip() != product["table"]: raise ValueError("BEA table contract mismatch")
        if str(raw.get("CL_UNIT", "")).strip() != product["provider_unit"] or str(raw.get("UNIT_MULT", "")).strip() != "6": raise ValueError("BEA unit contract mismatch")
        if str(raw.get("LineDescription", "")).strip() != "All industry total": raise ValueError("BEA line-description contract mismatch")
        rows.append({"geo_id": mapping[code]["geo_id"], "metric_id": product["metric_id"], "date": _period(raw.get("TimePeriod"), product["frequency"]), "property_type_id": "all", "value": _number(raw.get("DataValue")), "source_id": source_id, "property_type": "all"})
    frame = pd.DataFrame(rows, columns=CANONICAL_COLUMNS).sort_values(["geo_id", "metric_id", "date", "property_type_id"], kind="mergesort").reset_index(drop=True)
    if frame.empty or frame.duplicated(["geo_id", "metric_id", "date", "property_type_id"]).any(): raise ValueError("empty or duplicate BEA canonical identity")
    diagnostics = {"applicability_count": len(plan), "returned_geography_count": len(returned), "available_direct": sorted(expected), "provider_unavailable": sorted(unavailable), "provider_unavailable_count": len(unavailable), "synthesized_observation_count": 0}
    return frame, diagnostics
