"""Governed contract for the two physical Census ACS products."""
from __future__ import annotations

import csv
import hashlib
import math
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

PRODUCTS = {"census_acs1": "acs/acs1", "census_acs5": "acs/acs5"}
VARIABLES = {"B01003_001E": "pop_total", "B19013_001E": "median_household_income"}
DIVISIONS = {"11244", "15804", "23224", "31084", "35084", "35154", "36084", "41884", "42034", "47894"}
SENTINELS = {-666666666, -888888888, -999999999}
CONTRACT_VERSION = "census_acs_physical_source_v1"
CANONICAL_COLUMNS = ("geo_id", "metric_id", "date", "property_type_id", "value", "source_id", "property_type")
CONFIG_PATHS = ("config/geo_manifest.generated.csv", "config/source_metric_registry.csv",
                "config/source_refresh_revision_policy_v0_2.json", "config/monthly_refresh_policy.json")


def governed_config_hashes(root: Path = Path(".")) -> dict[str, str]:
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in CONFIG_PATHS}


def geography_plan(path: Path = Path("config/geo_manifest.generated.csv")) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    included, excluded = [], []
    for row in rows:
        if row["include_census"].strip() != "1" or not row["census_code"].strip():
            continue
        item = {"geo_id": row["geo_slug"].strip(), "level": row["level"].strip(),
                "census_code": row["census_code"].strip()}
        if item["census_code"] in DIVISIONS:
            excluded.append({**item, "classification": "CANONICAL_CONCEPT_MISMATCH",
                "disposition": "EXCLUDED_FROM_INITIAL_ACS_GOVERNED_CONTRACT"})
        else:
            included.append(item)
    return sorted(included, key=lambda x: x["geo_id"]), sorted(excluded, key=lambda x: x["geo_id"])


def request_params(item: Mapping[str, str]) -> dict[str, str]:
    level, code = item["level"], item["census_code"]
    base = {"get": "NAME," + ",".join(VARIABLES)}
    if level == "nation": return {**base, "for": "us:1"}
    if level == "state": return {**base, "for": f"state:{code.zfill(2)}"}
    if level == "county":
        code = code.zfill(5); return {**base, "for": f"county:{code[2:]}", "in": f"state:{code[:2]}"}
    if level == "cbsa_metro":
        return {**base, "for": f"metropolitan statistical area/micropolitan statistical area:{code}"}
    raise ValueError(f"unsupported ACS geography level: {level}")


def canonicalize(source_id: str, vintage: int, snapshot: Mapping[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    if source_id not in PRODUCTS or snapshot.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("ACS physical product contract mismatch")
    rows, unavailable = [], []
    for member in snapshot["members"]:
        if member["status"] != "available":
            unavailable.append({"geo_id": member["geo_id"], "classification": member["status"]})
            continue
        header, values = member["payload"]
        record = dict(zip(header, values))
        for variable, suffix in VARIABLES.items():
            raw = record.get(variable)
            try: value = float(raw)
            except (TypeError, ValueError): value = math.nan
            if not math.isfinite(value) or value in SENTINELS:
                unavailable.append({"geo_id": member["geo_id"], "variable": variable,
                                    "classification": "provider_unavailable_value"})
                continue
            rows.append({"geo_id": member["geo_id"], "metric_id": f"{source_id}_{suffix}",
                "date": date(vintage, 12, 31), "property_type_id": "all", "value": value,
                "source_id": source_id, "property_type": "all"})
    frame = pd.DataFrame(rows, columns=CANONICAL_COLUMNS).sort_values(
        ["geo_id", "metric_id"], kind="mergesort").reset_index(drop=True)
    if frame.duplicated(["geo_id", "metric_id", "date", "property_type_id"]).any():
        raise ValueError("duplicate ACS canonical identity")
    membership = sorted(m["geo_id"] for m in snapshot["members"] if m["status"] == "available")
    return frame, {"available_membership": membership, "available_geographies": len(membership),
        "provider_ineligible": unavailable, "excluded_geographies": snapshot["excluded_geographies"]}
