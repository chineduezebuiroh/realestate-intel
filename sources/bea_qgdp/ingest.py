#!/usr/bin/env python
"""
sources/bea_qgdp/ingest.py

Fetch BEA Regional quarterly Real GDP by state (SQGDP9, LineCode=1)
for geos flagged in config/geo_manifest.csv (include_bea_qgdp=1).

Outputs a raw long file:
  data/bea/bea_qgdp_raw_long.csv

No DB writes here. That's handled by transform/bea_qgdp_transform.py.
"""

from dotenv import load_dotenv
load_dotenv()

import os
import csv
import re
from pathlib import Path
from datetime import date
from typing import Dict, Tuple, List

import requests
import pandas as pd


# ----------------- Paths / Config -----------------
REPO_ROOT = Path(__file__).resolve().parents[2]
GEO_MANIFEST = REPO_ROOT / "config" / "geo_manifest.csv"

OUT_DIR = REPO_ROOT / "data" / "bea"
RAW_PATH = OUT_DIR / "bea_qgdp_raw_long.csv"

BEA_API_URL = "https://apps.bea.gov/api/data"
BEA_API_KEY = (os.getenv("BEA_API_KEY") or os.getenv("BEA_API_USER_ID") or "").strip()
if not BEA_API_KEY:
    raise SystemExit(
        "BEA_API_KEY (or BEA_API_USER_ID) not set in env. "
        "Get a key at https://apps.bea.gov/API/signup/"
    )

REGIONAL_DATASET = "Regional"
REGIONAL_TABLE = "SQGDP9"
REGIONAL_LINECODE_TOTAL = 1


# ----------------- Helpers -----------------

def parse_quarter_to_month_end(qstr: str) -> pd.Timestamp:
    """
    Convert BEA 'TimePeriod' like '2005Q1' to a quarter-end *month-end* timestamp.
    """
    m = re.fullmatch(r"(\d{4})Q([1-4])", qstr)
    if not m:
        raise ValueError(f"Unexpected TimePeriod format: {qstr}")
    year = int(m.group(1))
    q = int(m.group(2))
    month = {1: 3, 2: 6, 3: 9, 4: 12}[q]
    return pd.Timestamp(year=year, month=month, day=1).to_period("M").to_timestamp("M")


def bea_get(params: Dict[str, str], label: str = "") -> List[Dict[str, str]]:
    all_params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "ResultFormat": "JSON",
        **params,
    }

    prefix = f"[bea:{label}]" if label else "[bea]"
    print(f"{prefix} calling with params:")
    for k, v in all_params.items():
        print(f"  {k}={v}")

    r = requests.get(BEA_API_URL, params=all_params, timeout=60)
    r.raise_for_status()
    js = r.json()

    api = js.get("BEAAPI", {})
    if not api:
        print(f"{prefix} Unexpected response (no BEAAPI): {js}")
        return []

    if "Error" in api:
        print(f"{prefix} ERROR: {api['Error']}")
        return []

    results = api.get("Results", {})
    data = results.get("Data", [])
    note = results.get("Note")

    if not data:
        print(f"{prefix} returned no data.")
        if note:
            print(f"{prefix} Note:", note)
        return []

    print(f"{prefix} got {len(data)} rows.")
    return data


def load_bea_geo_targets() -> Dict[str, Tuple[str, str]]:
    """
    Returns dict[bea_geo_fips] -> (geo_id, geo_name)
    where include_bea_qgdp is truthy.
    """
    if not GEO_MANIFEST.exists():
        raise SystemExit(f"Missing geo_manifest at {GEO_MANIFEST}")

    out: Dict[str, Tuple[str, str]] = {}
    with GEO_MANIFEST.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            flag = (r.get("include_bea_qgdp") or "0").strip()
            if flag not in ("1", "true", "True"):
                continue
            geo_id = (r.get("geo_id") or "").strip()
            name = (r.get("geo_name") or "").strip()
            code = (r.get("bea_geo_fips") or "").strip()
            if not geo_id or not code:
                continue
            out[code] = (geo_id, name)
    return out


def fetch_regional_qgdp_raw(geo_map: Dict[str, Tuple[str, str]]) -> pd.DataFrame:
    if not geo_map:
        return pd.DataFrame()

    geo_fips_param = ",".join(list(geo_map.keys()))
    params = {
        "DataSetName": REGIONAL_DATASET,
        "TableName": REGIONAL_TABLE,
        "LineCode": str(REGIONAL_LINECODE_TOTAL),
        "Year": "ALL",
        "GeoFips": geo_fips_param,
    }

    data = bea_get(params, label="regional")
    if not data:
        return pd.DataFrame()

    rows = []
    for row in data:
        geo_fips = (row.get("GeoFips") or "").strip()
        time_period = (row.get("TimePeriod") or "").strip()
        val_str = (row.get("DataValue") or "").strip()

        if geo_fips not in geo_map:
            continue

        try:
            dt = parse_quarter_to_month_end(time_period)
        except Exception:
            continue

        try:
            value = float(val_str.replace(",", ""))
        except Exception:
            continue

        unit = (row.get("CL_UNIT") or row.get("Unit") or "").strip() or None
        line_desc = (row.get("LineDescription") or "").strip() or None

        geo_id, geo_name = geo_map[geo_fips]
        rows.append({
            "geo_id": geo_id,
            "geo_name": geo_name,
            "bea_geo_fips": geo_fips,
            "metric_id": "bea_qgdp_real_total_chained2017_saar",
            "date": dt.date().isoformat(),
            "value": value,
            "property_type_id": "all",
            "source_id": "bea_gdp_qtr",
            "unit": unit,
            "line_description": line_desc,
            "table_name": REGIONAL_TABLE,
            "linecode": REGIONAL_LINECODE_TOTAL,
        })

    return pd.DataFrame(rows)


def main():
    print("[bea] START bea_qgdp_ingest")

    geo_map = load_bea_geo_targets()
    print(f"[bea] geo_manifest BEA targets: {len(geo_map)}")
    print("       sample:", list(geo_map.items())[:5])

    df = fetch_regional_qgdp_raw(geo_map)
    if df.empty:
        print("[bea] No rows fetched.")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(RAW_PATH, index=False)
    print(f"[bea] wrote {len(df):,} rows -> {RAW_PATH}")

    print("[bea] DONE ingest")


if __name__ == "__main__":
    main()
