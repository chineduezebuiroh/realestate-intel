from __future__ import annotations
# sources/bea_gdp/ingest.py

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

"""
sources/bea_gdp/ingest.py

Fetch BEA Regional GDP data:

1. Quarterly real GDP:
   - TableName = SQGDP9
   - LineCode = 1
   - geos flagged with include_bea_qgdp=1
   - output: data/bea/bea_qgdp_raw_long.csv

2. Annual real GDP:
   - TableName = CAGDP9
   - LineCode = 1
   - geos flagged with include_bea_agdp=1
   - output: data/bea/bea_agdp_raw_long.csv

No DB writes here. DB writes are handled by sources/bea_gdp/transform.py.
"""

# =================================================
# ----------- Paths / Config Constants -----------
# =================================================
REPO_ROOT = Path(__file__).resolve().parents[2]
GEO_MANIFEST = REPO_ROOT / "config" / "geo_manifest.generated.csv"

OUT_DIR = REPO_ROOT / "data" / "bea"
RAW_QGDP_PATH = OUT_DIR / "bea_qgdp_raw_long.csv"
RAW_AGDP_PATH = OUT_DIR / "bea_agdp_raw_long.csv"

BEA_API_URL = "https://apps.bea.gov/api/data"
BEA_API_KEY = (os.getenv("BEA_API_KEY") or os.getenv("BEA_API_USER_ID") or "").strip()
if not BEA_API_KEY:
    raise SystemExit(
        "BEA_API_KEY (or BEA_API_USER_ID) not set in env. "
        "Get a key at https://apps.bea.gov/API/signup/"
    )

REGIONAL_DATASET = "Regional"
QGDP_TABLE = "SQGDP9"
AGDP_TABLE = "CAGDP9"
GDP_LINECODE_TOTAL = 1

# =================================================
# ----------- Helpers -----------
# =================================================
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


def parse_year_to_year_end(ystr: str) -> pd.Timestamp:
    """
    Convert BEA annual TimePeriod like '2024' to year-end timestamp.
    """
    year = int(str(ystr).strip())
    return pd.Timestamp(year=year, month=12, day=31)


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


def load_bea_geo_targets(include_col: str) -> Dict[str, Tuple[str, str]]:
    """
    Returns dict[bea_geo_fips] -> (geo_id, geo_name)
    where include_col is truthy.
    """
    if not GEO_MANIFEST.exists():
        raise SystemExit(f"Missing geo_manifest at {GEO_MANIFEST}")

    out: Dict[str, Tuple[str, str]] = {}
    with GEO_MANIFEST.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            flag = (r.get(include_col) or "0").strip()
            if flag not in ("1", "true", "True"):
                continue

            geo_id = (r.get("geo_slug") or r.get("geo_id") or "").strip()
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
        "TableName": QGDP_TABLE,
        "LineCode": str(GDP_LINECODE_TOTAL),
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
            "table_name": QGDP_TABLE,
            "linecode": GDP_LINECODE_TOTAL,
        })

    return pd.DataFrame(rows)


def fetch_regional_agdp_raw(geo_map: Dict[str, Tuple[str, str]]) -> pd.DataFrame:
    if not geo_map:
        return pd.DataFrame()

    geo_fips_param = ",".join(list(geo_map.keys()))
    params = {
        "DataSetName": REGIONAL_DATASET,
        "TableName": AGDP_TABLE,
        "LineCode": str(GDP_LINECODE_TOTAL),
        "Year": "ALL",
        "GeoFips": geo_fips_param,
    }

    data = bea_get(params, label="annual")
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
            dt = parse_year_to_year_end(time_period)
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
            "metric_id": "bea_agdp_real_total_chained2017",
            "date": dt.date().isoformat(),
            "value": value,
            "property_type_id": "all",
            "source_id": "bea_gdp_ann",
            "unit": unit,
            "line_description": line_desc,
            "table_name": AGDP_TABLE,
            "linecode": GDP_LINECODE_TOTAL,
        })

    return pd.DataFrame(rows)


def main():
    print("[bea] START bea_gdp_ingest")

    q_geo_map = load_bea_geo_targets("include_bea_qgdp")
    print(f"[bea] quarterly BEA targets: {len(q_geo_map)}")
    print("       quarterly sample:", list(q_geo_map.items())[:5])

    a_geo_map = load_bea_geo_targets("include_bea_agdp")
    print(f"[bea] annual BEA targets: {len(a_geo_map)}")
    print("       annual sample:", list(a_geo_map.items())[:5])

    qdf = fetch_regional_qgdp_raw(q_geo_map)
    adf = fetch_regional_agdp_raw(a_geo_map)

    if qdf.empty and adf.empty:
        print("[bea] No rows fetched.")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not qdf.empty:
        qdf.to_csv(RAW_QGDP_PATH, index=False)
        print(f"[bea] wrote quarterly {len(qdf):,} rows -> {RAW_QGDP_PATH}")
    else:
        print("[bea] no quarterly rows fetched.")

    if not adf.empty:
        adf.to_csv(RAW_AGDP_PATH, index=False)
        print(f"[bea] wrote annual {len(adf):,} rows -> {RAW_AGDP_PATH}")
    else:
        print("[bea] no annual rows fetched.")

    print("[bea] DONE ingest")

if __name__ == "__main__":
    main()
