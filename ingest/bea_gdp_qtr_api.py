#!/usr/bin/env python
"""
ingest/bea_gdp_qtr_api.py

Fetch quarterly *real* GDP from BEA and load into DuckDB fact_timeseries:

1) Regional dataset (state-level, etc.)
   - TableName: SQGDP9N (Real GDP by state, chained 2017 dollars, SAAR)
   - We pull LineCode=1 (All industry total)
   - Geos are driven by config/geo_manifest.csv via:
       - include_bea_qgdp = 1
       - bea_geo_fips     = BEA GeoFips code (e.g. '11' for DC, '24' for MD, '00000' for US total if available)

2) GDPbyIndustry dataset (U.S.-total sector-level)
   - Quarterly value added (real, chained dollars) by industry.
   - We use a configurable TableID for the "real, chained" table.
   - One pseudo-geo (e.g. 'us_total') is used to attach these series.

NOTE: A couple of constants (e.g. TABLE_ID_REAL_QTR) and exact table names/units
may need to be confirmed against BEA docs:
    https://apps.bea.gov/developers/
"""

import os
import csv
from pathlib import Path
from datetime import date
import re
from typing import Dict, List, Tuple

import requests
import pandas as pd
import duckdb

# ----------------- Config -----------------

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
GEO_MANIFEST = Path("config/geo_manifest.csv")

BEA_API_URL = "https://apps.bea.gov/api/data"
BEA_API_KEY = (os.getenv("BEA_API_KEY") or os.getenv("BEA_API_USER_ID") or "").strip()

if not BEA_API_KEY:
    raise SystemExit(
        "BEA_API_KEY (or BEA_API_USER_ID) not set in env. "
        "Get a key at https://apps.bea.gov/API/signup/"
    )

# ---- Regional quarterly real GDP by state/etc. ----
REGIONAL_DATASET = "Regional"
# Real GDP by state, quarterly, chained 2017 dollars.
# This table name is based on BEA conventions (SQ* = quarterly, *9N = real GDP).
# If BEA has changed naming, adjust here.
REGIONAL_TABLE = "SQGDP9N"
# LineCode=1 is usually "All industry total" in these tables.
REGIONAL_LINECODE_TOTAL = 1

# ---- GDP by industry, quarterly, real, U.S. total ----
GDPBYIND_DATASET = "GDPbyIndustry"

# IMPORTANT: You will likely want to confirm which table ID is the real
# (chained-dollar) quarterly "Value added by industry" table.
# Commonly:
#   - 1: Value added by industry (current dollars)
#   - 2: Value added by industry (chained dollars)
# but this can vary by BEA vintage. Check via GetParameterValues(TableID).
TABLE_ID_REAL_QTR = 2  # <-- TUNE THIS IF NEEDED

# We pull *all* industries that have quarterly data and then filter
# to those whose IndustryDescription looks like a sector.
GDPBYIND_FREQUENCY = "Q"
GDPBYIND_YEAR = "ALL"
GDPBYIND_INDUSTRY = "ALL"  # let BEA return all industries; we’ll filter

# Which geo_id in geo_manifest should receive U.S.-total sector GDP?
# We’ll look up a row with bea_geo_fips == '00000' first; if not found,
# we default to this id and DO NOT write if it’s missing in dim_market.
DEFAULT_US_GEO_ID = "us_total"

# Time window: BEA quarterly GDP-by-industry only begins in 2005.
QSTART_YEAR = 1990  # safe lower bound; BEA will just return what’s there.
QEND_YEAR = date.today().year

# -------------- Helpers / plumbing --------------

def parse_quarter_to_date(qstr: str) -> pd.Timestamp:
    """
    Convert BEA 'TimePeriod' like '2005Q1' to a quarter-end date (month-end).
    """
    m = re.fullmatch(r"(\d{4})Q([1-4])", qstr)
    if not m:
        raise ValueError(f"Unexpected TimePeriod format: {qstr}")
    year = int(m.group(1))
    q = int(m.group(2))
    month = {1: 3, 2: 6, 3: 9, 4: 12}[q]
    # Use month-end for consistency with your monthly series style
    return pd.Timestamp(year=year, month=month, day=1).to_period("M").to_timestamp("M")


def bea_get(params: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Call BEA API GetData and return the raw 'Data' rows list.
    Raises if BEA reports an error.
    """
    base_params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "ResultFormat": "json",
    }
    all_params = {**base_params, **params}
    r = requests.get(BEA_API_URL, params=all_params, timeout=60)
    r.raise_for_status()
    j = r.json()

    if "BEAAPI" not in j:
        raise RuntimeError(f"Unexpected BEA response: {j}")

    # Handle API-level errors if present
    results = j["BEAAPI"].get("Results", {})
    if "Error" in results:
        raise RuntimeError(f"BEA error: {results['Error']}")

    data = results.get("Data") or []
    return data


def load_bea_geo_targets() -> Dict[str, Tuple[str, str]]:
    """
    Read geo_manifest and return:
        dict[bea_geo_fips] -> (geo_id, geo_name)
    for rows where include_bea_qgdp is truthy (1/true/True).
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


def ensure_dims(con: duckdb.DuckDBPyConnection, metrics_meta: Dict[str, Dict[str, str]]):
    """
    Ensure dim_source and dim_metric have entries for BEA GDP metrics.
    metrics_meta: dict[metric_id] -> {name, frequency, unit, category}
    """
    # Source (idempotent)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_source(
      source_id TEXT PRIMARY KEY,
      name TEXT,
      url TEXT,
      cadence TEXT,
      license TEXT
    );
    """)

    con.execute("""
    INSERT INTO dim_source(source_id, name, url, cadence, license)
    SELECT 'bea_gdp_qtr',
           'BEA GDP (Quarterly)',
           'https://www.bea.gov/data',
           'quarterly',
           'public'
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_source WHERE source_id = 'bea_gdp_qtr'
    );
    """)

    # Metric dim
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_metric(
      metric_id TEXT PRIMARY KEY,
      name TEXT,
      frequency TEXT,
      unit TEXT,
      category TEXT
    );
    """)

    for mid, meta in metrics_meta.items():
        name = meta.get("name") or "BEA GDP"
        freq = meta.get("frequency") or "quarterly"
        unit = meta.get("unit") or "millions"
        cat  = meta.get("category") or "gdp"

        con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
          SELECT 1 FROM dim_metric WHERE metric_id = ?
        );
        """, [mid, name, freq, unit, cat, mid])


def upsert_fact(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    """
    Upsert rows into fact_timeseries keyed by (geo_id, metric_id, date, property_type_id).
    """
    if df.empty:
        return

    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_timeseries(
      geo_id TEXT NOT NULL,
      metric_id TEXT NOT NULL,
      date DATE NOT NULL,
      property_type_id TEXT NOT NULL DEFAULT 'all',
      value DOUBLE,
      source_id TEXT,
      PRIMARY KEY (geo_id, metric_id, date, property_type_id)
    );
    """)

    # Deduplicate to last value
    df = (
        df.sort_values(["geo_id", "metric_id", "date", "property_type_id"])
          .drop_duplicates(subset=["geo_id", "metric_id", "date", "property_type_id"], keep="last")
    )

    con.register("bea_stage", df[["geo_id", "metric_id", "date", "property_type_id", "value", "source_id"]])

    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1 FROM bea_stage s
      WHERE s.geo_id = f.geo_id
        AND s.metric_id = f.metric_id
        AND s.date = f.date
        AND s.property_type_id = f.property_type_id
    );
    """)

    con.execute("""
    INSERT INTO fact_timeseries(geo_id, metric_id, date, property_type_id, value, source_id)
    SELECT geo_id, metric_id, date, property_type_id, CAST(value AS DOUBLE), source_id
    FROM bea_stage;
    """)

# -------------- Regional: state-level quarterly real GDP --------------

def fetch_regional_state_gdp(geo_map: Dict[str, Tuple[str, str]]) -> Tuple[pd.DataFrame, Dict[str, Dict[str, str]]]:
    """
    Fetch quarterly *real* GDP (chained 2017 dollars, SAAR) from the Regional dataset
    for the GeoFips codes in geo_map.

    Returns:
        df  - DataFrame with columns: geo_id, metric_id, date, value, property_type_id, source_id
        meta- metric metadata dict[metric_id] -> {name, frequency, unit, category}
    """
    if not geo_map:
        return pd.DataFrame(), {}

    geo_fips_list = list(geo_map.keys())
    geo_fips_param = ",".join(geo_fips_list)

    # Use ALL years, but BEA will only return from 2005 onward for quarterly GDP.
    params = {
        "DataSetName": REGIONAL_DATASET,
        "TableName": REGIONAL_TABLE,
        "LineCode": str(REGIONAL_LINECODE_TOTAL),
        "Year": "ALL",
        "GeoFips": geo_fips_param,
    }

    data = bea_get(params)
    if not data:
        print("[bea] Regional returned no data.")
        return pd.DataFrame(), {}

    # Inspect a sample row to guess unit/description
    sample = data[0]
    unit = (sample.get("CL_UNIT") or sample.get("Unit") or "").strip()
    desc = (sample.get("LineDescription") or "Real GDP (total, chained 2017 dollars)").strip()

    rows = []
    for row in data:
        geo_fips = (row.get("GeoFips") or "").strip()
        time_period = (row.get("TimePeriod") or "").strip()
        val_str = (row.get("DataValue") or "").strip()

        if not geo_fips or not time_period:
            continue
        if geo_fips not in geo_map:
            # Ignore geos we didn't ask for (shouldn't happen but harmless)
            continue

        try:
            dt = parse_quarter_to_date(time_period)
        except Exception:
            # skip non-quarter rows
            continue

        # Some BEA cells are "(NA)" etc.
        try:
            value = float(val_str.replace(",", ""))
        except Exception:
            continue

        geo_id, _name = geo_map[geo_fips]
        rows.append({
            "geo_id": geo_id,
            "metric_id": "gdp_real_total",   # one metric for all geos
            "date": dt.date(),
            "value": value,
            "property_type_id": "all",
            "source_id": "bea_gdp_qtr",
        })

    df = pd.DataFrame(rows)

    metrics_meta = {
        "gdp_real_total": {
            "name": f"Real GDP, total (regional: {REGIONAL_TABLE}, LineCode {REGIONAL_LINECODE_TOTAL})",
            "frequency": "quarterly",
            "unit": unit or "millions of chained 2017 dollars",
            "category": "gdp",
        }
    }
    return df, metrics_meta


# -------------- GDPbyIndustry: U.S. sector-level quarterly real GDP --------------

def slugify(text: str) -> str:
    """
    Make a simple slug for metric_id from an industry description.
    """
    text = text.lower()
    text = re.sub(r"[^\w]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def fetch_us_sector_gdp(us_geo_id: str) -> Tuple[pd.DataFrame, Dict[str, Dict[str, str]]]:
    """
    Fetch quarterly sector-level real GDP (chained dollars) for U.S. total
    from GDPbyIndustry dataset.

    Returns:
        df   - DataFrame with columns: geo_id, metric_id, date, value, property_type_id, source_id
        meta - dict[metric_id] -> {name, frequency, unit, category}
    """
    if not us_geo_id:
        # No geo configured to host U.S. totals
        return pd.DataFrame(), {}

    params = {
        "DataSetName": GDPBYIND_DATASET,
        "TableID": str(TABLE_ID_REAL_QTR),
        "Frequency": GDPBYIND_FREQUENCY,
        "Year": GDPBYIND_YEAR,
        "Industry": GDPBYIND_INDUSTRY,
    }

    data = bea_get(params)
    if not data:
        print("[bea] GDPbyIndustry returned no data.")
        return pd.DataFrame(), {}

    rows = []
    metrics_meta: Dict[str, Dict[str, str]] = {}

    for row in data:
        time_period = (row.get("TimePeriod") or "").strip()
        industry = (row.get("Industry") or "").strip()
        ind_desc = (row.get("IndustryDescription") or
                    row.get("IndustryDesc") or
                    industry).strip()
        val_str = (row.get("DataValue") or "").strip()
        unit = (row.get("CL_UNIT") or row.get("Unit") or "").strip()

        if not time_period or not industry:
            continue

        try:
            dt = parse_quarter_to_date(time_period)
        except Exception:
            # skip non-quarter rows (e.g., annual or other)
            continue

        try:
            value = float(val_str.replace(",", ""))
        except Exception:
            continue

        # Build metric_id from industry. Example: "Real GDP, Private industries"
        # -> "gdp_real_sector_private_industries"
        metric_id = f"gdp_real_sector_{slugify(industry)}"
        # If industry codes are like "ALL", "11", "21", this will create
        # "gdp_real_sector_all", "gdp_real_sector_11", etc.

        if metric_id not in metrics_meta:
            metrics_meta[metric_id] = {
                "name": f"Real GDP by industry: {ind_desc}",
                "frequency": "quarterly",
                "unit": unit or "millions of chained dollars",
                "category": "gdp_sector",
            }

        rows.append({
            "geo_id": us_geo_id,
            "metric_id": metric_id,
            "date": dt.date(),
            "value": value,
            "property_type_id": "all",
            "source_id": "bea_gdp_qtr",
        })

    df = pd.DataFrame(rows)
    return df, metrics_meta


# -------------- Main --------------

def main():
    print("[bea] START bea_gdp_qtr_api")

    geo_map = load_bea_geo_targets()
    if not geo_map:
        print("[bea] NOTE: No BEA geos enabled in geo_manifest (include_bea_qgdp=1).")
        return

    print(f"[bea] geo_manifest BEA targets: {len(geo_map)}")
    print("       sample:", list(geo_map.items())[:5])

    # Identify US total geo (bea_geo_fips == '00000') if present
    us_geo_id = None
    for code, (gid, _name) in geo_map.items():
        if code == "00000":
            us_geo_id = gid
            break
    if not us_geo_id:
        # Fall back to DEFAULT_US_GEO_ID if user created it
        # (we'll still insert even if it isn't in dim_market yet; dim_market will
        # be populated lazily in this script).
        us_geo_id = DEFAULT_US_GEO_ID

    # 1) Regional state-level total real GDP
    reg_df, reg_meta = fetch_regional_state_gdp(geo_map)

    # 2) US-total sector-level real GDP
    sector_df, sector_meta = fetch_us_sector_gdp(us_geo_id)

    # Combine
    all_df = pd.concat([reg_df, sector_df], ignore_index=True) if not reg_df.empty or not sector_df.empty else pd.DataFrame()

    if all_df.empty:
        print("[bea] No rows to upsert.")
        return

    # Metric metadata
    metrics_meta = {**reg_meta, **sector_meta}

    # Connect DB and load
    con = duckdb.connect(DB_PATH)

    # Ensure dim_market has our geos (minimal entries)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_market(
      geo_id TEXT PRIMARY KEY,
      name TEXT,
      type TEXT,
      fips TEXT
    );
    """)

    mkts = (
        all_df[["geo_id"]]
        .drop_duplicates()
        .assign(name=lambda d: d["geo_id"], type=None, fips=None)
    )
    con.register("bea_mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id, name, type, fips)
    SELECT geo_id, name, type, fips FROM bea_mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market);
    """)

    ensure_dims(con, metrics_meta)
    upsert_fact(con, all_df)

    # Summary
    summary = con.execute("""
        SELECT
          metric_id,
          MIN(date) AS first,
          MAX(date) AS last,
          COUNT(*)  AS rows
        FROM fact_timeseries
        WHERE source_id = 'bea_gdp_qtr'
        GROUP BY 1
        ORDER BY 1;
    """).df()

    print("[bea] DONE. Summary:")
    print(summary)

    con.close()


if __name__ == "__main__":
    main()
