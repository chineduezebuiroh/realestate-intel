#!/usr/bin/env python
"""
ingest/fred_unemployment_api.py

Fetch unemployment rates from FRED for any geos configured in geo_manifest.csv:

Expected geo_manifest columns:
  - geo_id
  - include_fred_unemp (0/1)
  - fred_unemp_series_id (FRED series code, e.g. 'DCUR')

All series are treated as monthly, seasonally adjusted unemployment rates.
Metric:
  - metric_id = 'fred_unemployment_rate_sa'

Rows are written into fact_timeseries:
  geo_id, metric_id, date, property_type_id='all', value, source_id='fred'
"""

import os
from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd
from dotenv import load_dotenv

try:
    from fredapi import Fred
except ImportError:
    Fred = None

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
GEO_MANIFEST_PATH = Path("config/geo_manifest.csv")
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

SOURCE_ID = "fred"
UNEMP_METRIC_ID = "fred_unemployment_rate_sa"


def get_fred_client() -> Fred | None:
    if not Fred:
        print("[fred-unemp] fredapi not installed; add it to requirements.txt")
        return None
    if not FRED_API_KEY:
        print("[fred-unemp] FRED_API_KEY not set; skipping FRED unemployment ingest.")
        return None
    return Fred(api_key=FRED_API_KEY)


def load_unemp_targets() -> Dict[str, str]:
    """
    Returns dict[geo_id] -> fred_unemp_series_id
    for rows where include_fred_unemp is truthy.
    """
    if not GEO_MANIFEST_PATH.exists():
        raise SystemExit(f"[fred-unemp] Missing geo_manifest at {GEO_MANIFEST_PATH}")

    df = pd.read_csv(GEO_MANIFEST_PATH)

    missing = {"geo_id", "fred_unemp_series_id"} - set(df.columns)
    if missing:
        raise SystemExit(
            "[fred-unemp] geo_manifest.csv must have columns: "
            "'geo_id', 'fred_unemp_series_id' (and optional 'include_fred_unemp'). "
            f"Missing: {sorted(missing)}"
        )

    if "include_fred_unemp" in df.columns:
        df = df[df["include_fred_unemp"].fillna(0).astype(int) == 1]

    df = df[df["fred_unemp_series_id"].notna()]
    if df.empty:
        print("[fred-unemp] No rows enabled via include_fred_unemp / fred_unemp_series_id.")
        return {}

    targets = {
        str(r["geo_id"]).strip(): str(r["fred_unemp_series_id"]).strip()
        for _, r in df.iterrows()
        if str(r["geo_id"]).strip() and str(r["fred_unemp_series_id"]).strip()
    }

    print("[fred-unemp] Targets:", targets)
    return targets


def fetch_monthly_unemp(series_id: str, fred: Fred) -> pd.DataFrame:
    """
    Fetch monthly unemployment rate series from FRED and normalize to month-end.
    Returns columns: date, value
    """
    s = fred.get_series(series_id)
    if s is None or s.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = s.to_frame("value").dropna()
    # FRED monthly dates are usually month-begin; normalize to month-end
    idx = pd.to_datetime(df.index).tz_localize(None)
    idx = idx.to_period("M").to_timestamp("M")
    df.index = idx

    df = df.reset_index().rename(columns={"index": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df[["date", "value"]]


def ensure_dims(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure dim_source + dim_metric entry exists for unemployment metric.
    """
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
    SELECT ?, ?, ?, ?, ?
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_source WHERE source_id = ?
    );
    """, [
        SOURCE_ID,
        "FRED (Federal Reserve Economic Data)",
        "https://fred.stlouisfed.org/",
        "monthly",
        "public",
        SOURCE_ID,
    ])

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_metric(
      metric_id TEXT PRIMARY KEY,
      name TEXT,
      frequency TEXT,
      unit TEXT,
      category TEXT
    );
    """)

    con.execute("""
    INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
    SELECT ?, ?, ?, ?, ?
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_metric WHERE metric_id = ?
    );
    """, [
        UNEMP_METRIC_ID,
        "Unemployment Rate (FRED, SA, monthly)",
        "monthly",
        "percent",
        "labor",
        UNEMP_METRIC_ID,
    ])


def upsert_fact(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    if df.empty:
        print("[fred-unemp] No rows to upsert.")
        return

    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_timeseries(
      geo_id TEXT NOT NULL,
      metric_id TEXT NOT NULL,
      date DATE NOT NULL,
      property_type_id TEXT NOT NULL DEFAULT 'all',
      value DOUBLE,
      source_id TEXT,
      property_type TEXT,
      PRIMARY KEY (geo_id, metric_id, date, property_type_id)
    );
    """)

    con.register("fred_unemp_stage", df[[
        "geo_id", "metric_id", "date", "property_type_id", "value", "source_id"
    ]])

    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1 FROM fred_unemp_stage s
      WHERE s.geo_id = f.geo_id
        AND s.metric_id = f.metric_id
        AND s.date = f.date
        AND s.property_type_id = f.property_type_id
    );
    """)

    con.execute("""
    INSERT INTO fact_timeseries(geo_id, metric_id, date, property_type_id, value, source_id)
    SELECT geo_id, metric_id, date, property_type_id, CAST(value AS DOUBLE), source_id
    FROM fred_unemp_stage;
    """)

    print(f"[fred-unemp] Upserted {len(df):,} rows into fact_timeseries.")


def main():
    print("[fred-unemp] START")

    fred = get_fred_client()
    if not fred:
        return

    targets = load_unemp_targets()
    if not targets:
        print("[fred-unemp] No unemployment targets configured. Exiting.")
        return

    frames = []

    for geo_id, series_id in targets.items():
        print(f"[fred-unemp] Fetching {series_id} for geo_id={geo_id}")
        df = fetch_monthly_unemp(series_id, fred)
        if df.empty:
            print(f"[fred-unemp]   -> no data returned for {series_id}")
            continue

        df = df.assign(
            geo_id=geo_id,
            metric_id=UNEMP_METRIC_ID,
            property_type_id="all",
            source_id=SOURCE_ID,
        )
        frames.append(df)

    if not frames:
        print("[fred-unemp] No series returned any data; nothing to load.")
        return

    all_df = pd.concat(frames, ignore_index=True)

    con = duckdb.connect(DB_PATH)

    # Minimal dim_market entries for geos
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
    con.register("fred_unemp_mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id, name, type, fips)
    SELECT geo_id, name, type, fips
    FROM fred_unemp_mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market);
    """)

    ensure_dims(con)
    upsert_fact(con, all_df)

    summary = con.execute("""
        SELECT
          geo_id,
          MIN(date) AS first,
          MAX(date) AS last,
          COUNT(*)  AS rows
        FROM fact_timeseries
        WHERE source_id = ?
          AND metric_id = ?
        GROUP BY 1
        ORDER BY 1;
    """, [SOURCE_ID, UNEMP_METRIC_ID]).fetchdf()

    print("[fred-unemp] DONE. Summary:")
    print(summary)

    con.close()


if __name__ == "__main__":
    main()
