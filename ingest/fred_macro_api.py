#!/usr/bin/env python
"""
ingest/fred_macro_api.py

Unified FRED ingest that loads macro time series directly into DuckDB fact_timeseries.

Pulls (US-level only):

1) Rates & yields – resampled to monthly averages
   - 30Y mortgage rate (MORTGAGE30US)      -> fred_mortgage_30y_avg
   - 15Y mortgage rate (MORTGAGE15US)      -> fred_mortgage_15y_avg
   - 5/1 ARM mortgage rate (MORTGAGE5US)   -> fred_mortgage_5y_arm_avg
   - 2Y Treasury yield (GS2)               -> fred_gs2
   - 10Y Treasury yield (GS10)             -> fred_gs10
   - 30Y Treasury yield (GS30)             -> fred_gs30
   - Effective Fed funds rate (FEDFUNDS)   -> fred_fedfunds

2) Inflation – monthly
   - CPIAUCSL (CPI: All urban consumers, SA, 1982-84=100)
     -> fred_cpi_urban_sa_index

3) Yield spreads – derived monthly series (US)
   - fred_spread_2y_10y        = fred_gs2 - fred_gs10
   - fred_spread_10y_30y       = fred_gs10 - fred_gs30
   - fred_spread_2y_30y        = fred_gs2 - fred_gs30
   - fred_spread_2y_fedfunds   = fred_gs2 - fred_fedfunds
   - fred_spread_10y_fedfunds  = fred_gs10 - fred_fedfunds
   - fred_spread_30y_fedfunds  = fred_gs30 - fred_fedfunds

All series are written as:
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

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
GEO_MANIFEST_PATH = Path("config/geo_manifest.csv")

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

SOURCE_ID = "fred_macro"

# Definition of all base FRED series we want
# metric_id -> metadata
FRED_SERIES: Dict[str, Dict[str, str]] = {
    # ---- Mortgage rates: weekly → monthly avg (US total) ----
    "fred_mortgage_30y_avg": {
        "series_id": "MORTGAGE30US",
        "name": "30Y Mortgage Rate (monthly avg, FRED)",
        "unit": "percent",
        "category": "rates",
        "frequency": "monthly",
        "agg": "mean",
        "geo_code": "US",
    },
    "fred_mortgage_15y_avg": {
        "series_id": "MORTGAGE15US",
        "name": "15Y Mortgage Rate (monthly avg, FRED)",
        "unit": "percent",
        "category": "rates",
        "frequency": "monthly",
        "agg": "mean",
        "geo_code": "US",
    },
    "fred_mortgage_5y_arm_avg": {
        "series_id": "MORTGAGE5US",
        "name": "5/1 ARM Mortgage Rate (monthly avg, FRED)",
        "unit": "percent",
        "category": "rates",
        "frequency": "monthly",
        "agg": "mean",
        "geo_code": "US",
    },

    # ---- Yield curve: daily → monthly avg (US total) ----
    "fred_gs2": {
        "series_id": "GS2",
        "name": "2Y Treasury Constant Maturity Yield (monthly avg, FRED)",
        "unit": "percent",
        "category": "yields",
        "frequency": "monthly",
        "agg": "mean",
        "geo_code": "US",
    },
    "fred_gs10": {
        "series_id": "GS10",
        "name": "10Y Treasury Constant Maturity Yield (monthly avg, FRED)",
        "unit": "percent",
        "category": "yields",
        "frequency": "monthly",
        "agg": "mean",
        "geo_code": "US",
    },
    "fred_gs30": {
        "series_id": "GS30",
        "name": "30Y Treasury Constant Maturity Yield (monthly avg, FRED)",
        "unit": "percent",
        "category": "yields",
        "frequency": "monthly",
        "agg": "mean",
        "geo_code": "US",
    },

    # ---- Fed funds: daily → monthly avg (US total) ----
    "fred_fedfunds": {
        "series_id": "FEDFUNDS",
        "name": "Federal Funds Effective Rate (monthly avg, FRED)",
        "unit": "percent",
        "category": "rates",
        "frequency": "monthly",
        "agg": "mean",
        "geo_code": "US",
    },

    # ---- Inflation: monthly (US total) ----
    "fred_cpi_urban_sa_index": {
        "series_id": "CPIAUCSL",
        "name": "CPI: All Urban Consumers (SA, 1982-84=100)",
        "unit": "index (1982-84=100)",
        "category": "inflation",
        "frequency": "monthly",
        "agg": "as_is",  # already monthly
        "geo_code": "US",
    },
}

# Derived spread metrics (no series_id; computed from FRED_SERIES)
SPREAD_SERIES_META: Dict[str, Dict[str, str]] = {
    "fred_spread_2y_10y": {
        "name": "Yield Spread: 2Y - 10Y",
        "unit": "percentage points",
        "category": "spreads",
        "frequency": "monthly",
    },
    "fred_spread_10y_30y": {
        "name": "Yield Spread: 10Y - 30Y",
        "unit": "percentage points",
        "category": "spreads",
        "frequency": "monthly",
    },
    "fred_spread_2y_30y": {
        "name": "Yield Spread: 2Y - 30Y",
        "unit": "percentage points",
        "category": "spreads",
        "frequency": "monthly",
    },
    "fred_spread_2y_fedfunds": {
        "name": "Yield Spread: 2Y - Fed Funds",
        "unit": "percentage points",
        "category": "spreads",
        "frequency": "monthly",
    },
    "fred_spread_10y_fedfunds": {
        "name": "Yield Spread: 10Y - Fed Funds",
        "unit": "percentage points",
        "category": "spreads",
        "frequency": "monthly",
    },
    "fred_spread_30y_fedfunds": {
        "name": "Yield Spread: 30Y - Fed Funds",
        "unit": "percentage points",
        "category": "spreads",
        "frequency": "monthly",
    },
}


def get_fred_client() -> Fred | None:
    if not Fred:
        print("[fred] fredapi not installed; add it to requirements.txt")
        return None
    if not FRED_API_KEY:
        print("[fred] FRED_API_KEY not set; skipping FRED macro ingest.")
        return None
    return Fred(api_key=FRED_API_KEY)


def to_month_end_index(idx: pd.Index) -> pd.DatetimeIndex:
    dt = pd.to_datetime(idx).tz_localize(None)
    return dt.to_period("M").to_timestamp("M")


def fetch_monthly_avg(series_id: str, fred: Fred) -> pd.DataFrame:
    s = fred.get_series(series_id)
    if s is None or s.empty:
        return pd.DataFrame(columns=["date", "value"])
    df = s.to_frame("value").dropna()
    df.index = to_month_end_index(df.index)
    monthly = (
        df.resample("ME")
          .mean()
          .reset_index()
          .rename(columns={"index": "date"})
    )
    monthly["date"] = pd.to_datetime(monthly["date"]).dt.date
    return monthly[["date", "value"]]


def fetch_monthly_as_is(series_id: str, fred: Fred) -> pd.DataFrame:
    s = fred.get_series(series_id)
    if s is None or s.empty:
        return pd.DataFrame(columns=["date", "value"])
    df = s.to_frame("value").dropna()
    df.index = to_month_end_index(df.index)
    df = df.reset_index().rename(columns={"index": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df[["date", "value"]]


def load_fred_geo_map() -> Dict[str, str]:
    """
    Map FRED geo codes -> geo_id in your schema.

    If geo_manifest has [include_fred, fred_geo_code], use that.
    Otherwise, fallback to:
      'US' -> 'us_nation'
    """
    fallback = {"US": "us_nation"}

    if not GEO_MANIFEST_PATH.exists():
        print(f"[fred] geo_manifest not found at {GEO_MANIFEST_PATH}, using fallback geo map: {fallback}")
        return fallback

    df = pd.read_csv(GEO_MANIFEST_PATH)

    if "fred_geo_code" not in df.columns:
        print("[fred] geo_manifest has no 'fred_geo_code' column; "
              f"using fallback geo map: {fallback}")
        return fallback

    if "include_fred" in df.columns:
        df = df[df["include_fred"].fillna(0).astype(int) == 1]

    df = df[df["fred_geo_code"].notna()]
    if df.empty:
        print("[fred] No rows with fred_geo_code in geo_manifest; using fallback map.")
        return fallback

    geo_map = {
        str(r["fred_geo_code"]).strip(): str(r["geo_id"]).strip()
        for _, r in df.iterrows()
        if str(r["fred_geo_code"]).strip() and str(r["geo_id"]).strip()
    }

    for code, gid in fallback.items():
        if code not in geo_map:
            geo_map[code] = gid

    print("[fred] geo map:", geo_map)
    return geo_map


def ensure_dims(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure dim_source and dim_metric entries exist for FRED metrics and spreads.
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
        "daily/weekly/monthly (aggregated to monthly)",
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

    all_meta = {}
    all_meta.update(FRED_SERIES)
    all_meta.update(SPREAD_SERIES_META)

    for metric_id, meta in all_meta.items():
        name = meta.get("name", metric_id)
        freq = meta.get("frequency", "monthly")
        unit = meta.get("unit", "")
        cat  = meta.get("category", "fred")

        con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
          SELECT 1 FROM dim_metric WHERE metric_id = ?
        );
        """, [metric_id, name, freq, unit, cat, metric_id])


def upsert_fact(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    if df.empty:
        print("[fred] No rows to upsert into fact_timeseries.")
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

    con.register(
        "fred_stage",
        df[["geo_id", "metric_id", "date", "property_type_id", "value", "source_id"]]
    )

    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1 FROM fred_stage s
      WHERE s.geo_id = f.geo_id
        AND s.metric_id = f.metric_id
        AND s.date = f.date
        AND s.property_type_id = f.property_type_id
    );
    """)

    con.execute("""
    INSERT INTO fact_timeseries(geo_id, metric_id, date, property_type_id, value, source_id)
    SELECT geo_id, metric_id, date, property_type_id, CAST(value AS DOUBLE), source_id
    FROM fred_stage;
    """)

    print(f"[fred] Upserted {len(df):,} rows into fact_timeseries.")


def main():
    print("[fred] START fred_macro_api")

    fred = get_fred_client()
    if not fred:
        return

    geo_map = load_fred_geo_map()

    frames = []

    # 1) Fetch all base FRED series
    for metric_id, meta in FRED_SERIES.items():
        sid = meta["series_id"]
        agg = meta.get("agg", "as_is")
        geo_code = meta.get("geo_code", "US")

        print(f"[fred] Fetching {metric_id} ({sid}), agg={agg}, geo_code={geo_code}")

        if agg == "mean":
            df = fetch_monthly_avg(sid, fred)
        else:
            df = fetch_monthly_as_is(sid, fred)

        if df.empty:
            print(f"[fred]   -> no data returned for {sid}")
            continue

        df = df.assign(metric_id=metric_id, geo_code=geo_code)
        frames.append(df)

    if not frames:
        print("[fred] No series returned any data; nothing to load.")
        return

    all_df = pd.concat(frames, ignore_index=True)

    # 2) Map geo_code → geo_id
    all_df["geo_id"] = all_df["geo_code"].map(geo_map)
    missing = all_df["geo_id"].isna().sum()
    if missing:
        print(f"[fred] Warning: {missing} rows have geo_code not mapped to a geo_id; dropping them.")
        all_df = all_df[all_df["geo_id"].notna()]

    if all_df.empty:
        print("[fred] All rows were dropped after geo mapping; nothing to load.")
        return

    # Base time series
    ts = all_df[["geo_id", "metric_id", "date", "value"]].copy()
    ts["property_type_id"] = "all"
    ts["source_id"] = SOURCE_ID

    # 3) Compute yield spreads per geo_id
    spread_frames = []

    def add_spread(df_wide, geo_id, new_metric, a, b):
        if a in df_wide.columns and b in df_wide.columns:
            s = df_wide[a] - df_wide[b]
            s = s.dropna()
            if not s.empty:
                tmp = s.reset_index().rename(columns={0: "value"})
                tmp.columns = ["date", "value"]
                tmp["geo_id"] = geo_id
                tmp["metric_id"] = new_metric
                tmp["property_type_id"] = "all"
                tmp["source_id"] = SOURCE_ID
                spread_frames.append(tmp)

    for geo_id in ts["geo_id"].unique():
        sub = ts[ts["geo_id"] == geo_id]
        wide = sub.pivot(index="date", columns="metric_id", values="value")

        add_spread(wide, geo_id, "fred_spread_2y_10y", "fred_gs2", "fred_gs10")
        add_spread(wide, geo_id, "fred_spread_10y_30y", "fred_gs10", "fred_gs30")
        add_spread(wide, geo_id, "fred_spread_2y_30y", "fred_gs2", "fred_gs30")
        add_spread(wide, geo_id, "fred_spread_2y_fedfunds", "fred_gs2", "fred_fedfunds")
        add_spread(wide, geo_id, "fred_spread_10y_fedfunds", "fred_gs10", "fred_fedfunds")
        add_spread(wide, geo_id, "fred_spread_30y_fedfunds", "fred_gs30", "fred_fedfunds")

    if spread_frames:
        spreads = pd.concat(spread_frames, ignore_index=True)
        ts = pd.concat([ts, spreads], ignore_index=True)
        print(f"[fred] Created {len(spreads):,} spread rows across {ts['geo_id'].nunique()} geos.")
    else:
        print("[fred] No spreads created (missing base yield / fed funds series).")

    con = duckdb.connect(DB_PATH)

    # Ensure dim_market has basic entries for these geo_ids
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_market(
      geo_id TEXT PRIMARY KEY,
      name TEXT,
      type TEXT,
      fips TEXT
    );
    """)
    mkts = (
        ts[["geo_id"]]
        .drop_duplicates()
        .assign(name=lambda d: d["geo_id"], type=None, fips=None)
    )
    con.register("fred_mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id, name, type, fips)
    SELECT geo_id, name, type, fips
    FROM fred_mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market);
    """)

    ensure_dims(con)
    upsert_fact(con, ts)

    summary = con.execute("""
        SELECT
          metric_id,
          MIN(date) AS first,
          MAX(date) AS last,
          COUNT(*)  AS rows
        FROM fact_timeseries
        WHERE source_id = ?
        GROUP BY 1
        ORDER BY 1;
    """, [SOURCE_ID]).fetchdf()

    print("[fred] DONE. Summary:")
    print(summary)

    con.close()


if __name__ == "__main__":
    main()
