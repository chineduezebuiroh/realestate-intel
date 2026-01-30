#!/usr/bin/env python
"""
sources/bea_qgdp/transform.py

Read BEA QGDP raw artifact from:
  data/bea/bea_qgdp_raw_long.csv

Then:
- ensure dim_source, dim_metric, dim_market
- upsert into fact_timeseries (PK: geo_id, metric_id, date, property_type_id)
"""

from pathlib import Path
import pandas as pd

from core.db import connect  # you said this DEFINITELY exists


REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = REPO_ROOT / "data" / "bea" / "bea_qgdp_raw_long.csv"


def ensure_dims(con, df: pd.DataFrame) -> None:
    # dim_source
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
    WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='bea_gdp_qtr');
    """)

    # dim_metric
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_metric(
      metric_id TEXT PRIMARY KEY,
      name TEXT,
      frequency TEXT,
      unit TEXT,
      category TEXT
    );
    """)
    # single metric in this pass
    mid = "bea_qgdp_real_total_chained2017_saar"
    unit = None
    if "unit" in df.columns and df["unit"].notna().any():
        unit = str(df.loc[df["unit"].notna(), "unit"].iloc[0])
    con.execute("""
    INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
    SELECT ?, ?, ?, ?, ?
    WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?);
    """, [
        mid,
        "Real GDP by state (BEA Regional SQGDP9, LineCode=1, chained 2017 dollars, SAAR)",
        "quarterly",
        unit or "chained 2017 dollars (SAAR)",
        "gdp",
        mid
    ])

    # dim_market
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_market(
      geo_id TEXT PRIMARY KEY,
      name TEXT,
      type TEXT,
      fips TEXT
    );
    """)
    mkts = (
        df[["geo_id", "geo_name", "bea_geo_fips"]]
        .drop_duplicates()
        .rename(columns={"geo_name": "name", "bea_geo_fips": "fips"})
        .assign(type="bea_geo")
    )
    con.register("bea_mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id, name, type, fips)
    SELECT geo_id, name, type, fips FROM bea_mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market);
    """)


def upsert_fact_timeseries(con, df: pd.DataFrame) -> None:
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

    stage = df[["geo_id", "metric_id", "date", "property_type_id", "value", "source_id"]].copy()
    stage["date"] = pd.to_datetime(stage["date"]).dt.date

    # dedupe
    stage = (
        stage.sort_values(["geo_id", "metric_id", "date", "property_type_id"])
             .drop_duplicates(["geo_id", "metric_id", "date", "property_type_id"], keep="last")
    )

    con.register("bea_stage", stage)

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


def main():
    if not RAW_PATH.exists():
        raise SystemExit(f"[bea:transform] Missing raw file: {RAW_PATH}")

    df = pd.read_csv(RAW_PATH)
    if df.empty:
        raise SystemExit("[bea:transform] Raw file is empty.")

    con = connect()

    ensure_dims(con, df)
    upsert_fact_timeseries(con, df)

    summary = con.execute("""
      SELECT geo_id, COUNT(*) n, MIN(date) first, MAX(date) last
      FROM fact_timeseries
      WHERE source_id='bea_gdp_qtr'
      GROUP BY 1
      ORDER BY 1
    """).fetchdf()
    print("[bea:transform] DONE. fact_timeseries rows by geo:")
    print(summary)

    con.close()


if __name__ == "__main__":
    main()
