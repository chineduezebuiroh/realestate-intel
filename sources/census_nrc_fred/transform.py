from __future__ import annotations
# sources/census_nrc_fred/transform.py

import argparse
from pathlib import Path
from typing import Optional, List

import duckdb
import pandas as pd

from core.db import connect

SOURCE_ID = "census_nrc_fred"
RAW_LONG = Path("data/census/nrc_fred_raw_long.csv")


DIM_METRICS = [
    ("census_housing_starts_total_saar", "Housing Starts: Total (SAAR)", "monthly", "units_saar", "census"),
    ("census_housing_completions_total_saar", "Housing Completions: Total (SAAR)", "monthly", "units_saar", "census"),
]


def ensure_dims(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_source(
          source_id TEXT PRIMARY KEY, name TEXT, url TEXT, cadence TEXT, license TEXT
        );
    """)
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT ?, 'Census New Residential Construction (via FRED)', 'https://fred.stlouisfed.org/', 'monthly', 'public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id=?);
    """, [SOURCE_ID, SOURCE_ID])

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_metric(
            metric_id TEXT PRIMARY KEY,
            name TEXT,
            frequency TEXT,
            unit TEXT,
            category TEXT
        );
    """)
    for mid, name, freq, unit, cat in DIM_METRICS:
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(metric_id) DO UPDATE SET
              name=excluded.name,
              frequency=excluded.frequency,
              unit=excluded.unit,
              category=excluded.category
        """, [mid, name, freq, unit, cat])


def ensure_fact_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_timeseries (
            geo_id           VARCHAR NOT NULL,
            metric_id        VARCHAR NOT NULL,
            date             DATE    NOT NULL,
            value            DOUBLE,
            source_id        VARCHAR,
            property_type_id VARCHAR DEFAULT 'all',
            property_type    VARCHAR,
            CONSTRAINT fact_timeseries_pk PRIMARY KEY (geo_id, metric_id, date, property_type_id)
        );
        """
    )


def load_raw(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise SystemExit(f"[nrc_fred:transform] missing input CSV: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)
    required = {"geo_id", "metric_id", "date", "value", "source_id"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"[nrc_fred:transform] input missing columns: {sorted(missing)}")

    df["geo_id"] = df["geo_id"].astype(str).str.strip()
    df["metric_id"] = df["metric_id"].astype(str).str.strip()
    df["source_id"] = df["source_id"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df[df["geo_id"] != ""]
    df = df[df["metric_id"] != ""]
    df = df[df["date"].notna()]
    df = df[df["value"].notna()]

    # Fact schema
    out = pd.DataFrame(
        {
            "geo_id": df["geo_id"],
            "metric_id": df["metric_id"],
            "date": df["date"].dt.date.astype(str),
            "value": df["value"],
            "source_id": df["source_id"],
            "property_type_id": "all",
            "property_type": "all",
        }
    )
    return out


def insert_into_fact(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    # wipe this source only
    con.execute("DELETE FROM fact_timeseries WHERE source_id = ?", [SOURCE_ID])

    con.register("nrc_df", df)
    con.execute(
        """
        INSERT INTO fact_timeseries (
            geo_id, metric_id, date, value, source_id, property_type_id, property_type
        )
        SELECT
            geo_id,
            metric_id,
            CAST(date AS DATE),
            value,
            source_id,
            property_type_id,
            property_type
        FROM nrc_df
        """
    )
    con.unregister("nrc_df")

    print(f"[nrc_fred:transform] inserted {len(df):,} rows into fact_timeseries")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Transform NRC (via FRED) raw long → fact_timeseries")
    parser.add_argument("--csv", default=str(RAW_LONG))
    args = parser.parse_args(argv)

    df = load_raw(Path(args.csv))

    con = connect()
    ensure_fact_table(con)
    ensure_dims(con)
    insert_into_fact(con, df)
    con.close()

    print("[nrc_fred:transform] done.")


if __name__ == "__main__":
    main()

