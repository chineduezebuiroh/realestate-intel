#!/usr/bin/env python
"""
Transform BPS building permits (units) into fact_timeseries.

Input:
    data/census/census_bps_timeseries.csv
        Columns (from ingest):
            geo_id
            date
            year
            month
            state_fips
            county_fips
            place_fips
            cbsa_code
            location_type
            unit_size_band   ∈ {"1","2","3_4","5plus","total"}
            units

Output:
    Inserts rows into DuckDB fact_timeseries with:
        geo_id
        metric_id
        date
        value
        source_id      = 'census_bps'
        property_type_id = 'all'
        property_type    = 'all'

NOTE: This currently loads UNIT metrics only.
TODO: Extend ingest + transform to also support BLDG metrics
      (e.g., census_bp_total_bldgs, census_bp_1_unit_bldgs, etc.).
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional, List

import duckdb
import pandas as pd

BPS_CSV = Path("data/census/census_bps_timeseries.csv")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/market.duckdb")

SOURCE_ID = "census_bps"

# Mapping from unit_size_band -> metric_id
UNIT_METRIC_MAP = {
    "total": "census_bp_total_units",
    "1": "census_bp_1_unit",
    "2": "census_bp_2_units",
    "3_4": "census_bp_3_4_units",
    "5plus": "census_bp_5plus_units",
}


def load_bps_timeseries(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise SystemExit(f"[bps → fact] missing input CSV: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)
    required_cols = {"geo_id", "date", "unit_size_band", "units"}
    missing = required_cols - set(df.columns)
    if missing:
        raise SystemExit(
            f"[bps → fact] input CSV missing required columns: {sorted(missing)}"
        )

    # Normalize
    df["geo_id"] = df["geo_id"].astype(str).str.strip()
    df["unit_size_band"] = df["unit_size_band"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Drop bad rows
    df = df[df["geo_id"] != ""]
    df = df[df["date"].notna()]
    df = df[df["unit_size_band"].isin(UNIT_METRIC_MAP.keys())]

    # Map unit_size_band -> metric_id
    df["metric_id"] = df["unit_size_band"].map(UNIT_METRIC_MAP)

    # Use "units" as value
    df["value"] = pd.to_numeric(df["units"], errors="coerce")
    df = df[df["value"].notna()]

    # Build final frame for insertion
    out = pd.DataFrame(
        {
            "geo_id": df["geo_id"],
            "metric_id": df["metric_id"],
            "date": df["date"].dt.date.astype(str),  # ISO yyyy-mm-dd
            "value": df["value"],
            "source_id": SOURCE_ID,
            "property_type_id": "all",
            "property_type": "all",
        }
    )

    print(f"[bps → fact] prepared {len(out):,} rows for insertion")
    return out


def ensure_fact_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure fact_timeseries exists with the expected schema.
    This is idempotent and matches your project summary.
    """
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
            -- composite primary key (if not already set)
            CONSTRAINT fact_timeseries_pk PRIMARY KEY (geo_id, metric_id, date, property_type_id)
        );
        """
    )


def insert_into_fact(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    # Clear existing BPS data (idempotent upsert strategy)
    con.execute(
        "DELETE FROM fact_timeseries WHERE source_id = ?",
        [SOURCE_ID],
    )

    con.register("bps_df", df)
    con.execute(
        """
        INSERT INTO fact_timeseries (
            geo_id,
            metric_id,
            date,
            value,
            source_id,
            property_type_id,
            property_type
        )
        SELECT
            geo_id,
            metric_id,
            CAST(date AS DATE),
            value,
            source_id,
            property_type_id,
            property_type
        FROM bps_df
        """
    )
    con.unregister("bps_df")

    print(f"[bps → fact] inserted {len(df):,} rows into fact_timeseries")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Load Census BPS building permit units into fact_timeseries"
    )
    parser.add_argument(
        "--csv",
        default=str(BPS_CSV),
        help=f"Input BPS timeseries CSV (default: {BPS_CSV})",
    )
    parser.add_argument(
        "--duckdb-path",
        default=DUCKDB_PATH,
        help=f"DuckDB path (default env DUCKDB_PATH or {DUCKDB_PATH})",
    )
    args = parser.parse_args(argv)

    df = load_bps_timeseries(Path(args.csv))

    con = duckdb.connect(args.duckdb_path)
    ensure_fact_table(con)
    insert_into_fact(con, df)
    con.close()

    print("[bps → fact] done.")


if __name__ == "__main__":
    main()
