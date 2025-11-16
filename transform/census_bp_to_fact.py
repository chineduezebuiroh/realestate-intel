#!/usr/bin/env python
"""
Transform BPS building permits into fact_timeseries.

Supports three measure families:
    - units   (UNIT counts)
    - bldgs   (BUILDING counts)
    - value   (DOLLAR value)

Input CSV: data/census/census_bps_timeseries.csv
Columns used:
    geo_id
    date
    measure        ∈ {"units","bldgs","value"}
    size_band      ∈ {"1","2","3_4","5plus","total"}
    value

Output: Inserts rows into fact_timeseries with metric_ids like:
    census_bp_total_units
    census_bp_1_unit
    census_bp_total_bldgs
    census_bp_total_value
    etc.
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

# (measure, size_band) -> metric_id
METRIC_MAP = {
    # Units
    ("units", "total"): "census_bp_total_units",
    ("units", "1"): "census_bp_1_unit",
    ("units", "2"): "census_bp_2_units",
    ("units", "3_4"): "census_bp_3_4_units",
    ("units", "5plus"): "census_bp_5plus_units",

    # Buildings
    ("bldgs", "total"): "census_bp_total_bldgs",
    ("bldgs", "1"): "census_bp_1_unit_bldgs",
    ("bldgs", "2"): "census_bp_2_units_bldgs",
    ("bldgs", "3_4"): "census_bp_3_4_units_bldgs",
    ("bldgs", "5plus"): "census_bp_5plus_units_bldgs",

    # Value ($)
    ("value", "total"): "census_bp_total_value",
    ("value", "1"): "census_bp_1_unit_value",
    ("value", "2"): "census_bp_2_units_value",
    ("value", "3_4"): "census_bp_3_4_units_value",
    ("value", "5plus"): "census_bp_5plus_units_value",
}


def load_bps_timeseries(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise SystemExit(f"[bps → fact] missing input CSV: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)
    required = {"geo_id", "date", "measure", "size_band", "value"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(
            f"[bps → fact] input CSV missing required columns: {sorted(missing)}"
        )

    df["geo_id"] = df["geo_id"].astype(str).str.strip()
    df["measure"] = df["measure"].astype(str).str.strip().str.lower()
    df["size_band"] = df["size_band"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df[df["geo_id"] != ""]
    df = df[df["date"].notna()]
    df = df[df["value"].notna()]

    # Map to metric_id
    df["metric_id"] = df.apply(
        lambda r: METRIC_MAP.get((r["measure"], r["size_band"])), axis=1
    )
    df = df[df["metric_id"].notna()]

    out = pd.DataFrame(
        {
            "geo_id": df["geo_id"],
            "metric_id": df["metric_id"],
            "date": df["date"].dt.date.astype(str),
            "value": df["value"],
            "source_id": SOURCE_ID,
            "property_type_id": "all",
            "property_type": "all",
        }
    )

    print(f"[bps → fact] prepared {len(out):,} rows for insertion")
    return out


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


def insert_into_fact(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    # Clear existing BPS rows (all metrics)
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
        description="Load Census BPS (units, buildings, value) into fact_timeseries"
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

