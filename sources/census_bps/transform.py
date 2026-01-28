from __future__ import annotations
# sources/census_bps/transform.py

import argparse
import os
from pathlib import Path
from typing import Optional, List
from core.db import connect

import duckdb
import pandas as pd

BPS_CSV = Path("data/census/census_bps_timeseries.csv")

SOURCE_ID = "census_bps"


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



DIM_METRICS = [
    ("census_bp_total_units", "Building Permits: Total Units", "monthly", "units", "census"),
    ("census_bp_1_unit", "Building Permits: 1 Unit", "monthly", "units", "census"),
    ("census_bp_2_units", "Building Permits: 2 Units", "monthly", "units", "census"),
    ("census_bp_3_4_units", "Building Permits: 3–4 Units", "monthly", "units", "census"),
    ("census_bp_5plus_units", "Building Permits: 5+ Units", "monthly", "units", "census"),
    ("census_bp_total_bldgs", "Building Permits: Total Buildings", "monthly", "bldgs", "census"),
    ("census_bp_1_unit_bldgs", "Building Permits: 1 Unit Buildings", "monthly", "bldgs", "census"),
    ("census_bp_2_units_bldgs", "Building Permits: 2 Unit Buildings", "monthly", "bldgs", "census"),
    ("census_bp_3_4_units_bldgs", "Building Permits: 3–4 Unit Buildings", "monthly", "bldgs", "census"),
    ("census_bp_5plus_units_bldgs", "Building Permits: 5+ Unit Buildings", "monthly", "bldgs", "census"),
    ("census_bp_total_value", "Building Permits: Total Value", "monthly", "usd", "census"),
    ("census_bp_1_unit_value", "Building Permits: 1 Unit Value", "monthly", "usd", "census"),
    ("census_bp_2_units_value", "Building Permits: 2 Units Value", "monthly", "usd", "census"),
    ("census_bp_3_4_units_value", "Building Permits: 3–4 Units Value", "monthly", "usd", "census"),
    ("census_bp_5plus_units_value", "Building Permits: 5+ Units Value", "monthly", "usd", "census"),
]


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


def ensure_dims(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_source(
          source_id TEXT PRIMARY KEY, name TEXT, url TEXT, cadence TEXT, license TEXT
        );
    """)
    
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT 'census_bps', 'Census Building Permits Survey', 'https://www.census.gov/construction/bps/', 'monthly', 'public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='census_bps');
    """)
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

    missing = (
        df.loc[df["metric_id"].isna(), ["measure", "size_band"]]
          .drop_duplicates()
          .sort_values(["measure","size_band"])
    )
    if not missing.empty:
        raise SystemExit(
            "[bps → fact] unmapped (measure,size_band) values:\n"
            + missing.to_string(index=False)
        )

    out = pd.DataFrame(
        {
            "geo_id": df["geo_id"],
            "metric_id": df["metric_id"],
            "date": df["date"].dt.date,
            "value": df["value"],
            "source_id": SOURCE_ID,
            "property_type_id": "all",
            "property_type": None,
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
    if df.empty:
        print("[bps:transform] no rows to insert")
        return

    # Normalize key fields to avoid whitespace variants creating “fake” uniqueness
    for c in ["geo_id", "metric_id", "property_type_id", "source_id"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Ensure date is a real date (not string)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"].notna()].copy()

    # Deterministic dedupe on the fact PK
    pk = ["geo_id", "metric_id", "date", "property_type_id"]
    before = len(df)
    df = (
        df.sort_values(pk)
          .drop_duplicates(subset=pk, keep="last")
    )
    dropped = before - len(df)
    if dropped:
        print(f"[bps:transform] dropped {dropped} duplicate staging rows on PK")

    con.register("bps_stage", df)

    # KEY CHANGE: delete existing rows for the same PK keys (regardless of source_id)
    con.execute("""
        DELETE FROM fact_timeseries f
        WHERE EXISTS (
            SELECT 1
            FROM bps_stage s
            WHERE s.geo_id = f.geo_id
              AND s.metric_id = f.metric_id
              AND CAST(s.date AS DATE) = f.date
              AND s.property_type_id = f.property_type_id
        )
    """)
    print("[bps:transform] cleared existing rows for staged keys (any source_id)")

    con.execute("""
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
        FROM bps_stage
    """)

    con.unregister("bps_stage")
    print(f"[bps:transform] inserted {len(df):,} rows into fact_timeseries")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Load Census BPS (units, buildings, value) into fact_timeseries"
    )
    parser.add_argument(
        "--csv",
        default=str(BPS_CSV),
        help=f"Input BPS timeseries CSV (default: {BPS_CSV})",
    )
    args = parser.parse_args(argv)

    df = load_bps_timeseries(Path(args.csv))

    con = connect()
    ensure_fact_table(con)
    ensure_dims(con)
    insert_into_fact(con, df)
    con.close()

    print("[bps → fact] done.")


if __name__ == "__main__":
    main()

