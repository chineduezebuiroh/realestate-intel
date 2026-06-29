from __future__ import annotations
# sources/census_bps_provisional/transform.py

import argparse
from pathlib import Path
from typing import Optional, List

import duckdb
import pandas as pd

from core.db import connect

BPS_CSV = Path("data/census/census_bps_provisional_timeseries.csv")
SOURCE_ID = "census_bps_provisional"

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

METRIC_MAP = {
    ("units", "total"): "census_bp_total_units",
    ("units", "1"): "census_bp_1_unit",
    ("units", "2"): "census_bp_2_units",
    ("units", "3_4"): "census_bp_3_4_units",
    ("units", "5plus"): "census_bp_5plus_units",

    ("bldgs", "total"): "census_bp_total_bldgs",
    ("bldgs", "1"): "census_bp_1_unit_bldgs",
    ("bldgs", "2"): "census_bp_2_units_bldgs",
    ("bldgs", "3_4"): "census_bp_3_4_units_bldgs",
    ("bldgs", "5plus"): "census_bp_5plus_units_bldgs",

    ("value", "total"): "census_bp_total_value",
    ("value", "1"): "census_bp_1_unit_value",
    ("value", "2"): "census_bp_2_units_value",
    ("value", "3_4"): "census_bp_3_4_units_value",
    ("value", "5plus"): "census_bp_5plus_units_value",
}


def ensure_fact_table(con: duckdb.DuckDBPyConnection) -> None:
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


def ensure_dims(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_source(
      source_id TEXT PRIMARY KEY,
      name TEXT,
      url TEXT,
      cadence TEXT,
      license TEXT
    );
    
    INSERT INTO dim_source(source_id, name, url, cadence, license)
    SELECT 'census_bps_provisional',
           'Census Building Permits Survey Provisional',
           'https://www.census.gov/construction/bps/',
           'monthly',
           'public'
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_source WHERE source_id = 'census_bps_provisional'
    );
    """)
    
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_metric(
      metric_id TEXT PRIMARY KEY, name TEXT, frequency TEXT, unit TEXT, category TEXT
    );
    """)


    metrics = [
        ("bps_units_1", "Building Permits Units - 1 Unit", "units", "housing_supply"),
        ("bps_units_2", "Building Permits Units - 2 Units", "units", "housing_supply"),
        ("bps_units_3_4", "Building Permits Units - 3-4 Units", "units", "housing_supply"),
        ("bps_units_5plus", "Building Permits Units - 5+ Units", "units", "housing_supply"),
        ("bps_units_total", "Building Permits Units - Total", "units", "housing_supply"),
    
        ("bps_bldgs_1", "Building Permits Buildings - 1 Unit", "buildings", "housing_supply"),
        ("bps_bldgs_2", "Building Permits Buildings - 2 Units", "buildings", "housing_supply"),
        ("bps_bldgs_3_4", "Building Permits Buildings - 3-4 Units", "buildings", "housing_supply"),
        ("bps_bldgs_5plus", "Building Permits Buildings - 5+ Units", "buildings", "housing_supply"),
        ("bps_bldgs_total", "Building Permits Buildings - Total", "buildings", "housing_supply"),
    
        ("bps_value_1", "Building Permits Value - 1 Unit", "thousand_dollars", "housing_supply"),
        ("bps_value_2", "Building Permits Value - 2 Units", "thousand_dollars", "housing_supply"),
        ("bps_value_3_4", "Building Permits Value - 3-4 Units", "thousand_dollars", "housing_supply"),
        ("bps_value_5plus", "Building Permits Value - 5+ Units", "thousand_dollars", "housing_supply"),
        ("bps_value_total", "Building Permits Value - Total", "thousand_dollars", "housing_supply"),
    ]
    
    for metric_id, name, unit, category in metrics:
        con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT ?, ?, 'monthly', ?, ?
        WHERE NOT EXISTS (
          SELECT 1 FROM dim_metric WHERE metric_id = ?
        )
        """, [metric_id, name, unit, category, metric_id])


def load_timeseries(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"[bps:prov→fact] missing {path}")

    df = pd.read_csv(path, low_memory=False)
    required = {"geo_id","date","measure","size_band","value"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"[bps:prov→fact] missing columns: {sorted(missing)}")

    df["geo_id"] = df["geo_id"].astype(str).str.strip()
    df["measure"] = df["measure"].astype(str).str.strip().str.lower()
    df["size_band"] = df["size_band"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df[(df["geo_id"] != "") & df["date"].notna() & df["value"].notna()].copy()

    df["metric_id"] = df.apply(lambda r: METRIC_MAP.get((r["measure"], r["size_band"])), axis=1)
    df = df[df["metric_id"].notna()].copy()

    out = pd.DataFrame({
        "geo_id": df["geo_id"],
        "metric_id": df["metric_id"],
        "date": df["date"],
        "property_type_id": "all",
        "value": df["value"],
        "source_id": SOURCE_ID,
        "property_type": None,
    })

    # Deterministic dedupe: if provisional repeats keys, keep last after sorting.
    pk = ["geo_id","metric_id","date","property_type_id"]
    before = len(out)
    out = out.sort_values(pk).drop_duplicates(pk, keep="last")
    dropped = before - len(out)
    if dropped:
        print(f"[bps:prov→fact] dropped {dropped} duplicate staging rows on PK")

    print(f"[bps:prov→fact] prepared {len(out):,} rows")
    return out


def insert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    # wipe existing provisional slice
    con.execute("DELETE FROM fact_timeseries WHERE source_id = ?", [SOURCE_ID])

    con.register("stg", df)
    con.execute("""
    INSERT INTO fact_timeseries(geo_id,metric_id,date,property_type_id,value,source_id,property_type)
    SELECT geo_id,metric_id,CAST(date AS DATE),property_type_id,CAST(value AS DOUBLE),source_id,property_type
    FROM stg
    """)
    con.unregister("stg")
    print(f"[bps:prov→fact] inserted {len(df):,} rows")


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=str(BPS_CSV))
    args = p.parse_args(argv)

    df = load_timeseries(Path(args.csv))

    con = connect()
    ensure_fact_table(con)
    ensure_dims(con)
    insert(con, df)
    con.close()

    print("[bps:prov→fact] done.")


if __name__ == "__main__":
    main()

