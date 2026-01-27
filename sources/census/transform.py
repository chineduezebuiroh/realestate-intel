from __future__ import annotations
# sources/census/transform.py

import os
from pathlib import Path
import duckdb
import pandas as pd

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

# Raw ingest output (new contract). We'll write to this path from the new ingest runner.
CENSUS_RAW = Path("data/staging/census_acs5_raw.csv")

# Choose a stable source_id. Keep it specific so permits can be its own source_id later.
SOURCE_ID = "census_acs5"

# Deterministic mapping: Census variable_code -> metric_id
# (Keep small for now; expand later via config file if you want.)
METRIC_BY_VAR = {
    "B01003_001E": "census_pop_total",
    "B19013_001E": "census_median_household_income",
}


REQUIRED_FACT_COLS = {"geo_id", "metric_id", "date", "value", "source_id", "property_type_id"}


def _assert_table_has_columns(con: duckdb.DuckDBPyConnection, table: str, cols: set[str]) -> None:
    info = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
    if info.empty:
        raise SystemExit(f"[census:transform] required table missing: {table}")
    existing = set(info["name"].astype(str))
    missing = cols - existing
    if missing:
        raise SystemExit(f"[census:transform] table {table} missing columns: {sorted(missing)}")


def main() -> None:
    if not CENSUS_RAW.exists():
        raise SystemExit(f"[census:transform] missing raw ingest file: {CENSUS_RAW}")

    con = duckdb.connect(DB_PATH)

    # Hard requirement: schema should be created by your migrations, not by ad-hoc transforms.
    _assert_table_has_columns(con, "fact_timeseries", REQUIRED_FACT_COLS)

    # Read raw ingest output
    raw = pd.read_csv(CENSUS_RAW, dtype={"geo_id": "string", "variable_code": "string", "date": "string"})
    needed = {"geo_id", "date", "variable_code", "value", "dataset", "vintage"}
    missing = needed - set(raw.columns)
    if missing:
        raise SystemExit(f"[census:transform] raw file missing columns: {sorted(missing)}")

    raw["variable_code"] = raw["variable_code"].astype(str).str.strip()
    raw["metric_id"] = raw["variable_code"].map(METRIC_BY_VAR)

    # Drop unknown variables loudly (don’t silently ingest garbage)
    unknown = raw[raw["metric_id"].isna()]["variable_code"].dropna().unique().tolist()
    if unknown:
        raise SystemExit(f"[census:transform] unknown variable_code(s) encountered: {unknown}")

    # Prepare fact rows
    df = raw.copy()
    df["source_id"] = SOURCE_ID
    df["property_type_id"] = "-1"
    df["property_type"] = None

    # Ensure date parses and is year-end (ACS 5y is annual)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Idempotent replace for this source_id
    con.execute("DELETE FROM fact_timeseries WHERE source_id = ?", [SOURCE_ID])

    # Insert
    con.register("census_stage", df)
    con.execute(
        """
        INSERT INTO fact_timeseries (geo_id, metric_id, date, value, source_id, property_type_id, property_type)
        SELECT
            geo_id,
            metric_id,
            date,
            CAST(value AS DOUBLE) AS value,
            source_id,
            property_type_id,
            property_type
        FROM census_stage
        WHERE value IS NOT NULL
        """
    )

    # Summary
    summary = con.execute(
        """
        SELECT
            geo_id, metric_id, source_id,
            MIN(date) AS first,
            MAX(date) AS last,
            COUNT(*)  AS rows
        FROM fact_timeseries
        WHERE source_id = ?
        GROUP BY 1, 2, 3
        ORDER BY 1, 2
        """,
        [SOURCE_ID],
    ).fetchdf()

    print("[census:transform] OK — Census facts loaded. Summary:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
