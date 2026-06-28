from __future__ import annotations
# scripts/build_serving_snapshot.py

import os
from pathlib import Path

import duckdb

from core.config import (
    MARKET_DB_PATH,
    MARKET_SERVING_DB_PATH,
    SERVING_START_DATE,
)

"""
FULL_DB_PATH = Path(os.getenv("FULL_DUCKDB_PATH", os.getenv("DUCKDB_PATH", "data/market.duckdb")))
SERVING_DB_PATH = Path(os.getenv("SERVING_DUCKDB_PATH", "data/market_serving.duckdb"))
SERVING_START_DATE = os.getenv("SERVING_START_DATE", "2015-01-01")
"""

DIM_TABLES = [
    "dim_source",
    "dim_metric",
]

KNOWN_VIEWS = [
    "fact_timeseries_bps",
]

LEGACY_GEO_IDS = [
    "dc_state",
    "md_state",
    "va_state",
    "us_nation",
    "dc_msa",
    "baltimore_msa",
]


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def view_exists(con: duckdb.DuckDBPyConnection, view_name: str) -> bool:
    return bool(
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.views
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [view_name],
        ).fetchone()[0]
    )


def copy_table_if_exists(
    serving: duckdb.DuckDBPyConnection,
    full_alias: str,
    table_name: str,
) -> None:
    exists = serving.execute(
        f"""
        SELECT COUNT(*)
        FROM {full_alias}.information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()[0]

    if not exists:
        print(f"[snapshot][warn] source table missing, skipping: {table_name}")
        return

    serving.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {full_alias}.{table_name}")
    rows = serving.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"[snapshot] copied {table_name}: {rows:,} rows")


def create_fact_timeseries(serving: duckdb.DuckDBPyConnection, full_alias: str) -> None:
    exists = serving.execute(
        f"""
        SELECT COUNT(*)
        FROM {full_alias}.information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = 'fact_timeseries'
        """
    ).fetchone()[0]

    if not exists:
        raise SystemExit(f"[snapshot][fatal] fact_timeseries missing in {MARKET_DB_PATH}")

    serving.execute(
        f"""
        CREATE TABLE fact_timeseries AS
        SELECT *
        FROM {full_alias}.fact_timeseries
        WHERE date >= DATE ?
        """,
        [SERVING_START_DATE],
    )

    rows = serving.execute("SELECT COUNT(*) FROM fact_timeseries").fetchone()[0]
    first, last = serving.execute("SELECT MIN(date), MAX(date) FROM fact_timeseries").fetchone()
    print(f"[snapshot] copied fact_timeseries: {rows:,} rows ({first} → {last})")


def create_bps_view(serving: duckdb.DuckDBPyConnection) -> None:
    source_counts = dict(
        serving.execute(
            """
            SELECT source_id, COUNT(*) AS rows
            FROM fact_timeseries
            WHERE source_id IN ('census_bps', 'census_bps_provisional')
            GROUP BY 1
            """
        ).fetchall()
    )

    if "census_bps" not in source_counts:
        print("[snapshot][warn] skipping fact_timeseries_bps view: census_bps missing")
        return

    serving.execute(
        """
        CREATE OR REPLACE VIEW fact_timeseries_bps AS
        SELECT *
        FROM fact_timeseries
        WHERE source_id = 'census_bps'

        UNION ALL

        SELECT p.*
        FROM fact_timeseries p
        WHERE p.source_id = 'census_bps_provisional'
          AND NOT EXISTS (
              SELECT 1
              FROM fact_timeseries c
              WHERE c.source_id = 'census_bps'
                AND c.geo_id = p.geo_id
                AND c.metric_id = p.metric_id
                AND c.date = p.date
                AND c.property_type_id = p.property_type_id
          )
        """
    )

    rows = serving.execute("SELECT COUNT(*) FROM fact_timeseries_bps").fetchone()[0]
    first, last = serving.execute("SELECT MIN(date), MAX(date) FROM fact_timeseries_bps").fetchone()
    print(f"[snapshot] created fact_timeseries_bps: {rows:,} rows ({first} → {last})")


def validate_snapshot(serving: duckdb.DuckDBPyConnection) -> None:
    if not table_exists(serving, "fact_timeseries"):
        raise SystemExit("[snapshot][fatal] serving snapshot missing fact_timeseries")

    rows = serving.execute("SELECT COUNT(*) FROM fact_timeseries").fetchone()[0]
    if rows == 0:
        raise SystemExit("[snapshot][fatal] serving fact_timeseries has 0 rows")

    print("[snapshot] source summary:")
    print(
        serving.execute(
            """
            SELECT
              source_id,
              COUNT(*) AS rows,
              MIN(date) AS first_date,
              MAX(date) AS last_date
            FROM fact_timeseries
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchdf().to_string(index=False)
    )

    legacy_count = serving.execute(
        f"""
        SELECT COUNT(*)
        FROM fact_timeseries
        WHERE geo_id IN ({",".join(["?"] * len(LEGACY_GEO_IDS))})
        """,
        LEGACY_GEO_IDS,
    ).fetchone()[0]

    if legacy_count:
        raise SystemExit(f"[snapshot][fatal] found {legacy_count:,} rows with legacy geo IDs")

    print("[snapshot] legacy geo ID check passed")

    for view_name in KNOWN_VIEWS:
        if view_exists(serving, view_name):
            view_rows = serving.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            print(f"[snapshot] view validation: {view_name} rows={view_rows:,}")
        else:
            print(f"[snapshot][warn] known view missing from serving snapshot: {view_name}")


def main() -> int:
    if not MARKET_DB_PATH.exists():
        raise SystemExit(f"[snapshot][fatal] full DB not found: {MARKET_DB_PATH}")

    MARKET_SERVING_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if MARKET_SERVING_DB_PATH.exists():
        MARKET_SERVING_DB_PATH.unlink()

    print(f"[snapshot] full DB: {MARKET_DB_PATH}")
    print(f"[snapshot] serving DB: {MARKET_SERVING_DB_PATH}")
    print(f"[snapshot] serving start date: {SERVING_START_DATE}")

    serving = duckdb.connect(str(MARKET_SERVING_DB_PATH))
    serving.execute(f"ATTACH '{MARKET_DB_PATH}' AS full_db")

    for table_name in DIM_TABLES:
        copy_table_if_exists(serving, "full_db", table_name)

    create_fact_timeseries(serving, "full_db")
    create_bps_view(serving)
    validate_snapshot(serving)

    serving.close()

    size_mb = MARKET_SERVING_DB_PATH.stat().st_size / (1024 * 1024)
    print(f"[snapshot] done: {MARKET_SERVING_DB_PATH} ({size_mb:,.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
