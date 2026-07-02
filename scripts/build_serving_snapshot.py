from __future__ import annotations
# scripts/build_serving_snapshot.py

import duckdb

from core.config import (
    FULL_DB_PATH,
    SERVING_DB_PATH,
    SERVING_START_DATE
)


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

"""
SOURCE_SERVING_START_DATES = {
    "redfin": "2012-01-01",
    "census_acs5": "2005-12-31",
    "bea_gdp_ann": "2001-12-31",
    "bea_gdp_qtr": "2005-03-31",
    "ces": "2015-01-01",
    "laus": "2015-01-01",
    "fred_macro": "2015-01-01",
    "fred_unemp": "2015-01-01",
    "census_bps": "2015-01-01",
    "census_nrc_fred": "2015-01-01",
}
"""
SOURCE_HISTORY_POLICY = {
    # keep all available history
    "redfin": None,
    "census_acs1": None,
    "census_acs5": None,
    "bea_gdp_ann": None,
    "bea_gdp_qtr": None,
    "census_nrc_fred": None,

    # cap long monthly / routine macro series
    "ces": {"years": 20},
    "laus": {"years": 20},
    "fred_macro": {"years": 20},
    "fred_unemp": {"years": 20},
    "census_bps": {"years": 20},
    "census_bps_provisional": {"years": 20},
}

DEFAULT_HISTORY_POLICY = None

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
    exists = bool(
        serving.execute(
            """
            SELECT COUNT(*)
            FROM duckdb_tables()
            WHERE database_name = ?
              AND schema_name = 'main'
              AND table_name = ?
            """,
            [full_alias, table_name],
        ).fetchone()[0]
    )

    if not exists:
        print(f"[snapshot][warn] source table missing, skipping: {table_name}")
        return

    serving.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {full_alias}.{table_name}")
    rows = serving.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"[snapshot] copied {table_name}: {rows:,} rows")


def create_fact_timeseries(serving: duckdb.DuckDBPyConnection, full_alias: str) -> None:
    if not table_exists(serving, full_alias, "fact_timeseries"):
        raise SystemExit(f"[snapshot][fatal] fact_timeseries missing in {FULL_DB_PATH}")

    serving.execute("""
        CREATE TABLE fact_timeseries AS
        SELECT *
        FROM full_db.fact_timeseries
        WHERE 1 = 0
    """)

    source_ids = [
        r[0]
        for r in serving.execute(f"""
            SELECT DISTINCT source_id
            FROM {full_alias}.fact_timeseries
            WHERE source_id IS NOT NULL
            ORDER BY source_id
        """).fetchall()
    ]

    for source_id in source_ids:
        policy = SOURCE_HISTORY_POLICY.get(source_id, DEFAULT_HISTORY_POLICY)

        if policy is None:
            serving.execute(f"""
                INSERT INTO fact_timeseries
                SELECT *
                FROM {full_alias}.fact_timeseries
                WHERE source_id = ?
            """, [source_id])
        else:
            years = int(policy["years"])
            serving.execute(f"""
                INSERT INTO fact_timeseries
                SELECT *
                FROM {full_alias}.fact_timeseries
                WHERE source_id = ?
                  AND date >= (
                      SELECT MAX(date) - INTERVAL '{years} years'
                      FROM {full_alias}.fact_timeseries
                      WHERE source_id = ?
                  )
            """, [source_id, source_id])

        rows, first, last = serving.execute("""
            SELECT COUNT(*), MIN(date), MAX(date)
            FROM fact_timeseries
            WHERE source_id = ?
        """, [source_id]).fetchone()

        print(f"[snapshot] copied {source_id}: {rows:,} rows ({first} → {last}) policy={policy}")


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
        print(f"[snapshot][warn] found {legacy_count:,} rows with known legacy geo IDs")
        print(
            serving.execute(
                f"""
                SELECT source_id, geo_id, COUNT(*) AS rows
                FROM fact_timeseries
                WHERE geo_id IN ({",".join(["?"] * len(LEGACY_GEO_IDS))})
                GROUP BY 1,2
                ORDER BY rows DESC
                """,
                LEGACY_GEO_IDS,
            ).fetchdf().to_string(index=False)
        )
    else:
        print("[snapshot] legacy geo ID check passed")

    for view_name in KNOWN_VIEWS:
        if view_exists(serving, view_name):
            view_rows = serving.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            print(f"[snapshot] view validation: {view_name} rows={view_rows:,}")
        else:
            print(f"[snapshot][warn] known view missing from serving snapshot: {view_name}")


def main() -> int:
    if not FULL_DB_PATH.exists():
        raise SystemExit(f"[snapshot][fatal] full DB not found: {FULL_DB_PATH}")

    SERVING_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if SERVING_DB_PATH.exists():
        SERVING_DB_PATH.unlink()

    print(f"[snapshot] full DB: {FULL_DB_PATH}")
    print(f"[snapshot] serving DB: {SERVING_DB_PATH}")
    print(f"[snapshot] serving start date: {SERVING_START_DATE}")

    serving = duckdb.connect(str(SERVING_DB_PATH))
    serving.execute(f"ATTACH '{FULL_DB_PATH}' AS full_db")

    for table_name in DIM_TABLES:
        copy_table_if_exists(serving, "full_db", table_name)

    create_fact_timeseries(serving, "full_db")
    create_bps_view(serving)
    validate_snapshot(serving)

    serving.close()

    size_mb = SERVING_DB_PATH.stat().st_size / (1024 * 1024)
    print(f"[snapshot] done: {SERVING_DB_PATH} ({size_mb:,.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
