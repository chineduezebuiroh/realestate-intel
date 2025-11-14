# transform/create_views.py
"""
Create convenience tables/views in DuckDB for analysis and dashboards.

- dim_geo:                 basic geo dimension from config/geo_manifest.csv
- dim_metric:              metric metadata from config/metric_metadata.csv
- v_fact_timeseries_enriched: fact_timeseries joined with dim_geo
- v_latest_metric_by_geo:  latest value per geo & metric_id
"""

import os
from pathlib import Path

import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
GEO_MANIFEST = Path("config/geo_manifest.csv")
METRIC_META = Path("config/metric_metadata.csv")


def main() -> None:
    if not GEO_MANIFEST.exists():
        raise SystemExit(f"[views] missing {GEO_MANIFEST}")
    if not METRIC_META.exists():
        raise SystemExit(f"[views] missing {METRIC_META}")

    con = duckdb.connect(DUCKDB_PATH)

    # 1) dim_geo
    con.execute("""
        CREATE OR REPLACE TABLE dim_geo AS
        SELECT *
        FROM read_csv_auto(?, header=True)
    """, [str(GEO_MANIFEST)])
    print("[views] dim_geo created from geo_manifest.")

    # 2) dim_metric
    con.execute("""
        CREATE OR REPLACE TABLE dim_metric AS
        SELECT *
        FROM read_csv_auto(?, header=True)
    """, [str(METRIC_META)])
    print("[views] dim_metric created from metric_metadata.")

    # 3) enriched fact view (same as before)
    con.execute("""
        CREATE OR REPLACE VIEW v_fact_timeseries_enriched AS
        SELECT
            f.geo_id,
            f.metric_id,
            f.date,
            f.value,
            g.level,
            g.census_code,
            g.include_ces,
            g.include_laus,
            g.include_census
        FROM fact_timeseries f
        LEFT JOIN dim_geo g
          ON f.geo_id = g.geo_id
    """)
    print("[views] v_fact_timeseries_enriched created.")

    # 4) latest snapshot per geo+metric
    con.execute("""
        CREATE OR REPLACE VIEW v_latest_metric_by_geo AS
        SELECT *
        FROM (
            SELECT
                v.*,
                ROW_NUMBER() OVER (
                    PARTITION BY v.geo_id, v.metric_id
                    ORDER BY v.date DESC
                ) AS rn
            FROM v_fact_timeseries_enriched v
        )
        WHERE rn = 1
    """)
    print("[views] v_latest_metric_by_geo created.")

    # quick summary
    df = con.execute("""
        SELECT
            metric_id,
            MIN(date) AS first,
            MAX(date) AS last,
            COUNT(*)  AS n
        FROM v_fact_timeseries_enriched
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print("[views] summary from v_fact_timeseries_enriched:")
    print(df)


if __name__ == "__main__":
    main()
