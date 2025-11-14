# transform/create_views.py
"""
Create convenience tables/views in DuckDB for analysis and dashboards.

- dim_geo:   basic geo dimension from config/geo_manifest.csv
- v_fact_timeseries_enriched: fact_timeseries joined with dim_geo
"""

import os
from pathlib import Path

import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
GEO_MANIFEST = Path("config/geo_manifest.csv")


def main() -> None:
    if not GEO_MANIFEST.exists():
        raise SystemExit(f"[views] missing {GEO_MANIFEST}")

    con = duckdb.connect(DUCKDB_PATH)

    # 1) dim_geo from geo_manifest
    # We just materialize the CSV as-is; you can refine columns later.
    con.execute("""
        CREATE OR REPLACE TABLE dim_geo AS
        SELECT *
        FROM read_csv_auto(?, header=True)
    """, [str(GEO_MANIFEST)])

    print("[views] dim_geo created from geo_manifest.")

    # 2) Enriched fact view: join facts with geo attributes
    # This keeps the fact table raw, but gives you a nicer surface to query.
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
            -- add more geo columns here if/when you have them
        FROM fact_timeseries f
        LEFT JOIN dim_geo g
          ON f.geo_id = g.geo_id
    """)

    print("[views] v_fact_timeseries_enriched created.")

    # Optional: quick summary printout
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
