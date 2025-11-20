# transform/create_views.py

import os
import duckdb

def main():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    con = duckdb.connect(db_path)

    # ------------------------------------------------------------------
    # 1) v_geo_manifest as a view sitting on top of geo_manifest
    # ------------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW v_geo_manifest AS
        SELECT
            geo_id,
            level,
            census_code,
            geo_name AS name
        FROM read_csv_auto('config/geo_manifest.csv', header=True);        
        """)
    print("[views] v_geo_manifest created from geo_manifest.")


    # ------------------------------------------------------------------
    # 2) Enriched fact view
    # ------------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW v_fact_timeseries_enriched AS
        SELECT
            f.geo_id,
            g.level,
            g.census_code,
            f.metric_id,
            f.date,
            f.value,
            f.source_id,
            f.property_type_id,
            f.property_type
        FROM fact_timeseries f
        LEFT JOIN v_geo_manifest g
        USING (geo_id);
    """)
    print("[views] v_fact_timeseries_enriched created.")

    
    # ------------------------------------------------------------------
    # 3) Quick summary
    # ------------------------------------------------------------------
    print("[views] summary from v_fact_timeseries_enriched:")
    print(con.execute("""
        SELECT metric_id,
               MIN(date) AS first,
               MAX(date) AS last,
               COUNT(*)  AS n
        FROM v_fact_timeseries_enriched
        GROUP BY 1
        ORDER BY 1
    """).fetchdf())

    con.close()

if __name__ == "__main__":
    main()
