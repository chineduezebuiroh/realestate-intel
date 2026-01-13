# transform/census_to_fact.py

"""
Load ACS 5-year Census data from CSV into fact_timeseries.

Input:
  data/census_acs5_timeseries.csv with columns:
    geo_id, level, census_code, year, date, metric_id, value

Behavior:
  - Ensures fact_timeseries exists with the full schema.
  - Deletes existing rows where source_id = 'census_acs'.
  - Inserts new census rows.
  - Prints a summary of what was loaded.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
CENSUS_CSV = Path("data/census_acs5_timeseries.csv")

SOURCE_ID = "census_acs"


def main() -> None:
    if not CENSUS_CSV.exists():
        raise SystemExit(f"[census:transform] missing {CENSUS_CSV}")

    con = duckdb.connect(DB_PATH)

    # Make sure fact_timeseries exists with the full schema used elsewhere.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_timeseries (
            geo_id           VARCHAR,
            metric_id        VARCHAR,
            date             DATE,
            value            DOUBLE,
            source_id        VARCHAR,
            property_type_id VARCHAR,
            property_type    VARCHAR
        );
        """
    )

  
    # Clear out prior Census data so this transform is idempotent.
    con.execute(
        """
        DELETE FROM fact_timeseries
        WHERE source_id = 'census_acs'
        """
    )


    # Ensure dim_source exists
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT 'census_acs', 'Census ACS 5-year', 'https://www.census.gov/programs-surveys/acs', 'annual', 'public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='census_acs');
    """)
    
    # Ensure dim_metric entries exist for all census metrics in the CSV
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_metric(
            metric_id TEXT PRIMARY KEY,
            name TEXT,
            frequency TEXT,
            unit TEXT,
            category TEXT
        );
    """)
    
    # Pull distinct metric_id from the CSV and upsert with stable metadata
    con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT
          metric_id,
          metric_id AS name,
          'annual' AS frequency,
          NULL AS unit,
          'census' AS category
        FROM (
          SELECT DISTINCT metric_id
          FROM read_csv_auto(?, header=True)
          WHERE metric_id IS NOT NULL AND TRIM(metric_id) <> ''
        )
        ON CONFLICT(metric_id) DO UPDATE SET
          category=excluded.category,
          frequency=excluded.frequency
    """, [str(CENSUS_CSV)])

  

    # Insert from the CSV directly.
    con.execute(
        """
        INSERT INTO fact_timeseries (geo_id, metric_id, date, value, source_id, property_type_id, property_type)
        SELECT
            geo_id,
            metric_id,
            CAST(date AS DATE)      AS date,
            CAST(value AS DOUBLE)   AS value,
            ?                       AS source_id,          -- bind Python SOURCE_ID
            'all'                   AS property_type_id,   -- ACS has no property type
            NULL                    AS property_type
        FROM read_csv_auto(?, header=True)
        WHERE value IS NOT NULL;
        """,
        [SOURCE_ID, str(CENSUS_CSV)],
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
        WHERE source_id = 'census_acs'
        GROUP BY 1, 2, 3
        ORDER BY 1, 2;
        """
    ).fetchdf()

    print("[census:transform] OK — Census facts loaded. Summary:")
    print(summary)


if __name__ == "__main__":
    main()
