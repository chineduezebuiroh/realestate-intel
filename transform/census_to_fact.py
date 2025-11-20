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
            property_type_id VARCHAR,
            source_id        VARCHAR,
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
            "all"                   AS property_type_id,   -- ACS has no property type
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
        GROUP BY 1
        ORDER BY 1;
        """
    ).fetchdf()

    print("[census:transform] OK — Census facts loaded. Summary:")
    print(summary)


if __name__ == "__main__":
    main()
