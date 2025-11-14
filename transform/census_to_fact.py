# transform/census_to_fact.py

"""
Load ACS 5-year Census data from CSV into fact_timeseries.

Input:
  data/census_acs5_timeseries.csv with columns:
    geo_id, level, census_code, year, date, metric_id, value

Behavior:
  - Ensures fact_timeseries exists.
  - Deletes existing rows where metric_id LIKE 'census_%'.
  - Inserts new census rows (geo_id, metric_id, date, value).
  - Prints a summary of what was loaded.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb


DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
CENSUS_CSV = Path("data/census_acs5_timeseries.csv")


def main() -> None:
    if not CENSUS_CSV.exists():
        raise SystemExit(f"[census:transform] missing {CENSUS_CSV}")

    con = duckdb.connect(DB_PATH)

    # Make sure fact_timeseries exists (minimal schema).
    # If you've already created this table elsewhere with the same columns,
    # this is a no-op.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_timeseries (
            geo_id    VARCHAR,
            metric_id VARCHAR,
            date      DATE,
            value     DOUBLE
        )
        """
    )

    # Clear out prior Census data so this transform is idempotent.
    con.execute(
        """
        DELETE FROM fact_timeseries
        WHERE metric_id LIKE 'census_%'
        """
    )

    # Insert from the CSV directly.
    con.execute(
        """
        INSERT INTO fact_timeseries (geo_id, metric_id, date, value)
        SELECT
            geo_id,
            metric_id,
            CAST(date AS DATE) AS date,
            value
        FROM read_csv_auto(?, header=True)
        WHERE value IS NOT NULL
        """,
        [str(CENSUS_CSV)],
    )

    # Summary
    summary = con.execute(
        """
        SELECT
            metric_id,
            MIN(date) AS first,
            MAX(date) AS last,
            COUNT(*)  AS rows
        FROM fact_timeseries
        WHERE metric_id LIKE 'census_%'
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()

    print("[census:transform] OK — Census facts loaded. Summary:")
    print(summary)


if __name__ == "__main__":
    main()
