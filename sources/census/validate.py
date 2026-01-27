from __future__ import annotations
# sources/census/validate.py

import os
import duckdb

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
SOURCE_ID = "census_acs5"


def main() -> None:
    con = duckdb.connect(DB_PATH)

    # Basic sanity checks
    bad = con.execute(
        """
        SELECT COUNT(*) AS n_bad
        FROM fact_timeseries
        WHERE source_id = ?
          AND (geo_id IS NULL OR metric_id IS NULL OR date IS NULL OR value IS NULL)
        """,
        [SOURCE_ID],
    ).fetchone()[0]
    if bad:
        raise SystemExit(f"[census:validate] FAIL: found {bad} rows with null critical fields")

    # ACS annual series should be year-end dates (Dec 31)
    not_year_end = con.execute(
        """
        SELECT COUNT(*) AS n_not_year_end
        FROM fact_timeseries
        WHERE source_id = ?
          AND (EXTRACT(month FROM date) != 12 OR EXTRACT(day FROM date) != 31)
        """,
        [SOURCE_ID],
    ).fetchone()[0]
    if not_year_end:
        raise SystemExit(f"[census:validate] FAIL: {not_year_end} rows are not year-end (12/31)")

    summary = con.execute(
        """
        SELECT
          metric_id,
          COUNT(*) AS n,
          MIN(date) AS first,
          MAX(date) AS last
        FROM fact_timeseries
        WHERE source_id = ?
        GROUP BY 1
        ORDER BY 1
        """,
        [SOURCE_ID],
    ).fetchdf()

    print("[census:validate] OK — summary:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
