from __future__ import annotations
# sources/redfin/validate.py

import os
import duckdb

DB_PATH = os.getenv("DUCKDB_PATH")
if not DB_PATH:
    raise SystemExit("[redfin:validate] DUCKDB_PATH not set")


EXPECTED_METRICS = {
    "average_sale_to_list_ratio",
    "homes_sold",
    "inventory",
    "median_days_on_market_days",
    "median_new_listing_price",
    "median_sale_price_nsa",
    "median_sale_price_per_sqft",
    "months_of_supply",
    "new_listings",
    "pending_sales",
    "percent_off_market_in_two_weeks",
    "share_sold_above_original_list",
}


def main() -> int:
    con = duckdb.connect(DB_PATH)

    bad = con.execute("""
        SELECT COUNT(*)
        FROM fact_timeseries
        WHERE source_id='redfin'
          AND (geo_id IS NULL OR metric_id IS NULL OR date IS NULL OR property_type_id IS NULL OR value IS NULL)
    """).fetchone()[0]
    if bad:
        raise SystemExit(f"[redfin:validate] FAIL null critical fields: {bad}")

    dupes = con.execute("""
        SELECT COUNT(*)
        FROM (
          SELECT geo_id, metric_id, date, property_type_id, COUNT(*) n
          FROM fact_timeseries
          WHERE source_id='redfin'
          GROUP BY 1,2,3,4
          HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if dupes:
        raise SystemExit(f"[redfin:validate] FAIL duplicate fact keys: {dupes}")

    metrics = {
        r[0] for r in con.execute("""
            SELECT DISTINCT metric_id
            FROM fact_timeseries
            WHERE source_id='redfin'
        """).fetchall()
    }

    unexpected = sorted(metrics - EXPECTED_METRICS)
    missing = sorted(EXPECTED_METRICS - metrics)

    if unexpected:
        raise SystemExit(f"[redfin:validate] FAIL unexpected metrics: {unexpected}")
    if missing:
        raise SystemExit(f"[redfin:validate] FAIL missing expected metrics: {missing}")

    print(con.execute("""
        SELECT metric_id, COUNT(*) rows, MIN(date) first, MAX(date) last
        FROM fact_timeseries
        WHERE source_id='redfin'
        GROUP BY 1
        ORDER BY 1
    """).fetchdf())

    print(con.execute("""
        SELECT COUNT(DISTINCT geo_id) geos
        FROM fact_timeseries
        WHERE source_id='redfin'
    """).fetchdf())

    con.close()
    print("[redfin:validate] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
