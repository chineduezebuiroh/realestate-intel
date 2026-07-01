# sources/census/validate.py
import os
import duckdb

DB_PATH = os.getenv("DUCKDB_PATH")
if not DB_PATH:
    raise SystemExit("[census:validate] DUCKDB_PATH not set (refusing to run against default db)")

SOURCE_IDS = ["census_acs5", "census_acs1"]


def main():
    con = duckdb.connect(DB_PATH)

    bad = con.execute("""
      SELECT COUNT(*) FROM fact_timeseries
      WHERE source_id IN ('census_acs5','census_acs1')
        AND (geo_id IS NULL OR metric_id IS NULL OR date IS NULL OR value IS NULL OR property_type_id IS NULL)
    """).fetchone()[0]
    if bad:
        raise SystemExit(f"[census:validate] FAIL: {bad} rows have null critical fields")

    not_year_end = con.execute("""
      SELECT COUNT(*) FROM fact_timeseries
      WHERE source_id IN ('census_acs5','census_acs1')
        AND (EXTRACT(month FROM date) != 12 OR EXTRACT(day FROM date) != 31)
    """).fetchone()[0]
    if not_year_end:
        raise SystemExit(f"[census:validate] FAIL: {not_year_end} rows are not 12/31 year-end")

    print(con.execute("""
      SELECT metric_id, COUNT(*) AS n, MIN(date) AS first, MAX(date) AS last
      FROM fact_timeseries
      WHERE source_id IN ('census_acs5','census_acs1')
      GROUP BY 1
      ORDER BY 1
    """).fetchdf())

    con.close()
    print("[census:validate] OK")


if __name__ == "__main__":
    main()
