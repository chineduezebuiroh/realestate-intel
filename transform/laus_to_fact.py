# transform/laus_to_fact.py
import os, sys, duckdb

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

def main():
    con = duckdb.connect(DB_PATH)

    # If LAUS facts already exist (from ingest/laus_api_bulk.py), skip.
    n = con.execute("""
        SELECT COUNT(*) AS n
        FROM fact_timeseries
        WHERE metric_id LIKE 'laus_%'
    """).fetchdf().loc[0, "n"]

    if n and int(n) > 0:
        print("[laus:transform] Skipping — LAUS already loaded via ingest/laus_api_bulk.py")
        # Optional quick summary so the Make step still prints something useful
        print(con.execute("""
            SELECT metric_id,
                   MIN(date) AS first,
                   MAX(date) AS last,
                   COUNT(*)  AS rows
            FROM fact_timeseries
            WHERE metric_id LIKE 'laus_%'
            GROUP BY 1
            ORDER BY 1
        """).fetchdf())
        con.close()
        sys.exit(0)

    # If you ever fall back to a parquet-based LAUS flow, you can put it here.
    print("[laus:transform] No LAUS facts found and no legacy transform defined — nothing to do.")
    con.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
