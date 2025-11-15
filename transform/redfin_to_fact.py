# transform/redfin_to_fact.py

import os
import duckdb
import pandas as pd

REDFIN_TS_PATH = "data/redfin/redfin_metro_timeseries.csv"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

def main():
    if not os.path.exists(REDFIN_TS_PATH):
        raise FileNotFoundError(f"Redfin timeseries not found at: {REDFIN_TS_PATH}")

    con = duckdb.connect(DUCKDB_PATH)

    # Load CSV
    df = pd.read_csv(REDFIN_TS_PATH)

    # Ensure types are reasonable
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Coerce property_type_id to int where possible
    if "property_type_id" in df.columns:
        df["property_type_id"] = pd.to_numeric(df["property_type_id"], errors="coerce").astype("Int64")

    # Optional: drop obviously bad rows
    df = df.dropna(subset=["geo_id", "date", "metric_id", "value"])

    # Register dataframe as DuckDB table
    con.register("redfin_df", df)

    # Clear any existing Redfin rows if you've loaded them before
    # Here I'm assuming all Redfin metrics start with something like 'median_' / 'homes_sold' etc.
    # If you later prefix them with 'redfin_', you can tighten this to `WHERE metric_id LIKE 'redfin_%'`.
    con.execute("""
        DELETE FROM fact_timeseries
        WHERE metric_id IN (
            SELECT DISTINCT metric_id FROM redfin_df
        );
    """)

    # Insert, including the new columns
    con.execute("""
        INSERT INTO fact_timeseries (geo_id, date, metric_id, value, property_type_id, property_type)
        SELECT
            geo_id,
            CAST(date AS DATE) AS date,
            metric_id,
            value,
            property_type_id,
            property_type
        FROM redfin_df;
    """)

    # Quick summary
    summary = con.execute("""
        SELECT metric_id,
               MIN(date) AS first,
               MAX(date) AS last,
               COUNT(*) AS n
        FROM fact_timeseries
        WHERE metric_id IN (
            SELECT DISTINCT metric_id FROM redfin_df
        )
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()

    print("[redfin_to_fact] loaded metrics:")
    print(summary)

if __name__ == "__main__":
    main()
