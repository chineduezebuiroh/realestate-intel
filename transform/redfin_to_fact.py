# transform/redfin_to_fact.py
"""
import os
import duckdb
import pandas as pd

REDFIN_TS_PATH = "data/redfin/redfin_metro_timeseries.csv"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

SOURCE_ID = "redfin"

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
    con.execute(
"""
    
        #DELETE FROM fact_timeseries
        #WHERE metric_id IN (
            #SELECT DISTINCT metric_id FROM redfin_df
        #);

""")

    # Insert, including the new columns
    con.execute(
"""
        #INSERT INTO fact_timeseries (geo_id, date, metric_id, value, property_type_id, property_type, source_id)
        #SELECT
            #geo_id,
            #CAST(date AS DATE) AS date,
            #metric_id,
            #value,
            #property_type_id,
            #property_type,
            #SOURCE_ID
        #FROM redfin_df;
""")

    # Quick summary
    summary = con.execute(
"""
        #SELECT metric_id,
               #MIN(date) AS first,
               #MAX(date) AS last,
               #COUNT(*) AS n
        #FROM fact_timeseries
        #WHERE metric_id IN (
            #SELECT DISTINCT metric_id FROM redfin_df
        #)
        #GROUP BY 1
        #ORDER BY 1
"""    ).fetchdf()

    print("[redfin_to_fact] loaded metrics:")
    print(summary)

if __name__ == "__main__":
    main()
"""


# transform/redfin_to_fact.py
import os
from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
REDFIN_TS_PATH = Path("data/redfin/redfin_timeseries.csv")

def main():
    if not REDFIN_TS_PATH.exists():
        raise SystemExit(f"Missing {REDFIN_TS_PATH}; run Redfin ingest first.")

    df = pd.read_csv(REDFIN_TS_PATH)

    # normalize
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # ensure required columns
    required = {"geo_id", "date", "metric_id", "value", "property_type_id"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns in {REDFIN_TS_PATH}: {missing}")

    df["source_id"] = "redfin"

    con = duckdb.connect(DB_PATH)

    # fact_timeseries schema (matches your existing)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_timeseries(
      geo_id TEXT NOT NULL,
      metric_id TEXT NOT NULL,
      date DATE NOT NULL,
      property_type_id TEXT NOT NULL DEFAULT 'all',
      value DOUBLE,
      source_id TEXT,
      property_type TEXT,
      PRIMARY KEY (geo_id, metric_id, date, property_type_id)
    );
    """)

    # Deduplicate
    df = (
        df.sort_values(["geo_id", "metric_id", "date", "property_type_id"])
          .drop_duplicates(subset=["geo_id", "metric_id", "date", "property_type_id"], keep="last")
    )

    con.register("rf_stage", df[[
        "geo_id", "metric_id", "date", "property_type_id", "value", "source_id", "property_type"
    ]])

    # Upsert
    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1 FROM rf_stage s
      WHERE s.geo_id = f.geo_id
        AND s.metric_id = f.metric_id
        AND s.date = f.date
        AND s.property_type_id = f.property_type_id
        AND f.source_id = 'redfin'
    );
    """)

    con.execute("""
    INSERT INTO fact_timeseries(geo_id, metric_id, date, property_type_id, value, source_id, property_type)
    SELECT geo_id, metric_id, date, property_type_id, CAST(value AS DOUBLE), source_id, property_type
    FROM rf_stage;
    """)

    # Quick sanity summary
    print(con.execute("""
      SELECT geo_id, COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
      FROM fact_timeseries
      WHERE source_id = 'redfin'
      GROUP BY 1
      ORDER BY geo_id
    """).df())

    con.close()

if __name__ == "__main__":
    main()

