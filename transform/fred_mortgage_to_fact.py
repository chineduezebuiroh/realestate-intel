# transform/fred_mortgage_to_fact.py
import os
import duckdb
import pandas as pd

PARQUET = "./data/parquet/fred_mortgage_rates.parquet"

METRICS = [
    ("fred_mortgage_30y_avg", "30Y Mortgage Rate (FRED, monthly avg)"),
    ("fred_mortgage_15y_avg", "15Y Mortgage Rate (FRED, monthly avg)"),
    ("fred_mortgage_5y_arm_avg", "5/1 ARM Mortgage Rate (FRED, monthly avg)"),
]

def ensure_dims(con):
    # Market: United States (National) for macro rates
    con.execute("""
        INSERT INTO dim_market(geo_id, name, type, fips)
        SELECT 'us_national','United States (National)','national',NULL
        WHERE NOT EXISTS (SELECT 1 FROM dim_market WHERE geo_id='us_national');
    """)

    # Source: FRED
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT 'fred','Federal Reserve Economic Data (FRED)','https://fred.stlouisfed.org/','weekly->monthly','public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='fred');
    """)

    # Metrics
    for mid, name in METRICS:
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'monthly', 'percent', 'rates'
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?);
        """, [mid, name, mid])

def upsert(con):
    if not os.path.exists(PARQUET) and not os.path.exists(PARQUET.replace(".parquet",".csv")):
        print("[fred:rates] no parquet/csv found, skipping")
        return

    df = pd.read_parquet(PARQUET) if os.path.exists(PARQUET) else pd.read_csv(PARQUET.replace(".parquet",".csv"))
    if df.empty:
        print("[fred:rates] empty input, skipping")
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date","value","metric_id"])
    df["geo_id"] = "us_national"
    df["source_id"] = "fred"

    con.register("df_stage", df[["geo_id","metric_id","date","value","source_id"]])

    con.execute("""
        DELETE FROM fact_timeseries
        WHERE geo_id='us_national'
          AND metric_id IN ('fred_mortgage_30y_avg','fred_mortgage_15y_avg','fred_mortgage_5y_arm_avg')
          AND date IN (SELECT DISTINCT date FROM df_stage)
    """)

    con.execute("""
        INSERT INTO fact_timeseries(geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, CAST(value AS DOUBLE), source_id
        FROM df_stage
    """)

def main():
    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con)
    upsert(con)
    print(con.execute("""
        SELECT metric_id, MIN(date) AS first, MAX(date) AS last, COUNT(*) AS rows
        FROM fact_timeseries
        WHERE geo_id='us_national'
          AND metric_id IN ('fred_mortgage_30y_avg','fred_mortgage_15y_avg','fred_mortgage_5y_arm_avg')
        GROUP BY 1 ORDER BY 1
    """).fetchdf())
    con.close()

if __name__ == "__main__":
    main()
