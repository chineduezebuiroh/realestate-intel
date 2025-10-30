# transform/fred_yields_to_fact.py
import os
import duckdb
import pandas as pd

PARQUET = "./data/parquet/fred_yields.parquet"

DERIVED_SPREADS = [
    ("spread_10y_2y", "10Y - 2Y Treasury Spread", "fred_gs10", "fred_gs2"),
    ("spread_30y_10y", "30Y - 10Y Treasury Spread", "fred_gs30", "fred_gs10"),
    ("spread_mortgage_10y", "30Y Mortgage - 10Y Treasury Spread", "fred_mortgage_30y_avg", "fred_gs10"),
]

def ensure_dims(con):
    con.execute("""
        INSERT INTO dim_market(geo_id, name, type, fips)
        SELECT 'us_national','United States (National)','national',NULL
        WHERE NOT EXISTS (SELECT 1 FROM dim_market WHERE geo_id='us_national');
    """)
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT 'fred','Federal Reserve Economic Data (FRED)','https://fred.stlouisfed.org/','daily->monthly','public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='fred');
    """)
    base_metrics = [
        ("fred_gs2", "2Y Treasury Yield", "percent"),
        ("fred_gs10", "10Y Treasury Yield", "percent"),
        ("fred_gs30", "30Y Treasury Yield", "percent"),
        ("fred_fedfunds", "Effective Fed Funds Rate", "percent"),
    ]
    for mid, name, unit in base_metrics:
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'monthly', ?, 'rates'
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?);
        """, [mid, name, unit, mid])

    for sid, name, _, _ in DERIVED_SPREADS:
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'monthly', 'percent', 'spreads'
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?);
        """, [sid, name, sid])

def upsert_base(con):
    if not os.path.exists(PARQUET):
        print("[fred:yields] no parquet found, skipping base upsert")
        return
    df = pd.read_parquet(PARQUET)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["geo_id"] = "us_national"
    df["source_id"] = "fred"
    con.register("df_stage", df)
    con.execute("""
        DELETE FROM fact_timeseries
        WHERE geo_id='us_national'
          AND metric_id IN ('fred_gs2','fred_gs10','fred_gs30','fred_fedfunds')
          AND date IN (SELECT DISTINCT date FROM df_stage)
    """)
    con.execute("""
        INSERT INTO fact_timeseries(geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, CAST(value AS DOUBLE), source_id
        FROM df_stage
    """)

def upsert_spreads(con):
    for sid, _name, long_mid, short_mid in DERIVED_SPREADS:
        df = con.execute(f"""
            SELECT a.date,
                   a.value - b.value AS value
            FROM fact_timeseries a
            JOIN fact_timeseries b
              ON a.date=b.date
            WHERE a.metric_id='{long_mid}' AND b.metric_id='{short_mid}'
                  AND a.geo_id='us_national' AND b.geo_id='us_national'
            ORDER BY a.date
        """).fetchdf()
        if df.empty:
            print(f"[spread] {sid}: no data, skipping")
            continue
        df["geo_id"] = "us_national"
        df["metric_id"] = sid
        df["source_id"] = "fred"
        con.register("df_spread", df)
        con.execute(f"DELETE FROM fact_timeseries WHERE metric_id='{sid}' AND geo_id='us_national'")
        con.execute("""
            INSERT INTO fact_timeseries(geo_id, metric_id, date, value, source_id)
            SELECT geo_id, metric_id, date, CAST(value AS DOUBLE), source_id
            FROM df_spread
        """)
        print(f"[spread] upserted {len(df)} rows for {sid}")

def main():
    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con)
    upsert_base(con)
    upsert_spreads(con)
    print(con.execute("""
        SELECT metric_id, MIN(date), MAX(date), COUNT(*) AS rows
        FROM fact_timeseries
        WHERE geo_id='us_national' AND metric_id LIKE 'fred_gs%' OR metric_id LIKE 'spread_%'
        GROUP BY 1 ORDER BY 1
    """).fetchdf())
    con.close()

if __name__ == "__main__":
    main()
