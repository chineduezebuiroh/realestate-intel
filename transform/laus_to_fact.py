# transform/laus_to_fact.py
import os
import duckdb
import pandas as pd

PARQUET = "./data/parquet/bls_laus_dc_state.parquet"

def ensure_dims(con):
    # Market: DC statewide (FIPS state '11')
    con.execute("""
        INSERT INTO dim_market(geo_id, name, type, fips)
        VALUES ('dc_state','District of Columbia (Statewide)','state','11')
        ON CONFLICT (geo_id) DO NOTHING
    """)

    # Metrics (seasonally adjusted)
    metrics = [
        ("laus_unemployment_rate_sa", "Unemployment Rate (BLS LAUS, SA)"),
        ("laus_unemployment_sa", "Unemployment (BLS LAUS, SA)"),
        ("laus_employment_sa", "Employment (BLS LAUS, SA)"),
        ("laus_labor_force_sa", "Labor Force (BLS LAUS, SA)"),
    ]
    for mid, name in metrics:
        con.execute("""
            INSERT INTO dim_metric(metric_id, name)
            VALUES (?, ?)
            ON CONFLICT (metric_id) DO NOTHING
        """, [mid, name])

    # Source
    con.execute("""
        INSERT INTO dim_source(source_id, name, url)
        VALUES ('bls_laus','BLS LAUS','https://www.bls.gov/lau/')
        ON CONFLICT (source_id) DO NOTHING
    """)

def upsert_from_parquet(con):
    if not os.path.exists(PARQUET) and not os.path.exists(PARQUET.replace(".parquet",".csv")):
        print("[laus] no parquet/csv found, skipping")
        return

    df = pd.read_parquet(PARQUET) if os.path.exists(PARQUET) else pd.read_csv(PARQUET.replace(".parquet",".csv"))
    if df.empty:
        print("[laus] empty input, skipping")
        return

    # Map metric_id from ingest to our canonical IDs
    mapper = {
        "unemployment_rate": "laus_unemployment_rate_sa",
        "unemployment": "laus_unemployment_sa",
        "employment": "laus_employment_sa",
        "labor_force": "laus_labor_force_sa",
    }
    df = df.rename(columns={"date":"date", "value":"value","metric_id":"raw_metric"})
    df["metric_id"] = df["raw_metric"].map(mapper)
    df["geo_id"] = "dc_state"
    df["source_id"] = "bls_laus"
    df = df.dropna(subset=["metric_id"])

    # normalize s
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    # register and upsert
    con.register("df_stage", df[["geo_id","metric_id","date","value","source_id"]])

    # delete overlapping dates for idempotency
    con.execute("""
        DELETE FROM fact_timeseries
        WHERE geo_id='dc_state'
          AND metric_id IN ('laus_unemployment_rate_sa','laus_unemployment_sa','laus_employment_sa','laus_labor_force_sa')
          AND date IN (SELECT DISTINCT date FROM df_stage)
    """)

    con.execute("""
        INSERT INTO fact_timeseries (geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, CAST(value AS DOUBLE), source_id
        FROM df_stage
    """)

def main():
    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con)
    upsert_from_parquet(con)
    n = con.execute("""
        SELECT metric_id, COUNT(*) AS rows
        FROM fact_timeseries WHERE geo_id='dc_state'
        GROUP BY 1 ORDER BY 1
    """).fetchdf()
    print(n)
    con.close()

if __name__ == "__main__":
    main()
