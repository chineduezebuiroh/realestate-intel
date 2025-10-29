import os, duckdb, pandas as pd
from datetime import date

DUCKDB_PATH = os.getenv("DUCKDB_PATH","./data/market.duckdb")
PARQUET_DIR = os.getenv("PARQUET_DIR","./data/parquet")

def ensure_dims(con):
    # Market
    con.execute("""
        INSERT INTO dim_market (geo_id, name, type, fips)
        SELECT 'dc_city','Washington, DC','city','11001'
        WHERE NOT EXISTS (SELECT 1 FROM dim_market WHERE geo_id='dc_city');
    """)
    # Sources
    con.execute("""
        INSERT INTO dim_source (source_id, name, url, cadence, license)
        SELECT 'zillow_zori','Zillow ZORI','https://www.zillow.com/research/data/','monthly','public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='zillow_zori');
    """)
    con.execute("""
        INSERT INTO dim_source (source_id, name, url, cadence, license)
        SELECT 'fred_dc','FRED DC Unemployment','https://fred.stlouisfed.org/','monthly','public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='fred_dc');
    """)
    # Metrics
    metrics = [
        ("zori_rent","ZORI Rent (All Homes)","monthly","USD","rental"),
        ("unemployment_rate","Unemployment Rate (DC)","monthly","percent","macro"),
    ]
    for metric_id, name, freq, unit, cat in metrics:
        con.execute("""
            INSERT INTO dim_metric (metric_id, name, frequency, unit, category)
            SELECT ?,?,?,?,?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?);
        """, [metric_id, name, freq, unit, cat, metric_id])

def upsert_from_parquet_or_csv(con, path_parquet, path_csv, metric_id, source_id, value_col):
    if os.path.exists(path_parquet):
        df = pd.read_parquet(path_parquet)
    elif os.path.exists(path_csv):
        df = pd.read_csv(path_csv, low_memory=False)
    else:
        print(f"[monthlies] Skip {metric_id}: no file found at {path_parquet} or {path_csv}")
        return

    if df.empty or "date" not in df.columns:
        print(f"[monthlies] Skip {metric_id}: empty or missing date col")
        return

    # Normalize
    df = df[["date", value_col]].dropna()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    df["geo_id"] = "dc_city"
    df["metric_id"] = metric_id
    df["source_id"] = source_id
    df = df[["geo_id","metric_id","date",value_col,"source_id"]].rename(columns={value_col:"value"})

    con.execute("""
        INSERT INTO fact_timeseries (geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, value, source_id FROM df
    """, {"df": df})
    n = con.execute("""SELECT COUNT(*) FROM fact_timeseries WHERE metric_id=? AND geo_id='dc_city'""", [metric_id]).fetchone()[0]
    print(f"[monthlies] fact_timeseries now has {n:,} rows for {metric_id}")

def main():
    con = duckdb.connect(DUCKDB_PATH)
    ensure_dims(con)

    # ZORI
    upsert_from_parquet_or_csv(
        con,
        os.path.join(PARQUET_DIR,"zillow_zori_dc.parquet"),
        os.path.join(PARQUET_DIR,"zillow_zori_dc.csv"),
        metric_id="zori_rent",
        source_id="zillow_zori",
        value_col="zori"
    )

    # DC unemployment (FRED)
    upsert_from_parquet_or_csv(
        con,
        os.path.join(PARQUET_DIR,"fred_dc_unemployment.parquet"),
        os.path.join(PARQUET_DIR,"fred_dc_unemployment.csv"),
        metric_id="unemployment_rate",
        source_id="fred_dc",
        value_col="value"
    )

    con.close()

if __name__ == "__main__":
    main()
