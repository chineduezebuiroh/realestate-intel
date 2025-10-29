import os, duckdb, pandas as pd, argparse, pathlib
from datetime import datetime

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
PARQUET_DIR = os.getenv("PARQUET_DIR", "./data/parquet")

# Simple DC filter: city-level rows where region == 'Washington, DC'
# (We can add MSA/submarkets later.)
DC_REGION = "Washington, DC"

METRIC_MAP = {
    "median_sale_price":      ("price_median_sale",  "USD",   "price",   "weekly"),
    "homes_sold":             ("sales_count",        "count", "demand",  "weekly"),
    "new_listings":           ("new_listings_count", "count", "supply",  "weekly"),
    "inventory":              ("active_inventory",   "count", "supply",  "weekly"),
    "median_days_on_market":  ("dom_median",         "days",  "price",   "weekly"),
    "sale_to_list_ratio":     ("sale_to_list_ratio", "ratio", "price",   "weekly"),
}

def ensure_dims(con):
    # Minimal entries so the fact table has foreigns we understand
    con.execute("INSERT INTO dim_market VALUES ('dc_city','Washington, DC','city','11001') ON CONFLICT DO NOTHING")
    for src in [("redfin_weekly","Redfin Weekly","https://redfin.com","weekly","public")]:
        con.execute("INSERT INTO dim_source VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING", src)
    for raw, (metric_id, unit, category, freq) in METRIC_MAP.items():
        con.execute("INSERT INTO dim_metric VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                    (metric_id, metric_id.replace("_"," ").title(), freq, unit, category))

def load_redfin_to_fact():
    con = duckdb.connect(DUCKDB_PATH)
    ensure_dims(con)

    parquet_path = os.path.join(PARQUET_DIR, "redfin_weekly.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"{parquet_path} not found. Run ingest/redfin.py first.")

    df = pd.read_parquet(parquet_path)

    # Filter to DC city, all property types combined
    dcf = df[(df["region_type"]=="city") & (df["region"]==DC_REGION)]
    if dcf.empty:
        print("[transform] No DC city rows found in Redfin file.")
        con.close()
        return

    # Unpivot metrics to long format
    value_cols = list(METRIC_MAP.keys())
    long = dcf.melt(id_vars=["date","region","region_type","property_type"], value_vars=value_cols,
                    var_name="raw_metric", value_name="value")
    # Map to canonical metric_id
    long["metric_id"] = long["raw_metric"].map(lambda k: METRIC_MAP[k][0])
    long["geo_id"] = "dc_city"
    long["source_id"] = "redfin_weekly"
    long["date"] = pd.to_datetime(long["date"]).dt.date
    long = long[["geo_id","metric_id","date","value","source_id"]].dropna(subset=["value"])

    # Insert
    con.execute("""
        INSERT INTO fact_timeseries (geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, value, source_id FROM long_df
    """, {"long_df": long})

    n = con.execute("SELECT COUNT(*) FROM fact_timeseries WHERE geo_id='dc_city'").fetchone()[0]
    print(f"[transform] fact_timeseries rows for dc_city: {n:,}")
    con.close()

if __name__ == "__main__":
    pathlib.Path(os.path.dirname(DUCKDB_PATH) or ".").mkdir(parents=True, exist_ok=True)
    load_redfin_to_fact()
