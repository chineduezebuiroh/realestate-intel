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
    # dim_market (DC city)
    con.execute("""
        INSERT INTO dim_market (geo_id, name, type, fips)
        SELECT 'dc_city','Washington, DC','city','11001'
        WHERE NOT EXISTS (SELECT 1 FROM dim_market WHERE geo_id='dc_city');
    """)

    # dim_source (redfin_weekly)
    con.execute("""
        INSERT INTO dim_source (source_id, name, url, cadence, license)
        SELECT 'redfin_weekly','Redfin Weekly','https://www.redfin.com','weekly','public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='redfin_weekly');
    """)

    # dim_metric rows
    for raw, (metric_id, unit, category, freq) in METRIC_MAP.items():
        con.execute("""
            INSERT INTO dim_metric (metric_id, name, frequency, unit, category)
            SELECT ?, ?, ?, ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?);
        """, [metric_id, metric_id.replace("_"," ").title(), freq, unit, category, metric_id])

def load_redfin_to_fact():
    con = duckdb.connect(DUCKDB_PATH)
    ensure_dims(con)

    parquet_path = os.path.join(PARQUET_DIR, "redfin_weekly.parquet")
    csv_path = os.path.join(PARQUET_DIR, "redfin_weekly.csv")
    
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
    elif os.path.exists(csv_path):
        df = pd.read_csv(csv_path, low_memory=False)
        # If it’s the empty placeholder, bail out gracefully
        if df.empty or df.columns.tolist() == ["date","region","region_type","property_type","median_sale_price","homes_sold","new_listings","inventory","median_days_on_market","sale_to_list_ratio"] and len(df) == 0:
            print("[transform] Redfin placeholder detected; skipping load.")
            con.close()
            return
    else:
        raise FileNotFoundError(
            f"Neither {parquet_path} nor {csv_path} found. Run ingest/redfin.py first."
        )


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
