# transform/redfin_to_fact_v2.py
import os, duckdb, pandas as pd

CSV = "./data/raw/redfin/weekly_market_totals.csv"

MARKET = ("dc_city","Washington, DC","city","11001")
SOURCE = ("redfin","Redfin Market Trends","https://redfin.com/news/data-center/","weekly","public")

# flexible column -> metric mapping (only apply if column exists)
COL_MAP = {
    "median_sale_price":        ("redfin_median_sale_price",        "Median Sale Price",        "usd",      "prices"),
    "homes_sold":               ("redfin_homes_sold",               "Homes Sold",               "homes",    "sales"),
    "inventory":                ("redfin_inventory",                "Active Inventory",         "homes",    "supply"),
    "new_listings":             ("redfin_new_listings",             "New Listings",             "homes",    "supply"),
    "median_days_on_market":    ("redfin_median_days_on_market",    "Median Days on Market",    "days",     "speed"),
    "months_of_supply":         ("redfin_months_of_supply",         "Months of Supply",         "months",   "supply"),
    "sale_to_list_ratio":       ("redfin_sale_to_list_ratio",       "Sale-to-List Ratio",       "ratio",    "prices"),
    "off_market_in_two_weeks":  ("redfin_off_market_2w_share",      "Off-Market in 2 Weeks %",  "percent",  "speed"),
    "pending_sales":            ("redfin_pending_sales",            "Pending Sales",            "homes",    "sales"),
}

def ensure_dims(con):
    # market
    con.execute("""
        INSERT INTO dim_market(geo_id, name, type, fips)
        SELECT ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_market WHERE geo_id=?)
    """, [*MARKET, MARKET[0]])
    # source
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id=?)
    """, [*SOURCE, SOURCE[0]])

    # metrics (only those we actually will use)
    for _, (mid, name, unit, cat) in COL_MAP.items():
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'weekly', ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?)
        """, [mid, name, unit, cat, mid])

def main():
    if not os.path.exists(CSV):
        print("[redfin] no csv found, skipping transform")
        return

    df = pd.read_csv(CSV)
    # Filter to DC city (Redfin: region_type, region columns may vary by case)
    cols = {c.lower(): c for c in df.columns}
    region_col = cols.get("region") or cols.get("city")
    rtype_col  = cols.get("region_type") or cols.get("city_type") or "region_type"
    date_col   = cols.get("period_end") or cols.get("period_end_date") or "period_end"

    if region_col is None or rtype_col is None or date_col is None:
        raise RuntimeError(f"[redfin] expected columns missing. got: {df.columns.tolist()}")

    df = df[(df[rtype_col].str.lower()=="city") & (df[region_col].str.lower().isin(["washington, dc","washington","dc"]))].copy()
    if df.empty:
        print("[redfin] no DC city rows found after filter; nothing to load")
        return

    # Build a long table of (metric_id, date, value) for available columns
    long_frames = []
    for col, (mid, _name, _unit, _cat) in COL_MAP.items():
        if col in df.columns:
            sub = df[[date_col, col]].dropna()
            if not sub.empty:
                sub = sub.rename(columns={date_col:"date", col:"value"})
                sub["metric_id"] = mid
                long_frames.append(sub)

    if not long_frames:
        print("[redfin] none of the mapped columns were present.")
        return

    tall = pd.concat(long_frames, ignore_index=True)
    tall["date"] = pd.to_datetime(tall["date"], errors="coerce").dt.date
    tall["value"] = pd.to_numeric(tall["value"], errors="coerce")
    tall = tall.dropna(subset=["date","value"])
    tall["geo_id"] = MARKET[0]
    tall["source_id"] = SOURCE[0]

    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con)
    con.register("df_stage", tall[["geo_id","metric_id","date","value","source_id"]])

    # upsert by (geo, metric, date)
    con.execute("""
        DELETE FROM fact_timeseries
        WHERE geo_id=?
          AND metric_id IN (SELECT DISTINCT metric_id FROM df_stage)
          AND date IN (SELECT DISTINCT date FROM df_stage)
    """, [MARKET[0]])
    con.execute("""
        INSERT INTO fact_timeseries(geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, CAST(value AS DOUBLE), source_id
        FROM df_stage
    """)
    # quick summary
    print(con.execute("""
        SELECT metric_id, COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
        FROM fact_timeseries
        WHERE geo_id=? AND metric_id LIKE 'redfin_%'
        GROUP BY 1 ORDER BY 1
    """, [MARKET[0]]).fetchdf())
    con.close()

if __name__ == "__main__":
    main()
