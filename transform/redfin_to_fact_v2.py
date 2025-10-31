# transform/redfin_to_fact_v2.py
import os, duckdb, pandas as pd

CSV = "./data/raw/redfin/monthly_market_totals.csv"  # <- monthly

MARKET = ("dc_city","Washington, DC","city","11001")
SOURCE = ("redfin","Redfin Data Center","https://www.redfin.com/news/data-center/","monthly","public")

# Map only columns that actually exist in your CSV
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
    # Market
    con.execute("""
        INSERT INTO dim_market(geo_id, name, type, fips)
        SELECT ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_market WHERE geo_id=?)
    """, [*MARKET, MARKET[0]])

    # Source
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id=?)
    """, [*SOURCE, SOURCE[0]])

    # Metrics (monthly frequency)
    for _, (mid, name, unit, cat) in COL_MAP.items():
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'monthly', ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?)
        """, [mid, name, unit, cat, mid])

def normalize_redfin_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Put all custom data cleaning/transforms here:
    - column normalization
    - type coercions
    - percent to ratio (if needed)
    - deduping / latest-per-month, etc.
    """
    # Normalize column name case (we’ll refer to lowercase)
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    lc_map = {c.lower(): c for c in df.columns}

    # Key columns: date, region, region_type
    date_col  = lc_map.get("period_end") or lc_map.get("period_end_date") or lc_map.get("month")
    region_col = lc_map.get("region") or lc_map.get("city")
    rtype_col  = lc_map.get("region_type") or lc_map.get("city_type")

    if not date_col or not region_col or not rtype_col:
        raise RuntimeError(f"[redfin] missing key columns. Got: {list(df.columns)}")

    # Filter to DC city
    df = df[(df[rtype_col].str.lower()=="city") &
            (df[region_col].str.lower().isin(["washington, dc","washington","dc"]))]

    # Coerce date to month-end date
    # (Redfin monthly usually provides a month-end date already; this keeps it consistent)
    df["date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["date"] = (df["date"] + pd.offsets.MonthEnd(0)).dt.date

    # Example: if any percent fields are 0–100, convert to 0–1 ratio (or leave as-is if you prefer %)
    # if "off_market_in_two_weeks" in lc_map:
    #     col = lc_map["off_market_in_two_weeks"]
    #     if df[col].notna().any() and df[col].max() > 1.5:  # crude check (values like 12, 37, etc.)
    #         df[col] = pd.to_numeric(df[col], errors="coerce") / 100.0

    return df

def main():
    if not os.path.exists(CSV):
        print("[redfin] monthly csv not found, skipping transform")
        return

    raw = pd.read_csv(CSV)
    df = normalize_redfin_monthly(raw)

    long_frames = []
    for source_col, (mid, _name, _unit, _cat) in COL_MAP.items():
        if source_col in [c.lower() for c in df.columns]:
            # map back to exact-case column in df
            exact = next(c for c in df.columns if c.lower() == source_col)
            sub = df[["date", exact]].dropna()
            if not sub.empty:
                sub = sub.rename(columns={exact:"value"})
                sub["metric_id"] = mid
                long_frames.append(sub)

    if not long_frames:
        print("[redfin] no mapped columns present; nothing to load")
        return

    tall = pd.concat(long_frames, ignore_index=True)
    tall["value"] = pd.to_numeric(tall["value"], errors="coerce")
    tall = tall.dropna(subset=["date","value"])
    tall["geo_id"] = MARKET[0]
    tall["source_id"] = SOURCE[0]

    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con)
    con.register("df_stage", tall[["geo_id","metric_id","date","value","source_id"]])

    # Upsert
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

    print(con.execute("""
        SELECT metric_id, COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
        FROM fact_timeseries
        WHERE geo_id=? AND metric_id LIKE 'redfin_%'
        GROUP BY 1 ORDER BY 1
    """, [MARKET[0]]).fetchdf())
    con.close()

if __name__ == "__main__":
    main()
