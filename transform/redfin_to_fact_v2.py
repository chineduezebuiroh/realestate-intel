# transform/redfin_to_fact_v2.py
import os, duckdb, pandas as pd, yaml
from glob import glob
from pathlib import Path
from typing import Optional, List

ROOT = Path("data/raw/redfin")

SOURCE = (
    "redfin",
    "Redfin Data Center",
    "https://www.redfin.com/news/data-center/",
    "monthly",
    "public",
)

# Optional: load market metadata from config/markets.yml (if available)
MARKETS_YAML = Path("config/markets.yml")
MARKETS = {}
if MARKETS_YAML.exists():
    with open(MARKETS_YAML, "r") as f:
        try:
            MARKETS = yaml.safe_load(f) or {}
            print(f"[redfin] loaded {len(MARKETS)} market entries from config/markets.yml")
        except Exception as e:
            print("[redfin] warning: couldn't parse markets.yml:", e)

# Metric map (unchanged)
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


def _read_tsv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path): return None
    try:
        return pd.read_csv(path, sep="\t")
    except Exception as e:
        print(f"[redfin:{path}] read failed: {e}")
        return None



def ensure_dims(con: duckdb.DuckDBPyConnection, geo_df: pd.DataFrame):
    # source
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id = ?)
    """, [*SOURCE, SOURCE[0]])

    # metrics (monthly)
    for _, (mid, name, unit, cat) in COL_MAP.items():
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'monthly', ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id = ?)
        """, [mid, name, unit, cat, mid])

    # markets (auto from geo_df)
    con.register("df_geo", geo_df)
    con.execute("DELETE FROM dim_market WHERE geo_id IN (SELECT geo_id FROM df_geo)")
    con.execute("""
        INSERT INTO dim_market (geo_id, name, type, fips)
        SELECT geo_id, name, type, fips FROM df_geo
    """)






def main():

    #from glob import glob
    #from pathlib import Path
    
    pieces = []
    geo_meta = []
    
    files = glob("data/raw/redfin/*/*_monthly_latest.tsv")
    if not files:
        print("[redfin] ❌ no per-geo Redfin slice files found under data/raw/redfin/*/")
        raise SystemExit(1)
    
    print(f"[redfin] discovered {len(files)} slice(s)")
    for path in files:
        path = Path(path)
        level = path.parent.name                     # city/county/state
        geo_id = path.stem.split("_monthly_")[0]     # e.g., dc_city
    
        try:
            df = pd.read_csv(path, sep="\t")
        except Exception as e:
            print(f"[redfin:{geo_id}] failed to read: {e}")
            continue
        if df.empty:
            continue
    
        # locate date column and normalize to month-end
        lc = {c.lower(): c for c in df.columns}
        date_col = lc.get("period_end") or lc.get("period_end_date") or lc.get("month")
        if not date_col:
            print(f"[redfin:{geo_id}] ⚠️ missing date column")
            continue
        df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.to_period("M").dt.to_timestamp("M")
        df["date"] = (
            pd.to_datetime(df[date_col], errors="coerce")
              .dt.to_period("M").dt.to_timestamp("M")
              .dt.date
        )

    
        # flatten to tall per COL_MAP
        for source_col_lc, (metric_id, _name, _unit, _cat) in COL_MAP.items():
            exact = next((c for c in df.columns if c.lower() == source_col_lc), None)
            if exact is None:
                continue
            sub = df[["date", exact]].dropna().rename(columns={exact: "value"}).copy()
            if sub.empty:
                continue
            sub["metric_id"] = metric_id
            sub["geo_id"] = geo_id
            sub["source_id"] = SOURCE[0]
            pieces.append(sub)
    
        # capture minimal market metadata (can be enriched from config later)
        geo_meta.append({
            "geo_id": geo_id,
            "name":   geo_id,     # fallback; we’ll improve via markets.yml if desired
            "type":   level,
            "fips":   None,
        })


    
    if not pieces:
        print("[redfin] no usable data after parsing.")
        raise SystemExit(0)
    
    tall = pd.concat(pieces, ignore_index=True)
    tall["value"] = pd.to_numeric(tall["value"], errors="coerce")
    tall = tall.dropna(subset=["date","value","metric_id","geo_id"])
    
    geo_df = pd.DataFrame(geo_meta).drop_duplicates(subset=["geo_id"])



    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con, geo_df)
    con.register("df_stage", tall[["geo_id","metric_id","date","value","source_id"]])


    # Upsert per (geo, metric, date)
    con.execute("""
        DELETE FROM fact_timeseries AS t
        USING df_stage AS s
        WHERE t.geo_id = s.geo_id
            AND t.metric_id = s.metric_id
            AND t.date = s.date
    """)
    con.execute("""
        INSERT INTO fact_timeseries(geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, CAST(value AS DOUBLE), source_id
        FROM df_stage
    """)



    
    
    print(con.execute("""
        SELECT geo_id, metric_id, COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
        FROM fact_timeseries
        WHERE metric_id LIKE 'redfin_%'
        GROUP BY 1,2 ORDER BY 1,2
    """).fetchdf())
    con.close()





if __name__ == "__main__":
    main()
