# transform/redfin_to_fact_v2.py
import os, duckdb, pandas as pd
from typing import Optional, Dict, Tuple, List

ROOT = "./data/raw/redfin"
CANDIDATES = {
    "city":   f"{ROOT}/city/monthly_latest.tsv",
    "county": f"{ROOT}/county/monthly_latest.tsv",
    "state":  f"{ROOT}/state/monthly_latest.tsv",
}

# (geo_id, name, type, fips) – county inserted only if data exists
MARKETS = {
    "city":  ("dc_city",  "Washington, DC",                 "city",   "11001"),
    "state": ("dc_state", "District of Columbia (Statewide)","state",  "11"),
    "county":("dc_county","District of Columbia County, DC","county", "11001"),
}

SOURCE = ("redfin","Redfin Data Center","https://www.redfin.com/news/data-center/","monthly","public")

# Only map columns that exist in file(s)
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

def _normalize(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """Return rows for DC at the requested level, with standardized date + columns."""
    df = df.copy()
    # normalize column names (keep original for values; use lowercase for lookup)
    orig_cols = df.columns.tolist()
    lower_map = {c.lower(): c for c in orig_cols}

    date_col  = lower_map.get("period_end") or lower_map.get("period_end_date") or lower_map.get("month")
    region    = lower_map.get("region")     or lower_map.get("city") or lower_map.get("state")
    region_ty = lower_map.get("region_type") or lower_map.get("city_type") or lower_map.get("state_type")

    if not date_col or not region or not region_ty:
        raise RuntimeError(f"[redfin:{level}] missing key columns. have={orig_cols}")

    # Filter DC by level
    if level == "city":
        mask = (df[region_ty].str.lower()=="city") & (df[region].str.lower().isin(["washington, dc","washington","dc"]))
        geo_id = "dc_city"
    elif level == "state":
        # Redfin states typically like "District of Columbia"
        mask = (df[region_ty].str.lower()=="state") & (df[region].str.lower().isin(["district of columbia","washington dc","dc"]))
        geo_id = "dc_state"
    else:  # county
        # DC county is typically "District of Columbia County, DC"
        mask = (df[region_ty].str.lower()=="county") & (df[region].str.lower().isin([
            "district of columbia county, dc", "washington, dc", "district of columbia"
        ]))
        geo_id = "dc_county"

    dcf = df.loc[mask].copy()
    if dcf.empty:
        return dcf  # return empty; caller will skip

    # Standardize date to month-end
    dcf["date"] = pd.to_datetime(dcf[date_col], errors="coerce")
    dcf["date"] = (dcf["date"] + pd.offsets.MonthEnd(0)).dt.date

    # Keep for value mapping later; also stash geo_id
    dcf["__geo_id__"] = geo_id
    dcf["__level__"]  = level
    dcf.attrs["lower_map"] = lower_map  # keep mapping for column lookups
    return dcf

def ensure_dims(con: duckdb.DuckDBPyConnection, have_geo: List[str]):
    # Source
    con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id=?)
    """, [*SOURCE, SOURCE[0]])

    # Markets (only the ones we actually saw data for)
    for level in have_geo:
        geo_id, name, typ, fips = MARKETS[level]
        con.execute("""
            INSERT INTO dim_market(geo_id, name, type, fips)
            SELECT ?, ?, ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_market WHERE geo_id=?)
        """, [geo_id, name, typ, fips, geo_id])

    # Metrics
    for _, (mid, name, unit, cat) in COL_MAP.items():
        con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, 'monthly', ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?)
        """, [mid, name, unit, cat, mid])

def main():
    pieces = []
    levels_with_rows = []

    for level, path in CANDIDATES.items():
        df = _read_tsv(path)
        if df is None:
            print(f"[redfin:{level}] file not found: {path} (skipping)")
            continue
        dcf = _normalize(df, level)
        if dcf.empty:
            print(f"[redfin:{level}] no DC rows found (skipping)")
            continue

        lower_map = dcf.attrs.get("lower_map", {})
        # Build long form for mapped columns that exist
        for source_col_lc, (metric_id, _name, _unit, _cat) in COL_MAP.items():
            exact = lower_map.get(source_col_lc)
            if exact and exact in dcf.columns:
                sub = dcf[["date", exact]].dropna().rename(columns={exact:"value"}).copy()
                if not sub.empty:
                    sub["metric_id"] = metric_id
                    sub["geo_id"]    = dcf["__geo_id__"].iloc[0]
                    sub["source_id"] = SOURCE[0]
                    pieces.append(sub)

        levels_with_rows.append(level)

    if not pieces:
        print("[redfin] nothing to load (no mapped columns across provided levels).")
        return

    tall = pd.concat(pieces, ignore_index=True)
    tall["value"] = pd.to_numeric(tall["value"], errors="coerce")
    tall = tall.dropna(subset=["date","value","metric_id","geo_id"])

    con = duckdb.connect("./data/market.duckdb")
    ensure_dims(con, levels_with_rows)
    con.register("df_stage", tall[["geo_id","metric_id","date","value","source_id"]])

    # Upsert per (geo, metric, date)
    con.execute("""
        DELETE FROM fact_timeseries
        WHERE (geo_id, metric_id, date) IN (
          SELECT geo_id, metric_id, date FROM df_stage
        )
    """)
    con.execute("""
        INSERT INTO fact_timeseries(geo_id, metric_id, date, value, source_id)
        SELECT geo_id, metric_id, date, CAST(value AS DOUBLE), source_id
        FROM df_stage
    """)

    print(con.execute("""
        SELECT geo_id, metric_id, COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
        FROM fact_timeseries
        WHERE metric_id LIKE 'redfin_%' AND geo_id IN ('dc_city','dc_state','dc_county')
        GROUP BY 1,2 ORDER BY 1,2
    """).fetchdf())
    con.close()

if __name__ == "__main__":
    main()
