# ingest/redfin_metro_to_timeseries.py

from pathlib import Path
import pandas as pd
import os

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

RAW_REDFIN_PATH = "data/redfin/raw/redfin_metro_market_tracker.tsv000"
GEO_MANIFEST_PATH = "config/geo_manifest.csv"
OUTPUT_PATH = "data/redfin/redfin_metro_timeseries.csv"

"""
RAW_REDFIN_PATH = Path("data/redfin/raw/redfin_metro_market_tracker.tsv000")
OUT_PATH = Path("data/redfin_timeseries.csv")
GEO_MANIFEST_PATH = Path("config/geo_manifest.csv")  # adjust if different
"""

"""
# Redfin metric column -> canonical metric_id
METRIC_COLUMNS = {
    "property_type": "redfin_property_type",
    "property_type_id": "redfin_property_type_id",
    "median_sale_price": "redfin_median_sale_price",
    "median_sale_price_mom": "redfin_median_sale_price_mom",
    "median_sale_price_yoy": "redfin_median_sale_price_yoy",
    "median_list_price": "redfin_median_list_price",
    "median_list_price_mom": "redfin_median_list_price_mom",
    "median_list_price_yoy": "redfin_median_list_price_yoy",
    "median_ppsf": "redfin_median_ppsf",
    "median_ppsf_mom": "redfin_median_ppsf_mom",
    "median_ppsf_yoy": "redfin_median_ppsf_yoy",
    "median_list_ppsf": "redfin_median_list_ppsf",
    "median_list_ppsf_mom": "redfin_median_list_ppsf_mom",
    "median_list_ppsf_yoy": "redfin_median_list_ppsf_yoy",
    "homes_sold": "redfin_homes_sold",
    "homes_sold_mom": "redfin_homes_sold_mom",
    "homes_sold_yoy": "redfin_homes_sold_yoy",
    "pending_sales": "redfin_pending_sales",
    "pending_sales_mom": "redfin_pending_sales_mom",
    "pending_sales_yoy": "redfin_pending_sales_yoy",
    "new_listings": "redfin_new_listings",
    "new_listings_mom": "redfin_new_listings_mom",
    "new_listings_yoy": "redfin_new_listings_yoy",
    "inventory": "redfin_inventory",
    "inventory_mom": "redfin_inventory_mom",
    "inventory_yoy": "redfin_inventory_yoy",
    "months_of_supply": "redfin_months_of_supply",
    "months_of_supply_mom": "redfin_months_of_supply_mom",
    "months_of_supply_yoy": "redfin_months_of_supply_yoy",
    "median_dom": "redfin_median_dom",
    "median_dom_mom": "redfin_median_dom_mom",
    "median_dom_yoy": "redfin_median_dom_yoy",
    "avg_sale_to_list": "redfin_avg_sale_to_list",
    "avg_sale_to_list_mom": "redfin_avg_sale_to_list_mom",
    "avg_sale_to_list_yoy": "redfin_avg_sale_to_list_yoy",
    "sold_above_list": "redfin_sold_above_list",
    "sold_above_list_mom": "redfin_sold_above_list_mom",
    "sold_above_list_yoy": "redfin_sold_above_list_yoy",
    "price_drops": "redfin_price_drops",
    "price_drops_mom": "redfin_price_drops_mom",
    "price_drops_yoy": "redfin_price_drops_yoy",
    "off_market_in_two_weeks": "redfin_off_market_in_two_weeks",
    "off_market_in_two_weeks_mom": "redfin_off_market_in_two_weeks_mom",
    "off_market_in_two_weeks_yoy": "redfin_off_market_in_two_weeks_yoy",
    
    # add more as needed
}
"""

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    if not os.path.exists(RAW_REDFIN_PATH):
        raise FileNotFoundError(f"Redfin raw file not found at: {RAW_REDFIN_PATH}")

    if not os.path.exists(GEO_MANIFEST_PATH):
        raise FileNotFoundError(f"geo_manifest not found at: {GEO_MANIFEST_PATH}")

    # --- 1) Load Redfin file and normalize columns --------------------------------
    df = pd.read_csv(RAW_REDFIN_PATH, sep="\t")
    df.columns = df.columns.str.lower()

    # Choose date column: prefer period_end, else period_begin
    if "period_end" in df.columns:
        date_col = "period_end"
    elif "period_begin" in df.columns:
        date_col = "period_begin"
    else:
        raise ValueError(
            "Expected 'period_end' or 'period_begin' in Redfin file.\n"
            f"Available columns: {df.columns.tolist()}"
        )

    # --- 2) Load geo_manifest with redfin_code ------------------------------------
    geo = pd.read_csv(GEO_MANIFEST_PATH)

    if "redfin_code" not in geo.columns:
        raise ValueError(
            "geo_manifest.csv must contain a 'redfin_code' column "
            "to join with Redfin's 'table_id'."
        )

    if "geo_id" not in geo.columns:
        raise ValueError("geo_manifest.csv must contain a 'geo_id' column.")

    # Optional include flag, if you've added one (ignore if it doesn't exist)
    if "include_redfin" in geo.columns:
        geo = geo[geo["include_redfin"].fillna(False)]

    # Only rows with a non-null redfin_code
    geo = geo[geo["redfin_code"].notna()]

    # --- 3) Join Redfin rows to geo_manifest on table_id ↔ redfin_code -----------
    if "table_id" not in df.columns:
        raise ValueError(
            "Redfin file is missing 'table_id' column.\n"
            f"Available columns: {df.columns.tolist()}"
        )

    merged = df.merge(
        geo[["geo_id", "redfin_code"]],
        left_on="table_id",
        right_on="redfin_code",
        how="inner",
    )

    if merged.empty:
        raise ValueError(
            "No rows matched between Redfin data and geo_manifest on "
            "'table_id' ↔ 'redfin_code'.\n"
            "Check that geo_manifest.redfin_code values match those in "
            "Redfin's table_id column."
        )

    print(f"[redfin] matched {merged['geo_id'].nunique()} geos from geo_manifest.")
    print("[redfin] example matches:")
    print(
        merged[["geo_id", "region", "state", "table_id"]]
        .drop_duplicates()
        .head(10)
    )

    # --- 4) Optional: filter to non-seasonally adjusted -----
    if "is_seasonally_adjusted" in merged.columns:
        before = len(merged)
        # 0 = not seasonally adjusted in most Redfin extracts
        merged = merged[merged["is_seasonally_adjusted"] == 0]
        after = len(merged)
        print(f"[redfin] filtered to is_seasonally_adjusted=0: {before} → {after} rows")

    if merged.empty:
        raise ValueError("No rows remain after seasonality filter.")

    # --- 5) Prepare for melt: id_vars vs value columns ----------------------------
    # Core identifiers we want to keep
    id_vars = ["geo_id", date_col, "property_type", "property_type_id"]

    for col in ["region", "city", "state", "state_code"]:
        if col in merged.columns:
            id_vars.append(col)

    # Columns that are NOT metrics (to exclude from melt)
    exclude_cols = set(
        id_vars
        + [
            "table_id",
            "redfin_code",
            "period_begin",
            "period_end",
            "period_duration",
            "is_seasonally_adjusted",
            "region_type",
            "region_type_id",
            "parent_metro_region",
            "parent_metro_region_metro_code",
            "last_updated",
        ]
    )

    value_cols = [c for c in merged.columns if c not in exclude_cols]

    if not value_cols:
        raise ValueError(
            "No metric columns detected to melt. "
            "Check Redfin schema and exclude_cols list."
        )

    print(f"[redfin] metric columns (sample): {value_cols[:15]}")

    # --- 6) Melt to long format ---------------------------------------------------
    long_df = merged[id_vars + value_cols].melt(
        id_vars=id_vars,
        value_vars=value_cols,
        var_name="metric_id",
        value_name="value",
    )

    # Drop rows with no value
    long_df = long_df.dropna(subset=["value"])

    # Normalize date to a standard 'date' column
    long_df[date_col] = pd.to_datetime(long_df[date_col])
    long_df["date"] = long_df[date_col].dt.date

    # Final tidy frame
    ts = long_df[["geo_id", "date", "metric_id", "value"]].copy()
    ts = ts.sort_values(["geo_id", "metric_id", "date"])

    # --- 7) Write to CSV ----------------------------------------------------------
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    ts.to_csv(OUTPUT_PATH, index=False)

    # --- 8) Log summary -----------------------------------------------------------
    print(f"[redfin] wrote {len(ts)} rows → {OUTPUT_PATH}")
    print("[redfin] sample:")
    print(ts.head(10))
    print("[redfin] metrics (first 30):")
    print(sorted(ts["metric_id"].unique())[:30])


if __name__ == "__main__":
    main()
