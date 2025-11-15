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

    # Optional include flag, if you've added one
    if "include_redfin" in geo.columns:
        geo = geo[geo["include_redfin"].fillna(0).astype(int) == 1]

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
    # Core identifiers we want to keep (NOT melted)
    id_vars = ["geo_id", date_col, "property_type", "property_type_id"]

    # Optionally keep some descriptive columns around in the long_df phase
    for col in ["region", "city", "state", "state_code"]:
        if col in merged.columns and col not in id_vars:
            id_vars.append(col)

    print("[redfin] id_vars:", id_vars)

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

    print("[redfin] long_df columns:", long_df.columns.tolist())

    # Final tidy frame — KEEP property_type + property_type_id
    ts = long_df[["geo_id", "date", "property_type", "property_type_id", "metric_id", "value"]].copy()
    ts = ts.sort_values(["geo_id", "property_type", "metric_id", "date"])

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
