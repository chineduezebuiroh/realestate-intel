# ingest/redfin_metro_to_timeseries.py

from pathlib import Path
import pandas as pd
import os

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
"""
RAW_REDFIN_PATH = "data/redfin/raw/redfin_metro_market_tracker.tsv000"
GEO_MANIFEST_PATH = "config/geo_manifest.csv"
OUTPUT_PATH = "data/redfin/redfin_metro_timeseries.csv"
"""

RAW_REDFIN_DIR = Path("data/redfin/raw")
RAW_REDFIN_PATHS = sorted(RAW_REDFIN_DIR.glob("*.tsv*"))

GEO_MANIFEST_PATH = "config/geo_manifest.csv"
OUTPUT_PATH = "data/redfin/redfin_timeseries.csv"


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    if not os.path.exists(RAW_REDFIN_DIR):
        raise FileNotFoundError(f"Redfin raw file not found at: {RAW_REDFIN_DIR}")

    if not os.path.exists(GEO_MANIFEST_PATH):
        raise FileNotFoundError(f"geo_manifest not found at: {GEO_MANIFEST_PATH}")

    # --- 1) Load ALL Redfin TSVs and normalize columns ---------------------------
    frames = []
    
    for path in RAW_REDFIN_PATHS:
        print(f"[redfin] loading {path}")
        tmp = pd.read_csv(path, sep="\t")
        tmp.columns = tmp.columns.str.lower()
    
        # Choose date column: prefer period_end, else period_begin
        if "period_end" in tmp.columns:
            date_col = "period_end"
        elif "period_begin" in tmp.columns:
            date_col = "period_begin"
        else:
            raise ValueError(
                f"[redfin] Expected 'period_end' or 'period_begin' in file {path}.\n"
                f"Available columns: {tmp.columns.tolist()}"
            )
    
        # Standardize to a single 'date' column
        tmp["date"] = pd.to_datetime(tmp[date_col]).dt.to_period("M").dt.to_timestamp("M")

        # Optional: drop original period_* columns now that we have 'date'
        for c in ["period_begin", "period_end"]:
            if c in tmp.columns:
                tmp.drop(columns=c, inplace=True)

        frames.append(tmp)
    
    if not frames:
        raise ValueError("[redfin] No Redfin files loaded. Check RAW_REDFIN_PATHS / glob pattern.")
    
    df = pd.concat(frames, ignore_index=True)
    
    # Optional: dedupe if multiple files contain overlapping rows
    df = df.drop_duplicates()
    

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
    

    # --- 3) Join Redfin rows to geo_manifest on (table_id, region_type) ↔ (redfin_code, level)
    required_geo_cols = {"geo_id", "redfin_code", "level"}
    missing_geo = required_geo_cols - set(geo.columns)
    if missing_geo:
        raise ValueError(f"geo_manifest is missing columns: {sorted(missing_geo)}")

    required_df_cols = {"table_id", "region_type"}
    missing_df = required_df_cols - set(df.columns)
    if missing_df:
        raise ValueError(
            "Redfin data is missing expected columns.\n"
            f"Missing: {sorted(missing_df)}\n"
            f"Available: {df.columns.tolist()}"
        )

    # Normalize region_type in the Redfin data
    df["region_type_norm"] = (
        df["region_type"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # Map geo.level -> expected Redfin region_type
    LEVEL_TO_REGION_TYPE = {
        "state": "state",
        "metro_area": "metro",
        "county": "county",
        "city": "place",
        "neighborhood": "neighborhood",
        "zip_code": "zip code",
    }

    # Normalize levels and map to region_type
    geo = geo.copy()
    geo["level_norm"] = (
        geo["level"]
        .astype(str)
        .str.strip()
        .str.lower()
    )
    geo["region_type_norm"] = geo["level_norm"].map(LEVEL_TO_REGION_TYPE)

    # Drop any geo rows that don't have a mapped region_type
    before_geo = len(geo)
    geo = geo[geo["region_type_norm"].notna()]
    after_geo = len(geo)
    if after_geo == 0:
        raise ValueError(
            "After mapping geo.level to Redfin region_type, no rows remain.\n"
            "Check LEVEL_TO_REGION_TYPE mapping vs geo_manifest.level values."
        )
    if after_geo < before_geo:
        print(f"[redfin] warning: dropped {before_geo - after_geo} geo_manifest rows with unmapped level.")

    # Now join on (table_id, region_type_norm) ↔ (redfin_code, region_type_norm)
    merged = df.merge(
        geo[["geo_id", "redfin_code", "region_type_norm"]],
        left_on=["table_id", "region_type_norm"],
        right_on=["redfin_code", "region_type_norm"],
        how="inner",
    )

    if merged.empty:
        raise ValueError(
            "No rows matched between Redfin data and geo_manifest on "
            "(table_id, region_type) ↔ (redfin_code, level).\n"
            "Check that:\n"
            "  • geo_manifest.redfin_code matches Redfin table_id\n"
            "  • geo_manifest.level values map correctly via LEVEL_TO_REGION_TYPE\n"
            "  • Redfin region_type values look like: "
            "'state', 'metro', 'county', 'place', 'neighborhood', 'zip code'."
        )

    print(f"[redfin] matched {merged['geo_id'].nunique()} geos from geo_manifest.")
    print("[redfin] example matches:")
    cols_to_show = [c for c in ["geo_id", "region", "state", "region_type", "table_id"] if c in merged.columns]
    print(
        merged[cols_to_show]
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
    # IMPORTANT: use the normalized 'date' column, not the original period_* column
    if "date" not in merged.columns:
        raise ValueError("[redfin] Expected a normalized 'date' column before melt.")

    id_vars = ["geo_id", "date", "property_type", "property_type_id"]

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
            "period_duration",
            "is_seasonally_adjusted",
            "region_type",
            "region_type_id",
            "region_type_norm",
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


    # Normalize date to a standard 'date' column (just ensure it's date type)
    long_df["date"] = pd.to_datetime(long_df["date"]).dt.date


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
