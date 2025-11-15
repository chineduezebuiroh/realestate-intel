# ingest/redfin_metro_to_timeseries.py

from pathlib import Path
import pandas as pd

RAW_REDFIN_PATH = Path("data/redfin/raw/redfin_metro_timeseries.csv")
OUT_PATH = Path("data/redfin_timeseries.csv")
GEO_MANIFEST_PATH = Path("config/geo_manifest.csv")  # adjust if different

# Redfin metric column -> canonical metric_id
METRIC_COLUMNS = {
    "median_sale_price": "redfin_median_sale_price",
    "median_list_price": "redfin_median_list_price",
    "median_ppsf": "redfin_median_ppsf",
    "inventory": "redfin_inventory",
    "new_listings": "redfin_new_listings",
    "pending_sales": "redfin_pending_sales",
    "homes_sold": "redfin_homes_sold",
    "months_of_supply": "redfin_months_of_supply",
    # add more as needed
}

# 👇 this is where we translate your `level` → Redfin `region_type`
LEVEL_TO_REDFIN_REGION_TYPE = {
    "state": "state",
    "metro_area": "msa",   # or "metro" if that's what you see in the CSV
    "county": "county",
    "city": "city",
    # add others later if you ingest them (zip, neighborhood, etc.)
}


def load_redfin_geo_mapping() -> pd.DataFrame:
    """
    Load geo_manifest and build a mapping from
    (redfin_region_name, derived_region_type) -> geo_id
    for rows with include_redfin = 1.
    """
    if not GEO_MANIFEST_PATH.exists():
        raise FileNotFoundError(f"geo_manifest not found at {GEO_MANIFEST_PATH}")

    g = pd.read_csv(GEO_MANIFEST_PATH)

    required_cols = ["geo_id", "level", "include_redfin", "redfin_region_name"]
    missing = [c for c in required_cols if c not in g.columns]
    if missing:
        raise ValueError(f"geo_manifest missing columns: {missing}")

    g = g[g["include_redfin"] == 1].copy()
    g["level"] = g["level"].str.lower()

    # derive the Redfin region_type from our level
    g["redfin_region_type"] = g["level"].map(LEVEL_TO_REDFIN_REGION_TYPE)

    # sanity check
    if g["redfin_region_type"].isna().any():
        bad_levels = g.loc[g["redfin_region_type"].isna(), "level"].unique()
        raise ValueError(
            f"Unmapped levels in geo_manifest for Redfin: {bad_levels}. "
            f"Update LEVEL_TO_REDFIN_REGION_TYPE."
        )

    return g[["geo_id", "redfin_region_name", "redfin_region_type"]]


def main():
    if not RAW_REDFIN_PATH.exists():
        raise FileNotFoundError(
            f"Raw Redfin file not found at {RAW_REDFIN_PATH}. "
            "Download the metro CSV and put it there."
        )

    df = pd.read_csv(RAW_REDFIN_PATH)

    # Normalize date
    if "period_end" in df.columns:
        df["date"] = pd.to_datetime(df["period_end"])
    elif "period_begin" in df.columns:
        df["date"] = pd.to_datetime(df["period_begin"])
    else:
        raise ValueError("Expected 'period_end' or 'period_begin' in Redfin file.")

    # Redfin geography columns
    if "region_type" not in df.columns or "region" not in df.columns:
        raise ValueError("Expected 'region_type' and 'region' columns in Redfin file.")

    df["region_type"] = df["region_type"].str.lower()

    # (Optional) debug: see what region types are present
    # print("Redfin region_type values:", sorted(df["region_type"].unique()))

    geo_map = load_redfin_geo_mapping()

    # Join Redfin rows to manifest-derived mapping
    merged = df.merge(
        geo_map,
        left_on=["region", "region_type"],
        right_on=["redfin_region_name", "redfin_region_type"],
        how="inner",
    )

    if merged.empty:
        raise ValueError(
            "No Redfin rows matched any geo_manifest rows with include_redfin=1.\n"
            "Check that redfin_region_name in geo_manifest matches 'region' in the "
            "Redfin CSV, and that LEVEL_TO_REDFIN_REGION_TYPE is correct."
        )

    # Keep only the metric columns we care about
    expected = list(METRIC_COLUMNS.keys())
    missing_metrics = [c for c in expected if c not in merged.columns]
    if missing_metrics:
        print("[redfin] WARNING: missing expected metric columns:", missing_metrics)

    keep_metrics = [c for c in expected if c in merged.columns]
    keep_cols = ["date", "geo_id"] + keep_metrics
    merged = merged[keep_cols]

    # wide → long
    long = (
        merged.melt(
            id_vars=["date", "geo_id"],
            var_name="raw_metric",
            value_name="value",
        )
        .dropna(subset=["value"])
        .copy()
    )

    long["metric_id"] = long["raw_metric"].map(METRIC_COLUMNS).fillna(long["raw_metric"])

    long = long[["geo_id", "date", "metric_id", "value"]].sort_values(
        ["geo_id", "metric_id", "date"]
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    long.to_csv(OUT_PATH, index=False)

    print(f"[redfin] wrote {len(long):,} rows → {OUT_PATH}")
    print("[redfin] sample:")
    print(long.head(10))


if __name__ == "__main__":
    main()
