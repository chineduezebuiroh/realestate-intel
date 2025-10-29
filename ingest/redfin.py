import pandas as pd, os, argparse, pathlib, sys

URL = "https://redfin-public-data.s3-us-west-2.amazonaws.com/redfin_market_trends/latest/weekly_market_totals.csv"

def main(out_dir: str = "./data/parquet"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)

    # More robust read (explicit dtype inference off)
    df = pd.read_csv(URL, low_memory=False)
    keep = ["period_end","region","region_type","property_type",
            "median_sale_price","homes_sold","new_listings","inventory",
            "median_days_on_market","sale_to_list_ratio"]
    missing = [c for c in keep if c not in df.columns]
    if missing:
        print(f"[redfin] ERROR: missing expected columns: {missing}", file=sys.stderr)
        print(f"[redfin] Columns present: {list(df.columns)[:20]} ...", file=sys.stderr)
        raise SystemExit(1)

    df = df[keep].rename(columns={"period_end":"date"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])

    # Prefer Parquet; fallback to CSV if pyarrow not available for any reason
    parquet_path = os.path.join(out_dir, "redfin_weekly.parquet")
    csv_path = os.path.join(out_dir, "redfin_weekly.csv")
    try:
        df.to_parquet(parquet_path, index=False)
        print(f"[redfin] rows={len(df):,} -> {parquet_path}")
    except Exception as e:
        print(f"[redfin] Parquet write failed ({e}); falling back to CSV.", file=sys.stderr)
        df.to_csv(csv_path, index=False)
        print(f"[redfin] rows={len(df):,} -> {csv_path} (fallback)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_dir", default="./data/parquet")
    args = parser.parse_args()
    main(args.out_dir)
