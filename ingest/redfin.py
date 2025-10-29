import pandas as pd, os, argparse, pathlib
URL = "https://redfin-public-data.s3-us-west-2.amazonaws.com/redfin_market_trends/latest/weekly_market_totals.csv"

def main(out_dir: str = "./data/parquet"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(URL)
    keep = ["period_end","region","region_type","property_type",
            "median_sale_price","homes_sold","new_listings","inventory",
            "median_days_on_market","sale_to_list_ratio"]
    df = df[keep].rename(columns={"period_end":"date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    out = os.path.join(out_dir, "redfin_weekly.parquet")
    df.to_parquet(out, index=False)
    print(f"[redfin] rows={len(df):,} -> {out}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_dir", default="./data/parquet")
    args = parser.parse_args()
    main(args.out_dir)
