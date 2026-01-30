import os, io, sys, argparse, pathlib, time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URLS = [
    # primary and alt S3 host styles
    "https://redfin-public-data.s3-us-west-2.amazonaws.com/redfin_market_trends/latest/weekly_market_totals.csv",
    "https://redfin-public-data.s3.amazonaws.com/redfin_market_trends/latest/weekly_market_totals.csv",
]

HEADERS = {
    # mimic a browser; some S3 configs check UA & referer
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Referer": "https://www.redfin.com/",
    "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

KEEP = [
    "period_end","region","region_type","property_type",
    "median_sale_price","homes_sold","new_listings","inventory",
    "median_days_on_market","sale_to_list_ratio"
]

def sess():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.75,
        status_forcelist=[403, 408, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def try_download():
    s = sess()
    last = None
    for url in URLS:
        try:
            r = s.get(url, timeout=40, allow_redirects=True)
            if r.status_code == 200 and r.content:
                return r.content
            last = f"HTTP {r.status_code} from {url}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(1.0)
    raise RuntimeError(last or "Unknown download error")

def write_placeholder(out_dir: str):
    # write a tiny valid CSV so downstream steps can skip gracefully
    p = os.path.join(out_dir, "redfin_weekly.csv")
    os.makedirs(out_dir, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write("date,region,region_type,property_type,median_sale_price,homes_sold,new_listings,inventory,median_days_on_market,sale_to_list_ratio\n")
    print("[redfin] Wrote empty placeholder CSV for downstream skip:", p)

def main(out_dir: str = "./data/parquet"):
    os.makedirs(out_dir, exist_ok=True)
    allow_skip = os.getenv("ALLOW_REDFIN_FAIL", "0") == "1"

    try:
        raw = try_download()
        df = pd.read_csv(io.BytesIO(raw), low_memory=False)
        missing = [c for c in KEEP if c not in df.columns]
        if missing:
            raise RuntimeError(f"Missing expected columns: {missing}")

        df = df[KEEP].rename(columns={"period_end": "date"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df.dropna(subset=["date"])

        # prefer parquet; fallback to csv
        pq = os.path.join(out_dir, "redfin_weekly.parquet")
        try:
            df.to_parquet(pq, index=False)
            print(f"[redfin] rows={len(df):,} -> {pq}")
        except Exception as e:
            csv_path = os.path.join(out_dir, "redfin_weekly.csv")
            df.to_csv(csv_path, index=False)
            print(f"[redfin] Parquet failed ({e}); wrote CSV fallback:", csv_path)

    except Exception as e:
        msg = f"[redfin] Download failed: {e}"
        if allow_skip:
            print(msg, file=sys.stderr)
            write_placeholder(out_dir)
            # exit success so workflow continues
            sys.exit(0)
        else:
            print(msg + " (set ALLOW_REDFIN_FAIL=1 to skip)", file=sys.stderr)
            sys.exit(2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_dir", default="./data/parquet")
    args = parser.parse_args()
    main(args.out_dir)
