import os, io, sys, argparse, pathlib, time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Primary + fallback URLs (some CI networks get 403 on the regional host)
URLS = [
    "https://redfin-public-data.s3-us-west-2.amazonaws.com/redfin_market_trends/latest/weekly_market_totals.csv",
    "https://redfin-public-data.s3.amazonaws.com/redfin_market_trends/latest/weekly_market_totals.csv",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)

KEEP = [
    "period_end","region","region_type","property_type",
    "median_sale_price","homes_sold","new_listings","inventory",
    "median_days_on_market","sale_to_list_ratio"
]

def get_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": UA})
    return s

def fetch_csv_bytes():
    session = get_session()
    last_err = None
    for url in URLS:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200 and resp.content:
                return resp.content
            else:
                last_err = f"HTTP {resp.status_code} from {url}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(0.5)  # small backoff between fallbacks
    raise RuntimeError(f"Failed to download Redfin CSV. Last error: {last_err}")

def main(out_dir: str = "./data/parquet"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    raw = fetch_csv_bytes()

    # Read CSV from in-memory bytes
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)

    # Validate schema
    missing = [c for c in KEEP if c not in df.columns]
    if missing:
        print(f"[redfin] ERROR: missing expected columns: {missing}", file=sys.stderr)
        print(f"[redfin] Columns present: {list(df.columns)[:25]} ...", file=sys.stderr)
        sys.exit(1)

    df = df[KEEP].rename(columns={"period_end": "date"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])

    # Prefer Parquet; fall back to CSV if pyarrow unavailable
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
