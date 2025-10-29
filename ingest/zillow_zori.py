import os, io, sys, argparse, pathlib, time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Zillow public monthly ZORI (city) — updated regularly
URLS = [
    "https://files.zillowstatic.com/research/public_csvs/zori/City_zori_uc_sfr_tier_0.33_0.67_sm_sa_month.csv",
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"

KEEP_META = ["RegionID","SizeRank","RegionName","RegionType","StateName"]
# Data columns are monthly YYYY-MM strings; we’ll melt them later

def session():
    s = requests.Session()
    r = Retry(total=5, backoff_factor=0.7, status_forcelist=[403,429,500,502,503,504], allowed_methods=["GET","HEAD"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": UA, "Accept": "text/csv,*/*"})
    return s

def download_csv_bytes():
    s = session()
    last = None
    for url in URLS:
        try:
            r = s.get(url, timeout=60)
            if r.status_code == 200 and r.content:
                return r.content
            last = f"HTTP {r.status_code} {url}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(0.5)
    raise RuntimeError(last or "Unknown error downloading ZORI CSV")

def main(out_dir="./data/parquet"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    raw = download_csv_bytes()
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    # Keep only DC city
    dc = df[(df["RegionType"]=="city") & (df["RegionName"]=="Washington") & (df["StateName"]=="DC")].copy()
    if dc.empty:
        print("[zillow] No Washington, DC city row found; columns:", list(df.columns)[:20], file=sys.stderr)
        sys.exit(2)

    # Melt monthly columns
    date_cols = [c for c in dc.columns if c not in KEEP_META]
    long = dc.melt(id_vars=KEEP_META, value_vars=date_cols, var_name="date", value_name="zori")
    long["date"] = pd.to_datetime(long["date"], errors="coerce").dt.date
    long = long.dropna(subset=["date"])
    long = long[["date","zori"]].sort_values("date")

    # Save parquet (fallback to CSV if needed)
    pq = os.path.join(out_dir, "zillow_zori_dc.parquet")
    try:
        long.to_parquet(pq, index=False)
        print(f"[zillow] rows={len(long):,} -> {pq}")
    except Exception as e:
        csv = os.path.join(out_dir, "zillow_zori_dc.csv")
        long.to_csv(csv, index=False)
        print(f"[zillow] Parquet failed ({e}); wrote CSV fallback: {csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="./data/parquet")
    args = ap.parse_args()
    main(args.out_dir)
