import os, io, sys, argparse, pathlib, time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Zillow changes names periodically. Try several known patterns.
CANDIDATE_FILES = [
    # current (as of late 2025) – “sfrcondomfr” + smoothed monthly
    "City_zori_uc_sfrcondomfr_sm_month.csv",
    # alternate older names seen in the wild
    "City_zori_sm_month.csv",
    "City_zori_uc_sfr_tier_0.33_0.67_sm_sa_month.csv",
]

BASE = "https://files.zillowstatic.com/research/public_csvs/zori/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"

KEEP_META = ["RegionID","SizeRank","RegionName","RegionType","StateName"]

def session():
    s = requests.Session()
    r = Retry(total=5, backoff_factor=0.7,
              status_forcelist=[403, 404, 408, 429, 500, 502, 503, 504],
              allowed_methods=["GET", "HEAD"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": UA, "Accept": "text/csv,*/*"})
    return s

def try_download():
    s = session()
    last = None
    for fname in CANDIDATE_FILES:
        url = BASE + fname
        try:
            resp = s.get(url, timeout=60, allow_redirects=True)
            if resp.status_code == 200 and resp.content:
                return resp.content, fname
            last = f"{resp.status_code} {url}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(0.5)
    raise RuntimeError(last or "No candidate ZORI CSV succeeded")

def main(out_dir="./data/parquet"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    allow_skip = os.getenv("ALLOW_ZILLOW_FAIL", "0") == "1"

    try:
        raw, fname = try_download()
        df = pd.read_csv(io.BytesIO(raw), low_memory=False)

        if not set(KEEP_META).issubset(df.columns):
            raise RuntimeError(f"Unexpected columns in {fname}. Saw: {list(df.columns)[:15]}")

        # Washington, DC at city level
        dc = df[(df["RegionType"]=="city") & (df["RegionName"]=="Washington") & (df["StateName"]=="DC")].copy()
        if dc.empty:
            raise RuntimeError("Could not find Washington, DC (city) row in ZORI file.")

        # Melt monthly columns to long
        date_cols = [c for c in dc.columns if c not in KEEP_META]
        long = dc.melt(id_vars=KEEP_META, value_vars=date_cols, var_name="date", value_name="zori")
        long["date"] = pd.to_datetime(long["date"], errors="coerce").dt.date
        long = long.dropna(subset=["date"])[["date","zori"]].sort_values("date")

        # Write parquet (fallback to csv)
        pq = os.path.join(out_dir, "zillow_zori_dc.parquet")
        try:
            long.to_parquet(pq, index=False)
            print(f"[zillow] rows={len(long):,} from {fname} -> {pq}")
        except Exception as e:
            csv = os.path.join(out_dir, "zillow_zori_dc.csv")
            long.to_csv(csv, index=False)
            print(f"[zillow] Parquet failed ({e}); wrote CSV fallback: {csv}")

    except Exception as e:
        msg = f"[zillow] Ingest failed: {e}"
        if allow_skip:
            print(msg + " (ALLOW_ZILLOW_FAIL=1 -> continuing without ZORI)", file=sys.stderr)
            sys.exit(0)
        else:
            print(msg + " (set ALLOW_ZILLOW_FAIL=1 to skip)", file=sys.stderr)
            sys.exit(2)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="./data/parquet")
    args = ap.parse_args()
    main(args.out_dir)
