import os, pathlib, pandas as pd
from dotenv import load_dotenv

# FRED via pandas-datareader (no key) can be finicky on CI; we’ll use fredapi if available
try:
    from fredapi import Fred
except:
    Fred = None

load_dotenv()
PARQUET_DIR = os.getenv("PARQUET_DIR","./data/parquet")
FRED_API_KEY = os.getenv("FRED_API_KEY")  # optional but recommended

# DC unemployment rate monthly (seasonally adjusted) — FRED series code
SERIES = {"DCUR": "unemployment_rate"}

def main():
    pathlib.Path(PARQUET_DIR).mkdir(parents=True, exist_ok=True)
    if not Fred:
        print("[fred-ur] fredapi not installed; add to requirements.txt", flush=True)
        return
    fred = Fred(api_key=FRED_API_KEY) if FRED_API_KEY else Fred()
    frames = []
    for code, metric in SERIES.items():
        s = fred.get_series(code)
        df = s.to_frame("value").reset_index().rename(columns={"index":"date"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["metric_id"] = metric
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    pq = os.path.join(PARQUET_DIR, "fred_dc_unemployment.parquet")
    try:
        out.to_parquet(pq, index=False)
        print(f"[fred-ur] rows={len(out):,} -> {pq}")
    except Exception as e:
        csv = os.path.join(PARQUET_DIR, "fred_dc_unemployment.csv")
        out.to_csv(csv, index=False)
        print(f"[fred-ur] Parquet failed ({e}); wrote CSV fallback: {csv}")

if __name__ == "__main__":
    main()
