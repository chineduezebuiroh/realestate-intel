import os, pathlib, pandas as pd
from dotenv import load_dotenv
try:
    from fredapi import Fred
except:
    Fred = None

load_dotenv()
PARQUET_DIR = os.getenv("PARQUET_DIR", "./data/parquet")
FRED_API_KEY = os.getenv("FRED_API_KEY")

SERIES = {"MORTGAGE30US":"mortgage_30y_rate","DGS10":"ust_10y","FEDFUNDS":"fed_funds_rate"}

def main():
    pathlib.Path(PARQUET_DIR).mkdir(parents=True, exist_ok=True)
    if not (Fred and FRED_API_KEY):
        print("[fred] No key or fredapi missing; skipping.")
        return
    fred = Fred(api_key=FRED_API_KEY)
    frames = []
    for sid, metric in SERIES.items():
        s = fred.get_series(sid).to_frame("value").reset_index().rename(columns={"index":"date"})
        s["metric_id"] = metric
        frames.append(s)
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    out = os.path.join(PARQUET_DIR, "fred_rates.parquet")
    df.to_parquet(out, index=False)
    print(f"[fred] rows={len(df):,} -> {out}")

if __name__ == "__main__":
    main()
