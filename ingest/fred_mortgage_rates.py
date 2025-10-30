# ingest/fred_mortgage_rates.py
import os
import pathlib
import pandas as pd
from fredapi import Fred

OUT_PATH = "./data/parquet/fred_mortgage_rates.parquet"

# FRED weekly series → we aggregate to monthly averages
SERIES = {
    "fred_mortgage_30y_avg": ("MORTGAGE30US", "30Y Mortgage Rate (FRED, monthly avg)"),
    "fred_mortgage_15y_avg": ("MORTGAGE15US", "15Y Mortgage Rate (FRED, monthly avg)"),
    "fred_mortgage_5y_arm_avg": ("MORTGAGE5US", "5/1 ARM Mortgage Rate (FRED, monthly avg)"),
}

def fetch_monthly_avg(series_id: str, fred: Fred) -> pd.DataFrame:
    s = fred.get_series(series_id)
    if s is None or s.empty:
        return pd.DataFrame(columns=["date","value"])
    df = s.to_frame("value").dropna()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    monthly = (
        df.resample("M").mean()
          .reset_index()
          .rename(columns={"index": "date"})
    )
    monthly["date"] = pd.to_datetime(monthly["date"]).dt.date
    return monthly[["date","value"]]

def main(out_path=OUT_PATH):
    fred_key = os.getenv("FRED_API_KEY", "")
    fred = Fred(api_key=fred_key) if fred_key else Fred()

    frames = []
    for metric_id, (sid, _name) in SERIES.items():
        m = fetch_monthly_avg(sid, fred)
        if not m.empty:
            m = m.assign(metric_id=metric_id)
            frames.append(m)

    if not frames:
        raise RuntimeError("No mortgage rate data returned from FRED")

    out = pd.concat(frames, ignore_index=True).sort_values(["metric_id","date"])
    pathlib.Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(out_path, index=False)
        print(f"[fred:rates] rows={len(out):,} -> {out_path}")
    except Exception as e:
        csv = out_path.replace(".parquet",".csv")
        out.to_csv(csv, index=False)
        print(f"[fred:rates] parquet failed ({e}); wrote CSV fallback: {csv}")

if __name__ == "__main__":
    main()
