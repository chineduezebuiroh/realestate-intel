# ingest/fred_yields.py
import os
import pathlib
import pandas as pd
from fredapi import Fred

OUT_PATH = "./data/parquet/fred_yields.parquet"

SERIES = {
    "fred_gs2": ("GS2", "2Y Treasury Constant Maturity Yield"),
    "fred_gs10": ("GS10", "10Y Treasury Constant Maturity Yield"),
    "fred_gs30": ("GS30", "30Y Treasury Constant Maturity Yield"),
    "fred_fedfunds": ("FEDFUNDS", "Federal Funds Effective Rate"),
}

def fetch_monthly_avg(series_id: str, fred: Fred) -> pd.DataFrame:
    s = fred.get_series(series_id)
    if s is None or s.empty:
        return pd.DataFrame(columns=["date", "value"])
    df = s.to_frame("value").dropna()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    monthly = (
        df.resample("M").mean()
          .reset_index()
          .rename(columns={"index": "date"})
    )
    monthly["date"] = pd.to_datetime(monthly["date"]).dt.date
    return monthly[["date", "value"]]

def main(out_path=OUT_PATH):
    fred_key = os.getenv("FRED_API_KEY", "")
    fred = Fred(api_key=fred_key) if fred_key else Fred()

    frames = []
    for metric_id, (sid, _desc) in SERIES.items():
        df = fetch_monthly_avg(sid, fred)
        if not df.empty:
            df = df.assign(metric_id=metric_id)
            frames.append(df)

    if not frames:
        raise RuntimeError("No yield data returned from FRED")

    out = pd.concat(frames, ignore_index=True).sort_values(["metric_id", "date"])
    pathlib.Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print(f"[fred:yields] rows={len(out):,} -> {out_path}")

if __name__ == "__main__":
    main()
