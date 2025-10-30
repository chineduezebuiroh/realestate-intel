# ingest/bls_laus_dc.py
import os, io, time, pathlib, argparse
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://download.bls.gov/pub/time.series/la/"
FILES = {
    "area": "la.area",
    "series": "la.series",
    "measure": "la.measure",
    "data_dc": "la.data.15.DC",  # BLS publishes state-sliced data files; "15.DC" is District of Columbia
}

# BLS measure codes we care about
KEEP_MEASURES = {"03": "unemployment_rate", "04": "unemployment", "05": "employment", "06": "labor_force"}

# Area_type A = Statewide; area_code for DC statewide is ST1100000000000 (from la.area)
STATEWIDE_AREA_PREFIX = "ST"
DC_STATEWIDE_AREA_CODE = "ST1100000000000"
GEO_ID = "dc_state"   # we’ll add this to dim_market in the transform

def _session():
    s = requests.Session()
    r = Retry(total=5, backoff_factor=0.6, status_forcelist=[403,404,408,429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": "market-intel/1.0 (+streamlit)","Accept":"text/plain"})
    return s

def _get_text(path):
    url = BASE + path
    resp = _session().get(url, timeout=60)
    resp.raise_for_status()
    return resp.text

def load_tables():
    # All LAUS files are tab-separated with header row
    area = pd.read_csv(io.StringIO(_get_text(FILES["area"])), sep=r"\s{1,}|\t", engine="python")
    measure = pd.read_csv(io.StringIO(_get_text(FILES["measure"])), sep=r"\s{1,}|\t", engine="python")
    series = pd.read_csv(io.StringIO(_get_text(FILES["series"])), sep=r"\s{1,}|\t", engine="python")
    data_dc = pd.read_csv(io.StringIO(_get_text(FILES["data_dc"])), sep=r"\s{1,}|\t", engine="python")

    # Normalize column names (sometimes BLS files have spaces)
    area.columns = [c.strip().lower() for c in area.columns]
    measure.columns = [c.strip().lower() for c in measure.columns]
    series.columns = [c.strip().lower() for c in series.columns]
    data_dc.columns = [c.strip().lower() for c in data_dc.columns]

    return area, measure, series, data_dc

def filter_dc_state(area, measure, series, data_dc):
    # Keep DC statewide area
    dc_area = area[(area["area_type_code"]=="A") & (area["area_code"]==DC_STATEWIDE_AREA_CODE)].copy()
    if dc_area.empty:
        raise RuntimeError("Could not find DC statewide area in la.area")

    # Keep our measures
    meas = measure[measure["measure_code"].isin(KEEP_MEASURES.keys())].copy()

    # Join to series metadata to identify series for DC statewide + our measures
    meta = series.merge(meas, on="measure_code", how="inner")
    meta = meta[(meta["area_code"]==DC_STATEWIDE_AREA_CODE)]

    # Join data points
    df = data_dc.merge(meta[["series_id","seasonal","measure_code"]], on="series_id", how="inner")

    # Keep only seasonally adjusted (S); if you want unadjusted too, drop this line
    df = df[df["seasonal"]=="S"].copy()

    # The monthly period key in la.data.* is like "M01", "M02" etc. Build a proper date
    # Columns present: series_id year period value footnote_codes
    df = df[df["period"].str.startswith("M")]
    df["month"] = df["period"].str[1:].astype(int)
    df["date"] = pd.to_datetime(dict(year=df["year"].astype(int), month=df["month"], day=1))

    # Map measure code to readable metric_id
    df["metric_id"] = df["measure_code"].map(KEEP_MEASURES)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    # Canonical shape for our fact table
    out = df[["date","metric_id","value"]].sort_values(["metric_id","date"]).reset_index(drop=True)
    return out

def main(out_dir="./data/parquet"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    area, measure, series, data_dc = load_tables()
    out = filter_dc_state(area, measure, series, data_dc)
    # Save one combined parquet
    out_path = os.path.join(out_dir, "bls_laus_dc_state.parquet")
    try:
        out.to_parquet(out_path, index=False)
        print(f"[laus] rows={len(out):,} -> {out_path}")
    except Exception as e:
        # csv fallback
        csv = out_path.replace(".parquet",".csv")
        out.to_csv(csv, index=False)
        print(f"[laus] parquet failed ({e}); wrote CSV fallback: {csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="./data/parquet")
    args = ap.parse_args()
    main(args.out_dir)
