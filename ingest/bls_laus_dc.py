# ingest/bls_laus_dc.py
import os, io, time, pathlib, argparse, json
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://download.bls.gov/pub/time.series/la/"
FILES = {
    "area": "la.area",
    "series": "la.series",
    "measure": "la.measure",
    "data_dc": "la.data.15.DC",  # DC slice
}

# Map of LAUS measure code -> our canonical metric_id
KEEP_MEASURES = {"03": "unemployment_rate", "04": "unemployment", "05": "employment", "06": "labor_force"}

# DC statewide (state FIPS=11) area_code format used in LAUS files
DC_STATEWIDE_AREA_CODE = "ST1100000000000"
GEO_ID = "dc_state"

def _session():
    s = requests.Session()
    r = Retry(
        total=5,
        backoff_factor=0.7,
        status_forcelist=[403,404,408,429,500,502,503,504],
        allowed_methods=["GET","POST"]
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "realestate-intel/1.0 (+streamlit)",
        "Accept": "text/plain,application/json"
    })
    return s

def _get_text(path):
    url = BASE + path
    resp = _session().get(url, timeout=60)
    resp.raise_for_status()
    return resp.text

def _load_flatfiles():
    # Try the official flat files first
    area = pd.read_csv(io.StringIO(_get_text(FILES["area"])), sep=r"\s{1,}|\t", engine="python")
    measure = pd.read_csv(io.StringIO(_get_text(FILES["measure"])), sep=r"\s{1,}|\t", engine="python")
    series = pd.read_csv(io.StringIO(_get_text(FILES["series"])), sep=r"\s{1,}|\t", engine="python")
    data_dc = pd.read_csv(io.StringIO(_get_text(FILES["data_dc"])), sep=r"\s{1,}|\t", engine="python")
    # normalize
    for df in (area, measure, series, data_dc):
        df.columns = [c.strip().lower() for c in df.columns]
    return area, measure, series, data_dc

def _filter_dc_state(area, measure, series, data_dc):
    # keep DC statewide + SA and our measures
    meas = measure[measure["measure_code"].isin(KEEP_MEASURES.keys())].copy()
    meta = series.merge(meas, on="measure_code", how="inner")
    meta = meta[(meta["area_code"] == DC_STATEWIDE_AREA_CODE)]
    df = data_dc.merge(meta[["series_id","seasonal","measure_code"]], on="series_id", how="inner")
    df = df[df["seasonal"]=="S"].copy()
    df = df[df["period"].str.startswith("M")]
    df["month"] = df["period"].str[1:].astype(int)
    df["date"] = pd.to_datetime(dict(year=df["year"].astype(int), month=df["month"], day=1))
    df["metric_id"] = df["measure_code"].map(KEEP_MEASURES)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    out = df[["date","metric_id","value"]].sort_values(["metric_id","date"]).reset_index(drop=True)
    return out

# --- API fallback (no key needed up to 25 requests/day) ---
# Series IDs (seasonally adjusted) for DC statewide:
# Unemployment rate (03), unemployment (04), employment (05), labor force (06)
# Format per BLS docs: LA + S/U + area_code + measure_code
# Example ref to DC unemployment rate series on BLS/FRED pages.
SERIES_IDS_SA = [
    "LASST110000000000003",  # Unemployment Rate (SA)
    "LASST110000000000004",  # Unemployment (SA)
    "LASST110000000000005",  # Employment (SA)
    "LASST110000000000006",  # Labor Force (SA)
]

# replace your existing _fetch_via_api() with this version
from datetime import datetime

def _fetch_via_api():
    sess = _session()
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data"
    this_year = datetime.utcnow().year

    rows = []
    for sid in SERIES_IDS_SA:
        # pull in decade chunks for robustness
        start = 1976
        while start <= this_year:
            end = min(start + 9, this_year)
            payload = {"seriesid": [sid], "startyear": str(start), "endyear": str(end)}
            resp = sess.post(url, json=payload, timeout=60)
            resp.raise_for_status()
            js = resp.json()
            if js.get("status") != "REQUEST_SUCCEEDED":
                raise RuntimeError(f"BLS API failed for {sid} {start}-{end}: {js}")

            meas = sid[-2:]  # last two chars map to LAUS measure code
            metric = KEEP_MEASURES.get(meas)
            for s in js["Results"].get("series", []):
                for obs in s.get("data", []):
                    per = obs.get("period", "")
                    if per.startswith("M"):
                        year = int(obs["year"])
                        month = int(per[1:])
                        dt = pd.Timestamp(year=year, month=month, day=1)
                        val = pd.to_numeric(obs.get("value"), errors="coerce")
                        if pd.notna(val):
                            rows.append((dt, metric, float(val)))
            start = end + 1  # next block

    if not rows:
        raise RuntimeError("BLS API returned no rows")
    out = pd.DataFrame(rows, columns=["date","metric_id","value"]).sort_values(["metric_id","date"])
    # drop duplicates in case blocks overlapped
    out = out.drop_duplicates(subset=["metric_id","date"])
    return out


def main(out_dir="./data/parquet"):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    try:
        # 1) preferred: flat files
        area, measure, series, data_dc = _load_flatfiles()
        out = _filter_dc_state(area, measure, series, data_dc)
        method = "flat"
    except Exception as e:
        print(f"[laus] flat files failed ({e}); falling back to API…")
        # 2) fallback: API
        out = _fetch_via_api()
        method = "api"

    out_path = os.path.join(out_dir, "bls_laus_dc_state.parquet")
    try:
        out.to_parquet(out_path, index=False)
        print(f"[laus:{method}] rows={len(out):,} -> {out_path}")
    except Exception as e:
        csv = out_path.replace(".parquet",".csv")
        out.to_csv(csv, index=False)
        print(f"[laus:{method}] parquet failed ({e}); wrote CSV fallback: {csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="./data/parquet")
    args = ap.parse_args()
    main(args.out_dir)
