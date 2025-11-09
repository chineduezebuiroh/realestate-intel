# ingest/laus_expand_spec.py
import csv, sys
from pathlib import Path
import yaml
import pandas as pd

SPEC = Path("config/laus_spec.yml")
OUT_CSV = Path("config/laus_series.generated.csv")

# BLS lookup files (tab-delimited, standard LAUS formats)
LA_AREA   = Path("config/bls/la.area")
LA_SERIES = Path("config/bls/la.series")

# 03–06 are the LAUS measures we ingest
MEASURE_MAP = {
    "003": ("laus_unemployment_rate", "Unemployment Rate"),
    "004": ("laus_unemployment",      "Unemployment"),
    "005": ("laus_employment",        "Employment"),
    "006": ("laus_labor_force",       "Labor Force"),
}

# Decide which seasonal flags are allowed per area "level"
# - States: SA + NSA
# - Sub-state (county/city/MSA/division/CSA): NSA only
LEVEL_TO_SEASONALS = {
    "state": ("S", "U"),
    # everything else: only NSA
    "county": ("U",),
    "city": ("U",),
    "msa": ("U",),
    "msd": ("U",),
    "csa": ("U",),
    "division": ("U",),
    "area": ("U",),
}

def seasonal_tag_from_sid(series_id: str) -> str:
    sid = (series_id or "").upper()
    if sid.startswith("LAS"): return "SA"
    if sid.startswith("LAU"): return "NSA"
    # Defensive fallback; most LAUS series are LAS*/LAU*
    return "NSA"

def load_lookup():
    # BLS files are tab-delimited with stable schemas.
    # We read as raw and only use the columns we need.
    area = pd.read_csv(LA_AREA, sep="\t", dtype=str)
    series = pd.read_csv(LA_SERIES, sep="\t", dtype=str)

    # Normalize column names defensively
    area.columns = [c.strip().lower() for c in area.columns]
    series.columns = [c.strip().lower() for c in series.columns]

    # Expected columns (with common names in BLS flat files):
    # la.area   -> area_code, area_text (and sometimes state_code, county_code, etc.)
    # la.series -> series_id, area_code, measure_code, seasonal, begin_year, end_year
    must_area = {"area_code", "area_text"}
    must_series = {"series_id", "area_code", "measure_code", "seasonal", "begin_year", "end_year"}
    if not must_area.issubset(set(area.columns)) or not must_series.issubset(set(series.columns)):
        raise SystemExit("[laus:gen] Could not find expected columns in la.area/la.series")

    # Clean typical whitespace
    for df in (area, series):
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Coerce years for ranking; missing -> -inf
    for c in ("begin_year", "end_year"):
        series[c] = pd.to_numeric(series[c], errors="coerce")

    return area, series

def pick_latest_series(sdf: pd.DataFrame) -> pd.Series | None:
    """Pick the best row among candidate series:
       1) Highest end_year
       2) If tie, lowest begin_year (longest span)
    """
    if sdf.empty:
        return None
    sdf = sdf.copy()
    sdf["end_year_rank"] = sdf["end_year"].fillna(-1_000_000)
    sdf["begin_year_rank"] = sdf["begin_year"].fillna(9_999_999)
    sdf = sdf.sort_values(["end_year_rank", "begin_year_rank"], ascending=[False, True])
    return sdf.iloc[0]

def resolve_area_code(area_df: pd.DataFrame, spec_area: dict) -> str:
    """
    How we resolve:
    - If spec provides `area_code`, use it (fast path).
    - Else try an exact match on `name` to `area_text` (case-insensitive).
    - Else fail with a clear message so the user can add `area_code` into YAML.
    """
    if "area_code" in spec_area and spec_area["area_code"]:
        return str(spec_area["area_code"]).strip()

    target = (spec_area.get("name") or spec_area.get("geo_id") or "").strip().lower()
    if not target:
        raise SystemExit(f"[laus:gen] area has no name/geo_id: {spec_area}")

    hits = area_df[area_df["area_text"].str.lower() == target]
    if len(hits) == 1:
        return hits.iloc[0]["area_code"]

    # Try contains match as a fallback (guarded to 1 match)
    hits = area_df[area_df["area_text"].str.lower().str.contains(target, na=False)]
    if len(hits) == 1:
        return hits.iloc[0]["area_code"]

    raise SystemExit(f"[laus:gen] Could not resolve area_code for '{target}'. "
                     f"Provide 'area_code' in YAML for: {spec_area}")

def main():
    if not SPEC.exists():
        print(f"[laus:gen] missing {SPEC}")
        sys.exit(1)

    try:
        area_df, series_df = load_lookup()
    except Exception as e:
        print("[laus:gen] failed to load BLS lookup files:", e)
        sys.exit(1)

    with open(SPEC, "r") as f:
        spec = yaml.safe_load(f)

    measures = spec["series"]["measures"]  # expects keys "003".."006"
    areas = spec["areas"]

    # Validate measures: keep only 003..006 and map to our base metric names
    valid_measures = {m for m in measures.keys() if m in MEASURE_MAP}

    rows = []
    for ar in areas:
        level = (ar.get("level") or "area").strip().lower()
        allowed_seasonals = LEVEL_TO_SEASONALS.get(level, ("U",))  # default NSA only
        try:
            area_code = resolve_area_code(area_df, ar)
        except SystemExit as e:
            print(e)
            sys.exit(1)

        # For each required measure, pick the best series among allowed seasonals
        for mcode in sorted(valid_measures):
            base_metric, default_name = MEASURE_MAP[mcode]
            # Search candidates in la.series
            cand = series_df[
                (series_df["area_code"] == area_code) &
                (series_df["measure_code"] == mcode) &
                (series_df["seasonal"].isin(list(allowed_seasonals)))
            ]

            best = pick_latest_series(cand)
            if best is None:
                print(f"[laus:gen] WARNING: no series for area_code={area_code} "
                      f"({ar.get('name') or ar.get('geo_id')}), measure={mcode}, "
                      f"seasonals={allowed_seasonals} — skipping.")
                continue

            sid = best["series_id"]
            seas_hr = seasonal_tag_from_sid(sid)  # "SA"/"NSA"

            rows.append({
                "geo_id":      ar["geo_id"],
                "series_id":   sid,
                "metric_base": base_metric,
                "seasonal":    seas_hr,  # "SA" or "NSA" (not "S"/"U")
                "name":        f"{default_name} ({(ar.get('level','area')).title()}, {seas_hr})",
                "notes":       ar.get("name") or ar["geo_id"],
            })

    if not rows:
        print("[laus:gen] No rows generated — check your spec or lookup files.")
        sys.exit(1)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["geo_id","series_id","metric_base","seasonal","name","notes"]
        )
        w.writeheader()
        w.writerows(rows)

    print(f"[laus:gen] wrote {len(rows)} series rows → {OUT_CSV}")

if __name__ == "__main__":
    main()
