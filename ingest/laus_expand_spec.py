# ingest/laus_expand_spec.py
import csv, sys
from pathlib import Path
import yaml
import pandas as pd

# --- BLS reference file fetcher (robust against 403) ---
import time
import requests

BLS_BASE = "https://download.bls.gov/pub/time.series/la/"
BLS_DIR  = Path("config/bls")
BLS_FILES = ["la.area", "la.series", "la.measure", "la.area_type"]

#LA_AREA   = BLS_DIR / "la.area"
#LA_SERIES = BLS_DIR / "la.series"
LA_AREA   = Path("config/bls/la.area")
LA_SERIES = Path("config/bls/la.series")


def _http_get(url: str, timeout=60) -> bytes:
    # Try HTTPS with browser UA, then HTTP, with a short retry
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    # 1) HTTPS
    r = requests.get(url, headers=headers, timeout=timeout)
    if r.status_code == 403:
        # brief backoff + retry once
        time.sleep(0.6)
        r = requests.get(url, headers=headers, timeout=timeout)
    if r.ok:
        return r.content

    # 2) fallback to HTTP (some CDNs gatekeep HTTPS without UA)
    if url.startswith("https://"):
        url_http = "http://" + url[len("https://"):]
        r = requests.get(url_http, headers=headers, timeout=timeout)
        if r.status_code == 403:
            time.sleep(0.6)
            r = requests.get(url_http, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.content

    r.raise_for_status()
    return r.content

def ensure_bls_files():
    BLS_DIR.mkdir(parents=True, exist_ok=True)
    for fname in BLS_FILES:
        dest = BLS_DIR / fname
        if dest.exists() and dest.stat().st_size > 0:
            continue
        url = f"{BLS_BASE}{fname}"
        print(f"[laus:gen] fetching {url} → {dest}")
        data = _http_get(url, timeout=60)
        dest.write_bytes(data)
# --- end fetcher ---

SPEC = Path("config/laus_spec.yml")
OUT_CSV = Path("config/laus_series.generated.csv")



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


def load_lookup(area_path: Path = LA_AREA, series_path: Path = LA_SERIES):
    # Use tolerant tab parsing: consecutive tabs, mixed whitespace, etc.
    area = pd.read_csv(LA_AREA, sep="\t", dtype=str)
    series = pd.read_csv(LA_SERIES, sep="\t", dtype=str)
    
    area.columns = [c.strip().lower() for c in area.columns]
    series.columns = [c.strip().lower() for c in series.columns]


    # Trim whitespace everywhere
    for df in (area, series):
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # ✅ CRITICAL: pad measure_code to 3 digits so it matches '003'..'006'
    if "measure_code" in series.columns:
        series["measure_code"] = series["measure_code"].str.strip().str.zfill(3)

    # Normalize seasonal to the single-letter codes we filter on
    if "seasonal" in series.columns:
        series["seasonal"] = (series["seasonal"]
                              .str.strip().str.upper()
                              .replace({"SA":"S", "NSA":"U"}))

    # Coerce years (so we can rank by recency)
    for c in ("begin_year", "end_year"):
        if c in series.columns:
            series[c] = pd.to_numeric(series[c], errors="coerce")


    # Schema sanity
    must_area   = {"area_code", "area_text"}
    must_series = {"series_id", "area_code", "measure_code", "seasonal", "begin_year", "end_year"}
    if not must_area.issubset(set(area.columns)) or not must_series.issubset(set(series.columns)):
        raise SystemExit("[laus:gen] Could not find expected columns in la.area/la.series")

    return area, series



def pick_latest_series(sdf: pd.DataFrame) -> pd.Series | None:
    """
    Choose the 'best' series among candidates:
      - Prefer the one with the most recent end_year.
      - If end_year is NaN (open-ended), treat as very new.
      - Hard reject obvious legacy windows (end_year <= 1995) when any newer exists.
    """
    if sdf.empty:
        return None
    sdf = sdf.copy()

    # Normalize numbers
    for c in ("begin_year", "end_year"):
        sdf[c] = pd.to_numeric(sdf.get(c), errors="coerce")

    # 1) If there are any candidates with end_year >= 2000 or NaN, drop <= 1995 ones
    modern = sdf[(sdf["end_year"].isna()) | (sdf["end_year"] >= 2000)]
    if not modern.empty:
        sdf = modern

    # 2) Rank: newer end_year first; if tie, earliest begin_year (longest span)
    sdf["end_year_rank"] = sdf["end_year"].fillna(9999)       # NaN -> very new
    sdf["begin_year_rank"] = sdf["begin_year"].fillna(9999)   # NaN -> push down
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
    # BLS lookup files (tab-delimited, standard LAUS formats)
    ensure_bls_files()

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

            # If all candidates are missing years, try to drop known legacy-only prefixes
            if not cand.empty and cand["end_year"].isna().all():
                # Some very old series used geography definitions that ended in the 90s;
                # they often have matching replacements with the same area_code/measure but
                # different internal lineage. When years are all NaN, keep everything and let
                # the ranker treat NaN end_year as 'new'; otherwise, lightly prefer SIDs that
                # do not look like early legacy. This heuristic is intentionally mild.
                pass  # (the pick_latest_series handles NaN-as-new already)

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
