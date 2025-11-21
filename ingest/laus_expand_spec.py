# ingest/laus_expand_spec.py
import csv, sys, os, time, re
from pathlib import Path

import pandas as pd
import requests

# ----------------- Paths & constants -----------------

BLS_BASE = "https://download.bls.gov/pub/time.series/la/"
BLS_DIR  = Path("config/bls")
BLS_DIR.mkdir(parents=True, exist_ok=True)

GEO_MANIFEST = Path("config/geo_manifest.csv")

LA_AREA   = BLS_DIR / "la.area"
LA_SERIES = BLS_DIR / "la.series"

OUT_CSV = Path("config/laus_series.generated.csv")

FILES = [
    # metadata
    "la.area",
    "la.area_type",
    "la.series",
    "la.measure",
    "la.state_region_division",  # may 403 sometimes; treat as optional

    # BIG data files (the fallback source we need)
    "la.data.60.Metro",
    "la.data.61.Division",
    "la.data.62.Micro",
    "la.data.63.Combined",
    "la.data.64.County",
    "la.data.65.City",

    # states/regions (optional but nice to have)
    "la.data.2.AllStatesU",
    "la.data.3.AllStatesS",
    "la.data.4.RegionDivisionU",
    "la.data.5.RegionDivisionS",
]

OPTIONAL_FILES = {
    "la.state_region_division",
    "la.data.2.AllStatesU",
    "la.data.3.AllStatesS",
    "la.data.4.RegionDivisionU",
    "la.data.5.RegionDivisionS",
}

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

# ----------------- Helpers: robust HTTP -----------------


def _robust_get(url: str, timeout=120, max_retries=3, retry_sleep=1.0) -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Connection": "close",
    }
    last_err = None

    for attempt in range(1, max_retries + 1):
        for scheme in ("https://", "http://"):
            u = url if url.startswith("http") else (scheme + url.lstrip("/"))
            try:
                r = requests.get(u, headers=headers, timeout=timeout)
                if r.status_code in (403, 429, 503):
                    last_err = requests.HTTPError(f"{r.status_code} for {u}")
                    time.sleep(retry_sleep * attempt)
                    continue
                r.raise_for_status()
                return r.content
            except Exception as e:
                last_err = e
                time.sleep(retry_sleep * attempt)

    if last_err:
        raise last_err
    raise RuntimeError(f"Failed to fetch {url}")


def ensure_bls_files():
    BLS_DIR.mkdir(parents=True, exist_ok=True)

    # allow skipping specific files if needed: LAUS_SKIP_FILES="la.state_region_division,la.data.5.RegionDivisionS"
    skip = set(s.strip() for s in os.getenv("LAUS_SKIP_FILES", "").split(",") if s.strip())

    for name in FILES:
        if name in skip:
            print(f"[bls] skipping {name} (env LAUS_SKIP_FILES)")
            continue

        p = BLS_DIR / name
        if p.exists() and p.stat().st_size > 0:
            continue

        url = BLS_BASE + name
        print(f"[bls] downloading {url} → {p}")
        try:
            data = _robust_get(url, timeout=120, max_retries=4, retry_sleep=1.0)
            with open(p, "wb") as f:
                f.write(data)
        except Exception as e:
            if name in OPTIONAL_FILES:
                print(f"[bls] WARNING: could not fetch optional file {name}: {e} — continuing.")
                continue
            else:
                raise


# ----------------- NEW: geo_manifest-driven mapping -----------------

def load_laus_areas_from_manifest() -> list[dict]:
    """
    Build a list of area dicts from geo_manifest.csv for LAUS:

      [
        {
          "geo_id": "dc_state",
          "name": "District of Columbia",
          "level": "state",
          "area_code": "XXXXXXX",   # bls_laus_area_code
        },
        ...
      ]

    Only rows with include_laus=true and a non-empty bls_laus_area_code
    are included.
    """
    if not GEO_MANIFEST.exists():
        raise SystemExit("[laus:gen] missing config/geo_manifest.csv")

    gm = pd.read_csv(GEO_MANIFEST, dtype=str)

    required = {"geo_id", "geo_name", "level", "include_laus", "bls_laus_area_code"}
    missing = required - set(gm.columns)
    if missing:
        raise SystemExit(f"[laus:gen] geo_manifest.csv missing columns: {sorted(missing)}")

    # Normalize
    gm["geo_id"] = gm["geo_id"].astype(str).str.strip()
    gm["geo_name"] = gm["geo_name"].astype(str).str.strip()
    gm["level"] = gm["level"].astype(str).str.strip().str.lower()
    gm["include_laus"] = gm["include_laus"].astype(str).str.strip().str.lower()
    gm["bls_laus_area_code"] = gm["bls_laus_area_code"].astype(str).str.strip()

    # Keep only include_laus = true-ish
    gm = gm[gm["include_laus"].isin(["1", "y", "yes", "true", "t"])]

    # And only rows with a non-empty LAUS area code
    gm = gm[gm["bls_laus_area_code"] != ""]
    gm = gm[gm["geo_id"] != ""]

    areas = []
    for row in gm.itertuples():
        areas.append(
            {
                "geo_id": row.geo_id,
                "name": row.geo_name,
                "level": row.level,
                "area_code": row.bls_laus_area_code,
            }
        )

    print(f"[laus:gen] loaded {len(areas)} LAUS areas from geo_manifest.csv")
    return areas

# ----------------- LAUS lookup + series selection -----------------

def seasonal_tag_from_sid(series_id: str) -> str:
    sid = (series_id or "").upper()
    if sid.startswith("LAS"):
        return "SA"
    if sid.startswith("LAU"):
        return "NSA"
    # Defensive fallback; most LAUS series are LAS*/LAU*
    return "NSA"


def load_lookup(area_path: Path = LA_AREA, series_path: Path = LA_SERIES):
    # Use tolerant tab parsing: consecutive tabs, mixed whitespace, etc.
    area = pd.read_csv(area_path, sep="\t", dtype=str)
    series = pd.read_csv(series_path, sep="\t", dtype=str)

    area.columns = [c.strip().lower() for c in area.columns]
    series.columns = [c.strip().lower() for c in series.columns]

    # Trim whitespace everywhere
    for df in (area, series):
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Pad measure_code to 3 digits so it matches '003'..'006'
    if "measure_code" in series.columns:
        series["measure_code"] = series["measure_code"].str.strip().str.zfill(3)

    # Normalize seasonal to single-letter codes we filter on
    if "seasonal" in series.columns:
        series["seasonal"] = (
            series["seasonal"]
            .str.strip()
            .str.upper()
            .replace({"SA": "S", "NSA": "U"})
        )

    # Coerce years (so we can rank by recency)
    for c in ("begin_year", "end_year"):
        if c in series.columns:
            series[c] = pd.to_numeric(series[c], errors="coerce")

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

    modern = sdf[(sdf["end_year"].isna()) | (sdf["end_year"] >= 2000)]
    if not modern.empty:
        sdf = modern

    sdf["end_year_rank"] = sdf["end_year"].fillna(9999)       # NaN -> very new
    sdf["begin_year_rank"] = sdf["begin_year"].fillna(9999)   # NaN -> push down
    sdf = sdf.sort_values(["end_year_rank", "begin_year_rank"], ascending=[False, True])

    return sdf.iloc[0]

# ----------------- Main generator -----------------

def main():
    # 0) Ensure we have the BLS metadata files we need.
    ensure_bls_files()

    try:
        area_df, series_df = load_lookup()
    except Exception as e:
        print("[laus:gen] failed to load BLS lookup files:", e)
        sys.exit(1)

    # 1) Load areas straight from geo_manifest (include_laus=1 + bls_laus_area_code)
    areas = load_laus_areas_from_manifest()
    if not areas:
        print("[laus:gen] No LAUS areas found in geo_manifest (include_laus=1 + bls_laus_area_code set).")
        sys.exit(1)

    # 2) Measures: we just use MEASURE_MAP keys (003..006)
    valid_measures = set(MEASURE_MAP.keys())

    rows = []

    for ar in areas:
        gid   = ar["geo_id"]
        level = (ar.get("level") or "area").strip().lower()
        name  = ar.get("name") or gid
        area_code = (ar.get("area_code") or "").strip()

        if not area_code:
            print(f"[laus:gen] WARNING: geo_id={gid} has no bls_laus_area_code; skipping.")
            continue

        # States + nation: both SA and NSA. Others: NSA only.
        if level in ("state", "nation"):
            seasonal_sets = [("U",), ("S",)]   # NSA then SA
        else:
            seasonal_sets = [("U",)]          # sub-state → NSA only

        for mcode in sorted(valid_measures):
            base_metric, nice_name = MEASURE_MAP[mcode]

            for seasonals in seasonal_sets:
                cand = series_df[
                    (series_df["area_code"] == area_code) &
                    (series_df["measure_code"] == mcode) &
                    (series_df["seasonal"].isin(list(seasonals)))
                ]

                best = pick_latest_series(cand)
                if best is None:
                    # For states/nation we expect both SA + NSA, so warn; for others, this can be normal.
                    if level in ("state", "nation"):
                        print(
                            f"[laus:gen] WARNING: no series for geo_id={gid}, "
                            f"area_code={area_code}, measure={mcode}, "
                            f"seasonals={seasonals} — skipping."
                        )
                    continue

                sid = best["series_id"]
                seas_hr = seasonal_tag_from_sid(sid)  # "SA" or "NSA"

                rows.append(
                    {
                        "geo_id":      gid,
                        "series_id":   sid,
                        "metric_base": base_metric,    # e.g. laus_unemployment, laus_employment
                        "seasonal":    seas_hr,        # "SA" / "NSA"
                        "name":        f"{nice_name} ({level.title()}, {seas_hr})",
                        "notes":       name,
                    }
                )

    if not rows:
        print("[laus:gen] No rows generated — check geo_manifest or LAUS lookup files.")
        sys.exit(1)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["geo_id", "series_id", "metric_base", "seasonal", "name", "notes"],
        )
        w.writeheader()
        w.writerows(rows)

    print(f"[laus:gen] wrote {len(rows)} series rows → {OUT_CSV}")
