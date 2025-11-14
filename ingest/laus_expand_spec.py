# ingest/laus_expand_spec.py
import csv, sys, os, time, re
from pathlib import Path

import yaml
import pandas as pd
import requests

# ----------------- Paths & constants -----------------

BLS_BASE = "https://download.bls.gov/pub/time.series/la/"
BLS_DIR  = Path("config/bls")
BLS_DIR.mkdir(parents=True, exist_ok=True)

GEO_MANIFEST = Path("config/geo_manifest.csv")

LA_AREA   = BLS_DIR / "la.area"
LA_SERIES = BLS_DIR / "la.series"

SPEC    = Path("config/laus_spec.yml")
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


def load_laus_geo_map() -> dict[str, str]:
    """
    Build a mapping from geo_id -> laus_area_code using config/geo_manifest.csv.
    Only rows with a non-empty laus_area_code are included.
    """
    if not GEO_MANIFEST.exists():
        raise SystemExit("[laus:gen] missing config/geo_manifest.csv")

    gm = pd.read_csv(GEO_MANIFEST, dtype=str)

    if "laus_area_code" not in gm.columns:
        raise SystemExit("[laus:gen] geo_manifest.csv missing 'laus_area_code' column")

    df = gm[["geo_id", "laus_area_code"]].dropna(subset=["laus_area_code"])
    df["geo_id"] = df["geo_id"].astype(str).str.strip()
    df["laus_area_code"] = df["laus_area_code"].astype(str).str.strip()
    df = df[df["laus_area_code"] != ""]
    df = df[df["geo_id"] != ""]

    mapping = dict(zip(df["geo_id"], df["laus_area_code"]))

    print(f"[laus:gen] loaded {len(mapping)} LAUS geo mappings from geo_manifest.csv")
    return mapping


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


def resolve_area_code(area_df: pd.DataFrame, spec_area: dict, geo_map: dict[str, str]) -> str:
    """
    Resolve area_code for a given spec 'area':
      1. If geo_manifest has laus_area_code for this geo_id, use that.
      2. Else, if YAML provides `area_code`, use it (fast path).
      3. Else, try exact match on name -> area_text (case-insensitive).
      4. Else, try contains-match fallback.
      5. Else, fail with a clear message.
    """
    # 1) manifest-driven mapping: geo_id -> laus_area_code
    gid = (spec_area.get("geo_id") or "").strip()
    if gid and gid in geo_map:
        ac = (geo_map[gid] or "").strip()
        if ac:
            return ac

    # 2) YAML fast path
    if "area_code" in spec_area and spec_area["area_code"]:
        return str(spec_area["area_code"]).strip()

    # 3) Name-based fallback
    target = (spec_area.get("name") or spec_area.get("geo_id") or "").strip().lower()
    if not target:
        raise SystemExit(f"[laus:gen] area has no name/geo_id: {spec_area}")

    hits = area_df[area_df["area_text"].str.lower() == target]
    if len(hits) == 1:
        return hits.iloc[0]["area_code"]

    # 4) Contains match as a last resort (guarded to 1 match)
    hits = area_df[area_df["area_text"].str.lower().str.contains(target, na=False)]
    if len(hits) == 1:
        return hits.iloc[0]["area_code"]

    # 5) Give up
    raise SystemExit(
        f"[laus:gen] Could not resolve area_code for '{target}'. "
        f"Provide 'laus_area_code' in geo_manifest.csv or 'area_code' in YAML for: {spec_area}"
    )


# ----------------- Main generator -----------------


def main():
    # Ensure BLS reference files are present
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

    valid_measures = {m for m in measures.keys() if m in MEASURE_MAP}

    # NEW: load geo_id -> laus_area_code from geo_manifest
    geo_map = load_laus_geo_map()

    rows = []
    for ar in areas:
        level = (ar.get("level") or "area").strip().lower()

        try:
            area_code = resolve_area_code(area_df, ar, geo_map)
        except SystemExit as e:
            print(e)
            sys.exit(1)

        # For states we want TWO outputs per measure: NSA and SA.
        # For all other levels, only NSA.
        if level == "state":
            seasonal_sets = [("U",), ("S",)]   # NSA first, then SA
        else:
            seasonal_sets = [("U",)]          # NSA only for sub-state geos

        for mcode in sorted(valid_measures):
            base_metric, default_name = MEASURE_MAP[mcode]

            for seasonals in seasonal_sets:
                cand = series_df[
                    (series_df["area_code"] == area_code)
                    & (series_df["measure_code"] == mcode)
                    & (series_df["seasonal"].isin(list(seasonals)))
                ]

                best = pick_latest_series(cand)
                if best is None:
                    if level == "state":
                        print(
                            f"[laus:gen] WARNING: no state series for area_code={area_code} "
                            f"({ar.get('name') or ar.get('geo_id')}), measure={mcode}, "
                            f"seasonals={seasonals} — skipping that seasonal."
                        )
                    continue

                sid = best["series_id"]
                seas_hr = seasonal_tag_from_sid(sid)  # "SA" or "NSA"

                rows.append({
                    "geo_id":      ar["geo_id"],
                    "series_id":   sid,
                    "metric_base": base_metric,
                    "seasonal":    seas_hr,
                    "name":        f"{default_name} ({(ar.get('level','area')).title()}, {seas_hr})",
                    "notes":       ar.get("name") or ar["geo_id"],
                })

    if not rows:
        print("[laus:gen] No rows generated — check your spec, geo_manifest, or lookup files.")
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


if __name__ == "__main__":
    main()
