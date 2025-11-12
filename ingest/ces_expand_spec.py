# ingest/ces_expand_spec.py
import os
import csv
import re
from pathlib import Path
import requests

# Metric base label used downstream
METRIC_BASE = "ces_total_nonfarm"

GEO_MANIFEST = Path("config/geo_manifest.csv")

# 🔧 Populated at runtime in main()
CES_AREA_MAP = {}


BLS_DIR = Path("config/bls")
GEN_PATH = Path("config/ces_series.generated.csv")

# BLS CES flat files
SM_SERIES_URL = "https://download.bls.gov/pub/time.series/sm/sm.series"
SM_DATA_ALL_URL = "https://download.bls.gov/pub/time.series/sm/sm.data.1.AllData"


# We’re targeting:
# - industry_code = '000000' (Total Nonfarm)
# - data_type_code = '01' (All Employees)
# - seasonal in {'S','U'} (Seasonally adjusted / Not seasonally adjusted)
TARGET_INDUSTRY = {"00000000"}
TARGET_DATA_TYPE = {"01"}
TARGET_SEASONAL = {"S", "U"}


def load_ces_geo_targets():
    """
    Return dict[area_code] -> (geo_id, geo_name) for rows where include_ces=1
    and bls_ces_area_code is present.
    """
    out = {}
    with GEO_MANIFEST.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if (r.get("include_ces") or "0").strip() not in ("1", "true", "True"):
                continue
            area = (r.get("bls_ces_area_code") or "").strip()
            geo  = (r.get("geo_id") or "").strip()
            name = (r.get("geo_name") or "").strip()
            if area and geo:
                out[area] = (geo, name)
    return out

# Loaded once for lookups
CES_AREA_MAP = load_ces_geo_targets()




#def _download(url: str, dest: Path):
"""
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Only download if missing (idempotent). Force with CES_FORCE=1
    if dest.exists() and os.getenv("CES_FORCE", "0") not in ("1", "true", "True"):
        return
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)
"""

def _download(url: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Only download if missing (idempotent). Force with CES_FORCE=1
    if dest.exists() and os.getenv("CES_FORCE", "0") not in ("1", "true", "True"):
        return

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    }

    # Try HTTPS, then HTTP, a few times
    tries = [
        url,
        url.replace("https://", "http://", 1) if url.startswith("https://") else url
    ]

    last_exc = None
    for attempt in range(3):
        for u in tries:
            try:
                r = requests.get(u, headers=headers, timeout=60)
                if r.status_code == 403:
                    raise requests.HTTPError(f"403 from {u}")
                r.raise_for_status()
                dest.write_bytes(r.content)
                return
            except Exception as e:
                last_exc = e
    # If all attempts failed but file already exists, keep going
    if dest.exists():
        print(f"[ces] WARN: failed to refresh {dest.name} ({last_exc}); using existing file.")
        return
    raise last_exc



def ensure_bls_files():
    _download(SM_SERIES_URL, BLS_DIR / "sm.series")
    _download(SM_DATA_ALL_URL, BLS_DIR / "sm.data.1.AllData")


def _read_sm_series(path: Path):
    """
    Read sm.series in a whitespace-robust way.
    Returns list of dicts with keys from the header.
    """
    rows = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        header = None
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = re.split(r"\s*\t\s*|\s{2,}|\s+", line.strip())
            if header is None:
                header = [c.strip().lower() for c in parts]
                continue
            if len(parts) < len(header):
                # Pad if needed (rare)
                parts += [""] * (len(header) - len(parts))
            row = dict(zip(header, parts))
            rows.append(row)
    return rows



def _pick_geo(area_code: str):
    """Map sm.series area_code to (geo_id, display_name) via config file."""
    return CES_AREA_MAP.get(area_code, (None, None))




def _seasonal_tag(s: str) -> str:
    s = (s or "").upper()
    if s == "S":
        return "SA"
    if s == "U":
        return "NSA"
    return "NSA"


def generate_csv(sm_series_rows, out_path: Path):
    """
    Filter sm.series to DMV + Total Nonfarm (All Employees) and write the generator CSV.
    """
    want = []
    # --- DEBUG: confirm state area_code presence in sm.series ---
    state_codes = {"110000", "240000", "510000"}
    present_state_codes = set(r.get("area_code","").strip() for r in sm_series_rows if r.get("area_code"))
    missing_in_series = sorted(code for code in state_codes if code not in present_state_codes)
    if missing_in_series:
        print(f"[ces:gen][debug] these state area_code(s) not found in sm.series: {missing_in_series}")
    else:
        print("[ces:gen][debug] all state area_code(s) appear in sm.series")
    # ------------------------------------------------------------

    for r in sm_series_rows:
        # Expected fields in sm.series:
        # series_id, seasonal, supersector_code, industry_code, data_type_code,
        # area_code, series_title, footnote_codes, begin_year, begin_period, end_year, end_period
        series_id = (r.get("series_id") or "").strip()
        seasonal = (r.get("seasonal") or "").strip()
        area_code = (r.get("area_code") or "").strip()
        industry_code = (r.get("industry_code") or "").strip()
        data_type_code = (r.get("data_type_code") or "").strip()
        series_title = (r.get("series_title") or "").strip()

        if not series_id or not area_code:
            continue

        if seasonal not in TARGET_SEASONAL:
            continue
        if industry_code not in TARGET_INDUSTRY:
            continue
        if data_type_code not in TARGET_DATA_TYPE:
            continue

        # --- DEBUG: loosen & log ---
        if area_code in {"110000", "240000", "510000"}:
            print(f"[ces:gen][debug] saw state row: area_code={area_code} "
                  f"seasonal={seasonal} industry={industry_code} dtype={data_type_code} title={series_title!r}")
        # --------------------------------------------------------------

        geo_id, area_name = _pick_geo(area_code)
        if not geo_id:
            continue

        want.append({
            "geo_id": geo_id,
            "series_id": series_id,
            "metric_base": METRIC_BASE,
            "seasonal": _seasonal_tag(seasonal),
            "name": series_title,
            "area": area_name
        })

    # De-dup and stable sort
    dedup = {(w["geo_id"], w["series_id"]): w for w in want}
    rows = sorted(dedup.values(), key=lambda d: (d["geo_id"], d["series_id"]))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(
            f,
            fieldnames=["geo_id", "series_id", "metric_base", "seasonal", "name", "area"]
        )
        wr.writeheader()
        wr.writerows(rows)

    print(f"[ces:gen] wrote {len(rows)} series rows → {out_path}")



def main():
    ensure_bls_files()
    global CES_AREA_MAP
    CES_AREA_MAP = load_ces_geo_targets()
    if not CES_AREA_MAP:
        print("[ces:gen] NOTE: No CES geos enabled in config/geo_manifest.csv (include_ces=1).")
    rows = _read_sm_series(BLS_DIR / "sm.series")
    generate_csv(rows, GEN_PATH)



if __name__ == "__main__":
    main()
