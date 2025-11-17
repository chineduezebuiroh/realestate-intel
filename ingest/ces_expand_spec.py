# ingest/ces_expand_spec.py
import os
import csv
import re
from pathlib import Path
import requests

GEO_MANIFEST = Path("config/geo_manifest.csv")

# 🔧 Populated at runtime in main()
#CES_AREA_MAP = {}

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

# Map supersector_code -> metric_base used downstream.
# (You can prune this list later if you only care about a few sectors.)
SUPERSECTOR_TO_METRIC_BASE = {
    "00": "ces_total_nonfarm",             # Total nonfarm
    "05": "ces_total_private",             # Total private
    "10": "ces_mining_logging",
    "20": "ces_construction",
    "30": "ces_manufacturing",
    "40": "ces_trade_transport_utilities",
    "50": "ces_information",
    "55": "ces_financial_activities",
    "60": "ces_prof_business_services",
    "65": "ces_education_health_services",
    "70": "ces_leisure_hospitality",
    "80": "ces_other_services",
    "90": "ces_government",
}

TARGET_SUPERSECTOR = set(SUPERSECTOR_TO_METRIC_BASE.keys())


def load_ces_geo_targets():
    """
    Return dict[area_key_variant] -> (geo_id, geo_name) for rows where include_ces=1.
    We store multiple key variants so that '110000', '0110000', and '1100000' all map.
    """
    def variants(code: str) -> set[str]:
        code = re.sub(r"\D", "", code or "")
        if not code:
            return set()
        v = set()
        # raw and no-leading-zeros
        v.add(code)
        v.add(code.lstrip("0"))
        # left-pad to common lengths used in CES
        for w in (5, 6, 7):
            v.add(code.zfill(w))
        # if the manifest used 6-digit state-like codes (e.g., 110000),
        # also add a *trailing* zero to match the state+area '1100000'
        if len(code) == 6:
            v.add(code + "0")
        return {x for x in v if x}

    out = {}
    with GEO_MANIFEST.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if (r.get("include_ces") or "0").strip() not in ("1", "true", "True"):
                continue
            geo  = (r.get("geo_id") or "").strip()
            name = (r.get("geo_name") or "").strip()
            raw  = (r.get("bls_ces_area_code") or "").strip()
            for k in variants(raw):
                out[k] = (geo, name)
    return out



# Loaded once for lookups
CES_AREA_MAP = load_ces_geo_targets()


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
    If there is no header line, synthesize one using the observed column order:
    series_id, area_code, (pad1), (pad2), industry_code, data_type_code, seasonal,
    end_year?, begin_year, begin_period, end_year, end_period
    """
    rows = []
    default_header = [
        "series_id",
        "area_code",
        "pad1",
        "pad2",
        "industry_code",
        "data_type_code",
        "seasonal",
        "pad3",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
    ]
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        header = None
        first_line_buffer = None
        for raw in fh:
            line = raw.rstrip("\n")
            if not line:
                continue
            parts = re.split(r"\s*\t\s*|\s{2,}|\s+", line.strip())

            # Detect header: only treat as header if the first token literally says "series_id"
            if header is None:
                if parts and parts[0].lower() == "series_id":
                    header = [c.strip().lower() for c in parts]
                    continue
                else:
                    header = default_header[:]          # no header present → use default
                    first_line_buffer = parts           # keep this first data line
                    # FALL THROUGH to row handling below after we have a header

            # if the first data line was buffered, process it first
            if first_line_buffer is not None:
                parts_use = first_line_buffer
                first_line_buffer = None
            else:
                parts_use = parts

            # pad/truncate to header length
            if len(parts_use) < len(header):
                parts_use += [""] * (len(header) - len(parts_use))
            elif len(parts_use) > len(header):
                parts_use = parts_use[:len(header)]

            row = dict(zip(header, parts_use))
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

    # --- DEBUG: confirm state presence using state_code+area_code form ---
    # State series use state_code + '00000' in sm.series (e.g., 11 + 00000 -> 1100000)
    state_pairs = {("11","00000"), ("24","00000"), ("51","00000")}
    seen_pairs = {( (r.get("state_code") or "").strip(), (r.get("area_code") or "").strip() )
                  for r in sm_series_rows}
    missing_pairs = sorted([sc+ac for sc,ac in state_pairs if (sc,ac) not in seen_pairs])
    if missing_pairs:
        print(f"[ces:gen][debug] state pairs not found in sm.series (state_code+area_code): {missing_pairs}")
    else:
        print("[ces:gen][debug] state pairs present in sm.series")
    # --------------------------------------------------------------------

    for r in sm_series_rows:
        # Pull fields (robust to absent columns)
        series_id        = (r.get("series_id") or "").strip()
        seasonal         = (r.get("seasonal") or "").strip()
        supersector_code = (r.get("supersector_code") or "").strip()
        industry_code    = (r.get("industry_code") or "").strip()
        data_type_code   = (r.get("data_type_code") or "").strip()
        series_title     = (r.get("series_title") or "").strip()
        state_code       = (r.get("state_code") or "").strip()
        area_code        = (r.get("area_code") or "").strip()

        if not series_id or not area_code:
            continue

        # STRICT CES filter: headline supersector totals & All Employees (01)
        if seasonal not in TARGET_SEASONAL:
            continue
        if supersector_code not in TARGET_SUPERSECTOR:
            continue
        if industry_code not in TARGET_INDUSTRY:   # expect "00000000"
            continue
        if data_type_code not in TARGET_DATA_TYPE: # expect "01"
            continue

        # Map supersector to our metric_base (e.g., "ces_construction")
        metric_base = SUPERSECTOR_TO_METRIC_BASE.get(supersector_code)
        if not metric_base:
            continue

        # Map to our geo_id using tolerant keys:
        # - raw area_code (e.g. "47900" for MSAs)
        # - state_code+area_code for states (e.g. "11" + "00000" -> "1100000")
        # - zero-stripped and zero-filled variants
        sd = re.sub(r"\D", "", state_code)
        ad = re.sub(r"\D", "", area_code)

        def expand_keys(code: str) -> list[str]:
            out = set()
            out.add(code)
            out.add(code.lstrip("0"))
            for w in (5, 6, 7):
                out.add(code.zfill(w))
            if len(code) == 6:
                out.add(code + "0")  # 110000 -> 1100000
            return [k for k in out if k]

        geo_id, area_name = (None, None)
        candidates = []
        if ad:
            candidates.append(ad)             # pure area_code (MSA/CSA/city)
        if sd and ad:
            candidates.append(sd + ad)        # state rows look like 1100000, 2400000, 5100000

        for cand in candidates:
            for key in expand_keys(cand):
                geo_id, area_name = _pick_geo(key)
                if geo_id:
                    break
            if geo_id:
                break
        if not geo_id:
            continue

        want.append({
            "geo_id": geo_id,
            "series_id": series_id,
            "metric_base": metric_base,
            "seasonal": _seasonal_tag(seasonal),
            "name": series_title,   # may be empty for your file; that's fine
            "area": area_name
        })
    

    # De-dup and stable sort
    dedup = {(w["geo_id"], w["metric_base"], w["seasonal"]): w for w in want}
    rows = sorted(dedup.values(), key=lambda d: (d["geo_id"], d["metric_base"], d["series_id"]))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(
            f, fieldnames=["geo_id", "series_id", "metric_base", "seasonal", "name", "area"]
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
