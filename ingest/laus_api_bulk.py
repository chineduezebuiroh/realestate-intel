# ingest/laus_api_bulk.py
import os, json, time, csv
import requests
import pandas as pd
import duckdb
from pathlib import Path

from datetime import date

# add near imports
from io import StringIO

LA_SERIES_URL = "https://download.bls.gov/pub/time.series/la/la.series"

BLS_BASE = "https://download.bls.gov/pub/time.series/la/"
BLS_DIR  = Path("config/bls")
LA_SERIES_PATH = BLS_DIR / "la.series"

# --- Final-resort manual redirects for known legacy locals ---
# If an area_code has no modern coverage, fall back to a parent area_code we know is live.
MANUAL_AREA_REDIRECT = {
    # Alexandria city
    "CN5151000000000": "MT1147900000000",  # -> Washington–Arlington–Alexandria MSA
    "CT5101000000000": "MT1147900000000",  # some SIDs use CT for the city

    # Arlington County
    "CN5101300000000": "MT1147900000000",
}



import time
import requests

def _http_get(url: str, timeout=60) -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    if r.status_code == 403:
        time.sleep(0.6)
        r = requests.get(url, headers=headers, timeout=timeout)
    if r.ok:
        return r.content

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


LA_AREA_PATH = BLS_DIR / "la.area"



def load_la_series_index() -> pd.DataFrame:
    """
    Load la.series from local cache if present; otherwise fetch with a robust request.
    Parse with a tolerant tab regex and normalize fields we rely on.
    """
    BLS_DIR.mkdir(parents=True, exist_ok=True)

    if LA_SERIES_PATH.exists() and LA_SERIES_PATH.stat().st_size > 0:
        text = LA_SERIES_PATH.read_text()
    else:
        data = _http_get(BLS_BASE + "la.series", timeout=60)
        text = data.decode("utf-8", errors="replace")
        LA_SERIES_PATH.write_text(text)

    # Tolerant tab parsing; handles multiple tabs and stray spaces
    df = pd.read_csv(StringIO(text), sep=r"\t+", engine="python", dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    # Trim + normalize key columns
    for c in ("series_id", "area_code", "measure_code", "seasonal"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # ✅ pad measure codes to 3 digits so they match '003'..'006'
    if "measure_code" in df.columns:
        df["measure_code"] = df["measure_code"].str.zfill(3)

    # Normalize seasonal to single-letter codes we filter against ('S'/'U')
    if "seasonal" in df.columns:
        df["seasonal"] = (df["seasonal"].str.upper()
                          .replace({"SA": "S", "NSA": "U"}))

    # Cast year columns if present
    for c in ("begin_year", "end_year"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def load_la_area() -> pd.DataFrame:
    data = _http_get(BLS_BASE + "la.area", timeout=60)
    text = data.decode("utf-8", errors="replace")
    df = pd.read_csv(StringIO(text), sep=r"\t+", engine="python", dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    for c in ("area_code", "area_text"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


#def load_la_area() -> pd.DataFrame:
    """
    Load la.area from local cache (config/bls/la.area) written by ensure_bls_files().
    Parse with tolerant tab regex and keep only the columns we need.
    """
    """    
    path = BLS_DIR / "la.area"
    if not path.exists() or path.stat().st_size == 0:
        # Reuse your robust fetcher
        data = _http_get(BLS_BASE + "la.area", timeout=60)
        path.write_bytes(data)
    text = path.read_text(encoding="utf-8", errors="replace")
    df = pd.read_csv(StringIO(text), sep=r"\t+", engine="python", dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    for c in ("area_code", "area_text"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df[["area_code", "area_text"]]
    """




def parse_sid(sid: str):
    sid = (sid or "").strip().upper()
    area_code = sid[3:-3]           # after 'LAS'/'LAU' up to last 3 measure digits
    measure_code = sid[-3:]
    seasonal = "S" if sid.startswith("LAS") else "U"
    return area_code, measure_code, seasonal



#def _max_year_from_block(block) -> int:
    """Get the max data year from a single-series API response block."""
    """
    if not block:
        return -1
    s = block[0]  # one series
    mons = [d for d in s.get("data", []) if str(d.get("period","")).startswith("M")]
    if not mons:
        return -1
    try:
        return max(int(d["year"]) for d in mons if d.get("year"))
    except Exception:
        return -1
    """

def _max_year_from_block(series_block) -> int:
    years = []
    for s in series_block or []:
        for d in s.get("data", []):
            if str(d.get("period","")).startswith("M"):
                try:
                    years.append(int(d["year"]))
                except:
                    pass
    return max(years) if years else -1



#def pick_by_api_max_year(series_ids: list[str]) -> tuple[str|None, int]:
    """Fetch a small batch of candidate SIDs and return (best_sid, max_year)."""
    """
    if not series_ids:
        return (None, -1)
    # BLS allows batching; keep it small to be polite
    block = fetch_series(series_ids[:50])
    best_sid, best_year = None, -1
    for s in block:
        sid = s.get("seriesID")
        my = _max_year_from_block([s])
        if my > best_year:
            best_sid, best_year = sid, my
    return best_sid, best_year
    """

def pick_by_api_max_year(series_ids: list[str]) -> tuple[str|None, int]:
    """Among series_ids, fetch each quickly and return (sid, max_year) with the best (latest) coverage."""
    best_sid, best_year = None, -1
    for sid in series_ids:
        blk = fetch_series([sid])
        y = _max_year_from_block(blk)
        if y > best_year:
            best_sid, best_year = sid, y
    return best_sid, best_year



def choose_latest_series_wide(
    la_series_df: pd.DataFrame,
    la_area_df: pd.DataFrame,
    wanted_name: str | None,
    measure_code: str,
    seasonal_SU: str,
    area_code_family: str | None = None,
) -> str | None:
    """Search more broadly by area name/family for a better (newer) series."""
    mc = str(measure_code).zfill(3)
    su = (seasonal_SU or "U").upper().replace("SA","S").replace("NSA","U")

    cand = la_series_df[
        (la_series_df["measure_code"].astype(str).str.zfill(3) == mc) &
        (la_series_df["seasonal"].astype(str).str.upper().replace({"SA":"S","NSA":"U"}) == su)
    ].copy()

    if area_code_family:
        cand = cand[cand["area_code"].astype(str).str.startswith(area_code_family)]

    la_area_tmp = la_area_df[["area_code","area_text"]].copy()
    la_area_tmp["area_text_lc"] = la_area_tmp["area_text"].str.lower().str.strip()
    cand = cand.merge(la_area_tmp, on="area_code", how="left")

    if wanted_name:
        target = wanted_name.lower().strip()
        cand["score"] = cand["area_text_lc"].fillna("").apply(
            lambda t: 3 if t == target else (2 if target in t else 0)
        )
        cand = cand[cand["score"] > 0]

    sids = cand["series_id"].dropna().unique().tolist()
    if not sids:
        return None

    sid, yr = pick_by_api_max_year(sids)
    return sid if yr >= 2010 else None



#def pick_live_for_area_code(la_series_df: pd.DataFrame, area_code: str, measure_code: str, seasonal_SU: str) -> tuple[str|None, int]:
    """Given an area_code, return (best_sid, max_year) chosen by actual API coverage."""
    """
    cand = la_series_df[
        (la_series_df["area_code"] == area_code) &
        (la_series_df["measure_code"].astype(str).str.zfill(3) == str(measure_code).zfill(3)) &
        (la_series_df["seasonal"].astype(str).str.upper().replace({"SA":"S","NSA":"U"}) == seasonal_SU)
    ]
    sids = cand["series_id"].dropna().tolist()
    if not sids:
        return (None, -1)
    return pick_by_api_max_year(sids)
    """
    
def pick_live_for_area_code(la_series_df: pd.DataFrame, area_code: str, measure_code: str, seasonal_SU: str) -> tuple[str|None, int]:
    """Given a specific area_code, return (best_sid, max_year) by actual API coverage."""
    mc = str(measure_code).zfill(3)
    su = (seasonal_SU or "U").upper().replace("SA","S").replace("NSA","U")
    cand = la_series_df[
        (la_series_df["area_code"] == area_code) &
        (la_series_df["measure_code"].astype(str).str.zfill(3) == mc) &
        (la_series_df["seasonal"].astype(str).str.upper().replace({"SA":"S","NSA":"U"}) == su)
    ]
    sids = cand["series_id"].dropna().tolist()
    if not sids:
        return (None, -1)
    return pick_by_api_max_year(sids)



def choose_latest_series(la_series_df, area_code, measure_code, seasonal, allow_sa_to_nsa_fallback=True):
    # exact seasonal first
    cand = la_series_df[
        (la_series_df["area_code"] == area_code) &
        (la_series_df["measure_code"] == measure_code) &
        (la_series_df["seasonal"] == seasonal)
    ].copy()
    if cand.empty and (seasonal == "S") and allow_sa_to_nsa_fallback:
        # fallback to NSA at the same area/measure
        cand = la_series_df[
            (la_series_df["area_code"] == area_code) &
            (la_series_df["measure_code"] == measure_code) &
            (la_series_df["seasonal"] == "U")
        ].copy()
    if cand.empty:
        return None

    # Prefer the one with the most recent end_year (treat NaN as "open-ended"/max)
    # If end_year is missing, prefer the one with the most recent begin_year as tie-break.
    cand["end_year_fill"] = cand["end_year"].fillna(9999)
    cand["begin_year_fill"] = cand["begin_year"].fillna(-1)
    cand = cand.sort_values(["end_year_fill","begin_year_fill"], ascending=[True, True])
    latest = cand.iloc[-1]  # last row after sort → newest span
    return latest["series_id"]



def _norm_area_name(x: str) -> str:
    x = (x or "").lower()
    x = x.replace(" city,", ",").replace(" county,", ",")
    x = x.replace(" city", "").replace(" county", "")
    return " ".join(x.split())

def candidate_sids_wide(
    la_series_df: pd.DataFrame,
    la_area_df: pd.DataFrame,
    wanted_name: str | None,
    measure_code: str,
    seasonal_SU: str,               # 'S' or 'U'
    area_code_family: str | None
) -> list[str]:
    s = la_series_df.copy()
    a = la_area_df.copy()
    a["area_text_lc"] = a["area_text"].astype(str).str.strip().str.lower()
    a["name_n"] = a["area_text_lc"].map(_norm_area_name)
    s["measure_code"] = s["measure_code"].astype(str).str.zfill(3)
    s["seasonal"] = s["seasonal"].astype(str).str.upper().replace({"SA":"S", "NSA":"U"})

    m = s.merge(a[["area_code","area_text","area_text_lc","name_n"]], on="area_code", how="left")

    cand = pd.DataFrame()
    if wanted_name:
        tn = _norm_area_name(wanted_name)
        c1 = m[m["name_n"] == tn]
        c2 = m[m["name_n"].str.contains(tn, na=False)] if c1.empty else pd.DataFrame()
        cand = pd.concat([c1, c2], ignore_index=True)

    if cand.empty and area_code_family:
        cand = m[m["area_code"].str.startswith(area_code_family)]

    # filter to the measure + seasonal we need
    cand = cand[(cand["measure_code"] == str(measure_code).zfill(3)) &
                (cand["seasonal"] == seasonal_SU)]

    # de-dup and return SIDs
    return list(dict.fromkeys(cand["series_id"].dropna().tolist()))




def needs_refresh(n_rows: int, first_date: pd.Timestamp | None, last_date: pd.Timestamp | None) -> bool:
    # You already added a detector; keep your logic.
    if n_rows == 0:
        return True
    if last_date is not None and pd.Timestamp(last_date).year < 2000:
        return True
    return False


def detect_stale_series(series_block):
    """Return a list of (sid, min_year, max_year, n_months) for series that don't cover the current year."""
    CY = date.today().year
    out = []
    for s in series_block:
        sid = s.get("seriesID")
        months = [d for d in s.get("data", []) if str(d.get("period","")).startswith("M")]
        if not months:
            out.append((sid, None, None, 0))
            continue
        years = [int(d["year"]) for d in months if d.get("year")]
        miny, maxy = min(years), max(years)
        n_months = len(months)
        if maxy < CY - 1:  # lagging well behind present
            out.append((sid, miny, maxy, n_months))
    return out



def suffix_from_sid(series_id: str) -> str:
    sid = (series_id or "").upper().strip()
    # Any 'LAS' prefix => Seasonally Adjusted; any 'LAU' => Not Seasonally Adjusted
    if sid.startswith("LAS"):
        return "sa"
    if sid.startswith("LAU"):
        return "nsa"
    return "nsa"



def base_from_sid(series_id: str) -> str:
    # last 3 digits map the LAUS measure
    tail = (series_id or "")[-3:]
    return {
        "003": "laus_unemployment_rate",
        "004": "laus_unemployment",
        "005": "laus_employment",
        "006": "laus_labor_force",
    }.get(tail, "laus_unemployment_rate")

def sfx_from_csv(seasonal: str) -> str:
    v = (seasonal or "").strip().upper()
    if v in ("SA","S"): return "sa"
    if v in ("NSA","U"): return "nsa"
    return "nsa"


BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_KEY = (os.getenv("BLS_API_KEY") or "").strip()
DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")



def seasonal_suffix(series_id: str, seasonal_field: str | None) -> str:
    s = (seasonal_field or "").strip().upper()
    if s in ("SA", "S"):
        return "sa"
    if s in ("NSA", "U"):
        return "nsa"
    sid = (series_id or "").strip().upper()
    if sid.startswith("LAS"):
        return "sa"
    if sid.startswith("LAU"):
        return "nsa"
    return "nsa"



# ---- seasonal + metric id helpers ----
BASE_METRIC_ALIAS = {
    # allow short names in CSV; feel free to expand
    "employment": "laus_employment",
    "labor_force": "laus_labor_force",
    "unemployment": "laus_unemployment",
    "unemployment_rate": "laus_unemployment_rate",
}

def normalize_base_metric(s: str) -> str:
    s = (s or "").strip().lower()
    s = BASE_METRIC_ALIAS.get(s, s)
    if not s.startswith("laus_"):
        s = "laus_" + s
    return s

def make_metric_id(base_metric: str, seasonal: str) -> str:
    # seasonal expected 'SA' or 'NSA'
    tag = "_sa" if (seasonal or "").upper() == "SA" else "_nsa"
    return normalize_base_metric(base_metric) + tag



from datetime import date
def fetch_series(series_ids):
    payload = {
        "seriesid": series_ids,
        "startyear": "1976",           # LAUS has history back to 1976
        "endyear": str(date.today().year),
    }
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY
        print(f"[laus] using BLS key: yes (len={len(BLS_KEY)})")
    else:
        print("[laus] using BLS key: no (public quota)")

    r = requests.post(BLS_API, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS error: {data}")
    return data["Results"]["series"]




def to_df(series_block, sid_to_rowmeta):
    rows = []
    for s in series_block:
        sid = s["seriesID"]
        meta = sid_to_rowmeta.get(sid, {})
        for item in s.get("data", []):
            period = item["period"]
            if not period.startswith("M"):  # skip annual 'M13' etc
                continue
            month = int(period[1:])
            year  = int(item["year"])
            date = (pd.Timestamp(year=year, month=month, day=1)
                      .to_period("M").to_timestamp("M").date())
            try:
                val = float(item["value"])
            except:
                continue

            # ✅ Use the metric_id we computed in main()
            metric_id = meta.get("metric_id")
            if not metric_id:
                # ultra-safe fallback (shouldn’t happen once main() fills meta)
                metric_id = "laus_unemployment_rate_nsa"

            rows.append({
                "geo_id":           meta.get("geo_id"),
                "metric_id":        metric_id,
                "date":             date,
                "value":            val,
                "source_id":        "laus",
                "property_type_id": "all",
                "series_id":        sid,
            })
    return pd.DataFrame(rows)



def ensure_dims(con: duckdb.DuckDBPyConnection, metric_ids_needed):
    # Source (idempotent)
    con.execute("""
    INSERT INTO dim_source(source_id, name, url, cadence, license)
    SELECT 'laus','BLS Local Area Unemployment Statistics',
           'https://www.bls.gov/lau/','monthly','public'
    WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='laus')
    """)

    # Name/unit/category per base; SA/NSA share same name/unit/category
    META = {
        "laus_unemployment_rate": ("Unemployment Rate", "percent", "labor"),
        "laus_unemployment":      ("Unemployment",      "persons", "labor"),
        "laus_employment":        ("Employment",        "persons", "labor"),
        "laus_labor_force":       ("Labor Force",       "persons", "labor"),
    }

    needed = set(str(m) for m in metric_ids_needed if m)
    for mid in sorted(needed):
        base = mid.rsplit("_", 1)[0]  # strip _sa/_nsa
        name, unit, cat = META.get(base, ("LAUS Series", "value", "labor"))
        con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT ?, ?, 'monthly', ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id = ?)
        """, [mid, name, unit, cat, mid])



def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        return

    # 🔒 ensure one row per 4-key (geo, metric, date, ptype)
    df = (
        df.sort_values(["geo_id","metric_id","date","property_type_id"])
          .drop_duplicates(
              subset=["geo_id","metric_id","date","property_type_id"],
              keep="last"
          )
    )

    con.register("df_stage", df[["geo_id","metric_id","date","property_type_id","value","source_id"]])
    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1 FROM df_stage s
      WHERE s.geo_id=f.geo_id AND s.metric_id=f.metric_id
        AND s.date=f.date AND s.property_type_id=f.property_type_id
    )
    """)
    con.execute("""
    INSERT INTO fact_timeseries(geo_id,metric_id,date,property_type_id,value,source_id)
    SELECT geo_id,metric_id,date,property_type_id,CAST(value AS DOUBLE),source_id
    FROM df_stage
    """)



def main():
    # prefer generated, fall back to hand-maintained
    cfg_path = "config/laus_series.generated.csv"
    if not Path(cfg_path).exists():
        cfg_path = "config/laus_series.csv"


    # read config and group intended rows by series id
    rows, series_ids = [], []
    sid_to_rowmeta = {}
    
    with open(cfg_path, newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if not r:
                continue
            # allow comment lines
            if (r.get("geo_id","").strip().startswith("#")
                or r.get("series_id","").strip().startswith("#")):
                continue
    
            sid = (r.get("series_id") or "").strip()
            if not sid:
                print(f"[laus] skip row with empty series_id: {r}")
                continue
    
            geo_id = (r.get("geo_id") or "").strip()
    
            # Robust metric resolution:
            # 1) infer base from series_id tail (003/004/005/006)
            base_auto = base_from_sid(sid)
            # 2) prefer CSV column if valid, else use inferred
            base_csv  = (r.get("metric_base") or "").strip() or base_auto
            if base_csv not in {
                "laus_unemployment_rate","laus_unemployment","laus_employment","laus_labor_force"
            }:
                base_csv = base_auto
    
            # 3) SA/NSA from series_id (LASST/LAUST); fallback to CSV `seasonal`
            sfx = suffix_from_sid(sid)
            if sfx not in ("sa","nsa"):
                sfx = sfx_from_csv(r.get("seasonal"))
    
            metric_id = f"{base_csv}_{sfx}"
    
            series_ids.append(sid)

            area_name = (r.get("notes") or r.get("name") or "").strip() #delete later? (handled below)
            sid_to_rowmeta[sid] = {
                "geo_id": geo_id,
                "metric_id": metric_id,
                "metric_base": base_csv,
                "seasonal": sfx,
                "area_name": (r.get("notes") or r.get("name") or r.get("geo_id") or "").strip(),

            }

            
            rows.append(r)
    
    print("[laus] planned series + mapped metric_id:")

    def _to_SU(sfx: str) -> str:
        s = (sfx or "").strip().lower()
        return "S" if s == "sa" else "U"


    ### --- CAN I DELETE THIS BELOW? ---
    # \/-\/-\/-\/-\/-\/-\/-\/-\/-\/-\/-\/
    
    # helper to map our 'sa'/'nsa' to BLS single-letter codes used in la.series
    def _to_bls_seasonal(sfx: str) -> str:
        s = (sfx or "").strip().lower()
        if s == "sa":  return "S"
        if s == "nsa": return "U"
        return "U"

    # /\-/\-/\-/\-/\-/\-/\-/\-/\-/\-/\-/\
    ### --- CAN I DELETE THIS ABOVE? ---
    
    
    for sid in series_ids:
        print(f"  {sid} -> {sid_to_rowmeta[sid]['metric_id']}")

    # Load the la.series catalog once so we can auto-upgrade stale SIDs
    la_series_df = load_la_series_index()
    la_area_df   = load_la_area()
    
    if not series_ids:
        raise SystemExit("[laus] no series_id entries found in config/laus_series.csv")

    

    # batch up to 50 series per API call
    dfs = []

    for i in range(0, len(series_ids), 50):
        chunk = series_ids[i:i+50]
        print(f"[laus] fetching {len(chunk)} series…")
        series_block = fetch_series(chunk)
    
        new_block = []
        for s in series_block:
            sid = s["seriesID"]
            monthly = [d for d in s.get("data", []) if str(d.get("period","")).startswith("M")]
            n = len(monthly)
            first = last = None
            if monthly:
                months = [pd.Timestamp(int(d["year"]), int(d["period"][1:]), 1) for d in monthly]
                first, last = min(months), max(months)

            if needs_refresh(n, first, last):
                area_code, measure_code, _seas = parse_sid(sid)
                meta = sid_to_rowmeta.get(sid, {})
                seas_SU = "S" if (meta.get("seasonal") or suffix_from_sid(sid)) == "sa" else "U"
        
                chosen_sid, chosen_year = (None, -1)
        
                # (1) Catalog successor at the SAME area_code (only accept if modern by API)
                cat_sid = choose_latest_series(la_series_df, area_code, measure_code, seas_SU, allow_sa_to_nsa_fallback=True)
                if cat_sid:
                    tb = fetch_series([cat_sid])
                    y = _max_year_from_block(tb)
                    if y >= 2010:
                        chosen_sid, chosen_year = cat_sid, y
        
                # (2) Wide search by name/family (still keep same seasonality)
                if not chosen_sid:
                    wanted_name = meta.get("area_name") or meta.get("geo_id")
                    fam = area_code[:2] if len(area_code) >= 2 else None   # e.g., 'CN', 'CT', 'DV', 'MT'
                    wsid = choose_latest_series_wide(
                        la_series_df, la_area_df,
                        wanted_name=wanted_name,
                        measure_code=measure_code,
                        seasonal_SU=seas_SU,
                        area_code_family=fam
                    )
                    if wsid:
                        tb = fetch_series([wsid])
                        y = _max_year_from_block(tb)
                        if y >= 2010:
                            chosen_sid, chosen_year = wsid, y
        
                # (3) Final resort: manual area redirect (parent area_code)
                if not chosen_sid:
                    parent_area = MANUAL_AREA_REDIRECT.get(area_code)
                    if parent_area:
                        alt_sid, alt_year = pick_live_for_area_code(la_series_df, parent_area, measure_code, seas_SU)
                        if alt_sid and alt_year >= 2010:
                            chosen_sid, chosen_year = alt_sid, alt_year
        
                if chosen_sid and chosen_sid != sid:
                    print(f"[laus] remapping stale {sid} → {chosen_sid} (last={last.date() if last is not None else None}, max_year={chosen_year})")
                    repl_block = fetch_series([chosen_sid])
                    if repl_block:
                        s = repl_block[0]  # replace the series object we keep
                        # transfer/update meta (keep the same geo_id!)
                        meta = sid_to_rowmeta.pop(sid, {}).copy()
                        new_sfx = suffix_from_sid(chosen_sid)
                        if new_sfx and new_sfx != (meta.get("seasonal") or "").lower():
                            base = meta.get("metric_base") or base_from_sid(chosen_sid)
                            meta["metric_id"] = f"{base}_{new_sfx}"
                            meta["seasonal"]  = new_sfx
                        sid_to_rowmeta[chosen_sid] = meta
                    else:
                        print(f"[laus] WARNING: replacement fetch failed for {chosen_sid}; keeping original {sid}")
  
    
                if chosen_sid and chosen_sid != sid:
                    print(f"[laus] remapping stale {sid} → {chosen_sid} (prev last={last.date() if last is not None else None}, new last≈{chosen_year})")
                    # swap in series_ids array so later duplicates use the new id
                    try:
                        idx = series_ids.index(sid)
                        series_ids[idx] = chosen_sid
                    except ValueError:
                        pass
    
                    # fetch replacement data for the block and update meta
                    repl = fetch_series([chosen_sid])
                    if repl:
                        s = repl[0]
                        meta_old = sid_to_rowmeta.pop(sid, {}).copy()
                        old_sfx = (meta_old.get("seasonal") or "").lower()
                        new_sfx = suffix_from_sid(chosen_sid)
                        if new_sfx and new_sfx != old_sfx:
                            base = meta_old.get("metric_base") or base_from_sid(chosen_sid)
                            meta_old["metric_id"] = f"{base}_{new_sfx}"
                            meta_old["seasonal"] = new_sfx
                        # capture a friendly name to help future wide lookups
                        if "area_name" not in meta_old:
                            meta_old["area_name"] = area_name
                        sid_to_rowmeta[chosen_sid] = meta_old
                    else:
                        print(f"[laus] WARNING: could not fetch replacement for {chosen_sid}; keeping original {sid}")
    
            keep_sid = s["seriesID"]
            print(f"[laus] fetched {len([d for d in s.get('data', []) if str(d.get('period','')).startswith('M')]):4d} "
                  f"monthly rows for {keep_sid} -> {sid_to_rowmeta.get(keep_sid,{}).get('metric_id')}")
            new_block.append(s)
    
        series_block = new_block
        dfs.append(to_df(series_block, sid_to_rowmeta))
        time.sleep(0.5)
    

    
    all_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    print("[laus] sample of metric_id counts (pre-upsert):")
    print(all_df.groupby("metric_id").size().sort_index().to_string())

    if all_df.empty:
        print("[laus] no rows returned.")
        return

    # ensure markets exist minimally (name fallback)
    mkts = (
        all_df[["geo_id"]].drop_duplicates()
        .assign(name=lambda d: d["geo_id"],
                type=lambda d: d["geo_id"].str.split("_").str[-1],
                fips=None)
    )
    con = duckdb.connect(DB_PATH)

    # ensure tables exist (idempotent)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_source(source_id TEXT PRIMARY KEY, name TEXT, url TEXT, cadence TEXT, license TEXT);
    CREATE TABLE IF NOT EXISTS dim_market(geo_id TEXT PRIMARY KEY, name TEXT, type TEXT, fips TEXT);
    CREATE TABLE IF NOT EXISTS dim_metric(metric_id TEXT PRIMARY KEY, name TEXT, frequency TEXT, unit TEXT, category TEXT);
    CREATE TABLE IF NOT EXISTS dim_property_type(property_type_id TEXT PRIMARY KEY, name TEXT, "group" TEXT);
    CREATE TABLE IF NOT EXISTS fact_timeseries(
      geo_id TEXT NOT NULL,
      metric_id TEXT NOT NULL,
      date DATE NOT NULL,
      property_type_id TEXT NOT NULL DEFAULT 'all',
      value DOUBLE,
      source_id TEXT,
      PRIMARY KEY (geo_id, metric_id, date, property_type_id)
    );
    """)

    ensure_dims(con, all_df["metric_id"].unique())


    
    con.register("mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id,name,type,fips)
    SELECT geo_id,name,type,fips FROM mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market)
    """)

    # … after building all_df …
    all_df = (
        all_df.sort_values(["geo_id","metric_id","date","property_type_id"])
              .drop_duplicates(
                  subset=["geo_id","metric_id","date","property_type_id"],
                  keep="last"
              )
    )

    upsert(con, all_df)

    # quick summary
    print(con.execute("""
      SELECT geo_id, metric_id, MIN(date) AS first, MAX(date) AS last, COUNT(*) AS n
      FROM fact_timeseries
      WHERE metric_id LIKE 'laus_%'
      GROUP BY 1,2
      ORDER BY 1,2
    """).fetchdf())

    con.close()

if __name__ == "__main__":
    main()
