# ingest/laus_api_bulk.py
import os, json, time, csv
import requests
import pandas as pd
import duckdb
from pathlib import Path

from datetime import date

# add near imports
from io import StringIO

import time
import requests

# Where ensure_bls_files() writes the flat files
BLS_DIR = Path("config/bls")


# Remapping stale county/city SIDs to parent areas (disabled)
REMAP_STALE = bool(int(os.getenv("LAUS_REMAP", "0")))


LA_SERIES_URL = "https://download.bls.gov/pub/time.series/la/la.series"



from glob import glob



def fetch_lau_from_files(series_ids: list[str]):
    """
    Build an API-shaped block (Results.series) for LAU SIDs by reading the flat files
    under config/bls/la.data.*.* that ensure_bls_files() synced.
    """
    # Lazy imports in case this fn is moved around later
    import pandas as pd

    # Find all la.data.*.* files (County, City, MSA, MetroDiv, CSA, etc.)
    files = sorted(BLS_DIR.glob("la.data.*.*"))
    if not files:
        raise FileNotFoundError(f"No LAU data files found under {BLS_DIR} (expected la.data.*.*)")

    # We’ll parse line-by-line; these are tab-delimited with a header row
    wanted = set(s.strip() for s in series_ids)
    rows_by_sid = {sid: [] for sid in wanted}

    for p in files:
        with p.open("r", encoding="utf-8", errors="replace") as f:
            header = True
            for line in f:
                if header:
                    header = False
                    continue
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                # canonical columns: series_id, year, period, value, footnote_codes
                if len(parts) < 4:
                    continue
                sid = parts[0].strip()
                if sid not in wanted:
                    continue

                year   = parts[1].strip()
                period = parts[2].strip()   # 'M01'..'M13'
                value  = parts[3].strip()

                # match the API’s “monthly only” behavior (skip annual average M13)
                if not period.startswith("M") or period == "M13":
                    continue

                rows_by_sid[sid].append({
                    "year":   year,
                    "period": period,
                    "value":  value,
                })

    # Return API-shaped list
    out = []
    for sid in series_ids:
        out.append({
            "seriesID": sid,
            "data": rows_by_sid.get(sid, []),
        })
    return out



def fetch_series_any(series_ids: list[str]):
    """Route LAS* to API; route LAU* to flat files."""
    las = [s for s in series_ids if s.upper().startswith("LAS")]
    lau = [s for s in series_ids if s.upper().startswith("LAU")]

    blocks = []
    if las:
        blocks.extend(fetch_series(las))             # existing API path for LAS
    if lau:
        blocks.extend(fetch_lau_from_files(lau))     # file-based path for LAU
    return blocks





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
        "startyear": "1976",
        "endyear": str(date.today().year),
        "annualaverage": True,   # <-- include annual averages (M13)
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

        # Count true monthly rows per year so we can suppress M13 if months exist
        months_by_year = {}
        for item in s.get("data", []):
            p = str(item.get("period", ""))
            if p.startswith("M") and p != "M13":
                y = int(item["year"])
                months_by_year[y] = months_by_year.get(y, 0) + 1

        for item in s.get("data", []):
            period = str(item.get("period", ""))
            if not period.startswith("M"):
                continue  # skip non-monthly-like rows

            year = int(item["year"])

            # Annual average rows
            if period == "M13":
                # Keep M13 only when there are no true monthly rows in that year
                if months_by_year.get(year, 0) > 0:
                    continue
                dt = pd.Timestamp(year=year, month=12, day=31).date()
            else:
                # True monthly rows
                month = int(period[1:])
                if not (1 <= month <= 12):
                    continue
                dt = (pd.Timestamp(year=year, month=month, day=1)
                        .to_period("M").to_timestamp("M").date())

            try:
                val = float(item["value"])
            except Exception:
                continue

            metric_id = meta.get("metric_id") or "laus_unemployment_rate_nsa"

            rows.append({
                "geo_id":           meta.get("geo_id"),
                "metric_id":        metric_id,
                "date":             dt,
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



if os.getenv("LAUS_PROBE_ONE"):
    import json
    sid = os.getenv("LAUS_PROBE_ONE")
    print("[probe] will fetch", sid)
    payload = {
        "seriesid": [sid],
        "startyear": "1976",
        "endyear": str(date.today().year),
        "annualaverage": True,
    }
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY
    print("[probe] payload:", json.dumps(payload))

    r = requests.post(BLS_API, json=payload, timeout=60)
    print("[probe] status", r.status_code)
    r.raise_for_status()
    j = r.json()
    series = j.get("Results", {}).get("series", [])
    if not series:
        print("[probe] no series returned:", j)
    else:
        data = series[0].get("data", [])
        years = sorted({int(d["year"]) for d in data if str(d.get("period","")).startswith("M")})
        print("[probe] year span:", (min(years) if years else None), "→", (max(years) if years else None), "count:", len([1 for d in data if str(d.get("period","")).startswith("M")]))
    raise SystemExit(0)



def main():
    print("[laus] START laus_api_bulk")

    # prefer generated, fall back to hand-maintained
    cfg_path = "config/laus_series.generated.csv"
    if not Path(cfg_path).exists():
        cfg_path = "config/laus_series.csv"
    print(f"[laus] using config: {cfg_path}")

    # optional: env-driven subset filter (handy for debugging)
    FILTER_GEOS = set(
        g.strip().lower()
        for g in (os.getenv("LAUS_FILTER_GEOS", "").split(",")
                  if os.getenv("LAUS_FILTER_GEOS") else [])
    )
    if FILTER_GEOS:
        print(f"[laus] FILTER_GEOS active -> {sorted(FILTER_GEOS)}")

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

            # apply optional geo filter
            if FILTER_GEOS and geo_id.lower() not in FILTER_GEOS:
                continue

            # Robust metric resolution:
            # 1) infer base from series_id tail (003/004/005/006)
            base_auto = base_from_sid(sid)
            # 2) prefer CSV column if valid, else use inferred
            base_csv  = (r.get("metric_base") or "").strip() or base_auto
            if base_csv not in {
                "laus_unemployment_rate","laus_unemployment","laus_employment","laus_labor_force"
            }:
                base_csv = base_auto

            # 3) SA/NSA from series_id (LAS*/LAU*); fallback to CSV `seasonal`
            sfx = suffix_from_sid(sid)
            if sfx not in ("sa","nsa"):
                sfx = sfx_from_csv(r.get("seasonal"))

            metric_id = f"{base_csv}_{sfx}"

            series_ids.append(sid)
            sid_to_rowmeta[sid] = {
                "geo_id": geo_id,
                "metric_id": metric_id,
                "metric_base": base_csv,     # optional
                "seasonal": sfx,             # optional ("sa"/"nsa")
                "name": (r.get("name") or "").strip(),  # optional
            }
            rows.append(r)

    if not series_ids:
        raise SystemExit("[laus] no series_id entries found in config CSV")

    print("[laus] planned series + mapped metric_id:")
    for sid in series_ids:
        print(f"  {sid} -> {sid_to_rowmeta[sid]['metric_id']}")
    print(f"[laus] total series planned: {len(series_ids)}")

    # batch up to 50 series per API call
    dfs = []
    for i in range(0, len(series_ids), 50):
        chunk = series_ids[i:i+50]
        print(f"[laus] fetching {len(chunk)} series…")
        series_block = fetch_series_any(chunk)


        for s in series_block:
            sid = s["seriesID"]
        
            # mirror to_df’s keep rules
            months_by_year = {}
            for d in s.get("data", []):
                p = str(d.get("period",""))
                if p.startswith("M") and p != "M13":
                    y = int(d["year"])
                    months_by_year[y] = months_by_year.get(y, 0) + 1
        
            kept = 0
            for d in s.get("data", []):
                p = str(d.get("period",""))
                if not p.startswith("M"):
                    continue
                if p == "M13" and months_by_year.get(int(d["year"]), 0) > 0:
                    continue
                kept += 1
        
            print(f"[laus] fetched {kept:4d} rows used for {sid} -> {sid_to_rowmeta.get(sid,{}).get('metric_id')}")

        dfs.append(to_df(series_block, sid_to_rowmeta))
        time.sleep(0.5)  # small courtesy pause

    all_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    print("[laus] sample of metric_id counts (pre-upsert):")
    if all_df.empty:
        print("[laus] no rows returned.")
        return
    print(all_df.groupby("metric_id").size().sort_index().to_string())

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

    # de-dupe just in case
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

    print("[laus] session summary (inserted this run):")
    print(all_df.groupby(["geo_id", "metric_id"])
               .agg(first=("date", "min"), last=("date", "max"), n=("date", "size"))
               .sort_values(["geo_id", "metric_id"])
               .to_string())

    con.close()

if __name__ == "__main__":
    main()

