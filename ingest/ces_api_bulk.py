# ingest/ces_api_bulk.py
import os, csv, time, json
from pathlib import Path
from datetime import date

import requests
import pandas as pd
import duckdb

GEN_PATH = Path("config/ces_series.generated.csv")
DB_PATH  = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_KEY = (os.getenv("BLS_API_KEY") or "").strip()

# Optional: handy filter while debugging, comma-sep geo_id list
FILTER_GEOS = set(
    g.strip().lower()
    for g in (os.getenv("CES_FILTER_GEOS", "").split(",")
              if os.getenv("CES_FILTER_GEOS") else [])
)

# ---------- helpers ----------

def seasonal_suffix_from_sid(series_id: str) -> str:
    """SMS* => SA, SMU* => NSA."""
    s = (series_id or "").upper().strip()
    if s.startswith("SMS"):
        return "sa"
    if s.startswith("SMU"):
        return "nsa"
    # Default conservative
    return "nsa"

def metric_id_from_row(seasonal_tag: str) -> str:
    """Only one CES base metric in this phase: total nonfarm, all employees."""
    base = "ces_total_nonfarm"
    sfx  = "_sa" if (seasonal_tag or "").lower() == "sa" else "_nsa"
    return base + sfx

def ensure_dims(con: duckdb.DuckDBPyConnection, metric_ids: list[str]):
    # Source (idempotent)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_source(
      source_id TEXT PRIMARY KEY, name TEXT, url TEXT, cadence TEXT, license TEXT
    );
    INSERT INTO dim_source(source_id, name, url, cadence, license)
    SELECT 'ces','BLS Current Employment Statistics',
           'https://www.bls.gov/ces/','monthly','public'
    WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='ces');
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_metric(
      metric_id TEXT PRIMARY KEY, name TEXT, frequency TEXT, unit TEXT, category TEXT
    );
    """)

    # CES metrics we expect
    meta = {
        "ces_total_nonfarm_sa":  ("Total Nonfarm Employment", "monthly", "persons", "labor"),
        "ces_total_nonfarm_nsa": ("Total Nonfarm Employment", "monthly", "persons", "labor"),
    }

    for mid in set(metric_ids):
        name, freq, unit, cat = meta.get(mid, ("CES Series", "monthly", "value", "labor"))
        con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT ?,?,?,?,?
        WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?)
        """, [mid, name, freq, unit, cat, mid])

def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        return

    # ensure fact table
    con.execute("""
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
    # dedupe
    df = (df.sort_values(["geo_id","metric_id","date","property_type_id"])
            .drop_duplicates(subset=["geo_id","metric_id","date","property_type_id"], keep="last"))

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

def fetch_series(series_ids: list[str]) -> list[dict]:
    """Call BLS timeseries endpoint. Include annualaverage=True then drop M13 later."""
    payload = {
        "seriesid": series_ids,
        "startyear": "1990",                # CES typically starts ~1990/1991 for locals
        "endyear": str(date.today().year),
        "annualaverage": True,
    }
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY
        print(f"[ces] using BLS key: yes (len={len(BLS_KEY)})")
    else:
        print("[ces] using BLS key: no (public quota)")

    r = requests.post(BLS_API, json=payload, timeout=60)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS error: {j}")
    return j["Results"]["series"]

def to_df(series_block: list[dict], sid_to_meta: dict) -> pd.DataFrame:
    rows = []
    for s in series_block:
        sid = s.get("seriesID")
        meta = sid_to_meta.get(sid, {})
        # count real monthly rows per year to decide on keeping/dropping M13
        months_by_year = {}
        for d in s.get("data", []):
            p = str(d.get("period",""))
            if p.startswith("M") and p != "M13":
                y = int(d["year"])
                months_by_year[y] = months_by_year.get(y, 0) + 1

        for d in s.get("data", []):
            p = str(d.get("period",""))
            if not p.startswith("M"):
                continue

            y = int(d["year"])

            # drop annual M13 if monthly exists that year
            if p == "M13":
                if months_by_year.get(y, 0) > 0:
                    continue
                dt = pd.Timestamp(year=y, month=12, day=31).date()
            else:
                m = int(p[1:])
                if not (1 <= m <= 12):
                    continue
                dt = (pd.Timestamp(year=y, month=m, day=1)
                        .to_period("M").to_timestamp("M").date())

            try:
                val = float(d["value"])
            except Exception:
                continue

            rows.append({
                "geo_id":           meta.get("geo_id"),
                "metric_id":        meta.get("metric_id"),
                "date":             dt,
                "value":            val,
                "source_id":        "ces",
                "property_type_id": "all",
                "series_id":        sid,
            })
    return pd.DataFrame(rows)

# ---------- main ----------

def main():
    print("[ces] START ces_api_bulk")

    if not GEN_PATH.exists():
        raise SystemExit("[ces] missing config/ces_series.generated.csv — run ces_expand_spec.py first.")

    # Read the generated CES config and prepare series/meta
    rows, series_ids = [], []
    sid_to_meta = {}

    with GEN_PATH.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if not r: continue
            sid   = (r.get("series_id") or "").strip()
            geo   = (r.get("geo_id") or "").strip()
            if not sid or not geo:
                continue
            if FILTER_GEOS and geo.lower() not in FILTER_GEOS:
                continue

            # prefer seasonal from series_id, fallback to CSV
            sfx = seasonal_suffix_from_sid(sid)
            if sfx not in ("sa","nsa"):
                sfx = (r.get("seasonal") or "NSA").strip().lower()

            mid = metric_id_from_row("SA" if sfx=="sa" else "NSA")

            series_ids.append(sid)
            sid_to_meta[sid] = {
                "geo_id": geo,
                "metric_id": mid,
            }
            rows.append(r)

    if not series_ids:
        print("[ces] no series to fetch (check include flags or filters).")
        return

    print(f"[ces] total series planned: {len(series_ids)}")
    # Fetch in chunks of 50
    dfs = []
    for i in range(0, len(series_ids), 50):
        chunk = series_ids[i:i+50]
        print(f"[ces] fetching {len(chunk)} series …")
        series_block = fetch_series(chunk)

        # logging count
        for s in series_block:
            sid = s["seriesID"]
            n = sum(1 for d in s.get("data", []) if str(d.get("period","")).startswith("M"))
            print(f"[ces] fetched {n:4d} rows for {sid} -> {sid_to_meta.get(sid,{}).get('metric_id')}")

        dfs.append(to_df(series_block, sid_to_meta))
        time.sleep(0.5)

    all_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if all_df.empty:
        print("[ces] no rows returned.")
        return

    # create basic dims/tables + upsert
    con = duckdb.connect(DB_PATH)
    ensure_dims(con, all_df["metric_id"].unique().tolist())

    # ensure dim_market minimal entries
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_market(geo_id TEXT PRIMARY KEY, name TEXT, type TEXT, fips TEXT);
    """)
    mkts = (
        all_df[["geo_id"]].drop_duplicates()
        .assign(name=lambda d: d["geo_id"], type=None, fips=None)
    )
    con.register("mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id,name,type,fips)
    SELECT geo_id,name,type,fips FROM mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market)
    """)

    upsert(con, all_df)

    # summary
    print(con.execute("""
      SELECT geo_id, metric_id, MIN(date) AS first, MAX(date) AS last, COUNT(*) AS n
      FROM fact_timeseries
      WHERE metric_id LIKE 'ces_%'
      GROUP BY 1,2
      ORDER BY 1,2
    """).fetchdf())

    print("[ces] DONE")

    con.close()

if __name__ == "__main__":
    main()
