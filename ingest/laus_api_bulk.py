# ingest/laus_api_bulk.py
import os, json, time, csv
import requests
import pandas as pd
import duckdb

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
    if sid.startswith("LASST"):  # Seasonally Adjusted
        return "sa"
    if sid.startswith("LAUST"):  # Not Seasonally Adjusted
        return "nsa"
    return "nsa"  # safe default



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



def fetch_series(series_ids):
    payload = {"seriesid": series_ids}
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY
        print(f"[laus] using BLS key: yes (len={len(BLS_KEY)})")
    else:
        print("[laus] using BLS key: no (public quota)")

    r = requests.post(BLS_API, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "REQUEST_SUCCEEDED":
        # bubble up the exact API message so we don’t have to guess next time
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

            metric_id = make_metric_id(
                meta.get("metric_base", "unemployment_rate"),
                meta.get("seasonal", "NSA")
            )

            rows.append({
                "geo_id":         meta.get("geo_id"),
                "metric_id":      metric_id,           # <-- SA/NSA suffix here
                "date":           date,
                "value":          val,
                "source_id":      "laus",              # keep consistent with your dim_source insert
                "property_type_id": "all",
                "series_id":      sid,
                "seasonal":       meta.get("seasonal", "NSA"),
            })
    return pd.DataFrame(rows)



def ensure_dims(con: duckdb.DuckDBPyConnection, metric_ids=None):
    """
    Ensure dim_source and dim_metric rows exist.
    If metric_ids is provided, insert just those metric_ids with sensible names.
    Otherwise, insert the SA set (idempotent).
    """
    # source
    con.execute("""
    INSERT INTO dim_source(source_id, name, url, cadence, license)
    SELECT 'laus','BLS Local Area Unemployment Statistics',
           'https://www.bls.gov/lau/','monthly','public'
    WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='laus')
    """)

    # normalize incoming list -> unique list of strings
    mids = []
    if metric_ids is not None:
        mids = sorted(set(str(m) for m in metric_ids if m))

    def name_of(mid: str) -> tuple[str, str, str, str]:
        # (name, frequency, unit, category)
        base = mid.lower()
        if base.startswith("laus_unemployment_rate"):
            return ("Unemployment Rate (LAUS)", "monthly", "percent", "labor")
        if base.startswith("laus_unemployment"):
            return ("Unemployed (LAUS)", "monthly", "persons", "labor")
        if base.startswith("laus_employment"):
            return ("Employment (LAUS)", "monthly", "persons", "labor")
        if base.startswith("laus_labor_force"):
            return ("Labor Force (LAUS)", "monthly", "persons", "labor")
        # fallback
        return (mid, "monthly", "units", "labor")

    if mids:
        # insert only the metrics we actually have
        for mid in mids:
            nm, freq, unit, cat = name_of(mid)
            con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, ?, ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id = ?)
            """, [mid, nm, freq, unit, cat, mid])
    else:
        # safe defaults (idempotent)
        defaults = [
            "laus_unemployment_rate_sa",
            "laus_unemployment_rate_nsa",
            "laus_unemployment_sa",
            "laus_unemployment_nsa",
            "laus_employment_sa",
            "laus_employment_nsa",
            "laus_labor_force_sa",
            "laus_labor_force_nsa",
        ]
        for mid in defaults:
            nm, freq, unit, cat = name_of(mid)
            con.execute("""
            INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
            SELECT ?, ?, ?, ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id = ?)
            """, [mid, nm, freq, unit, cat, mid])



def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    if df.empty: return
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
    cfg_path = "config/laus_series.csv"
    if not os.path.exists(cfg_path):
        raise SystemExit(f"[laus] missing {cfg_path}")

    # read config and group intended rows by series id
    rows, series_ids = [], []
    sid_to_rowmeta = {}
    with open("config/laus_series.csv", newline="") as f:
        for r in csv.DictReader(f):
            # skip commented/blank rows defensively
            if not r or (r.get("geo_id", "").strip().startswith("#")):
                continue
            sid = (r.get("series_id") or "").strip()
            if not sid:
                print(f"[laus] skip row with empty series_id: {r}")
                continue
    
            base = (r.get("metric_base") or r.get("metric_id") or "laus_unemployment_rate").strip()
            suffix = seasonal_suffix(sid, r.get("seasonal"))
            metric_id = f"{base}_{suffix}"
    
            # optional validation warning if CSV says the opposite
            s_raw = (r.get("seasonal") or "").strip().upper()
            if s_raw in ("SA","S") and suffix != "sa":
                print(f"[laus] ⚠️ CSV seasonal=SA but series_id looks NSA: {sid}")
            if s_raw in ("NSA","U") and suffix != "nsa":
                print(f"[laus] ⚠️ CSV seasonal=NSA but series_id looks SA: {sid}")
    
            series_ids.append(sid)
            sid_to_rowmeta[sid] = {
                "geo_id": (r.get("geo_id") or "").strip(),
                "metric_id": metric_id,
            }
            rows.append(r)

            print("[laus] planned series + mapped metric_id:")
            for sid in series_ids:
                print("  ", sid, "->", sid_to_rowmeta[sid]["metric_id"])

    
    if not series_ids:
        raise SystemExit("[laus] no series_id entries found in config/laus_series.csv")

    

    # batch up to 50 series per API call
    dfs = []
    for i in range(0, len(series_ids), 50):
        chunk = series_ids[i:i+50]
        print(f"[laus] fetching {len(chunk)} series…")
        series_block = fetch_series(chunk)

        # DEBUG: count data points per SID
        for s in series_block:
            sid = s["seriesID"]
            n = sum(1 for d in s.get("data", []) if str(d.get("period","")).startswith("M"))
            print(f"[laus] fetched {n:4d} monthly rows for {sid} -> {sid_to_rowmeta.get(sid,{}).get('metric_id')}")
        
        dfs.append(to_df(series_block, sid_to_rowmeta))
        time.sleep(0.5)  # small courtesy pause

    
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

    # NEW: pass the distinct metric_ids we ingested so we add SA/NSA rows
    ensure_dims(con, all_df["metric_id"])

    
    con.register("mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id,name,type,fips)
    SELECT geo_id,name,type,fips FROM mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market)
    """)
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
