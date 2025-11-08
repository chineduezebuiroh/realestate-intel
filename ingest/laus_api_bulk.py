# ingest/laus_api_bulk.py
import os, json, time, csv
import requests
import pandas as pd
import duckdb


def suffix_from_sid(series_id: str) -> str:
    sid = (series_id or "").upper().strip()
    if sid.startswith("LASST"):  # Seasonally Adjusted
        return "sa"
    if sid.startswith("LAUST"):  # Not Seasonally Adjusted
        return "nsa"
    # Fallback: use CSV seasonal field later
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
            sid_to_rowmeta[sid] = {
                "geo_id": geo_id,
                "metric_id": metric_id,
                "metric_base": base_csv,   # optional
                "seasonal": sfx,           # optional ("sa"/"nsa")
            }
            
            rows.append(r)
    
    print("[laus] planned series + mapped metric_id:")
    for sid in series_ids:
        print(f"  {sid} -> {sid_to_rowmeta[sid]['metric_id']}")


    

    
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
