# ingest/laus_api_bulk.py
import os, json, time, csv
import requests
import pandas as pd
import duckdb

BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_KEY = os.getenv("BLS_API_KEY")  # optional
DB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

def fetch_series(series_ids):
    # BLS allows up to 50 series per request
    payload = {"seriesid": series_ids}
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY
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
            # BLS sends year, period ('M01'..'M12'), value as string
            period = item["period"]
            if not period.startswith("M"):  # skip annual 'M13' etc if present
                continue
            month = int(period[1:])
            year  = int(item["year"])
            # month end date
            date = pd.Timestamp(year=year, month=month, day=1).to_period("M").to_timestamp("M").date()
            try:
                val = float(item["value"])
            except:
                continue
            rows.append({
                "geo_id": meta.get("geo_id"),
                "metric_id": meta.get("metric_id", "laus_unemployment_rate"),
                "date": date,
                "value": val,
                "source_id": "laus",
                "property_type_id": "all"
            })
    return pd.DataFrame(rows)

def ensure_dims(con: duckdb.DuckDBPyConnection):
    con.execute("""
    INSERT INTO dim_source(source_id, name, url, cadence, license)
    SELECT 'laus','BLS Local Area Unemployment Statistics',
           'https://www.bls.gov/lau/','monthly','public'
    WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='laus')
    """)
    con.execute("""
    INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
    SELECT 'laus_unemployment_rate','Unemployment Rate','monthly','percent','labor'
    WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id='laus_unemployment_rate')
    """)

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
    with open(cfg_path, newline="") as f:
        for r in csv.DictReader(f):
            sid = r["series_id"].strip()
            if not sid: 
                print(f"[laus] skip row with empty series_id: {r}")
                continue
            series_ids.append(sid)
            sid_to_rowmeta[sid] = {
                "geo_id": r["geo_id"].strip(),
                "metric_id": r.get("metric_id","laus_unemployment_rate").strip()
            }
            rows.append(r)
    if not series_ids:
        raise SystemExit("[laus] no series_id entries found in config/laus_series.csv")

    # batch up to 50 series per API call
    dfs = []
    for i in range(0, len(series_ids), 50):
        chunk = series_ids[i:i+50]
        print(f"[laus] fetching {len(chunk)} series…")
        series_block = fetch_series(chunk)
        dfs.append(to_df(series_block, sid_to_rowmeta))
        time.sleep(0.5)  # small courtesy pause

    all_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if all_df.empty:
        print("[laus] no rows returned.")
        return

    # ensure markets exist minimally (name fallback)
    mkts = (
        all_df[["geo_id"]].drop_duplicates()
        .assign(name=lambda d: d["geo_id"], type=lambda d: d["geo_id"].str.split("_").str[-1], fips=None)
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
    ensure_dims(con)
    con.register("mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id,name,type,fips)
    SELECT geo_id,name,type,fips FROM mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market)
    """)
    upsert(con, all_df)
    # quick summary
    print(con.execute("""
      SELECT geo_id, metric_id, MIN(date) first, MAX(date) last, COUNT(*) n
      FROM fact_timeseries WHERE metric_id='laus_unemployment_rate'
      GROUP BY 1,2 ORDER BY 1
    """).fetchdf())
    con.close()

if __name__ == "__main__":
    main()
