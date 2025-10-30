import os
import duckdb
import pandas as pd
import streamlit as st
import subprocess   # <-- new import (for ensure_db)
from datetime import date

# --- 1️⃣ Define constants ---
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
GEO_ID = "dc_city"

# --- 2️⃣ Add ensure_db() near the top ---
def ensure_db():
    os.makedirs(os.path.dirname(DUCKDB_PATH) or ".", exist_ok=True)
    if not os.path.exists(DUCKDB_PATH):
        # build schema once if file missing
        subprocess.run(["python", "utils/db.py", "--build"], check=True)

ensure_db()  # <-- run this immediately so DB exists before anything else

# --- 3️⃣ Then continue with Streamlit config & UI ---
# ... existing imports and ensure_db() ...

st.set_page_config(page_title="Market Pulse — DC", layout="wide")
st.title("🏙️ Washington, DC — Market Pulse")

@st.cache_data
def load_markets():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("""
        SELECT geo_id, COALESCE(name, geo_id) AS geo_name
        FROM dim_market
        WHERE geo_id IN ('dc_city','dc_state')
        ORDER BY geo_name
    """).fetchdf()
    con.close()
    return df

mkts = load_markets()
geo_choice = st.selectbox("Market", options=mkts["geo_id"].tolist(),
                          format_func=lambda gid: mkts.set_index("geo_id").loc[gid,"geo_name"])

# then replace every hard-coded 'dc_city' with geo_choice in your queries:
# WHERE f.geo_id = ?  --> pass [geo_choice]



@st.cache_data
def load_metrics():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    dfm = con.execute("""
        SELECT DISTINCT f.metric_id, COALESCE(m.name, f.metric_id) AS metric_name
        FROM fact_timeseries f
        LEFT JOIN dim_metric m USING(metric_id)
        WHERE f.geo_id = ?
        ORDER BY metric_name
    """, [GEO_ID]).fetchdf()
    con.close()
    return dfm

# ... (rest of your code unchanged)




@st.cache_data
def load_series(metric_id: str):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("""
        SELECT date, value
        FROM fact_timeseries
        WHERE geo_id = ? AND metric_id = ?
        ORDER BY date
    """, [GEO_ID, metric_id]).fetchdf()
    con.close()
    return df

metrics = load_metrics()
if metrics.empty:
    st.warning("No data yet. Run your workflow (ingest + transform) and refresh this page.")
    st.stop()

left, right = st.columns([1, 2])
with left:
    choice = st.selectbox(
        "Metric",
        options=metrics["metric_id"].tolist(),
        format_func=lambda mid: metrics.set_index("metric_id").loc[mid, "metric_name"]
    )

df = load_series(choice)

if df.empty:
    st.warning("Selected metric has no data.")
    st.stop()

# KPIs
latest_row = df.dropna().iloc[-1]
latest_val = latest_row["value"]
latest_date = pd.to_datetime(latest_row["date"]).date()

# YoY
df["date"] = pd.to_datetime(df["date"])
this_month = df.iloc[-1]["date"]
yoy_val = None
try:
    last_year_same = df[df["date"] == (this_month - pd.DateOffset(years=1))]["value"].iloc[0]
    yoy_val = (latest_val - last_year_same) / last_year_same * 100.0
except Exception:
    pass

k1, k2, k3 = st.columns(3)
k1.metric("Latest value", f"{latest_val:,.2f}")
k2.metric("As of", latest_date.strftime("%Y-%m-%d"))
k3.metric("YoY change", f"{yoy_val:,.2f} %" if yoy_val is not None else "n/a")

# Chart
st.subheader("History")
st.line_chart(df.set_index("date")["value"])

st.caption("Data source(s): see dim_source in DuckDB. Geo: dc_city. This is a starter UI—forecasts & signals coming next.")
