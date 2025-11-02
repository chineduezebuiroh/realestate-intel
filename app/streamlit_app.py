import os
import duckdb
import pandas as pd
import streamlit as st
import subprocess   # <-- new import (for ensure_db)
from datetime import date

# --- 1️⃣ Define constants ---
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

# --- 2️⃣ Add ensure_db() near the top ---
def ensure_db():
    os.makedirs(os.path.dirname(DUCKDB_PATH) or ".", exist_ok=True)
    if not os.path.exists(DUCKDB_PATH):
        # build schema once if file missing
        subprocess.run(["python", "utils/db.py", "--build"], check=True)

ensure_db()  # <-- run this immediately so DB exists before anything else


import datetime as dt


@st.cache_data
def get_series_extent_with_ptype(geo_id: str, metric_id: str, ptypes: list[str]):
    """
    Returns (first_dt, last_dt, n_rows) for the currently selected property type(s).
    If ptypes is empty, we treat it as ['all'].
    """
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    if not ptypes:
        res = con.execute("""
            SELECT MIN(date) AS first_dt,
                   MAX(date) AS last_dt,
                   COUNT(DISTINCT date) AS n_rows
            FROM fact_timeseries
            WHERE geo_id = ? AND metric_id = ? AND property_type_id = 'all'
        """, [geo_id, metric_id]).fetchdf()
    else:
        placeholders = ",".join(["?"] * len(ptypes))
        res = con.execute(f"""
            SELECT MIN(date) AS first_dt,
                   MAX(date) AS last_dt,
                   COUNT(DISTINCT date) AS n_rows
            FROM fact_timeseries
            WHERE geo_id = ? AND metric_id = ? AND property_type_id IN ({placeholders})
        """, [geo_id, metric_id, *ptypes]).fetchdf()
    con.close()

    if res.empty:
        return None, None, 0
    first_dt = pd.to_datetime(res.loc[0, "first_dt"]) if pd.notna(res.loc[0, "first_dt"]) else None
    last_dt  = pd.to_datetime(res.loc[0, "last_dt"])  if pd.notna(res.loc[0, "last_dt"])  else None
    n_rows   = int(res.loc[0, "n_rows"] or 0)
    return first_dt, last_dt, n_rows


# Simple aggregation policy — evolve later (e.g., drive from dim_metric.unit/category)
AGG_POLICY = {
    # examples:
    # "redfin_homes_sold": "sum",
    # "permits_total": "sum",
    # "redfin_median_sale_price": "avg",
    # "fred_gs10": "avg",
}
def metric_agg(metric_id: str) -> str:
    # default to 'avg' unless explicitly set
    return AGG_POLICY.get(metric_id, "avg")



def freshness_status(last_dt: pd.Timestamp, freq="M"):
    """Return (label, emoji, color, pct) given last date vs today."""
    if last_dt is None or pd.isna(last_dt):
        return ("no data", "⛔", "error", 0.0)
    today = pd.Timestamp(dt.date.today())
    lag_days = (today - last_dt).days
    # thresholds: fresh <=45d, warming 46–90d, stale >90d
    if lag_days <= 45:
        return ("fresh", "✅", "success", 1.0)
    elif lag_days <= 90:
        # map 46–90d to 75%..50%
        pct = max(0.5, 1.0 - (lag_days - 45) / 180)
        return ("warming", "🟡", "warning", pct)
    else:
        # map >90d to 50%..0%
        pct = max(0.0, 0.5 - (lag_days - 90) / 180)
        return ("stale", "🟥", "error", pct)




# --- 3️⃣ Then continue with Streamlit config & UI ---
# ... existing imports and ensure_db() ...

st.set_page_config(page_title="Market Pulse — DC", layout="wide")
st.title("🏙️ Washington, DC — Market Pulse")



@st.cache_data
def load_multi_series(geo_id: str, metric_ids: list[str]):
    if not metric_ids:
        return pd.DataFrame(columns=["date","metric_id","value"])
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    # parameterize the metric list
    placeholders = ",".join(["?"] * len(metric_ids))
    q = f"""
        SELECT date, metric_id, value
        FROM fact_timeseries
        WHERE geo_id = ? AND metric_id IN ({placeholders})
        ORDER BY date
    """
    df = con.execute(q, [geo_id, *metric_ids]).fetchdf()
    con.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data
def load_markets(level: str | None = None, require_data: bool = False):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    if level is None:
        if require_data:
            df = con.execute("""
                SELECT m.geo_id, COALESCE(m.name, m.geo_id) AS geo_name, m.type
                FROM dim_market m
                WHERE EXISTS (SELECT 1 FROM fact_timeseries f WHERE f.geo_id = m.geo_id)
                ORDER BY geo_name
            """).fetchdf()
        else:
            df = con.execute("""
                SELECT m.geo_id, COALESCE(m.name, m.geo_id) AS geo_name, m.type
                FROM dim_market m
                ORDER BY geo_name
            """).fetchdf()
    else:
        if require_data:
            df = con.execute("""
                SELECT m.geo_id, COALESCE(m.name, m.geo_id) AS geo_name, m.type
                FROM dim_market m
                WHERE m.type = ?
                  AND EXISTS (SELECT 1 FROM fact_timeseries f WHERE f.geo_id = m.geo_id)
                ORDER BY geo_name
            """, [level]).fetchdf()
        else:
            df = con.execute("""
                SELECT m.geo_id, COALESCE(m.name, m.geo_id) AS geo_name, m.type
                FROM dim_market m
                WHERE m.type = ?
                ORDER BY geo_name
            """, [level]).fetchdf()
    con.close()
    return df


# ---- Level → Market cascade ----
level_map = {
    "City": "city",
    "County": "county",
    "State": "state",
    "National": "national",
}
lvl = st.radio("Geography level", options=list(level_map.keys()), horizontal=True)
lvl_code = level_map[lvl]

mkts = load_markets(lvl_code, require_data=False)  # show all known markets at this level
if mkts.empty:
    st.warning(f"No markets are configured at the {lvl.lower()} level.")
    st.stop()

geo_choice = st.selectbox(
    "Market",
    options=mkts["geo_id"].tolist(),
    format_func=lambda gid: mkts.set_index("geo_id").loc[gid, "geo_name"]
)







# ---- Macro overlay for US National ----
is_us = (geo_choice == "us_national")

if is_us:
    st.markdown("### 🇺🇸 Macro: Yields & Spreads")

    cat = st.radio(
        "Category",
        options=["Rates", "Spreads"],
        horizontal=True,
        help="Pick base rates (GS2/GS10/GS30/Fed Funds) or derived spreads (10Y-2Y, 30Y-10Y, Mortgage-10Y)."
    )


    if cat == "Rates":
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        rates = con.execute("""
            SELECT metric_id, COALESCE(name, metric_id) AS metric_name
            FROM dim_metric
            WHERE metric_id IN ('fred_gs2','fred_gs10','fred_gs30','fred_fedfunds')
            ORDER BY metric_name
        """).fetchdf()
        con.close()

        # robust name map + safe defaults
        name_map = dict(zip(rates["metric_id"], rates["metric_name"]))
        options = rates["metric_id"].tolist()
        default_candidates = ["fred_gs10","fred_gs2"]
        defaults = [m for m in default_candidates if m in options]

        rate_choices = st.multiselect(
            "Select rates",
            options=options,
            default=defaults,
            format_func=lambda mid: name_map.get(mid, mid)
        )

        if not rate_choices:
            st.info("Select one or more rates to display.")
        else:
            dfm = load_multi_series(geo_choice, rate_choices)
            if dfm.empty:
                st.info("No data for the selected rates yet.")
            else:
                pivot = dfm.pivot(index="date", columns="metric_id", values="value")
                st.line_chart(pivot)


    else:  # Spreads
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        spreads = con.execute("""
            SELECT metric_id, COALESCE(name, metric_id) AS metric_name
            FROM dim_metric
            WHERE metric_id IN ('spread_10y_2y','spread_30y_10y','spread_mortgage_10y')
            ORDER BY metric_name
        """).fetchdf()
        con.close()

        name_map = dict(zip(spreads["metric_id"], spreads["metric_name"]))
        options = spreads["metric_id"].tolist()
        default_candidates = ["spread_10y_2y","spread_mortgage_10y"]
        defaults = [m for m in default_candidates if m in options]

        spread_choices = st.multiselect(
            "Select spreads",
            options=options,
            default=defaults,
            format_func=lambda mid: name_map.get(mid, mid)
        )

        if not spread_choices:
            st.info("Select one or more spreads to display.")
        else:
            dfs = load_multi_series(geo_choice, spread_choices)
            if dfs.empty:
                st.info("No data for the selected spreads yet.")
            else:
                pivot = dfs.pivot(index="date", columns="metric_id", values="value")
                st.line_chart(pivot)

    

    st.divider()





@st.cache_data
def load_metrics(geo_id: str):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    dfm = con.execute("""
        SELECT DISTINCT f.metric_id, COALESCE(m.name, f.metric_id) AS metric_name
        FROM fact_timeseries f
        LEFT JOIN dim_metric m USING(metric_id)
        WHERE f.geo_id = ?
        ORDER BY metric_name
    """, [geo_id]).fetchdf()
    con.close()
    return dfm

@st.cache_data
def load_series(geo_id: str, metric_id: str):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("""
        SELECT date, value
        FROM fact_timeseries
        WHERE geo_id = ? AND metric_id = ?
        ORDER BY date
    """, [geo_id, metric_id]).fetchdf()
    con.close()
    return df



@st.cache_data
def available_property_types_labeled(geo_id: str, metric_id: str):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("""
        SELECT DISTINCT f.property_type_id,
               COALESCE(d.name, f.property_type_id) AS label
        FROM fact_timeseries f
        LEFT JOIN dim_property_type d USING(property_type_id)
        WHERE f.geo_id = ? AND f.metric_id = ?
        ORDER BY label
    """, [geo_id, metric_id]).fetchdf()
    con.close()
    return df  # columns: property_type_id, label


@st.cache_data
def load_series_with_ptype(geo_id: str, metric_id: str, ptypes: list[str]):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    agg = metric_agg(metric_id).lower()  # 'avg' (default), 'sum', 'min', 'max'
    if not ptypes:
        df = con.execute("""
            SELECT date, value
            FROM fact_timeseries
            WHERE geo_id = ? AND metric_id = ? AND property_type_id = 'all'
            ORDER BY date
        """, [geo_id, metric_id]).fetchdf()
    else:
        placeholders = ",".join(["?"] * len(ptypes))
        df = con.execute(f"""
            SELECT date, {agg}(value) AS value
            FROM fact_timeseries
            WHERE geo_id = ? AND metric_id = ? AND property_type_id IN ({placeholders})
            GROUP BY 1
            ORDER BY 1
        """, [geo_id, metric_id, *ptypes]).fetchdf()
    con.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


metrics = load_metrics(geo_choice)
if metrics.empty:
    st.warning("No data yet for this market. Run your workflow (ingest + transform) and refresh.")
    st.stop()

left, right = st.columns([1, 2])
with left:
    choice = st.selectbox(
        "Metric",
        options=metrics["metric_id"].tolist(),
        format_func=lambda mid: metrics.set_index("metric_id").loc[mid, "metric_name"]
    )

# --- Property type selector ---
df_pt = available_property_types_labeled(geo_choice, choice)
pt_ids = df_pt["property_type_id"].tolist()
label_map = dict(zip(df_pt["property_type_id"], df_pt["label"]))

if pt_ids and not (len(pt_ids) == 1 and pt_ids[0] == "all"):
    sel_ptypes = st.multiselect(
        "Property types",
        options=pt_ids,
        default=pt_ids,
        format_func=lambda pid: label_map.get(pid, pid)
    )
else:
    sel_ptypes = []


df = load_series_with_ptype(geo_choice, choice, sel_ptypes)
if df.empty:
    st.warning("Selected metric has no data.")
    st.stop()


# ---- Data Freshness bar (ptype-aware) ----
first_dt, last_dt, n_rows = get_series_extent_with_ptype(geo_choice, choice, sel_ptypes)
label, emoji, color, pct = freshness_status(last_dt)

c1, c2, c3, c4 = st.columns([1,1,1,2])
with c1:
    st.metric("First date", first_dt.date().isoformat() if first_dt is not None else "n/a")
with c2:
    st.metric("Last date",  last_dt.date().isoformat()  if last_dt  is not None else "n/a")
with c3:
    st.metric("Rows", f"{n_rows:,}")
with c4:
    st.write(f"**Freshness:** {emoji} {label}")
    st.progress(pct)

if color == "warning":
    st.info("This series is getting a bit old. Consider running your ETL.")
elif color == "error":
    st.warning("This series looks stale — run your ETL or check the source cadence.")



# KPIs (unchanged)
latest_row = df.dropna().iloc[-1]
latest_val = latest_row["value"]
latest_date = pd.to_datetime(latest_row["date"]).date()

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

st.subheader("History")
st.line_chart(df.set_index("date")["value"])


# === Compare Markets panel (insert here) ===
st.divider()
st.subheader("Compare markets")

# 2a) choose metric (reuse your metrics df from current market to keep list friendly)
cmp_metric = st.selectbox(
    "Metric to compare",
    options=metrics["metric_id"].tolist(),
    format_func=lambda mid: metrics.set_index("metric_id").loc[mid, "metric_name"]
)

# 2b) multi-select markets (offer all with data, preselect 2–3 nearby geos)
all_mkts = mkts.copy()
default_choices = [gid for gid in all_mkts["geo_id"].tolist() if gid in ("dc_state","md_state","va_state")][:3]
cmp_geos = st.multiselect(
    "Markets to overlay",
    options=all_mkts["geo_id"].tolist(),
    default=default_choices,
    format_func=lambda gid: all_mkts.set_index("geo_id").loc[gid,"geo_name"]
)

dfs = []
for gid in cmp_geos:
    dfg = load_series_with_ptype(gid, cmp_metric, sel_ptypes)  # reuse same ptype choices
    if not dfg.empty:
        dfg["geo_id"] = gid
        dfs.append(dfg)

if not dfs:
    st.info("No data found for the chosen metric/markets.")
else:
    df_cmp = pd.concat(dfs, ignore_index=True)
    # in case of any duplicate (date, geo_id) rows, average them
    df_cmp = df_cmp.groupby(["date","geo_id"], as_index=False)["value"].mean()
    piv = df_cmp.pivot(index="date", columns="geo_id", values="value")
    st.line_chart(piv)


    # small KPI table for quick deltas
    with st.expander("Show latest + changes"):
        latest = piv.dropna().iloc[-1:].T.reset_index()
        latest.columns = ["geo_id","latest"]
        # compute 3/6/12m deltas where available
        def pct_delta(series, months):
            try:
                return (series.iloc[-1] - series.iloc[-months]) / series.iloc[-months] * 100.0
            except Exception:
                return None
        rows = []
        for gid in piv.columns:
            s = piv[gid].dropna()
            rows.append({
                "geo_id": gid,
                "name": all_mkts.set_index("geo_id").loc[gid,"geo_name"],
                "latest": s.iloc[-1] if len(s) else None,
                "Δ3m%": pct_delta(s, 3),
                "Δ6m%": pct_delta(s, 6),
                "Δ12m%": pct_delta(s, 12),
            })
        tbl = pd.DataFrame(rows)
        st.dataframe(tbl.set_index("name"))


# (keep your caption below)
# dynamic caption
st.caption(f"Data sources: see dim_source. Market: {mkts.set_index('geo_id').loc[geo_choice,'geo_name']} ({geo_choice}).")
