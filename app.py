# app.py
import os
from typing import List

import duckdb
import pandas as pd
import streamlit as st

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")


@st.cache_resource
def get_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@st.cache_data
def get_geo_options() -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT DISTINCT geo_id, level
        FROM v_fact_timeseries_enriched
        ORDER BY geo_id
    """).fetchdf()


@st.cache_data
def get_metric_options() -> List[str]:
    con = get_connection()
    return con.execute("""
        SELECT DISTINCT metric_id
        FROM v_fact_timeseries_enriched
        ORDER BY metric_id
    """).fetchdf()["metric_id"].tolist()


@st.cache_data
def load_series(geo_id: str, metric_id: str) -> pd.DataFrame:
    con = get_connection()
    df = con.execute("""
        SELECT
            date,
            value
        FROM v_fact_timeseries_enriched
        WHERE geo_id = ?
          AND metric_id = ?
        ORDER BY date
    """, [geo_id, metric_id]).fetchdf()

    # Ensure date is datetime for plotting
    df["date"] = pd.to_datetime(df["date"])
    return df


def main():
    st.set_page_config(
        page_title="Market Intel – Time Series Explorer",
        layout="wide",
    )

    st.title("📈 Market Intel – Time Series Explorer")

    st.markdown(
        "Use the controls in the sidebar to pick a geography and metric "
        "from LAUS, CES, and Census."
    )

    # ---- Sidebar controls ----
    st.sidebar.header("Filters")

    geos = get_geo_options()
    metrics = get_metric_options()

    # Geo selector
    default_geo = "dc_state" if "dc_state" in set(geos["geo_id"]) else geos["geo_id"].iloc[0]
    selected_geo = st.sidebar.selectbox(
        "Geography (geo_id)",
        options=geos["geo_id"].tolist(),
        index=geos["geo_id"].tolist().index(default_geo)
        if default_geo in geos["geo_id"].tolist()
        else 0,
        format_func=lambda g: f"{g} ({geos.loc[geos['geo_id'] == g, 'level'].iloc[0]})",
    )

    # Metric selector (grouped by family)
    metric_families = {
        "Census": [m for m in metrics if m.startswith("census_")],
        "CES": [m for m in metrics if m.startswith("ces_")],
        "LAUS": [m for m in metrics if m.startswith("laus_")],
    }

    family = st.sidebar.selectbox("Metric family", options=list(metric_families.keys()))
    family_metrics = metric_families[family] or metrics  # fallback

    selected_metric = st.sidebar.selectbox(
        "Metric",
        options=family_metrics,
    )

    # ---- Load data ----
    df = load_series(selected_geo, selected_metric)

    if df.empty:
        st.warning("No data for this geo + metric combination.")
        return

    st.subheader(f"{selected_metric} – {selected_geo}")
    st.caption(
        "Source: LAUS (BLS), CES (BLS), and ACS 5-year (Census), via your DuckDB `fact_timeseries`."
    )

    # ---- Chart ----
    st.line_chart(
        df.set_index("date")["value"],
        use_container_width=True,
    )

    # ---- Latest snapshot (from the latest view) ----
    con = get_connection()
    latest = con.execute("""
        SELECT date, value
        FROM v_latest_metric_by_geo
        WHERE geo_id = ?
          AND metric_id = ?
    """, [selected_geo, selected_metric]).fetchdf()

    if not latest.empty:
        st.metric(
            label="Latest value",
            value=f"{latest['value'].iloc[0]:,.2f}",
            delta=None,
            help=f"As of {latest['date'].iloc[0]}",
        )

    # Raw data preview
    with st.expander("Show raw data"):
        st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
