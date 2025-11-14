# app.py
import os
from typing import List

import duckdb
import pandas as pd
import streamlit as st
import altair as alt

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")


@st.cache_resource
def get_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@st.cache_data
def get_geo_table() -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT DISTINCT geo_id, level
        FROM v_fact_timeseries_enriched
        ORDER BY geo_id
    """).fetchdf()


@st.cache_data
def get_metric_table() -> pd.DataFrame:
    con = get_connection()
    return con.execute("""
        SELECT metric_id, display_name, family, unit, seasonal_adj, frequency, description
        FROM dim_metric
        ORDER BY family, display_name
    """).fetchdf()


@st.cache_data
def load_series_multi(geo_ids: List[str], metric_id: str) -> pd.DataFrame:
    if not geo_ids:
        return pd.DataFrame(columns=["date", "geo_id", "value"])

    con = get_connection()
    # DuckDB `IN` uses list; streamlit passes list of strings
    df = con.execute("""
        SELECT
            date,
            geo_id,
            value
        FROM v_fact_timeseries_enriched
        WHERE geo_id IN ({})
          AND metric_id = ?
        ORDER BY date
    """.format(",".join(["?"] * len(geo_ids))), [*geo_ids, metric_id]).fetchdf()

    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data
def load_latest_multi(geo_ids: List[str], metric_id: str) -> pd.DataFrame:
    if not geo_ids:
        return pd.DataFrame(columns=["geo_id", "date", "value"])

    con = get_connection()
    df = con.execute("""
        SELECT geo_id, date, value
        FROM v_latest_metric_by_geo
        WHERE geo_id IN ({})
          AND metric_id = ?
        ORDER BY geo_id
    """.format(",".join(["?"] * len(geo_ids))), [*geo_ids, metric_id]).fetchdf()
    return df


def main():
    st.set_page_config(
        page_title="Market Intel – Time Series Explorer",
        layout="wide",
    )

    st.title("📈 Market Intel – Time Series Explorer")

    geo_table = get_geo_table()
    metric_table = get_metric_table()

    # ---- Sidebar: GEO selection (multi) ----
    st.sidebar.header("Filters")

    geo_options = geo_table["geo_id"].tolist()
    # nice DMV default if available
    default_geos = [g for g in ["dc_state", "md_state", "va_state"] if g in geo_options]
    if not default_geos:
        default_geos = geo_options[:3]

    selected_geos = st.sidebar.multiselect(
        "Geographies (geo_id)",
        options=geo_options,
        default=default_geos,
        format_func=lambda g: f"{g} ({geo_table.loc[geo_table['geo_id'] == g, 'level'].iloc[0]})",
    )

    # ---- Sidebar: Metric selection powered by dim_metric ----
    families = metric_table["family"].unique().tolist()
    family = st.sidebar.selectbox("Metric family", options=families)

    family_metrics = metric_table[metric_table["family"] == family].reset_index(drop=True)

    metric_display_to_id = {
        f"{row['display_name']} [{row['metric_id']}]": row["metric_id"]
        for _, row in family_metrics.iterrows()
    }

    selected_display = st.sidebar.selectbox(
        "Metric",
        options=list(metric_display_to_id.keys()),
    )
    selected_metric = metric_display_to_id[selected_display]

    metric_meta = family_metrics[family_metrics["metric_id"] == selected_metric].iloc[0]

    # ---- Load data ----
    df = load_series_multi(selected_geos, selected_metric)

    if df.empty:
        st.warning("No data for this combination of geographies + metric.")
        return

    # ---- Title + description ----
    left, right = st.columns([3, 2])
    with left:
        st.subheader(f"{metric_meta['display_name']} ({metric_meta['family']})")
        st.caption(metric_meta["description"])
    with right:
        st.markdown(f"**Unit:** {metric_meta['unit']}  ")
        st.markdown(f"**Seasonal adj.:** {metric_meta['seasonal_adj']}  ")
        st.markdown(f"**Frequency:** {metric_meta['frequency']}")

    # ---- Altair line chart with auto y-axis ----
    # Altair auto-scales the y-axis; we add nice=True to make it pretty.
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "value:Q",
                title=f"Value ({metric_meta['unit']})" if metric_meta["unit"] else "Value",
                scale=alt.Scale(nice=True),
            ),
            color=alt.Color("geo_id:N", title="Geography"),
            tooltip=["date:T", "geo_id:N", "value:Q"],
        )
        .properties(
            width="container",
            height=400,
        )
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)

    # ---- Latest values table ----
    latest = load_latest_multi(selected_geos, selected_metric)
    if not latest.empty:
        st.markdown("### Latest values")
        # join in geo level for context
        latest = latest.merge(geo_table, on="geo_id", how="left")
        st.dataframe(latest, use_container_width=True)

    # ---- Raw data ----
    with st.expander("Show raw timeseries data"):
        st.dataframe(df.sort_values(["geo_id", "date"]), use_container_width=True)


if __name__ == "__main__":
    main()
