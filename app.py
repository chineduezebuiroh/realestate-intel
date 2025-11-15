import os
from typing import List, Tuple

import altair as alt
import duckdb
import pandas as pd
import streamlit as st


# -------------------------------------------------------------------
# DB helpers
# -------------------------------------------------------------------

@st.cache_resource
def get_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path, read_only=True)


@st.cache_data
def load_geo_options() -> pd.DataFrame:
    """
    Get list of geos from the enriched view.
    We only rely on geo_id + level (robust even if geo_name changes).
    """
    con = get_connection()
    df = con.execute("""
        SELECT DISTINCT geo_id, level
        FROM v_fact_timeseries_enriched
        ORDER BY geo_id
    """).fetchdf()
    df["label"] = df["geo_id"] + " (" + df["level"] + ")"
    return df


@st.cache_data
def load_metric_options() -> List[str]:
    con = get_connection()
    df = con.execute("""
        SELECT DISTINCT metric_id
        FROM v_fact_timeseries_enriched
        ORDER BY metric_id
    """).fetchdf()
    return df["metric_id"].tolist()


def _metric_meta(metric_id: str) -> dict:
    """
    Minimal metric metadata mapping.
    You can extend this later or back it with a dim_metric table.
    """
    meta = {
        "census_pop_total": {
            "label": "Total Population (ACS 5-year)",
            "unit": "people",
        },
        "census_median_household_income": {
            "label": "Median Household Income (ACS 5-year)",
            "unit": "USD",
        },
        "ces_total_nonfarm_nsa": {
            "label": "Total Nonfarm Payrolls (NSA)",
            "unit": "jobs",
        },
        "ces_total_nonfarm_sa": {
            "label": "Total Nonfarm Payrolls (SA)",
            "unit": "jobs",
        },
        "laus_employment_nsa": {
            "label": "Employment (NSA)",
            "unit": "people",
        },
        "laus_employment_sa": {
            "label": "Employment (SA)",
            "unit": "people",
        },
        "laus_labor_force_nsa": {
            "label": "Labor Force (NSA)",
            "unit": "people",
        },
        "laus_labor_force_sa": {
            "label": "Labor Force (SA)",
            "unit": "people",
        },
        "laus_unemployment_nsa": {
            "label": "Unemployment (NSA)",
            "unit": "people",
        },
        "laus_unemployment_sa": {
            "label": "Unemployment (SA)",
            "unit": "people",
        },
        "laus_unemployment_rate_nsa": {
            "label": "Unemployment Rate (NSA)",
            "unit": "percent",
        },
        "laus_unemployment_rate_sa": {
            "label": "Unemployment Rate (SA)",
            "unit": "percent",
        },
    }
    return meta.get(metric_id, {"label": metric_id, "unit": ""})


@st.cache_data
def load_series_for_metric(geo_ids: List[str], metric_id: str) -> pd.DataFrame:
    """
    Load a single metric for one or more geos (long format).
    """
    if not geo_ids:
        return pd.DataFrame(columns=["date", "geo_id", "value"])

    con = get_connection()
    placeholders = ",".join(["?"] * len(geo_ids))
    sql = f"""
        SELECT date, geo_id, value
        FROM v_fact_timeseries_enriched
        WHERE metric_id = ?
          AND geo_id IN ({placeholders})
        ORDER BY date, geo_id
    """
    params = [metric_id] + geo_ids
    df = con.execute(sql, params).fetchdf()

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data
def load_series_for_geo_metric(geo_id: str, metric_id: str) -> pd.DataFrame:
    """
    Load a single metric for a single geo.
    """
    con = get_connection()
    df = con.execute("""
        SELECT date, value
        FROM v_fact_timeseries_enriched
        WHERE geo_id = ?
          AND metric_id = ?
        ORDER BY date
    """, [geo_id, metric_id]).fetchdf()

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


# -------------------------------------------------------------------
# Chart helpers
# -------------------------------------------------------------------

def make_line_with_points(
    df: pd.DataFrame,
    x_field: str,
    y_field: str,
    color_field: str = None,
    y_title: str = "Value",
    color_title: str = "Geo",
) -> alt.Chart:
    """
    Reusable line+point chart with y-axis auto-scaling (zero=False).
    """
    if df.empty:
        return alt.Chart(pd.DataFrame({"msg": ["No data"]})).mark_text().encode(
            text="msg"
        )

    base = alt.Chart(df).encode(
        x=alt.X(f"{x_field}:T", title="Date"),
        y=alt.Y(
            f"{y_field}:Q",
            title=y_title,
            # Don't force y to start at 0; small padding for readability
            scale=alt.Scale(zero=False, nice=True, padding=5),
        ),
    )

    if color_field:
        base = base.encode(
            color=alt.Color(f"{color_field}:N", title=color_title)
        )

    line = base.mark_line()
    points = base.mark_point(size=40)

    chart = (line + points).properties(
        width="container",
        height=400,
    ).interactive()

    return chart


def make_dual_axis_chart(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    metric_left: str,
    metric_right: str,
) -> alt.Chart:
    """
    Dual-axis chart: one geo, two metrics, independent y scales.
    """
    meta_left = _metric_meta(metric_left)
    meta_right = _metric_meta(metric_right)

    # ensure date is datetime
    if not df_left.empty:
        df_left["date"] = pd.to_datetime(df_left["date"])
    if not df_right.empty:
        df_right["date"] = pd.to_datetime(df_right["date"])

    left = (
        alt.Chart(df_left)
        .mark_line()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "value:Q",
                title=meta_left["label"],
                scale=alt.Scale(zero=False, nice=True, padding=5),
            ),
            color=alt.value("#1f77b4"),  # you can drop explicit colors if you prefer
        )
    )
    left_points = (
        alt.Chart(df_left)
        .mark_point(size=40)
        .encode(
            x="date:T",
            y=alt.Y(
                "value:Q",
                scale=alt.Scale(zero=False, nice=True, padding=5),
            ),
            color=alt.value("#1f77b4"),
        )
    )

    right = (
        alt.Chart(df_right)
        .mark_line(strokeDash=[4, 2])
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "value:Q",
                title=meta_right["label"],
                axis=alt.Axis(orient="right"),
                scale=alt.Scale(zero=False, nice=True, padding=5),
            ),
            color=alt.value("#ff7f0e"),
        )
    )
    right_points = (
        alt.Chart(df_right)
        .mark_point(size=40)
        .encode(
            x="date:T",
            y=alt.Y(
                "value:Q",
                axis=alt.Axis(orient="right"),
                scale=alt.Scale(zero=False, nice=True, padding=5),
            ),
            color=alt.value("#ff7f0e"),
        )
    )

    chart = (left + left_points + right + right_points).properties(
        width="container",
        height=400,
    ).resolve_scale(
        y="independent"
    ).interactive()

    return chart


# -------------------------------------------------------------------
# App layout
# -------------------------------------------------------------------

st.set_page_config(
    page_title="Market Intel Dashboard",
    layout="wide",
)

st.title("Market Intel Dashboard")

geo_df = load_geo_options()
metric_options = load_metric_options()

if geo_df.empty or not metric_options:
    st.error("No data found in DuckDB — run the ingest + transform pipeline first.")
    st.stop()

tabs = st.tabs(
    [
        "Compare geos (single metric)",
        "Single geo (2 metrics)",
        "Benchmark compare",
    ]
)

# -------------------------------------------------------------------
# TAB 1: Multi-geo, single metric
# -------------------------------------------------------------------
with tabs[0]:
    st.subheader("Compare geographies for one metric")

    col1, col2 = st.columns([1, 2])

    with col1:
        metric_id = st.selectbox("Metric", metric_options, index=0)
        meta = _metric_meta(metric_id)
        st.caption(f"**Metric:** {meta['label']}  \n**Unit:** {meta['unit'] or '—'}")

        # multi-select geos
        # default: a few key ones if present in data
        default_geo_ids = [
            g for g in [
                "dc_state",
                "md_state",
                "va_state",
                "dc_msa",
                "baltimore_msa",
            ]
            if g in geo_df["geo_id"].values
        ]
        if not default_geo_ids:
            default_geo_ids = [geo_df["geo_id"].iloc[0]]

        selected_labels = st.multiselect(
            "Geographies",
            options=geo_df["label"].tolist(),
            default=[
                geo_df.loc[geo_df["geo_id"] == g, "label"].iloc[0]
                for g in default_geo_ids
            ],
        )

    # map labels back to geo_ids
    label_to_id = dict(zip(geo_df["label"], geo_df["geo_id"]))
    selected_geos = [label_to_id[l] for l in selected_labels]

    with col2:
        df = load_series_for_metric(selected_geos, metric_id)
        chart = make_line_with_points(
            df,
            x_field="date",
            y_field="value",
            color_field="geo_id",
            y_title=f"Value ({meta['unit']})" if meta["unit"] else "Value",
            color_title="Geography",
        )
        st.altair_chart(chart, use_container_width=True)

# -------------------------------------------------------------------
# TAB 2: Single geo, up to 2 metrics (dual axis)
# -------------------------------------------------------------------
with tabs[1]:
    st.subheader("Single geography – up to 2 metrics")

    col1, col2 = st.columns([1, 2])

    with col1:
        # single geo select
        default_geo = "dc_state" if "dc_state" in geo_df["geo_id"].values else geo_df["geo_id"].iloc[0]
        default_label = geo_df.loc[geo_df["geo_id"] == default_geo, "label"].iloc[0]

        geo_label = st.selectbox(
            "Geography",
            options=geo_df["label"].tolist(),
            index=geo_df["label"].tolist().index(default_label),
        )
        geo_id = label_to_id[geo_label]

        metric1 = st.selectbox(
            "Metric 1 (left axis)",
            options=metric_options,
            index=metric_options.index("laus_unemployment_rate_sa") if "laus_unemployment_rate_sa" in metric_options else 0,
        )

        # Metric 2 can be optional; allow a "None" option
        metric2_options = ["(none)"] + metric_options
        metric2_raw = st.selectbox(
            "Metric 2 (right axis, optional)",
            options=metric2_options,
            index=metric2_options.index("ces_total_nonfarm_sa") if "ces_total_nonfarm_sa" in metric2_options else 0,
        )
        metric2 = None if metric2_raw == "(none)" else metric2_raw

    with col2:
        df_left = load_series_for_geo_metric(geo_id, metric1)
        if metric2:
            df_right = load_series_for_geo_metric(geo_id, metric2)
            chart = make_dual_axis_chart(df_left, df_right, metric1, metric2)
        else:
            meta_left = _metric_meta(metric1)
            df_plot = df_left.copy()
            df_plot["geo_id"] = geo_id  # to reuse same helper
            chart = make_line_with_points(
                df_plot,
                x_field="date",
                y_field="value",
                color_field=None,
                y_title=f"Value ({meta_left['unit']})" if meta_left["unit"] else "Value",
            )

        st.altair_chart(chart, use_container_width=True)

# -------------------------------------------------------------------
# TAB 3: Benchmark compare
# -------------------------------------------------------------------
with tabs[2]:
    st.subheader("Benchmark comparison")

    col1, col2 = st.columns([1, 2])

    with col1:
        metric_bench = st.selectbox(
            "Metric",
            options=metric_options,
            index=metric_options.index("laus_unemployment_rate_sa") if "laus_unemployment_rate_sa" in metric_options else 0,
        )
        meta_bench = _metric_meta(metric_bench)

        # choose benchmark geo (single)
        benchmark_default_geo = "dc_state" if "dc_state" in geo_df["geo_id"].values else geo_df["geo_id"].iloc[0]
        benchmark_default_label = geo_df.loc[geo_df["geo_id"] == benchmark_default_geo, "label"].iloc[0]

        benchmark_label = st.selectbox(
            "Benchmark geography (always included)",
            options=geo_df["label"].tolist(),
            index=geo_df["label"].tolist().index(benchmark_default_label),
        )
        benchmark_geo = label_to_id[benchmark_label]

        # other comparison geos
        other_default_ids = [
            g for g in [
                "md_state",
                "va_state",
                "dc_msa",
                "baltimore_msa",
            ]
            if g in geo_df["geo_id"].values and g != benchmark_geo
        ]
        other_default_labels = [
            geo_df.loc[geo_df["geo_id"] == g, "label"].iloc[0]
            for g in other_default_ids
        ]

        other_labels = st.multiselect(
            "Other geographies to compare",
            options=[l for l in geo_df["label"].tolist() if l != benchmark_label],
            default=other_default_labels,
        )

    with col2:
        other_geos = [label_to_id[l] for l in other_labels]
        all_geos = [benchmark_geo] + [g for g in other_geos if g != benchmark_geo]

        df = load_series_for_metric(all_geos, metric_bench)

        if df.empty:
            st.warning("No data for this metric + geo selection.")
        else:
            # make benchmark visually distinct: layered chart
            df_bench = df[df["geo_id"] == benchmark_geo]
            df_others = df[df["geo_id"] != benchmark_geo]

            base_title = f"Value ({meta_bench['unit']})" if meta_bench["unit"] else "Value"

            chart_bench = make_line_with_points(
                df_bench,
                x_field="date",
                y_field="value",
                color_field=None,
                y_title=base_title,
            ).encode(color=alt.value("#000000"))

            if not df_others.empty:
                chart_others = make_line_with_points(
                    df_others,
                    x_field="date",
                    y_field="value",
                    color_field="geo_id",
                    y_title=base_title,
                    color_title="Comparison geos",
                )
                chart = (chart_bench + chart_others).resolve_scale(y="shared")
            else:
                chart = chart_bench

            st.altair_chart(chart, use_container_width=True)
