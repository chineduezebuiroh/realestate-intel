import os
from typing import List, Tuple

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

import re

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


METRIC_FAMILIES = ["All", "Census", "CES", "LAUS"]


def filter_metrics_by_family(metric_ids, family: str):
    """
    Filter a list of metric_ids by family prefix.

    metric_ids: list of metric_id strings
    family: one of METRIC_FAMILIES
    """
    if not metric_ids:
        return []

    metric_ids = sorted(metric_ids)

    if family == "All":
        return metric_ids
    elif family == "Census":
        prefix = "census_"
    elif family == "CES":
        prefix = "ces_"
    elif family == "LAUS":
        prefix = "laus_"
    else:
        # fallback — don’t filter
        return metric_ids

    return [m for m in metric_ids if m.startswith(prefix)]


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
            scale=alt.Scale(zero=False, nice=True#, padding=5
                           ),
        ),
    )

    if color_field:
        base = base.encode(
            color=alt.Color(f"{color_field}:N", title=color_title)
        )

    line = base.mark_line()
    points = base.mark_point(size=20)

    chart = (line + points).properties(
        width="container",
        height=400,
    ).interactive()

    return chart



def make_dual_axis_chart(df_left, df_right, metric1_label, metric2_label):
    """
    df_left  -> first metric (left axis)
    df_right -> second metric (right axis)
    Both dataframes should have: date, value, geo_name
    """
    import altair as alt

    # Left axis series
    left = (
        alt.Chart(df_left)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "value:Q",
                axis=alt.Axis(title=metric1_label),
                scale=alt.Scale(zero=False, nice=True),
            ),
            color=alt.value("#1f77b4"),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("geo_name:N", title="Geo"),
                alt.Tooltip("value:Q", title=metric1_label, format=",.2f"),
            ],
        )
    )

    # Right axis series
    right = (
        alt.Chart(df_right)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "value:Q",
                axis=alt.Axis(title=metric2_label, orient="right"),
                scale=alt.Scale(zero=False, nice=True),
            ),
            color=alt.value("#ff7f0e"),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("geo_name:N", title="Geo"),
                alt.Tooltip("value:Q", title=metric2_label, format=",.2f"),
            ],
        )
    )

    # Layer + independent y scales
    chart = (
        alt.layer(left, right)
        .resolve_scale(y="independent")
        .properties(height=400)
    )

    return chart


def make_baseline_compare_chart(df, pinned_geo_id: str) -> alt.Chart:
    """
    Compare one 'pinned' geo against a set of other geos for a single metric.
    Expects columns: date, value, geo_id, geo_name, metric_id.
    """
    df = df.copy()
    df["is_pinned"] = df["geo_id"] == pinned_geo_id

    base = alt.Chart(df).encode(
        x=alt.X(
            "date:T",
            title="Date"
        ),
        y=alt.Y(
            "value:Q",
            title="Value",
            scale=alt.Scale(zero=False, nice=True)  # auto-scale, no forced zero
        ),
        color=alt.Color(
            "geo_name:N",
            legend=alt.Legend(title="Geography"),
        ),
        opacity=alt.condition(
            alt.datum.is_pinned,
            alt.value(1.0),
            alt.value(0.4),
        ),
        size=alt.condition(
            alt.datum.is_pinned,
            alt.value(3),
            alt.value(1.5),
        ),
        tooltip=[
            alt.Tooltip("geo_name:N", title="Geo"),
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("value:Q", title="Value"),
            alt.Tooltip("metric_id:N", title="Metric ID"),
        ],
    )

    line = base.mark_line()
    points = base.mark_point()

    return (line + points).interactive()


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
        # NEW: choose metric family first
        family = st.selectbox("Metric family", METRIC_FAMILIES, index=0)

        # NEW: filter the metric list by that family
        filtered_metric_options = filter_metrics_by_family(metric_options, family)

        # use filtered_metric_options instead of metric_options
        metric_id = st.selectbox("Metric", filtered_metric_options, index=0)

        meta = _metric_meta(metric_id)
        st.caption(
            f"**Metric:** {meta['label']}  \n"
            f"**Unit:** {meta['unit'] or '—'}"
        )

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
"""
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

"""


with tabs[2]:
    st.subheader("Compare geos vs a baseline")

    import duckdb
    import os

    # --- DB connection ---
    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    con = duckdb.connect(DUCKDB_PATH)

    # 1) Pick metric
    #metric_options = sorted(df["metric_id"].unique())
    metric_options = con.execute("""
        SELECT DISTINCT metric_id
        FROM v_fact_timeseries_enriched
        ORDER BY metric_id
    """).fetchdf()["metric_id"].tolist()
    metric_id = st.selectbox("Metric", metric_options, index=0)

    # 2) Pick baseline geo
    geo_options = sorted(df["geo_name"].unique())
    default_baseline = "Washington-Arlington-Alexandria, DC-VA-MD-WV MSA"  # adjust to your actual label if you want
    baseline_geo_name = st.selectbox(
        "Baseline geography",
        geo_options,
        index=geo_options.index(default_baseline) if default_baseline in geo_options else 0,
    )

    # 3) Pick comparison geos
    compare_geo_names = st.multiselect(
        "Compare against",
        options=[g for g in geo_options if g != baseline_geo_name],
        default=[],
        help="Leave empty to show only the baseline geo.",
    )

    selected_geo_names = [baseline_geo_name] + compare_geo_names

    df_metric = df[df["metric_id"] == metric_id].copy()
    df_metric = df_metric[df_metric["geo_name"].isin(selected_geo_names)]

    # Get baseline geo_id
    baseline_geo_ids = (
        df_metric.loc[df_metric["geo_name"] == baseline_geo_name, "geo_id"]
        .drop_duplicates()
        .tolist()
    )
    if not baseline_geo_ids:
        st.warning("No data found for selected baseline geography.")
    else:
        baseline_geo_id = baseline_geo_ids[0]
        chart = make_baseline_compare_chart(df_metric, pinned_geo_id=baseline_geo_id)
        st.altair_chart(chart, use_container_width=True)
