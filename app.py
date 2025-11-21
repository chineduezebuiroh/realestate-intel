import os
from typing import List

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

import re  # currently unused but harmless

st.set_page_config(
    page_title="Market Intel Dashboard",
    layout="wide",
)

# -------------------------------------------------------------------
# DB helpers
# -------------------------------------------------------------------

@st.cache_data
def load_metric_source_map() -> dict:
    """
    Map metric_id -> source_id
    (e.g., 'census_acs', 'census_bps', 'ces', 'laus', 'redfin', 'bea_gdp_qtr').
    """
    con = get_connection()
    df = con.execute("""
        SELECT DISTINCT metric_id, source_id
        FROM v_fact_timeseries_enriched
    """).fetchdf()
    return dict(zip(df["metric_id"], df["source_id"]))


def is_redfin_metric(metric_id: str) -> bool:
    src_map = load_metric_source_map()
    return src_map.get(metric_id) == "redfin"


def is_bea_metric(metric_id: str) -> bool:
    src_map = load_metric_source_map()
    return src_map.get(metric_id) == "bea_gdp_qtr"


def render_metric_help(metric_id: str | None):
    """
    Show a small help/tooltip-style note about data coverage / frequency
    for the selected metric. Called in each tab after the user picks a metric.
    """
    if not metric_id:
        st.caption(
            "ℹ️ Not all metrics are available for all geographies. "
            "If a chart is blank, try a broader geography (e.g., state or MSA) "
            "or choose a different metric."
        )
        return

    src_map = load_metric_source_map()
    source_id = src_map.get(metric_id)

    # Base note (applies everywhere)
    notes = [
        "Not all metrics are available for all geographies. "
        "If a chart is blank or very short, try a broader geography "
        "(state, MSA, CSA) or a different metric."
    ]

    # CES-specific nuance
    if source_id == "ces":
        notes.append(
            "Sector-level CES payroll metrics (e.g., construction, "
            "manufacturing, leisure & hospitality) are typically only "
            "published for states, metro areas (MSAs/MDs), CSAs, and a "
            "few large cities. Counties and smaller cities usually do not "
            "have CES payroll data."
        )

    # LAUS nuance
    if source_id == "laus":
        notes.append(
            "LAUS labor metrics (employment, unemployment, labor force, "
            "unemployment rate) are available for states, counties, and many "
            "metros and cities, but not every custom geography."
        )

    # Redfin nuance (multi-level + 90-day windows)
    if source_id == "redfin":
        notes.append(
            "Redfin metrics are currently loaded for metro areas, states, counties, "
            "cities, ZIP codes, and neighborhoods (primarily in the DC / Baltimore "
            "region). Some series use rolling ~90-day windows rather than strict " 
            "calendar months, so spikes and timing may differ from other monthly "
            "data sources. Some geographies outside that footprint may still have "
            "partial or no coverage."
        )

    # Census BPS nuance
    if source_id == "census_bps":
        notes.append(
            "Census Building Permits data is only reported for selected "
            "geographies (states, large metros, and certain "
            "permit-issuing places). Some smaller counties or cities may "
            "not appear."
        )

    # Generic Census ACS
    if source_id == "census_acs":
        notes.append(
            "ACS metrics are typically available for states, counties, and "
            "large cities, but small geos can be suppressed or have higher "
            "sampling noise."
        )

    # BEA GDP nuance
    if source_id == "bea_gdp_qtr":
        notes.append(
            "BEA quarterly real GDP is available from about 2005 onward and "
            "is reported at a quarterly frequency (Q1, Q2, Q3, Q4). "
            "Series appear as quarter-end dates (e.g., March 31 for Q1)."
        )

    # FRED Macro Rates & CPI
    if source_id == "fred_macro":
        notes.append(
            "FRED macro metrics (mortgage rates, Treasury yields, federal funds, CPI, "
            "and yield spreads) are loaded only at the U.S. national level "
            "(geo_id = 'us_nation'). Charts will be blank if you select a different "
            "geography."
        )

    # FRED Unemployment
    if source_id == "fred_unemp":
        notes.append(
            "FRED unemployment metrics currently cover the U.S. total and a limited "
            "set of states (DC, Maryland, Virginia). Other geographies will not have "
            "FRED unemployment data yet."
        )

    st.caption("ℹ️ " + " ".join(notes))

"""
@st.cache_resource
def get_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path, read_only=True)
"""

@st.cache_resource
def get_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market_public.duckdb")
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


# -------------------------------------------------------------------
# Geo helpers
# -------------------------------------------------------------------

def build_geo_family_options(geo_df: pd.DataFrame):
    """
    Build dynamic 'geo family' options (All states, All MSAs, etc.)
    based on which levels actually exist in geo_df.
    """
    existing_levels = set(geo_df["level"].dropna().unique())

    FAMILY_DEFS = {
        "Custom selection": None,
        "All states": ["state"],
        "All MSAs": ["metro_area"],
        "All metro divisions": ["metro_division"],
        "All CSAs": ["combined_area"],
        "All counties": ["county"],
        "All cities": ["city"],
        "All ZIP codes": ["zip_code"],
        "All neighborhoods": ["neighborhood"],
    }

    options = []
    level_map: dict[str, list[str]] = {}
    for label, levels in FAMILY_DEFS.items():
        if levels is None:
            options.append(label)
            level_map[label] = []
        else:
            # Only include if at least one of these levels exists
            usable = [lv for lv in levels if lv in existing_levels]
            if usable:
                options.append(label)
                level_map[label] = usable

    return options, level_map


def show_missing_geo_notice(
    selected_geo_ids: list[str],
    df: pd.DataFrame,
    geo_df: pd.DataFrame,
    metric_id: str,
):
    """
    Given a set of selected geo_ids and the actual data frame returned
    for a metric, show which geos have *no* data for that metric.
    Works for any source (CES, Redfin, BEA, FRED, etc.).
    """
    if not selected_geo_ids:
        return

    present_ids = set(df["geo_id"].unique()) if not df.empty else set()
    missing = [g for g in selected_geo_ids if g not in present_ids]
    if not missing:
        return

    id_to_label = dict(zip(geo_df["geo_id"], geo_df["label"]))
    id_to_level = dict(zip(geo_df["geo_id"], geo_df["level"]))

    lines = []
    for gid in missing:
        label = id_to_label.get(gid, gid)
        level = id_to_level.get(gid) or "unknown level"
        lines.append(f"- {label} ({level})")

    st.info(
        "This metric is **not available** for the following geographies:\n\n"
        + "\n".join(lines)
        + "\n\nFor some data sources (e.g., CES, Redfin, BEA), certain "
          "geo levels are simply not published."
    )




@st.cache_data
def load_metric_options() -> List[str]:
    """
    Get distinct metric_ids from the enriched view.
    """
    con = get_connection()
    df = con.execute("""
        SELECT DISTINCT metric_id
        FROM v_fact_timeseries_enriched
        ORDER BY metric_id
    """).fetchdf()
    return df["metric_id"].tolist()


@st.cache_data
def load_redfin_property_types() -> pd.DataFrame:
    """
    Distinct property types from Redfin metrics.
    """
    con = get_connection()
    df = con.execute("""
        SELECT DISTINCT property_type_id, property_type
        FROM v_fact_timeseries_enriched
        WHERE source_id = 'redfin'
        ORDER BY property_type_id
    """).fetchdf()

    if df.empty:
        return df

    # Nice label: "Single Family (sf)" etc.
    df["label"] = df.apply(
        lambda r: (
            f"{r['property_type']} ({r['property_type_id']})"
            if pd.notna(r.get("property_type"))
            else r["property_type_id"]
        ),
        axis=1,
    )
    return df


def _metric_meta(metric_id: str) -> dict:
    """
    Minimal metric metadata mapping.
    Extend this as needed, fallback is metric_id as label.
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

        # --- Census BPS: Units ---
        "census_bp_total_units": {
            "label": "Building Permits – Total Units",
            "unit": "units",
        },
        "census_bp_1_unit": {
            "label": "Building Permits – 1-Unit Structures (Units)",
            "unit": "units",
        },
        "census_bp_2_units": {
            "label": "Building Permits – 2-Unit Structures (Units)",
            "unit": "units",
        },
        "census_bp_3_4_units": {
            "label": "Building Permits – 3–4 Unit Structures (Units)",
            "unit": "units",
        },
        "census_bp_5plus_units": {
            "label": "Building Permits – 5+ Unit Structures (Units)",
            "unit": "units",
        },

        # --- Census BPS: Buildings ---
        "census_bp_total_bldgs": {
            "label": "Building Permits – Total Buildings",
            "unit": "buildings",
        },
        "census_bp_1_unit_bldgs": {
            "label": "Building Permits – 1-Unit Structures (Buildings)",
            "unit": "buildings",
        },
        "census_bp_2_units_bldgs": {
            "label": "Building Permits – 2-Unit Structures (Buildings)",
            "unit": "buildings",
        },
        "census_bp_3_4_units_bldgs": {
            "label": "Building Permits – 3–4 Unit Structures (Buildings)",
            "unit": "buildings",
        },
        "census_bp_5plus_units_bldgs": {
            "label": "Building Permits – 5+ Unit Structures (Buildings)",
            "unit": "buildings",
        },

        # --- Census BPS: Valuation ($) ---
        "census_bp_total_value": {
            "label": "Building Permits – Total Value",
            "unit": "USD",
        },
        "census_bp_1_unit_value": {
            "label": "Building Permits – 1-Unit Structures (Value)",
            "unit": "USD",
        },
        "census_bp_2_units_value": {
            "label": "Building Permits – 2-Unit Structures (Value)",
            "unit": "USD",
        },
        "census_bp_3_4_units_value": {
            "label": "Building Permits – 3–4 Unit Structures (Value)",
            "unit": "USD",
        },
        "census_bp_5plus_units_value": {
            "label": "Building Permits – 5+ Unit Structures (Value)",
            "unit": "USD",
        },

        # --- BEA GDP ---
        "gdp_real_total": {
            "label": "Real GDP (Total, chained 2017 dollars)",
            "unit": "millions of chained 2017 USD",
        },

        
        # --- FRED macro: mortgage rates ---
        "fred_mortgage_30y_avg": {
            "label": "30Y Mortgage Rate (FRED, monthly avg)",
            "unit": "percent",
        },
        "fred_mortgage_15y_avg": {
            "label": "15Y Mortgage Rate (FRED, monthly avg)",
            "unit": "percent",
        },
        "fred_mortgage_5y_arm_avg": {
            "label": "5/1 ARM Mortgage Rate (FRED, monthly avg)",
            "unit": "percent",
        },

        # --- FRED macro: Treasury yields & fed funds ---
        "fred_gs2": {
            "label": "2Y Treasury Yield (Constant Maturity)",
            "unit": "percent",
        },
        "fred_gs10": {
            "label": "10Y Treasury Yield (Constant Maturity)",
            "unit": "percent",
        },
        "fred_gs30": {
            "label": "30Y Treasury Yield (Constant Maturity)",
            "unit": "percent",
        },
        "fred_fedfunds": {
            "label": "Effective Federal Funds Rate",
            "unit": "percent",
        },

        # --- FRED macro: CPI (price level) ---
        "fred_cpi_urban_sa_index": {
            "label": "CPI-U All Items (Urban Consumers, SA Index)",
            "unit": "index",
        },

        # --- FRED macro: yield spreads ---
        "fred_spread_2y_10y": {
            "label": "Yield Spread: 2Y – 10Y",
            "unit": "percent",
        },
        "fred_spread_2y_30y": {
            "label": "Yield Spread: 2Y – 30Y",
            "unit": "percent",
        },
        "fred_spread_10y_30y": {
            "label": "Yield Spread: 10Y – 30Y",
            "unit": "percent",
        },
        "fred_spread_2y_fedfunds": {
            "label": "Yield Spread: 2Y – Fed Funds",
            "unit": "percent",
        },
        "fred_spread_10y_fedfunds": {
            "label": "Yield Spread: 10Y – Fed Funds",
            "unit": "percent",
        },
        "fred_spread_30y_fedfunds": {
            "label": "Yield Spread: 30Y – Fed Funds",
            "unit": "percent",
        },

        # --- FRED unemployment (if metric_id matches) ---
        "fred_unemp_rate_sa": {
            "label": "Unemployment Rate (FRED, SA)",
            "unit": "percent",
        },
        
    }
    return meta.get(metric_id, {"label": metric_id, "unit": ""})


@st.cache_data
def load_series_for_metric(
    geo_ids: List[str],
    metric_id: str,
    property_type_id: str | None = None,
) -> pd.DataFrame:
    if not geo_ids:
        return pd.DataFrame(columns=["date", "geo_id", "value"])

    con = get_connection()
    placeholders = ",".join(["?"] * len(geo_ids))

    sql = f"""
        SELECT date, geo_id, value
        FROM v_fact_timeseries_enriched
        WHERE metric_id = ?
          AND geo_id IN ({placeholders})
    """
    params = [metric_id] + geo_ids

    if property_type_id is not None:
        sql += " AND property_type_id = ?"
        params.append(property_type_id)

    sql += " ORDER BY date, geo_id"

    df = con.execute(sql, params).fetchdf()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df



def metric_has_us_nation(metric_id: str) -> bool:
    """
    Quick check: does this metric have any rows for geo_id='us_nation'?
    Uses the enriched view so it works for BEA + FRED (and any others later).
    """
    con = get_connection()
    df = con.execute(
        """
        SELECT 1
        FROM v_fact_timeseries_enriched
        WHERE geo_id = 'us_nation'
          AND metric_id = ?
        LIMIT 1
        """,
        [metric_id],
    ).fetchdf()
    return not df.empty



@st.cache_data
def load_series_for_geo_metric(
    geo_id: str,
    metric_id: str,
    property_type_id: str | None = None,
) -> pd.DataFrame:
    con = get_connection()
    sql = """
        SELECT date, value
        FROM v_fact_timeseries_enriched
        WHERE geo_id = ?
          AND metric_id = ?
    """
    params = [geo_id, metric_id]

    if property_type_id is not None:
        sql += " AND property_type_id = ?"
        params.append(property_type_id)

    sql += " ORDER BY date"

    df = con.execute(sql, params).fetchdf()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


# Metric families for filters & tabs
METRIC_FAMILIES = [
    "All",
    "Census – ACS",
    "Census – Permits",
    "CES (Payrolls)",
    "LAUS (Labor)",
    "BEA – GDP (Quarterly)",
    "Redfin (Housing)",
    "FRED (Macro Rates & CPI)",
    "FRED (Unemployment)",
]


def filter_metrics_by_family(metric_ids, family: str):
    if not metric_ids:
        return []

    metric_ids = sorted(metric_ids)
    if family == "All":
        return metric_ids

    src_map = load_metric_source_map()

    # Map family labels -> one or more source_id values
    FAMILY_SOURCES = {
        "Census – ACS": {"census_acs"},
        "Census – Permits": {"census_bps"},
        "CES (Payrolls)": {"ces"},
        "LAUS (Labor)": {"laus"},
        "BEA – GDP (Quarterly)": {"bea_gdp_qtr"},
        "Redfin (Housing)": {"redfin"},
        "FRED (Macro Rates & CPI)": {"fred_macro"},
        "FRED (Unemployment)": {"fred_unemp"},
    }    

    allowed_sources = FAMILY_SOURCES.get(family)
    if not allowed_sources:
        return metric_ids

    return [m for m in metric_ids if src_map.get(m) in allowed_sources]


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
    Works for monthly AND quarterly series.
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
            scale=alt.Scale(zero=False, nice=True),
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
            title="Date",
        ),
        y=alt.Y(
            "value:Q",
            title="Value",
            scale=alt.Scale(zero=False, nice=True),
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
# Reusable renderer: per-family tab (multi-geo, single metric)
# -------------------------------------------------------------------
def render_family_tab(
    family_name: str,
    tab_container,
    geo_df: pd.DataFrame,
    label_to_id: dict,
    metric_options: List[str],
):
    # Use family_name to make widget keys unique per tab
    key_prefix = (
        family_name.replace(" ", "_")
        .replace("–", "_")
        .replace("(", "")
        .replace(")", "")
        .lower()
    )

    with tab_container:
        st.subheader(f"{family_name} – compare geographies for one metric")

        col1, col2 = st.columns([1, 2])
        with col1:
            # Metric family selectbox (default to this tab's family)
            family_default_index = (
                METRIC_FAMILIES.index(family_name)
                if family_name in METRIC_FAMILIES
                else 0
            )

            family = st.selectbox(
                "Metric family",
                METRIC_FAMILIES,
                index=family_default_index,
                key=f"{key_prefix}_metric_family",
            )

            # Filter metrics by the selected family
            filtered_metric_options = filter_metrics_by_family(
                metric_options, family
            )

            metric_id = st.selectbox(
                "Metric",
                filtered_metric_options,
                index=0,
                key=f"{key_prefix}_metric",
            )

            meta = _metric_meta(metric_id)
            st.caption(
                f"**Metric:** {meta['label']}  \n"
                f"**Unit:** {meta['unit'] or '—'}"
            )

            # Tooltip-style help for availability / coverage
            render_metric_help(metric_id)

            # Redfin property type selector
            redfin_property_type_id = None
            if is_redfin_metric(metric_id):
                pt_df = load_redfin_property_types()
                if not pt_df.empty:
                    if "all" in pt_df["property_type_id"].values:
                        default_idx = pt_df.index[
                            pt_df["property_type_id"] == "all"
                        ][0]
                    else:
                        default_idx = 0

                    prop_label = st.selectbox(
                        "Property type",
                        options=pt_df["label"].tolist(),
                        index=default_idx,
                        key=f"{key_prefix}_redfin_property_type",
                    )
                    redfin_property_type_id = pt_df.loc[
                        pt_df["label"] == prop_label, "property_type_id"
                    ].iloc[0]

            # --- Optional: include U.S. national series if available ------------
            include_us = st.checkbox(
                "Include U.S. national series (if available)",
                value=False,
                key=f"{family_name}_include_us",
            )


            # --- Quick geo-family selector -----------------------------------
            geo_family_choice = st.selectbox(
                "Quick geography group",
                GEO_FAMILY_OPTIONS,
                index=GEO_FAMILY_OPTIONS.index("Custom selection")
                if "Custom selection" in GEO_FAMILY_OPTIONS
                else 0,
                key=f"{key_prefix}_geo_family",
            )

            if geo_family_choice == "Custom selection":
                default_geo_ids = [
                    g
                    for g in [
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
            else:
                target_levels = GEO_FAMILY_LEVEL_MAP.get(geo_family_choice, [])
                default_geo_ids = geo_df[
                    geo_df["level"].isin(target_levels)
                ]["geo_id"].tolist()
                if not default_geo_ids:
                    default_geo_ids = [geo_df["geo_id"].iloc[0]]

            # id_to_label is defined globally after geo_df load
            default_labels = [
                id_to_label[g] for g in default_geo_ids if g in id_to_label
            ]

            selected_labels = st.multiselect(
                "Geographies",
                options=geo_df["label"].tolist(),
                default=default_labels,
                key=f"{key_prefix}_geo_multiselect",
            )

        # Map labels back to geo_ids for this tab
        selected_geos = [label_to_id[l] for l in selected_labels]

        # Optionally append us_nation if the user wants it and the metric has data
        if include_us and "us_nation" in label_to_id.values():
            if metric_has_us_nation(metric_id):
                if "us_nation" not in selected_geos:
                    selected_geos.append("us_nation")


        with col2:
            df = load_series_for_metric(
                selected_geos,
                metric_id,
                property_type_id=redfin_property_type_id,
            )

            # Show which geos have no data for this metric
            show_missing_geo_notice(selected_geos, df, geo_df, metric_id)

            if df.empty:
                st.warning("No data for this metric and geography selection.")
            else:
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
# App layout
# -------------------------------------------------------------------

st.title("Market Intel Dashboard")

geo_df = load_geo_options()
metric_options = load_metric_options()

if geo_df.empty or not metric_options:
    st.error("No data found in DuckDB — run the ingest + transform pipeline first.")
    st.stop()

# NEW: build geo-family options once, for all tabs
GEO_FAMILY_OPTIONS, GEO_FAMILY_LEVEL_MAP = build_geo_family_options(geo_df)

# helpful map for all tabs
label_to_id = dict(zip(geo_df["label"], geo_df["geo_id"]))
id_to_label = dict(zip(geo_df["geo_id"], geo_df["label"]))
id_to_level = dict(zip(geo_df["geo_id"], geo_df["level"]))

# Tabs:
#  - One per metric family (excluding "All")
#  - Then Single-geo dual-metric
#  - Then Benchmark compare
family_tab_names = [f for f in METRIC_FAMILIES if f != "All"]

tabs = st.tabs(
    family_tab_names
    + [
        "Single geo (2 metrics)",
        "Benchmark compare",
    ]
)

family_tabs = tabs[:len(family_tab_names)]
single_geo_tab = tabs[len(family_tab_names)]
benchmark_tab = tabs[len(family_tab_names) + 1]

# -------------------------------------------------------------------
# FAMILY TABS (multi-geo, single metric)
# -------------------------------------------------------------------
for fam_name, tab in zip(family_tab_names, family_tabs):
    render_family_tab(
        fam_name,
        tab,
        geo_df=geo_df,
        label_to_id=label_to_id,
        metric_options=metric_options,
    )

# -------------------------------------------------------------------
# TAB 2: Single geo, up to 2 metrics (dual axis)
# -------------------------------------------------------------------
with single_geo_tab:
    st.subheader("Single geography – up to 2 metrics")

    col1, col2 = st.columns([1, 2])
    
    with col1:
        # --- NEW: geo level filter ------------------------------------------
        levels_present = sorted(
            [lv for lv in geo_df["level"].dropna().unique()]
        )
        level_filter_options = ["All levels"] + levels_present
    
        level_filter = st.selectbox(
            "Filter geographies by level (optional)",
            level_filter_options,
            index=0,
            key="tab2_geo_level_filter",
        )
    
        if level_filter == "All levels":
            geo_choices_df = geo_df
        else:
            geo_choices_df = geo_df[geo_df["level"] == level_filter]
            if geo_choices_df.empty:
                geo_choices_df = geo_df
    
        # single geo select
        default_geo = (
            "dc_state"
            if "dc_state" in geo_choices_df["geo_id"].values
            else geo_choices_df["geo_id"].iloc[0]
        )
        default_label = id_to_label.get(default_geo, geo_choices_df["label"].iloc[0])
    
        geo_label = st.selectbox(
            "Geography",
            options=geo_choices_df["label"].tolist(),
            index=geo_choices_df["label"].tolist().index(default_label)
            if default_label in geo_choices_df["label"].tolist()
            else 0,
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
    
        # Help text based on Metric 1's source (CES / Redfin / etc.)
        render_metric_help(metric1)
    
        # Redfin property type selector (applies to any Redfin metric here)
        redfin_property_type_id_tab2 = None
        if is_redfin_metric(metric1) or (metric2 and is_redfin_metric(metric2)):
            pt_df = load_redfin_property_types()
            if not pt_df.empty:
                if "all" in pt_df["property_type_id"].values:
                    default_idx = pt_df.index[
                        pt_df["property_type_id"] == "all"
                    ][0]
                else:
                    default_idx = 0
    
                prop_label2 = st.selectbox(
                    "Property type (Redfin metrics)",
                    options=pt_df["label"].tolist(),
                    index=default_idx,
                    key="tab2_redfin_property_type",
                )
                redfin_property_type_id_tab2 = pt_df.loc[
                    pt_df["label"] == prop_label2, "property_type_id"
                ].iloc[0]

    

    with col2:
        # Metric 1
        df_left = load_series_for_geo_metric(
            geo_id,
            metric1,
            property_type_id=(
                redfin_property_type_id_tab2 if is_redfin_metric(metric1) else None
            ),
        )
        df_left["geo_name"] = geo_label
    
        # Metric 2 (optional)
        df_right = pd.DataFrame()
        if metric2:
            df_right = load_series_for_geo_metric(
                geo_id,
                metric2,
                property_type_id=(
                    redfin_property_type_id_tab2 if (metric2 and is_redfin_metric(metric2)) else None
                ),
            )
            df_right["geo_name"] = geo_label
    
        if df_left.empty and (metric2 is None or df_right.empty):
            st.warning(
                "No data for the selected metric(s) at this geography. "
                "Try a different metric or a broader geography level."
            )
        elif df_left.empty and metric2 and not df_right.empty:
            # Only metric 2 has data → show single-metric chart for metric2
            meta_right = _metric_meta(metric2)
            df_plot = df_right.copy()
            df_plot["geo_id"] = geo_id
            st.info(
                f"No data for **{metric1}** at {geo_label}. "
                f"Showing only **{metric2}**."
            )
            chart = make_line_with_points(
                df_plot,
                x_field="date",
                y_field="value",
                color_field=None,
                y_title=f"Value ({meta_right['unit']})" if meta_right["unit"] else "Value",
            )
            st.altair_chart(chart, use_container_width=True)
        elif not df_left.empty and metric2 and df_right.empty:
            # Only metric 1 has data
            meta_left = _metric_meta(metric1)
            df_plot = df_left.copy()
            df_plot["geo_id"] = geo_id
            st.info(
                f"No data for **{metric2}** at {geo_label}. "
                f"Showing only **{metric1}**."
            )
            chart = make_line_with_points(
                df_plot,
                x_field="date",
                y_field="value",
                color_field=None,
                y_title=f"Value ({meta_left['unit']})" if meta_left["unit"] else "Value",
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            # Both have data → dual-axis chart
            chart = make_dual_axis_chart(df_left, df_right, metric1, metric2)
            st.altair_chart(chart, use_container_width=True)



# -------------------------------------------------------------------
# TAB 3: Benchmark compare (flexible baseline)
# -------------------------------------------------------------------
with benchmark_tab:
    st.subheader("Compare geos vs a baseline")

    col1, col2 = st.columns([1, 2])

    with col1:
        # Metric family filter here too (separate keys so widgets don't clash)
        family_bench = st.selectbox(
            "Metric family",
            METRIC_FAMILIES,
            index=0,
            key="bench_family",
        )

        bench_metric_options = filter_metrics_by_family(metric_options, family_bench)

        metric_bench = st.selectbox(
            "Metric",
            options=bench_metric_options,
            index=0,
            key="bench_metric",
        )

        # Help text / tooltip for benchmark metric
        render_metric_help(metric_bench)

        redfin_property_type_id_tab3 = None
        if is_redfin_metric(metric_bench):
            pt_df = load_redfin_property_types()
            if not pt_df.empty:
                if "all" in pt_df["property_type_id"].values:
                    default_idx = pt_df.index[
                        pt_df["property_type_id"] == "all"
                    ][0]
                else:
                    default_idx = 0

                prop_label3 = st.selectbox(
                    "Property type (Redfin)",
                    options=pt_df["label"].tolist(),
                    index=default_idx,
                    key="tab3_redfin_property_type",
                )
                redfin_property_type_id_tab3 = pt_df.loc[
                    pt_df["label"] == prop_label3, "property_type_id"
                ].iloc[0]

        meta_bench = _metric_meta(metric_bench)

        
        # choose baseline geo (single, always included)
        default_bench_geo_id = "dc_msa" if "dc_msa" in geo_df["geo_id"].values else geo_df["geo_id"].iloc[0]
        default_bench_label = id_to_label.get(default_bench_geo_id, geo_df["label"].iloc[0])
    
        benchmark_label = st.selectbox(
            "Baseline geography (always shown)",
            options=geo_df["label"].tolist(),
            index=geo_df["label"].tolist().index(default_bench_label),
            key="bench_baseline_geo",
        )
        benchmark_geo_id = label_to_id[benchmark_label]
    
        # --- NEW: quick geo-family selector for "other geos" ---------------
        family_bench_geo = st.selectbox(
            "Quick group for other geographies",
            GEO_FAMILY_OPTIONS,
            index=GEO_FAMILY_OPTIONS.index("Custom selection")
            if "Custom selection" in GEO_FAMILY_OPTIONS else 0,
            key="bench_geo_family",
        )
    
        if family_bench_geo == "Custom selection":
            other_default_ids = [
                g for g in [
                    "md_state",
                    "va_state",
                    "baltimore_msa",
                ]
                if g in geo_df["geo_id"].values and g != benchmark_geo_id
            ]
        else:
            lvls = GEO_FAMILY_LEVEL_MAP.get(family_bench_geo, [])
            other_default_ids = geo_df[
                (geo_df["level"].isin(lvls)) &
                (geo_df["geo_id"] != benchmark_geo_id)
            ]["geo_id"].tolist()
    
        other_default_labels = [
            id_to_label[g] for g in other_default_ids if g in id_to_label
        ]
    
        other_labels = st.multiselect(
            "Other geographies to compare",
            options=[l for l in geo_df["label"].tolist() if l != benchmark_label],
            default=other_default_labels,
            key="bench_other_geos",
        )



    with col2:
        
        other_geo_ids = [label_to_id[l] for l in other_labels]
        all_geo_ids = [benchmark_geo_id] + [g for g in other_geo_ids if g != benchmark_geo_id]
        
        df_metric = load_series_for_metric(
            all_geo_ids,
            metric_bench,
            property_type_id=redfin_property_type_id_tab3,
        )
        
        # Show which selected geos have no data for this metric
        show_missing_geo_notice(all_geo_ids, df_metric, geo_df, metric_bench)
        
        if df_metric.empty:
            st.warning("No data for this metric + geo selection.")
        else:
            # attach geo_name from geo_df.label
            geo_names = geo_df[["geo_id", "label"]].rename(columns={"label": "geo_name"})
            df_metric = df_metric.merge(geo_names, on="geo_id", how="left")
            df_metric["metric_id"] = metric_bench
        
            chart = make_baseline_compare_chart(df_metric, pinned_geo_id=benchmark_geo_id)
            st.altair_chart(chart, use_container_width=True)
        
