from __future__ import annotations
# regime/derived_metrics.py

from typing import Iterable

import pandas as pd


MONTHLY_KEYS = {
    "median_sale_price",
    "mortgage_30y",
    "permit_activity",
}

ANNUAL_FFILL_KEYS = {
    "median_household_income",
    "population",
}

DERIVED_METRIC_COMPONENTS = {
    "price_to_income": [
        "median_sale_price",
        "median_household_income",
    ],
    "payment_burden": [
        "median_sale_price",
        "median_household_income",
        "mortgage_30y",
    ],
    "permit_intensity": [
        "permit_activity",
        "population",
    ],
}

DERIVED_OUTPUT_COLUMNS = [
    "geo_id",
    "date",
    "canonical_metric_key",
    "value",
]

LINEAGE_OUTPUT_COLUMNS = [
    "geo_id",
    "date",
    "derived_metric_key",
    "component_metric_key",
    "component_value",
    "component_source_date",
    "component_source_geo_id",
    "component_age_days",
    "component_age_months",
    "was_carried_forward",
]


def _empty_derived() -> pd.DataFrame:
    return pd.DataFrame(columns=DERIVED_OUTPUT_COLUMNS)


def _empty_lineage() -> pd.DataFrame:
    return pd.DataFrame(columns=LINEAGE_OUTPUT_COLUMNS)


def _source_date_column(metric_key: str) -> str:
    return f"{metric_key}__source_date"


def _source_geo_column(metric_key: str) -> str:
    return f"{metric_key}__source_geo_id"


def _build_wide_source_panel(
    raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a wide canonical metric table that retains each value's
    original source observation date and source geography.
    """
    work = raw.copy()
    work["date"] = pd.to_datetime(work["date"])

    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
    }
    missing = required - set(work.columns)

    if missing:
        raise ValueError(
            "Derived metric input is missing columns: "
            f"{sorted(missing)}"
        )

    duplicate_keys = work.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        duplicates = (
            work.loc[
                duplicate_keys,
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                ],
            ]
            .sort_values(
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                ]
            )
            .head(30)
        )

        raise ValueError(
            "Canonical metric input is not unique by "
            "geo/date/metric:\n"
            + duplicates.to_string(index=False)
        )

    work["component_source_date"] = work["date"]
    work["component_source_geo_id"] = work["geo_id"]

    values = (
        work.pivot(
            index=["geo_id", "date"],
            columns="canonical_metric_key",
            values="value",
        )
        .reset_index()
    )

    source_dates = (
        work.pivot(
            index=["geo_id", "date"],
            columns="canonical_metric_key",
            values="component_source_date",
        )
        .rename(
            columns=lambda key: _source_date_column(str(key))
        )
        .reset_index()
    )

    source_geos = (
        work.pivot(
            index=["geo_id", "date"],
            columns="canonical_metric_key",
            values="component_source_geo_id",
        )
        .rename(
            columns=lambda key: _source_geo_column(str(key))
        )
        .reset_index()
    )

    return (
        values
        .merge(
            source_dates,
            on=["geo_id", "date"],
            how="outer",
            validate="one_to_one",
        )
        .merge(
            source_geos,
            on=["geo_id", "date"],
            how="outer",
            validate="one_to_one",
        )
        .sort_values(["geo_id", "date"])
        .reset_index(drop=True)
    )


def _monthly_panel(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Build the monthly calculation panel while preserving source lineage.

    Annual inputs are carried forward together with their original
    observation dates and source geographies.
    """
    work = raw.copy()
    work["date"] = pd.to_datetime(work["date"])

    wide = _build_wide_source_panel(work)

    monthly_dates = (
        work[
            work["canonical_metric_key"].isin(MONTHLY_KEYS)
        ][["geo_id", "date"]]
        .drop_duplicates()
        .copy()
    )

    panel = (
        monthly_dates
        .merge(
            wide,
            on=["geo_id", "date"],
            how="left",
            validate="one_to_one",
        )
        .sort_values(["geo_id", "date"])
        .reset_index(drop=True)
    )

    # Carry annual values and their source metadata together.
    for metric_key in ANNUAL_FFILL_KEYS:
        value_column = metric_key
        source_date_column = _source_date_column(metric_key)
        source_geo_column = _source_geo_column(metric_key)

        columns_to_fill = [
            column
            for column in [
                value_column,
                source_date_column,
                source_geo_column,
            ]
            if column in panel.columns
        ]

        if columns_to_fill:
            panel[columns_to_fill] = (
                panel.groupby("geo_id")[columns_to_fill]
                .ffill()
            )

    # Broadcast national mortgage observations to each local geography.
    if "mortgage_30y" in wide.columns:
        mortgage_columns = [
            "date",
            "mortgage_30y",
            _source_date_column("mortgage_30y"),
            _source_geo_column("mortgage_30y"),
        ]

        mortgage = (
            wide[mortgage_columns]
            .dropna(subset=["mortgage_30y"])
            .drop_duplicates(subset=["date"])
            .copy()
        )

        panel = panel.drop(
            columns=[
                "mortgage_30y",
                _source_date_column("mortgage_30y"),
                _source_geo_column("mortgage_30y"),
            ],
            errors="ignore",
        )

        panel = panel.merge(
            mortgage,
            on="date",
            how="left",
            validate="many_to_one",
        )

    return panel.sort_values(
        ["geo_id", "date"]
    ).reset_index(drop=True)


def _long(
    df: pd.DataFrame,
    metric_key: str,
    value_column: str,
) -> pd.DataFrame:
    out = df[
        [
            "geo_id",
            "date",
            value_column,
        ]
    ].copy()

    out = out.rename(columns={value_column: "value"})
    out["canonical_metric_key"] = metric_key
    out = out.dropna(subset=["value"])

    return out[DERIVED_OUTPUT_COLUMNS]


def _lineage_rows(
    df: pd.DataFrame,
    *,
    derived_metric_key: str,
    derived_value_column: str,
    component_metric_keys: Iterable[str],
) -> pd.DataFrame:
    """
    Produce one lineage row per derived observation and input component.
    """
    valid = df.dropna(
        subset=[derived_value_column]
    ).copy()

    rows: list[pd.DataFrame] = []

    for component_key in component_metric_keys:
        source_date_column = _source_date_column(
            component_key
        )
        source_geo_column = _source_geo_column(
            component_key
        )

        required = {
            component_key,
            source_date_column,
            source_geo_column,
        }

        missing = required - set(valid.columns)

        if missing:
            raise ValueError(
                f"Missing lineage columns for "
                f"{derived_metric_key}/{component_key}: "
                f"{sorted(missing)}"
            )

        component = valid[
            [
                "geo_id",
                "date",
                component_key,
                source_date_column,
                source_geo_column,
            ]
        ].copy()

        component = component.rename(
            columns={
                component_key: "component_value",
                source_date_column: (
                    "component_source_date"
                ),
                source_geo_column: (
                    "component_source_geo_id"
                ),
            }
        )

        component["derived_metric_key"] = (
            derived_metric_key
        )
        component["component_metric_key"] = (
            component_key
        )

        component["component_source_date"] = (
            pd.to_datetime(
                component["component_source_date"],
                errors="coerce",
            )
        )

        component["component_age_days"] = (
            component["date"]
            - component["component_source_date"]
        ).dt.days

        component["component_age_months"] = (
            component["component_age_days"]
            / 30.4375
        )

        component["was_carried_forward"] = (
            component["component_age_days"] > 0
        )

        invalid_age = component[
            component["component_age_days"] < 0
        ]

        if not invalid_age.empty:
            raise AssertionError(
                "Derived input has a future source date:\n"
                + invalid_age.head(20).to_string(
                    index=False
                )
            )

        rows.append(
            component[LINEAGE_OUTPUT_COLUMNS]
        )

    if not rows:
        return _empty_lineage()

    return pd.concat(
        rows,
        ignore_index=True,
    )


def build_derived_metrics_with_lineage(
    raw: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build derived canonical metrics and their component lineage.

    Returns:
      derived_metrics:
          geo_id, date, canonical_metric_key, value

      derived_lineage:
          one row per derived observation and component, including
          original source date, source geography, and component age.

    No freshness horizons or suppression rules are applied here.
    This function exposes current behavior without changing it.
    """
    if raw.empty:
        return _empty_derived(), _empty_lineage()

    panel = _monthly_panel(raw)

    derived_outputs: list[pd.DataFrame] = []
    lineage_outputs: list[pd.DataFrame] = []

    if {
        "median_sale_price",
        "median_household_income",
    }.issubset(panel.columns):
        tmp = panel.copy()

        tmp["price_to_income"] = (
            tmp["median_sale_price"]
            / tmp["median_household_income"]
        )

        derived_outputs.append(
            _long(
                tmp,
                "price_to_income",
                "price_to_income",
            )
        )

        lineage_outputs.append(
            _lineage_rows(
                tmp,
                derived_metric_key="price_to_income",
                derived_value_column="price_to_income",
                component_metric_keys=(
                    DERIVED_METRIC_COMPONENTS[
                        "price_to_income"
                    ]
                ),
            )
        )

    if {
        "median_sale_price",
        "median_household_income",
        "mortgage_30y",
    }.issubset(panel.columns):
        tmp = panel.copy()

        annual_rate = tmp["mortgage_30y"] / 100.0
        monthly_rate = annual_rate / 12.0
        term_months = 360

        principal = (
            tmp["median_sale_price"] * 0.80
        )

        growth_factor = (
            1.0 + monthly_rate
        ) ** term_months

        payment = principal * (
            monthly_rate * growth_factor
        ) / (
            growth_factor - 1.0
        )

        monthly_income = (
            tmp["median_household_income"] / 12.0
        )

        tmp["payment_burden"] = (
            payment / monthly_income
        )

        derived_outputs.append(
            _long(
                tmp,
                "payment_burden",
                "payment_burden",
            )
        )

        lineage_outputs.append(
            _lineage_rows(
                tmp,
                derived_metric_key="payment_burden",
                derived_value_column="payment_burden",
                component_metric_keys=(
                    DERIVED_METRIC_COMPONENTS[
                        "payment_burden"
                    ]
                ),
            )
        )

    if {
        "permit_activity",
        "population",
    }.issubset(panel.columns):
        tmp = panel.copy()

        tmp["permit_intensity"] = (
            tmp["permit_activity"]
            / tmp["population"]
        ) * 1000.0

        derived_outputs.append(
            _long(
                tmp,
                "permit_intensity",
                "permit_intensity",
            )
        )

        lineage_outputs.append(
            _lineage_rows(
                tmp,
                derived_metric_key="permit_intensity",
                derived_value_column="permit_intensity",
                component_metric_keys=(
                    DERIVED_METRIC_COMPONENTS[
                        "permit_intensity"
                    ]
                ),
            )
        )

    if derived_outputs:
        derived = pd.concat(
            derived_outputs,
            ignore_index=True,
        )
    else:
        derived = _empty_derived()

    if lineage_outputs:
        lineage = pd.concat(
            lineage_outputs,
            ignore_index=True,
        )
    else:
        lineage = _empty_lineage()

    duplicate_derived = derived.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_derived.any():
        raise AssertionError(
            "Duplicate derived metric rows detected:\n"
            + derived.loc[
                duplicate_derived
            ].head(30).to_string(index=False)
        )

    duplicate_lineage = lineage.duplicated(
        subset=[
            "geo_id",
            "date",
            "derived_metric_key",
            "component_metric_key",
        ],
        keep=False,
    )

    if duplicate_lineage.any():
        raise AssertionError(
            "Duplicate derived lineage rows detected:\n"
            + lineage.loc[
                duplicate_lineage
            ].head(30).to_string(index=False)
        )

    return (
        derived.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        ).reset_index(drop=True),
        lineage.sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "date",
                "component_metric_key",
            ]
        ).reset_index(drop=True),
    )


def build_derived_metrics(
    raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    Backward-compatible derived metric interface.

    Existing callers continue to receive only the canonical derived
    metric dataframe. Lineage is available through
    build_derived_metrics_with_lineage().
    """
    derived, _ = build_derived_metrics_with_lineage(
        raw
    )

    return derived
