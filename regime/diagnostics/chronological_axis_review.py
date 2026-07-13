from __future__ import annotations
# regime/diagnostics/chronological_axis_review.py

from pathlib import Path
from typing import Iterable

import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)


DEFAULT_RUN_ID = "macro_regime_v1_bps120"

DEFAULT_REVIEW_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

PRODUCTION_AXES = (
    "demand",
    "supply",
)

PRODUCTION_DIMENSIONS = (
    "demand",
    "supply",
    "affordability",
    "price",
    "capital_markets",
)


def _resolve_column(
    dataframe: pd.DataFrame,
    candidates: Iterable[str],
    *,
    label: str,
) -> str:
    for candidate in candidates:
        if candidate in dataframe.columns:
            return candidate

    raise ValueError(
        f"Could not resolve {label}. "
        f"Expected one of {list(candidates)}, "
        f"found columns {list(dataframe.columns)}"
    )


def _prepare_axis_scores(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "axis",
    }

    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(
            "axis_scores artifact is missing columns: "
            f"{sorted(missing)}"
        )

    date_column = _resolve_column(
        dataframe,
        [
            "evaluation_date",
            "date",
        ],
        label="axis-score date column",
    )

    score_column = _resolve_column(
        dataframe,
        [
            "axis_score",
            "score",
        ],
        label="axis-score value column",
    )

    out = dataframe[
        [
            "geo_id",
            date_column,
            "axis",
            score_column,
        ]
    ].copy()

    out = out.rename(
        columns={
            date_column: "date",
            score_column: "axis_score",
        }
    )

    production_axes = set(
        PRODUCTION_AXES
    )

    out = out[
        out["axis"].isin(production_axes)
    ].copy()

    actual_axes = set(
        out["axis"].dropna().astype(str)
    )

    missing_axes = (
        production_axes - actual_axes
    )

    if missing_axes:
        raise ValueError(
            "axis_scores artifact is missing required "
            "production axes: "
            f"{sorted(missing_axes)}"
        )

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    out["axis_score"] = pd.to_numeric(
        out["axis_score"],
        errors="coerce",
    )

    invalid = out[
        out["date"].isna()
        | out["axis_score"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "axis_scores contains invalid dates or scores:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicate_keys = out.duplicated(
        subset=[
            "geo_id",
            "date",
            "axis",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Duplicate axis-score rows detected:\n"
            + out.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    return out


def _prepare_dimension_scores(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "dimension",
    }

    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(
            "dimension_scores artifact is missing columns: "
            f"{sorted(missing)}"
        )

    date_column = _resolve_column(
        dataframe,
        [
            "evaluation_date",
            "date",
        ],
        label="dimension-score date column",
    )

    score_column = _resolve_column(
        dataframe,
        [
            "dimension_score",
            "score",
        ],
        label="dimension-score value column",
    )

    columns = [
        "geo_id",
        date_column,
        "dimension",
        score_column,
    ]

    optional_columns = [
        "metric_count",
        "metric_weight_sum",
        "min_metric_score",
        "max_metric_score",
    ]

    columns.extend(
        column
        for column in optional_columns
        if column in dataframe.columns
    )

    out = dataframe[columns].copy()

    out = out.rename(
        columns={
            date_column: "date",
            score_column: "dimension_score",
        }
    )

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    out["dimension_score"] = pd.to_numeric(
        out["dimension_score"],
        errors="coerce",
    )

    invalid = out[
        out["date"].isna()
        | out["dimension_score"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "dimension_scores contains invalid dates or scores:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicate_keys = out.duplicated(
        subset=[
            "geo_id",
            "date",
            "dimension",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Duplicate dimension-score rows detected:\n"
            + out.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    return out


def _prepare_regime_assignments(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "major_regime",
        "minor_regime",
    }

    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(
            "regime_assignments artifact is missing columns: "
            f"{sorted(missing)}"
        )

    out = dataframe.copy()

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    if out["date"].isna().any():
        raise ValueError(
            "regime_assignments contains invalid dates"
        )

    duplicate_keys = out.duplicated(
        subset=[
            "geo_id",
            "date",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Duplicate regime-assignment rows detected:\n"
            + out.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    return out


def _prepare_freshness(
    component_freshness: pd.DataFrame,
    evaluation_calendar: pd.DataFrame,
) -> pd.DataFrame:
    """
    Align component-level derived freshness to regime evaluation dates.

    The latest derived observation on or before each evaluation date is
    selected, and component age/status is recalculated at the evaluation
    date rather than copied unchanged from the derived observation date.
    """
    required = {
        "geo_id",
        "date",
        "derived_metric_key",
        "component_metric_key",
        "component_source_date",
        "warning_days",
        "hard_days",
    }

    missing = required - set(
        component_freshness.columns
    )

    if missing:
        raise ValueError(
            "derived_input_component_freshness artifact "
            "is missing columns: "
            f"{sorted(missing)}"
        )

    components = component_freshness.copy()

    components["date"] = (
        pd.to_datetime(
            components["date"],
            errors="coerce",
        )
        .astype("datetime64[ns]")
    )

    components["component_source_date"] = (
        pd.to_datetime(
            components[
                "component_source_date"
            ],
            errors="coerce",
        )
        .astype("datetime64[ns]")
    )

    components["warning_days"] = pd.to_numeric(
        components["warning_days"],
        errors="coerce",
    )

    components["hard_days"] = pd.to_numeric(
        components["hard_days"],
        errors="coerce",
    )

    invalid = components[
        components["date"].isna()
        | components[
            "component_source_date"
        ].isna()
        | components["warning_days"].isna()
        | components["hard_days"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "Component freshness contains invalid "
            "dates or policy horizons:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    calendar = (
        evaluation_calendar[
            [
                "geo_id",
                "date",
            ]
        ]
        .drop_duplicates()
        .copy()
    )

    calendar["date"] = (
        pd.to_datetime(
            calendar["date"],
            errors="coerce",
        )
        .astype("datetime64[ns]")
    )

    aligned_rows: list[pd.DataFrame] = []

    component_groups = components.groupby(
        [
            "geo_id",
            "derived_metric_key",
            "component_metric_key",
        ],
        dropna=False,
    )

    for (
        geo_id,
        derived_metric_key,
        component_metric_key,
    ), group in component_groups:
        geo_calendar = calendar[
            calendar["geo_id"].eq(geo_id)
        ].copy()

        if geo_calendar.empty:
            continue

        left = (
            geo_calendar[
                ["geo_id", "date"]
            ]
            .rename(
                columns={
                    "date": "evaluation_date"
                }
            )
            .sort_values("evaluation_date")
        )

        right = (
            group.rename(
                columns={
                    "date": (
                        "derived_observation_date"
                    )
                }
            )
            .sort_values(
                "derived_observation_date"
            )
        )

        left["evaluation_date"] = (
            pd.to_datetime(
                left["evaluation_date"],
                errors="coerce",
            )
            .astype("datetime64[ns]")
        )

        right["derived_observation_date"] = (
            pd.to_datetime(
                right[
                    "derived_observation_date"
                ],
                errors="coerce",
            )
            .astype("datetime64[ns]")
        )

        aligned = pd.merge_asof(
            left,
            right,
            left_on="evaluation_date",
            right_on=(
                "derived_observation_date"
            ),
            by="geo_id",
            direction="backward",
            allow_exact_matches=True,
        )

        aligned = aligned.dropna(
            subset=[
                "derived_observation_date",
                "component_source_date",
            ]
        )

        aligned["derived_metric_key"] = (
            derived_metric_key
        )
        aligned["component_metric_key"] = (
            component_metric_key
        )

        aligned_rows.append(aligned)

    if not aligned_rows:
        return pd.DataFrame()

    aligned = pd.concat(
        aligned_rows,
        ignore_index=True,
    )

    aligned["evaluation_component_age_days"] = (
        aligned["evaluation_date"]
        - aligned["component_source_date"]
    ).dt.days

    aligned["stale_input_flag"] = (
        aligned[
            "evaluation_component_age_days"
        ]
        > aligned["warning_days"]
    )

    aligned["exceeded_horizon_flag"] = (
        aligned[
            "evaluation_component_age_days"
        ]
        > aligned["hard_days"]
    )

    aligned["freshness_severity"] = 0

    aligned.loc[
        aligned["stale_input_flag"],
        "freshness_severity",
    ] = 1

    aligned.loc[
        aligned["exceeded_horizon_flag"],
        "freshness_severity",
    ] = 2

    summary = (
        aligned.groupby(
            [
                "geo_id",
                "evaluation_date",
            ],
            dropna=False,
        )
        .agg(
            derived_metric_count=(
                "derived_metric_key",
                "nunique",
            ),
            stale_derived_metric_count=(
                "stale_input_flag",
                "sum",
            ),
            exceeded_derived_metric_count=(
                "exceeded_horizon_flag",
                "sum",
            ),
            maximum_derived_freshness_severity=(
                "freshness_severity",
                "max",
            ),
            maximum_derived_component_age_days=(
                "evaluation_component_age_days",
                "max",
            ),
        )
        .reset_index()
        .rename(
            columns={
                "evaluation_date": "date"
            }
        )
    )

    reverse_status = {
        0: "fresh",
        1: "stale_warning",
        2: "hard_horizon_exceeded",
    }

    summary["derived_freshness_status"] = (
        summary[
            "maximum_derived_freshness_severity"
        ].map(reverse_status)
    )

    summary["any_stale_derived_input"] = (
        summary[
            "stale_derived_metric_count"
        ] > 0
    )

    summary[
        "any_exceeded_derived_horizon"
    ] = (
        summary[
            "exceeded_derived_metric_count"
        ] > 0
    )

    return summary


def _pivot_axis_scores(
    axis_scores: pd.DataFrame,
) -> pd.DataFrame:
    pivot = (
        axis_scores.pivot(
            index=[
                "geo_id",
                "date",
            ],
            columns="axis",
            values="axis_score",
        )
        .reset_index()
    )

    pivot.columns.name = None

    rename_map = {
        column: f"{column}_axis_score"
        for column in pivot.columns
        if column not in {
            "geo_id",
            "date",
        }
    }

    return pivot.rename(columns=rename_map)


def _pivot_dimension_scores(
    dimension_scores: pd.DataFrame,
) -> pd.DataFrame:
    pivot = (
        dimension_scores.pivot(
            index=[
                "geo_id",
                "date",
            ],
            columns="dimension",
            values="dimension_score",
        )
        .reset_index()
    )

    pivot.columns.name = None

    rename_map = {
        column: f"{column}_dimension_score"
        for column in pivot.columns
        if column not in {
            "geo_id",
            "date",
        }
    }

    return pivot.rename(columns=rename_map)


def _add_change_columns(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    out = timeline.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).copy()

    grouped = out.groupby(
        "geo_id",
        group_keys=False,
    )

    axis_score_columns = [
        f"{axis}_axis_score"
        for axis in PRODUCTION_AXES
        if f"{axis}_axis_score"
        in out.columns
    ]

    dimension_score_columns = [
        column
        for column in out.columns
        if column.endswith(
            "_dimension_score"
        )
    ]

    score_columns = (
        axis_score_columns
        + dimension_score_columns
    )

    for column in score_columns:
        out[f"{column}_change_1m"] = (
            grouped[column].diff()
        )

        out[
            f"{column}_absolute_change_1m"
        ] = out[
            f"{column}_change_1m"
        ].abs()

        out[f"{column}_change_3m"] = (
            grouped[column].diff(3)
        )

        out[f"{column}_change_12m"] = (
            grouped[column].diff(12)
        )

    out["previous_major_regime"] = (
        grouped["major_regime"].shift(1)
    )

    out["previous_minor_regime"] = (
        grouped["minor_regime"].shift(1)
    )

    out["major_changed"] = (
        out["major_regime"]
        != out["previous_major_regime"]
    ) & out["previous_major_regime"].notna()

    out["minor_changed"] = (
        out["minor_regime"]
        != out["previous_minor_regime"]
    ) & out["previous_minor_regime"].notna()

    return out


def _add_axis_dominance(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    out = timeline.copy()

    required = {
        "demand_axis_score",
        "supply_axis_score",
    }

    if not required.issubset(out.columns):
        out["dominant_axis"] = pd.NA
        out["dominant_axis_score"] = pd.NA
        out["axis_dominance_margin"] = pd.NA
        out["axis_dominance_ratio"] = pd.NA
        return out

    demand_absolute = out[
        "demand_axis_score"
    ].abs()

    supply_absolute = out[
        "supply_axis_score"
    ].abs()

    out["dominant_axis"] = "demand"

    out.loc[
        supply_absolute > demand_absolute,
        "dominant_axis",
    ] = "supply"

    ties = demand_absolute.eq(
        supply_absolute
    )

    out.loc[
        ties,
        "dominant_axis",
    ] = "balanced"

    out["dominant_axis_score"] = (
        out[
            [
                "demand_axis_score",
                "supply_axis_score",
            ]
        ]
        .abs()
        .max(axis=1)
    )

    out["axis_dominance_margin"] = (
        demand_absolute - supply_absolute
    ).abs()

    smaller_axis = (
        out[
            [
                "demand_axis_score",
                "supply_axis_score",
            ]
        ]
        .abs()
        .min(axis=1)
        .replace(0.0, pd.NA)
    )

    out["axis_dominance_ratio"] = (
        out["dominant_axis_score"]
        / smaller_axis
    )

    return out


def _build_axis_event_table(
    timeline: pd.DataFrame,
    *,
    top_n_per_axis: int,
) -> pd.DataFrame:
    axis_columns = [
        (
            f"{axis}_axis_score"
            "_absolute_change_1m"
        )
        for axis in PRODUCTION_AXES
        if (
            f"{axis}_axis_score"
            "_absolute_change_1m"
        ) in timeline.columns
    ]

    rows: list[pd.DataFrame] = []

    for absolute_change_column in axis_columns:
        axis_name = absolute_change_column.removesuffix(
            "_axis_score_absolute_change_1m"
        )

        score_column = (
            f"{axis_name}_axis_score"
        )

        change_column = (
            f"{score_column}_change_1m"
        )

        selected_columns = [
            "geo_id",
            "date",
            score_column,
            change_column,
            absolute_change_column,
            "major_regime",
            "minor_regime",
            "major_changed",
            "minor_changed",
        ]

        optional_columns = [
            "angle_degrees",
            "regime_strength",
            "distance_to_boundary_degrees",
            "derived_freshness_status",
            "any_stale_derived_input",
            "any_exceeded_derived_horizon",
        ]

        selected_columns.extend(
            column
            for column in optional_columns
            if column in timeline.columns
        )

        events = (
            timeline[
                selected_columns
            ]
            .dropna(
                subset=[
                    absolute_change_column
                ]
            )
            .sort_values(
                absolute_change_column,
                ascending=False,
            )
            .groupby(
                "geo_id",
                as_index=False,
                group_keys=False,
            )
            .head(top_n_per_axis)
            .copy()
        )

        events["axis"] = axis_name

        events = events.rename(
            columns={
                score_column: "axis_score",
                change_column: (
                    "axis_score_change_1m"
                ),
                absolute_change_column: (
                    "axis_score_absolute_change_1m"
                ),
            }
        )

        rows.append(events)

    if not rows:
        return pd.DataFrame()

    return (
        pd.concat(
            rows,
            ignore_index=True,
        )
        .sort_values(
            [
                "axis_score_absolute_change_1m",
                "date",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )


def _build_dimension_event_table(
    timeline: pd.DataFrame,
    *,
    top_n_per_dimension: int,
) -> pd.DataFrame:
    """
    Return the largest month-over-month dimension movements for each
    selected geography and production dimension.
    """
    if top_n_per_dimension <= 0:
        raise ValueError(
            "top_n_per_dimension must be greater than zero"
        )

    rows: list[pd.DataFrame] = []

    for dimension in PRODUCTION_DIMENSIONS:
        score_column = (
            f"{dimension}_dimension_score"
        )

        change_column = (
            f"{score_column}_change_1m"
        )

        absolute_change_column = (
            f"{score_column}_absolute_change_1m"
        )

        required_columns = {
            score_column,
            change_column,
            absolute_change_column,
        }

        if not required_columns.issubset(
            timeline.columns
        ):
            continue

        selected_columns = [
            "geo_id",
            "date",
            score_column,
            change_column,
            absolute_change_column,
            "demand_axis_score",
            "supply_axis_score",
            "demand_axis_score_change_1m",
            "supply_axis_score_change_1m",
            "major_regime",
            "minor_regime",
            "major_changed",
            "minor_changed",
        ]

        optional_columns = [
            "angle_degrees",
            "regime_strength",
            "distance_to_boundary_degrees",
            "derived_freshness_status",
            "any_stale_derived_input",
            "any_exceeded_derived_horizon",
        ]

        selected_columns.extend(
            column
            for column in optional_columns
            if column in timeline.columns
        )

        events = (
            timeline[selected_columns]
            .dropna(
                subset=[
                    absolute_change_column
                ]
            )
            .sort_values(
                absolute_change_column,
                ascending=False,
            )
            .groupby(
                "geo_id",
                as_index=False,
                group_keys=False,
            )
            .head(top_n_per_dimension)
            .copy()
        )

        events["dimension"] = dimension

        events = events.rename(
            columns={
                score_column: "dimension_score",
                change_column: (
                    "dimension_score_change_1m"
                ),
                absolute_change_column: (
                    "dimension_score_absolute_change_1m"
                ),
            }
        )

        rows.append(events)

    if not rows:
        return pd.DataFrame()

    return (
        pd.concat(
            rows,
            ignore_index=True,
        )
        .sort_values(
            [
                "dimension_score_absolute_change_1m",
                "date",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )
    
    
def _build_transition_timeline(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    return (
        timeline[
            timeline["major_changed"]
            | timeline["minor_changed"]
        ]
        .sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _build_axis_summary(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    axis_columns = [
        f"{axis}_axis_score"
        for axis in PRODUCTION_AXES
        if (
            f"{axis}_axis_score"
            in timeline.columns
            and (
                f"{axis}_axis_score"
                "_change_1m"
            ) in timeline.columns
        )
    ]

    if not axis_columns:
        raise ValueError(
            "No valid axis-score columns with change fields "
            "were found in the chronological timeline"
        )

    rows: list[dict[str, object]] = []

    for geo_id, geo_frame in timeline.groupby(
        "geo_id"
    ):
        for score_column in axis_columns:
            axis_name = score_column.removesuffix(
                "_axis_score"
            )

            change_column = (
                f"{score_column}_change_1m"
            )

            values = pd.to_numeric(
                geo_frame[score_column],
                errors="coerce",
            )

            changes = pd.to_numeric(
                geo_frame[change_column],
                errors="coerce",
            )

            valid_changes = changes.dropna()

            largest_positive_date = None
            largest_negative_date = None

            if not valid_changes.empty:
                largest_positive_index = (
                    valid_changes.idxmax()
                )
                largest_negative_index = (
                    valid_changes.idxmin()
                )

                largest_positive_date = (
                    geo_frame.loc[
                        largest_positive_index,
                        "date",
                    ]
                )

                largest_negative_date = (
                    geo_frame.loc[
                        largest_negative_index,
                        "date",
                    ]
                )

            rows.append(
                {
                    "geo_id": geo_id,
                    "axis": axis_name,
                    "rows": int(
                        values.notna().sum()
                    ),
                    "first_date": (
                        geo_frame["date"].min()
                    ),
                    "last_date": (
                        geo_frame["date"].max()
                    ),
                    "mean_axis_score": (
                        values.mean()
                    ),
                    "median_axis_score": (
                        values.median()
                    ),
                    "axis_score_std": (
                        values.std()
                    ),
                    "minimum_axis_score": (
                        values.min()
                    ),
                    "maximum_axis_score": (
                        values.max()
                    ),
                    "mean_absolute_change_1m": (
                        valid_changes.abs().mean()
                    ),
                    "median_absolute_change_1m": (
                        valid_changes.abs().median()
                    ),
                    "p90_absolute_change_1m": (
                        valid_changes.abs().quantile(
                            0.90
                        )
                    ),
                    "maximum_absolute_change_1m": (
                        valid_changes.abs().max()
                    ),
                    "largest_positive_change_1m": (
                        valid_changes.max()
                    ),
                    "largest_positive_change_date": (
                        largest_positive_date
                    ),
                    "largest_negative_change_1m": (
                        valid_changes.min()
                    ),
                    "largest_negative_change_date": (
                        largest_negative_date
                    ),
                }
            )

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "geo_id",
                "axis",
            ]
        )
        .reset_index(drop=True)
    )


def _build_dimension_summary(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    """
    Summarize level and month-over-month volatility for each production
    dimension and geography.
    """
    rows: list[dict[str, object]] = []

    for geo_id, geo_frame in timeline.groupby(
        "geo_id"
    ):
        for dimension in PRODUCTION_DIMENSIONS:
            score_column = (
                f"{dimension}_dimension_score"
            )

            change_column = (
                f"{score_column}_change_1m"
            )

            if (
                score_column not in geo_frame.columns
                or change_column
                not in geo_frame.columns
            ):
                continue

            values = pd.to_numeric(
                geo_frame[score_column],
                errors="coerce",
            )

            changes = pd.to_numeric(
                geo_frame[change_column],
                errors="coerce",
            )

            valid_values = values.dropna()
            valid_changes = changes.dropna()

            largest_positive_date = None
            largest_negative_date = None

            if not valid_changes.empty:
                positive_index = (
                    valid_changes.idxmax()
                )
                negative_index = (
                    valid_changes.idxmin()
                )

                largest_positive_date = (
                    geo_frame.loc[
                        positive_index,
                        "date",
                    ]
                )

                largest_negative_date = (
                    geo_frame.loc[
                        negative_index,
                        "date",
                    ]
                )

            rows.append(
                {
                    "geo_id": geo_id,
                    "dimension": dimension,
                    "rows": int(
                        valid_values.size
                    ),
                    "first_date": (
                        geo_frame.loc[
                            values.notna(),
                            "date",
                        ].min()
                    ),
                    "last_date": (
                        geo_frame.loc[
                            values.notna(),
                            "date",
                        ].max()
                    ),
                    "mean_dimension_score": (
                        valid_values.mean()
                    ),
                    "median_dimension_score": (
                        valid_values.median()
                    ),
                    "dimension_score_std": (
                        valid_values.std()
                    ),
                    "minimum_dimension_score": (
                        valid_values.min()
                    ),
                    "maximum_dimension_score": (
                        valid_values.max()
                    ),
                    "mean_absolute_change_1m": (
                        valid_changes.abs().mean()
                    ),
                    "median_absolute_change_1m": (
                        valid_changes.abs().median()
                    ),
                    "p90_absolute_change_1m": (
                        valid_changes.abs().quantile(
                            0.90
                        )
                    ),
                    "maximum_absolute_change_1m": (
                        valid_changes.abs().max()
                    ),
                    "largest_positive_change_1m": (
                        valid_changes.max()
                    ),
                    "largest_positive_change_date": (
                        largest_positive_date
                    ),
                    "largest_negative_change_1m": (
                        valid_changes.min()
                    ),
                    "largest_negative_change_date": (
                        largest_negative_date
                    ),
                }
            )

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "geo_id",
                "dimension",
            ]
        )
        .reset_index(drop=True)
    )
    
    
def _build_transition_dimension_context(
    transition_timeline: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach the largest dimension movement to every major or minor
    regime-transition month.

    This is descriptive attribution based on absolute month-over-month
    movement. It is not yet weighted causal contribution.
    """
    if transition_timeline.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []

    for _, row in transition_timeline.iterrows():
        dimension_changes: list[
            tuple[str, float, float]
        ] = []

        for dimension in PRODUCTION_DIMENSIONS:
            score_column = (
                f"{dimension}_dimension_score"
            )

            change_column = (
                f"{score_column}_change_1m"
            )

            if (
                score_column not in row.index
                or change_column not in row.index
            ):
                continue

            score = pd.to_numeric(
                pd.Series([row[score_column]]),
                errors="coerce",
            ).iloc[0]

            change = pd.to_numeric(
                pd.Series([row[change_column]]),
                errors="coerce",
            ).iloc[0]

            if pd.isna(change):
                continue

            dimension_changes.append(
                (
                    dimension,
                    float(score)
                    if not pd.isna(score)
                    else float("nan"),
                    float(change),
                )
            )

        if not dimension_changes:
            continue

        dimension_changes.sort(
            key=lambda item: abs(item[2]),
            reverse=True,
        )

        primary = dimension_changes[0]

        secondary = (
            dimension_changes[1]
            if len(dimension_changes) > 1
            else (
                None,
                float("nan"),
                float("nan"),
            )
        )

        sum_absolute_changes = sum(
            abs(item[2])
            for item in dimension_changes
        )

        primary_share = (
            abs(primary[2])
            / sum_absolute_changes
            if sum_absolute_changes > 0
            else float("nan")
        )

        output = {
            "geo_id": row["geo_id"],
            "date": row["date"],
            "previous_major_regime": (
                row["previous_major_regime"]
            ),
            "major_regime": row["major_regime"],
            "previous_minor_regime": (
                row["previous_minor_regime"]
            ),
            "minor_regime": row["minor_regime"],
            "major_changed": row["major_changed"],
            "minor_changed": row["minor_changed"],
            "demand_axis_score": (
                row["demand_axis_score"]
            ),
            "demand_axis_score_change_1m": (
                row[
                    "demand_axis_score_change_1m"
                ]
            ),
            "supply_axis_score": (
                row["supply_axis_score"]
            ),
            "supply_axis_score_change_1m": (
                row[
                    "supply_axis_score_change_1m"
                ]
            ),
            "primary_moving_dimension": (
                primary[0]
            ),
            "primary_dimension_score": (
                primary[1]
            ),
            "primary_dimension_change_1m": (
                primary[2]
            ),
            "primary_dimension_absolute_change_1m": (
                abs(primary[2])
            ),
            "primary_dimension_share_of_total_absolute_change": (
                primary_share
            ),
            "secondary_moving_dimension": (
                secondary[0]
            ),
            "secondary_dimension_score": (
                secondary[1]
            ),
            "secondary_dimension_change_1m": (
                secondary[2]
            ),
            "secondary_dimension_absolute_change_1m": (
                abs(secondary[2])
                if secondary[0] is not None
                else float("nan")
            ),
            "dimension_absolute_change_sum": (
                sum_absolute_changes
            ),
        }

        for optional_column in [
            "angle_degrees",
            "regime_strength",
            "distance_to_boundary_degrees",
            "derived_freshness_status",
            "any_stale_derived_input",
            "any_exceeded_derived_horizon",
        ]:
            if optional_column in row.index:
                output[optional_column] = (
                    row[optional_column]
                )

        rows.append(output)

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )
    
    
def build_chronological_axis_review(
    run_id: str = DEFAULT_RUN_ID,
    *,
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: list[str] | None = None,
    top_n_axis_events: int = 25,
) -> dict[str, pd.DataFrame]:
    """
    Build a chronological, run-aware review of persisted axis,
    dimension, regime, and freshness artifacts.

    No pipeline stages are recomputed.
    """
    if top_n_axis_events <= 0:
        raise ValueError(
            "top_n_axis_events must be greater than zero"
        )

    store = RegimeArtifactStore(
        artifact_root
    )

    manifest = store.read_manifest(
        run_id
    )

    if manifest.get("status") != "complete":
        raise ValueError(
            f"Run {run_id!r} is not complete: "
            f"{manifest.get('status')!r}"
        )

    axis_scores = _prepare_axis_scores(
        store.read_dataframe(
            run_id,
            "axis_scores",
        )
    )

    dimension_scores = _prepare_dimension_scores(
        store.read_dataframe(
            run_id,
            "dimension_scores",
        )
    )

    regimes = _prepare_regime_assignments(
        store.read_dataframe(
            run_id,
            "regime_assignments",
        )
    )

    if geo_ids is None:
        geo_ids = DEFAULT_REVIEW_GEOS.copy()

    axis_scores = axis_scores[
        axis_scores["geo_id"].isin(
            geo_ids
        )
    ].copy()

    dimension_scores = dimension_scores[
        dimension_scores["geo_id"].isin(
            geo_ids
        )
    ].copy()

    regimes = regimes[
        regimes["geo_id"].isin(
            geo_ids
        )
    ].copy()

    freshness = _prepare_freshness(
        store.read_dataframe(
            run_id,
            "derived_input_component_freshness",
        ),
        regimes[
            [
                "geo_id",
                "date",
            ]
        ],
    )

    freshness = freshness[
        freshness["geo_id"].isin(
            geo_ids
        )
    ].copy()

    if regimes.empty:
        raise ValueError(
            "No regime assignments found for "
            f"selected geographies: {geo_ids}"
        )

    axis_wide = _pivot_axis_scores(
        axis_scores
    )

    dimension_wide = _pivot_dimension_scores(
        dimension_scores
    )

    timeline = (
        regimes
        .merge(
            axis_wide,
            on=[
                "geo_id",
                "date",
            ],
            how="left",
            validate="one_to_one",
        )
        .merge(
            dimension_wide,
            on=[
                "geo_id",
                "date",
            ],
            how="left",
            validate="one_to_one",
        )
        .merge(
            freshness,
            on=[
                "geo_id",
                "date",
            ],
            how="left",
            validate="one_to_one",
        )
        .sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )

    required_axis_columns = [
        f"{axis}_axis_score"
        for axis in PRODUCTION_AXES
    ]

    missing_required_columns = (
        set(required_axis_columns)
        - set(timeline.columns)
    )

    if missing_required_columns:
        raise AssertionError(
            "Chronological timeline is missing "
            "production axis columns: "
            f"{sorted(missing_required_columns)}"
        )

    missing_axes = timeline[
        timeline[
            required_axis_columns
        ]
        .isna()
        .any(axis=1)
    ]

    if not missing_axes.empty:
        raise AssertionError(
            "Regime rows could not be matched to axis scores:\n"
            + missing_axes[
                [
                    "geo_id",
                    "date",
                    "major_regime",
                    "minor_regime",
                ]
            ]
            .head(30)
            .to_string(index=False)
        )

    timeline = _add_change_columns(
        timeline
    )

    timeline = _add_axis_dominance(
        timeline
    )

    axis_events = _build_axis_event_table(
        timeline,
        top_n_per_axis=top_n_axis_events,
    )

    dimension_events = (
        _build_dimension_event_table(
            timeline,
            top_n_per_dimension=(
                top_n_axis_events
            ),
        )
    )

    transition_timeline = (
        _build_transition_timeline(
            timeline
        )
    )

    axis_summary = _build_axis_summary(
        timeline
    )

    dimension_summary = (
        _build_dimension_summary(
            timeline
        )
    )

    transition_dimension_context = (
        _build_transition_dimension_context(
            transition_timeline
        )
    )

    latest_snapshot = (
        timeline.sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .groupby(
            "geo_id",
            as_index=False,
        )
        .tail(1)
        .reset_index(drop=True)
    )

    return {
        "monthly_timeline": timeline,
        "axis_events": axis_events,
        "axis_summary": axis_summary,
        "dimension_events": dimension_events,
        "dimension_summary": dimension_summary,
        "transition_timeline": (
            transition_timeline
        ),
        "transition_dimension_context": (
            transition_dimension_context
        ),
        "latest_snapshot": latest_snapshot,
    }
