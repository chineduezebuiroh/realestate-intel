from __future__ import annotations
# regime/diagnostics/chronological_axis_review.py

from pathlib import Path
from typing import Iterable

import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)


DEFAULT_RUN_ID = "macro_regime_v1_freshness"

DEFAULT_REVIEW_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]


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
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "derived_metric_key",
        "derived_freshness_status",
        "stale_input_flag",
        "exceeded_horizon_flag",
    }

    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(
            "derived_input_freshness artifact is missing columns: "
            f"{sorted(missing)}"
        )

    out = dataframe.copy()

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    if out["date"].isna().any():
        raise ValueError(
            "derived_input_freshness contains invalid dates"
        )

    severity_map = {
        "fresh": 0,
        "stale_warning": 1,
        "hard_horizon_exceeded": 2,
    }

    out["freshness_severity"] = (
        out["derived_freshness_status"]
        .map(severity_map)
    )

    if out["freshness_severity"].isna().any():
        bad = sorted(
            out.loc[
                out["freshness_severity"].isna(),
                "derived_freshness_status",
            ]
            .dropna()
            .unique()
        )

        raise ValueError(
            "Unknown derived freshness statuses: "
            f"{bad}"
        )

    summary = (
        out.groupby(
            [
                "geo_id",
                "date",
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
                "governing_component_age_days",
                "max",
            ),
        )
        .reset_index()
    )

    reverse_status = {
        0: "fresh",
        1: "stale_warning",
        2: "hard_horizon_exceeded",
    }

    summary["derived_freshness_status"] = (
        summary[
            "maximum_derived_freshness_severity"
        ]
        .map(reverse_status)
    )

    summary["any_stale_derived_input"] = (
        summary["stale_derived_metric_count"] > 0
    )

    summary["any_exceeded_derived_horizon"] = (
        summary["exceeded_derived_metric_count"] > 0
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

    score_columns = [
        column
        for column in out.columns
        if (
            column.endswith("_axis_score")
            or column.endswith(
                "_dimension_score"
            )
        )
    ]

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
        column
        for column in timeline.columns
        if (
            column.endswith(
                "_axis_score_absolute_change_1m"
            )
        )
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
        column
        for column in timeline.columns
        if (
            column.endswith("_axis_score")
            and not column.endswith(
                "_previous_axis_score"
            )
        )
    ]

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

    freshness = _prepare_freshness(
        store.read_dataframe(
            run_id,
            "derived_input_freshness",
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

    missing_axes = timeline[
        timeline[
            [
                column
                for column
                in timeline.columns
                if column.endswith(
                    "_axis_score"
                )
            ]
        ]
        .isna()
        .all(axis=1)
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

    transition_timeline = (
        _build_transition_timeline(
            timeline
        )
    )

    axis_summary = _build_axis_summary(
        timeline
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
        "transition_timeline": (
            transition_timeline
        ),
        "axis_summary": axis_summary,
        "latest_snapshot": latest_snapshot,
    }
