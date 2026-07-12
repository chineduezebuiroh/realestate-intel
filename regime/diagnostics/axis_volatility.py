from __future__ import annotations
# regime/diagnostics/axis_volatility.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
)
from regime.diagnostics.axis_contribution import (
    DEFAULT_AUDIT_GEOS,
    DEFAULT_RUN_ID,
    FOCUS_METRICS,
    build_axis_contribution_audit,
)
from regime.diagnostics.chronological_axis_review import (
    build_chronological_axis_review,
)


DEFAULT_NEAR_ORIGIN_THRESHOLD = 0.15
DEFAULT_ROLLING_WINDOW = 12

REDFIN_FOCUS_METRICS = {
    "active_inventory",
    "median_sale_price",
    "median_ppsf",
    "price_to_income",
    "payment_burden",
}


def _validate_parameters(
    *,
    near_origin_threshold: float,
    rolling_window: int,
) -> None:
    if near_origin_threshold < 0:
        raise ValueError(
            "near_origin_threshold must be non-negative"
        )

    if rolling_window < 2:
        raise ValueError(
            "rolling_window must be at least two"
        )


def _safe_divide(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    numerator = pd.to_numeric(
        numerator,
        errors="coerce",
    )

    denominator = pd.to_numeric(
        denominator,
        errors="coerce",
    )

    return pd.Series(
        np.where(
            denominator.abs() > 0,
            numerator / denominator,
            np.nan,
        ),
        index=numerator.index,
        dtype="float64",
    )


def _add_generic_series_diagnostics(
    dataframe: pd.DataFrame,
    *,
    group_columns: list[str],
    date_column: str,
    level_column: str,
    change_column: str,
    rolling_window: int,
) -> pd.DataFrame:
    """
    Add comparable volatility diagnostics to a long-form series.

    Large-jump thresholds are calculated separately for each series as
    its historical 90th percentile absolute one-period change.
    """
    required = (
        set(group_columns)
        | {
            date_column,
            level_column,
            change_column,
        }
    )

    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(
            "Series diagnostic input is missing columns: "
            f"{sorted(missing)}"
        )

    out = dataframe.copy()

    out[date_column] = pd.to_datetime(
        out[date_column],
        errors="coerce",
    )

    out[level_column] = pd.to_numeric(
        out[level_column],
        errors="coerce",
    )

    out[change_column] = pd.to_numeric(
        out[change_column],
        errors="coerce",
    )

    invalid_dates = out[
        out[date_column].isna()
    ]

    if not invalid_dates.empty:
        raise ValueError(
            "Series diagnostic input contains invalid dates:\n"
            + invalid_dates.head(30).to_string(
                index=False
            )
        )

    out = out.sort_values(
        group_columns + [date_column]
    ).reset_index(drop=True)

    grouped = out.groupby(
        group_columns,
        group_keys=False,
        dropna=False,
    )

    out["absolute_change_1m"] = (
        out[change_column].abs()
    )

    out["previous_level"] = grouped[
        level_column
    ].shift(1)

    out["sign_flip_flag"] = (
        out[level_column].notna()
        & out["previous_level"].notna()
        & (
            np.sign(out[level_column])
            != np.sign(out["previous_level"])
        )
        & out[level_column].ne(0)
        & out["previous_level"].ne(0)
    )

    out["rolling_change_std"] = (
        grouped[change_column]
        .rolling(
            window=rolling_window,
            min_periods=max(
                3,
                rolling_window // 2,
            ),
        )
        .std()
        .reset_index(
            level=group_columns,
            drop=True,
        )
    )

    out["large_jump_threshold"] = (
        grouped["absolute_change_1m"]
        .transform(
            lambda values: values.quantile(
                0.90
            )
        )
    )

    out["large_jump_flag"] = (
        out["absolute_change_1m"].notna()
        & out["large_jump_threshold"].notna()
        & (
            out["absolute_change_1m"]
            >= out["large_jump_threshold"]
        )
    )

    out["calendar_month"] = (
        out[date_column].dt.month
    )

    return out


def _summarize_generic_volatility(
    dataframe: pd.DataFrame,
    *,
    group_columns: list[str],
    level_column: str,
    change_column: str,
) -> pd.DataFrame:
    summary = (
        dataframe.groupby(
            group_columns,
            dropna=False,
        )
        .agg(
            rows=(level_column, "count"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            mean_level=(level_column, "mean"),
            level_std=(level_column, "std"),
            mean_absolute_change_1m=(
                "absolute_change_1m",
                "mean",
            ),
            median_absolute_change_1m=(
                "absolute_change_1m",
                "median",
            ),
            p75_absolute_change_1m=(
                "absolute_change_1m",
                lambda values: values.quantile(
                    0.75
                ),
            ),
            p90_absolute_change_1m=(
                "absolute_change_1m",
                lambda values: values.quantile(
                    0.90
                ),
            ),
            p95_absolute_change_1m=(
                "absolute_change_1m",
                lambda values: values.quantile(
                    0.95
                ),
            ),
            maximum_absolute_change_1m=(
                "absolute_change_1m",
                "max",
            ),
            sign_flip_count=(
                "sign_flip_flag",
                "sum",
            ),
            sign_flip_rate=(
                "sign_flip_flag",
                "mean",
            ),
            large_jump_count=(
                "large_jump_flag",
                "sum",
            ),
            large_jump_rate=(
                "large_jump_flag",
                "mean",
            ),
            average_rolling_change_std=(
                "rolling_change_std",
                "mean",
            ),
            maximum_rolling_change_std=(
                "rolling_change_std",
                "max",
            ),
        )
        .reset_index()
    )

    summary["change_column"] = change_column

    return summary.sort_values(
        group_columns
    ).reset_index(drop=True)


def _build_metric_volatility(
    metric_contributions: pd.DataFrame,
    *,
    rolling_window: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    metric_history = (
        _add_generic_series_diagnostics(
            metric_contributions,
            group_columns=[
                "geo_id",
                "dimension",
                "canonical_metric_key",
            ],
            date_column="date",
            level_column="metric_score",
            change_column=(
                "metric_score_change_1m"
            ),
            rolling_window=rolling_window,
        )
    )

    metric_summary = (
        _summarize_generic_volatility(
            metric_history,
            group_columns=[
                "geo_id",
                "dimension",
                "canonical_metric_key",
            ],
            level_column="metric_score",
            change_column=(
                "metric_score_change_1m"
            ),
        )
    )

    contribution_summary = (
        metric_history.groupby(
            [
                "geo_id",
                "dimension",
                "canonical_metric_key",
            ],
            dropna=False,
        )
        .agg(
            configured_metric_weight=(
                "metric_weight",
                "first",
            ),
            average_effective_metric_weight=(
                "effective_metric_weight",
                "mean",
            ),
            mean_absolute_dimension_contribution_change_1m=(
                (
                    "metric_dimension_"
                    "contribution_change_1m"
                ),
                lambda values: values.abs().mean(),
            ),
            p90_absolute_dimension_contribution_change_1m=(
                (
                    "metric_dimension_"
                    "contribution_change_1m"
                ),
                lambda values: values.abs().quantile(
                    0.90
                ),
            ),
            maximum_absolute_dimension_contribution_change_1m=(
                (
                    "metric_dimension_"
                    "contribution_change_1m"
                ),
                lambda values: values.abs().max(),
            ),
        )
        .reset_index()
    )

    metric_summary = metric_summary.merge(
        contribution_summary,
        on=[
            "geo_id",
            "dimension",
            "canonical_metric_key",
        ],
        how="left",
        validate="one_to_one",
    )

    return (
        metric_history,
        metric_summary,
    )


def _build_dimension_volatility(
    axis_contributions: pd.DataFrame,
    *,
    rolling_window: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dimension_history = (
        _add_generic_series_diagnostics(
            axis_contributions,
            group_columns=[
                "geo_id",
                "axis",
                "dimension",
            ],
            date_column="date",
            level_column="dimension_score",
            change_column=(
                "dimension_score_change_1m"
            ),
            rolling_window=rolling_window,
        )
    )

    dimension_summary = (
        _summarize_generic_volatility(
            dimension_history,
            group_columns=[
                "geo_id",
                "axis",
                "dimension",
            ],
            level_column="dimension_score",
            change_column=(
                "dimension_score_change_1m"
            ),
        )
    )

    contribution_summary = (
        dimension_history.groupby(
            [
                "geo_id",
                "axis",
                "dimension",
            ],
            dropna=False,
        )
        .agg(
            configured_dimension_weight=(
                "dimension_weight",
                "first",
            ),
            average_effective_dimension_weight=(
                "effective_dimension_weight",
                "mean",
            ),
            mean_absolute_axis_contribution_change_1m=(
                (
                    "dimension_axis_"
                    "contribution_change_1m"
                ),
                lambda values: values.abs().mean(),
            ),
            p90_absolute_axis_contribution_change_1m=(
                (
                    "dimension_axis_"
                    "contribution_change_1m"
                ),
                lambda values: values.abs().quantile(
                    0.90
                ),
            ),
            maximum_absolute_axis_contribution_change_1m=(
                (
                    "dimension_axis_"
                    "contribution_change_1m"
                ),
                lambda values: values.abs().max(),
            ),
        )
        .reset_index()
    )

    dimension_summary = (
        dimension_summary.merge(
            contribution_summary,
            on=[
                "geo_id",
                "axis",
                "dimension",
            ],
            how="left",
            validate="one_to_one",
        )
    )

    return (
        dimension_history,
        dimension_summary,
    )


def _build_axis_history(
    timeline: pd.DataFrame,
    *,
    rolling_window: int,
    near_origin_threshold: float,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "demand_axis_score",
        "supply_axis_score",
        "major_changed",
        "minor_changed",
    }

    missing = required - set(
        timeline.columns
    )

    if missing:
        raise ValueError(
            "Chronological timeline is missing columns: "
            f"{sorted(missing)}"
        )

    rows: list[pd.DataFrame] = []

    for axis in (
        "demand",
        "supply",
    ):
        score_column = (
            f"{axis}_axis_score"
        )

        change_column = (
            f"{score_column}_change_1m"
        )

        selected = [
            "geo_id",
            "date",
            score_column,
            change_column,
            "major_regime",
            "minor_regime",
            "major_changed",
            "minor_changed",
        ]

        optional = [
            "regime_strength",
            "distance_to_boundary_degrees",
            "derived_freshness_status",
            "any_stale_derived_input",
            "any_exceeded_derived_horizon",
        ]

        selected.extend(
            column
            for column in optional
            if column in timeline.columns
        )

        frame = timeline[
            selected
        ].copy()

        frame["axis"] = axis

        frame = frame.rename(
            columns={
                score_column: "axis_score",
                change_column: (
                    "axis_score_change_1m"
                ),
            }
        )

        rows.append(frame)

    axis_history = pd.concat(
        rows,
        ignore_index=True,
    )

    axis_history = (
        _add_generic_series_diagnostics(
            axis_history,
            group_columns=[
                "geo_id",
                "axis",
            ],
            date_column="date",
            level_column="axis_score",
            change_column=(
                "axis_score_change_1m"
            ),
            rolling_window=rolling_window,
        )
    )

    axis_history["transition_month_flag"] = (
        axis_history["major_changed"]
        | axis_history["minor_changed"]
    )

    if "regime_strength" in axis_history.columns:
        axis_history["near_origin_flag"] = (
            pd.to_numeric(
                axis_history[
                    "regime_strength"
                ],
                errors="coerce",
            )
            < near_origin_threshold
        )
    else:
        axis_history["near_origin_flag"] = False

    return axis_history


def _build_axis_volatility_summary(
    axis_history: pd.DataFrame,
) -> pd.DataFrame:
    summary = (
        _summarize_generic_volatility(
            axis_history,
            group_columns=[
                "geo_id",
                "axis",
            ],
            level_column="axis_score",
            change_column=(
                "axis_score_change_1m"
            ),
        )
    )

    contextual = (
        axis_history.groupby(
            [
                "geo_id",
                "axis",
            ],
            dropna=False,
        )
        .agg(
            transition_month_count=(
                "transition_month_flag",
                "sum",
            ),
            mean_absolute_change_transition_months=(
                "absolute_change_1m",
                lambda values: values[
                    axis_history.loc[
                        values.index,
                        "transition_month_flag",
                    ]
                ].mean(),
            ),
            mean_absolute_change_nontransition_months=(
                "absolute_change_1m",
                lambda values: values[
                    ~axis_history.loc[
                        values.index,
                        "transition_month_flag",
                    ]
                ].mean(),
            ),
            near_origin_month_count=(
                "near_origin_flag",
                "sum",
            ),
            mean_absolute_change_near_origin=(
                "absolute_change_1m",
                lambda values: values[
                    axis_history.loc[
                        values.index,
                        "near_origin_flag",
                    ]
                ].mean(),
            ),
            mean_absolute_change_away_from_origin=(
                "absolute_change_1m",
                lambda values: values[
                    ~axis_history.loc[
                        values.index,
                        "near_origin_flag",
                    ]
                ].mean(),
            ),
        )
        .reset_index()
    )

    return summary.merge(
        contextual,
        on=[
            "geo_id",
            "axis",
        ],
        how="left",
        validate="one_to_one",
    )


def _build_seasonality_summary(
    dataframe: pd.DataFrame,
    *,
    group_columns: list[str],
) -> pd.DataFrame:
    return (
        dataframe.groupby(
            group_columns
            + ["calendar_month"],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            mean_absolute_change_1m=(
                "absolute_change_1m",
                "mean",
            ),
            median_absolute_change_1m=(
                "absolute_change_1m",
                "median",
            ),
            p90_absolute_change_1m=(
                "absolute_change_1m",
                lambda values: values.quantile(
                    0.90
                ),
            ),
            sign_flip_rate=(
                "sign_flip_flag",
                "mean",
            ),
            large_jump_rate=(
                "large_jump_flag",
                "mean",
            ),
        )
        .reset_index()
        .sort_values(
            group_columns
            + ["calendar_month"]
        )
        .reset_index(drop=True)
    )


def _build_hidden_volatility_summary(
    axis_change_attribution: pd.DataFrame,
    *,
    near_origin_context: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = axis_change_attribution.copy()

    work["hidden_volatility_ratio"] = (
        _safe_divide(
            work[
                (
                    "gross_dimension_"
                    "contribution_change_1m"
                )
            ],
            work[
                "axis_score_change_1m"
            ].abs(),
        )
    )

    work = work.merge(
        near_origin_context[
            [
                "geo_id",
                "date",
                "axis",
                "transition_month_flag",
                "near_origin_flag",
                "major_regime",
                "minor_regime",
            ]
        ],
        on=[
            "geo_id",
            "date",
            "axis",
        ],
        how="left",
        validate="one_to_one",
    )

    summary = (
        work.groupby(
            [
                "geo_id",
                "axis",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            mean_gross_dimension_movement=(
                (
                    "gross_dimension_"
                    "contribution_change_1m"
                ),
                "mean",
            ),
            p90_gross_dimension_movement=(
                (
                    "gross_dimension_"
                    "contribution_change_1m"
                ),
                lambda values: values.quantile(
                    0.90
                ),
            ),
            mean_absolute_net_axis_change=(
                "axis_score_change_1m",
                lambda values: values.abs().mean(),
            ),
            mean_cancellation_ratio=(
                "dimension_cancellation_ratio",
                "mean",
            ),
            p90_cancellation_ratio=(
                "dimension_cancellation_ratio",
                lambda values: values.quantile(
                    0.90
                ),
            ),
            mean_hidden_volatility_ratio=(
                "hidden_volatility_ratio",
                "mean",
            ),
            median_hidden_volatility_ratio=(
                "hidden_volatility_ratio",
                "median",
            ),
            p90_hidden_volatility_ratio=(
                "hidden_volatility_ratio",
                lambda values: values.replace(
                    [np.inf, -np.inf],
                    np.nan,
                ).quantile(0.90),
            ),
        )
        .reset_index()
    )

    events = (
        work[
            work[
                (
                    "gross_dimension_"
                    "contribution_change_1m"
                )
            ].notna()
            & work[
                "hidden_volatility_ratio"
            ].notna()
        ]
        .sort_values(
            [
                "hidden_volatility_ratio",
                (
                    "gross_dimension_"
                    "contribution_change_1m"
                ),
            ],
            ascending=[
                False,
                False,
            ],
        )
        .head(200)
        .reset_index(drop=True)
    )

    return (
        summary,
        events,
    )


def _build_transition_volatility(
    axis_history: pd.DataFrame,
) -> pd.DataFrame:
    return (
        axis_history[
            axis_history[
                "transition_month_flag"
            ]
        ]
        .sort_values(
            [
                "absolute_change_1m",
                "date",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )


def _build_focus_metric_volatility(
    metric_summary: pd.DataFrame,
) -> pd.DataFrame:
    work = metric_summary[
        metric_summary[
            "canonical_metric_key"
        ].isin(
            FOCUS_METRICS
        )
    ].copy()

    work["redfin_focus_flag"] = (
        work[
            "canonical_metric_key"
        ].isin(
            REDFIN_FOCUS_METRICS
        )
    )

    return work.sort_values(
        [
            "geo_id",
            "mean_absolute_change_1m",
        ],
        ascending=[
            True,
            False,
        ],
    ).reset_index(drop=True)


def build_axis_volatility_audit(
    run_id: str = DEFAULT_RUN_ID,
    *,
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: list[str] | None = None,
    near_origin_threshold: float = (
        DEFAULT_NEAR_ORIGIN_THRESHOLD
    ),
    rolling_window: int = (
        DEFAULT_ROLLING_WINDOW
    ),
) -> dict[str, pd.DataFrame]:
    """
    Measure visible and hidden volatility across metric, dimension, and
    axis layers for an immutable regime run.

    This audit does not alter transforms, weights, scores, or regimes.
    """
    _validate_parameters(
        near_origin_threshold=(
            near_origin_threshold
        ),
        rolling_window=rolling_window,
    )

    if geo_ids is None:
        geo_ids = (
            DEFAULT_AUDIT_GEOS.copy()
        )

    contribution = (
        build_axis_contribution_audit(
            run_id=run_id,
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    chronology = (
        build_chronological_axis_review(
            run_id=run_id,
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    metric_history, metric_summary = (
        _build_metric_volatility(
            contribution[
                "metric_contributions"
            ],
            rolling_window=rolling_window,
        )
    )

    (
        dimension_history,
        dimension_summary,
    ) = _build_dimension_volatility(
        contribution[
            "axis_contributions"
        ],
        rolling_window=rolling_window,
    )

    axis_history = _build_axis_history(
        chronology["monthly_timeline"],
        rolling_window=rolling_window,
        near_origin_threshold=(
            near_origin_threshold
        ),
    )

    axis_summary = (
        _build_axis_volatility_summary(
            axis_history
        )
    )

    metric_seasonality = (
        _build_seasonality_summary(
            metric_history,
            group_columns=[
                "geo_id",
                "dimension",
                "canonical_metric_key",
            ],
        )
    )

    dimension_seasonality = (
        _build_seasonality_summary(
            dimension_history,
            group_columns=[
                "geo_id",
                "axis",
                "dimension",
            ],
        )
    )

    axis_seasonality = (
        _build_seasonality_summary(
            axis_history,
            group_columns=[
                "geo_id",
                "axis",
            ],
        )
    )

    (
        hidden_volatility_summary,
        hidden_volatility_events,
    ) = _build_hidden_volatility_summary(
        contribution[
            "axis_change_attribution"
        ],
        near_origin_context=axis_history,
    )

    transition_volatility = (
        _build_transition_volatility(
            axis_history
        )
    )

    focus_metric_volatility = (
        _build_focus_metric_volatility(
            metric_summary
        )
    )

    return {
        "metric_volatility_history": (
            metric_history
        ),
        "metric_volatility_summary": (
            metric_summary
        ),
        "dimension_volatility_history": (
            dimension_history
        ),
        "dimension_volatility_summary": (
            dimension_summary
        ),
        "axis_volatility_history": (
            axis_history
        ),
        "axis_volatility_summary": (
            axis_summary
        ),
        "metric_seasonality": (
            metric_seasonality
        ),
        "dimension_seasonality": (
            dimension_seasonality
        ),
        "axis_seasonality": (
            axis_seasonality
        ),
        "hidden_volatility_summary": (
            hidden_volatility_summary
        ),
        "hidden_volatility_events": (
            hidden_volatility_events
        ),
        "transition_volatility": (
            transition_volatility
        ),
        "focus_metric_volatility": (
            focus_metric_volatility
        ),
    }
