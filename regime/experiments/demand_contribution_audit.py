from __future__ import annotations
# regime/experiments/demand_contribution_audit.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime._06_axis_engine import _build_axis_weights
from regime.artifacts import RegimeArtifactStore
from regime.experiments.linked_price_family_comparison import (
    BASELINE_RUN_ID,
    DEFAULT_ARTIFACT_ROOT,
    FOCUS_GEOS,
    build_linked_price_family_comparison,
)


TARGET_AXIS = "demand"

TARGET_DIMENSIONS = {
    "price",
    "affordability",
}

NEAR_ZERO_THRESHOLDS = (
    0.05,
    0.10,
)


def _validate_dimension_history(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "run_role",
        "geo_id",
        "date",
        "dimension",
        "dimension_score",
    }

    missing = required - set(
        frame.columns
    )

    if missing:
        raise ValueError(
            "Dimension history is missing "
            f"required columns: {sorted(missing)}"
        )

    work = frame.copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["dimension_score"] = pd.to_numeric(
        work["dimension_score"],
        errors="coerce",
    )

    invalid = work[
        work["date"].isna()
        | work["dimension_score"].isna()
        | ~np.isfinite(
            work["dimension_score"]
        )
    ]

    if not invalid.empty:
        raise ValueError(
            "Dimension history contains invalid rows:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicates = work.duplicated(
        subset=[
            "run_role",
            "geo_id",
            "date",
            "dimension",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Dimension history is not unique by "
            "run/geo/date/dimension:\n"
            + work.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return work


def _validate_axis_history(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "run_role",
        "geo_id",
        "date",
        "axis",
        "axis_score",
    }

    missing = required - set(
        frame.columns
    )

    if missing:
        raise ValueError(
            "Axis history is missing "
            f"required columns: {sorted(missing)}"
        )

    work = frame.copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["axis_score"] = pd.to_numeric(
        work["axis_score"],
        errors="coerce",
    )

    invalid = work[
        work["date"].isna()
        | work["axis_score"].isna()
        | ~np.isfinite(
            work["axis_score"]
        )
    ]

    if not invalid.empty:
        raise ValueError(
            "Axis history contains invalid rows:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicates = work.duplicated(
        subset=[
            "run_role",
            "geo_id",
            "date",
            "axis",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Axis history is not unique by "
            "run/geo/date/axis:\n"
            + work.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return work


def _demand_weights() -> pd.DataFrame:
    weights = _build_axis_weights()

    demand = weights[
        weights["axis"].eq(
            TARGET_AXIS
        )
    ][
        [
            "axis",
            "dimension",
            "dimension_weight",
        ]
    ].copy()

    if demand.empty:
        raise ValueError(
            "No enabled Demand-axis weights found"
        )

    if (
        demand[
            "dimension_weight"
        ].sum()
        - 1.0
    ).__abs__() > 0.001:
        raise AssertionError(
            "Demand dimension weights do not sum "
            "to 1.0"
        )

    duplicates = demand.duplicated(
        subset=[
            "dimension",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Demand axis contains duplicate "
            "dimension weights"
        )

    return demand


def _build_weighted_contributions(
    dimension_history: pd.DataFrame,
) -> pd.DataFrame:
    dimensions = _validate_dimension_history(
        dimension_history
    )

    weights = _demand_weights()

    work = dimensions.merge(
        weights,
        on="dimension",
        how="inner",
        validate="many_to_one",
    )

    if work.empty:
        raise ValueError(
            "No Demand dimensions were found in "
            "dimension history"
        )

    work[
        "weighted_contribution"
    ] = (
        work["dimension_score"]
        * work["dimension_weight"]
    )

    work[
        "is_price_family_dimension"
    ] = work[
        "dimension"
    ].isin(
        TARGET_DIMENSIONS
    )

    return work.sort_values(
        [
            "run_role",
            "geo_id",
            "date",
            "dimension",
        ]
    ).reset_index(
        drop=True
    )


def _build_monthly_panel(
    contributions: pd.DataFrame,
    axis_history: pd.DataFrame,
) -> pd.DataFrame:
    axes = _validate_axis_history(
        axis_history
    )

    demand_axes = axes[
        axes["axis"].eq(
            TARGET_AXIS
        )
    ][
        [
            "run_role",
            "geo_id",
            "date",
            "axis_score",
        ]
    ].copy()

    contribution_wide = (
        contributions.pivot(
            index=[
                "run_role",
                "geo_id",
                "date",
            ],
            columns="dimension",
            values="weighted_contribution",
        )
        .reset_index()
    )

    score_wide = (
        contributions.pivot(
            index=[
                "run_role",
                "geo_id",
                "date",
            ],
            columns="dimension",
            values="dimension_score",
        )
        .reset_index()
    )

    score_wide = score_wide.rename(
        columns={
            column: (
                f"{column}_dimension_score"
            )
            for column in score_wide.columns
            if column not in {
                "run_role",
                "geo_id",
                "date",
            }
        }
    )

    panel = contribution_wide.merge(
        score_wide,
        on=[
            "run_role",
            "geo_id",
            "date",
        ],
        how="inner",
        validate="one_to_one",
    )

    panel = panel.merge(
        demand_axes,
        on=[
            "run_role",
            "geo_id",
            "date",
        ],
        how="inner",
        validate="one_to_one",
    )

    contribution_columns = [
        dimension
        for dimension
        in contributions[
            "dimension"
        ].unique()
        if dimension in panel.columns
    ]

    panel[
        "reconstructed_demand_score"
    ] = panel[
        contribution_columns
    ].sum(
        axis=1,
        min_count=1,
    )

    panel[
        "reconstruction_error"
    ] = (
        panel[
            "reconstructed_demand_score"
        ]
        - panel[
            "axis_score"
        ]
    )

    if (
        panel[
            "reconstruction_error"
        ].abs().max()
        > 1e-12
    ):
        raise AssertionError(
            "Weighted Demand contributions do not "
            "reconstruct the axis exactly"
        )

    for dimension in TARGET_DIMENSIONS:
        if dimension not in panel.columns:
            panel[dimension] = 0.0

    panel[
        "price_affordability_contribution"
    ] = (
        panel.get(
            "price",
            0.0,
        )
        + panel.get(
            "affordability",
            0.0,
        )
    )

    other_dimensions = [
        column
        for column in contribution_columns
        if column not in TARGET_DIMENSIONS
    ]

    panel[
        "other_demand_contribution"
    ] = panel[
        other_dimensions
    ].sum(
        axis=1,
        min_count=1,
    )

    panel[
        "price_affordability_same_sign"
    ] = (
        np.sign(
            panel["price"]
        )
        == np.sign(
            panel["affordability"]
        )
    )

    panel[
        "price_affordability_opposite_sign"
    ] = (
        panel["price"].ne(0)
        & panel["affordability"].ne(0)
        & ~panel[
            "price_affordability_same_sign"
        ]
    )

    panel[
        "price_affordability_gross_magnitude"
    ] = (
        panel["price"].abs()
        + panel["affordability"].abs()
    )

    panel[
        "price_affordability_net_magnitude"
    ] = panel[
        "price_affordability_contribution"
    ].abs()

    panel[
        "price_affordability_cancellation_amount"
    ] = (
        panel[
            "price_affordability_gross_magnitude"
        ]
        - panel[
            "price_affordability_net_magnitude"
        ]
    )

    panel[
        "price_affordability_cancellation_rate"
    ] = np.where(
        panel[
            "price_affordability_gross_magnitude"
        ].gt(0),
        panel[
            "price_affordability_cancellation_amount"
        ]
        / panel[
            "price_affordability_gross_magnitude"
        ],
        0.0,
    )

    for threshold in NEAR_ZERO_THRESHOLDS:
        suffix = str(
            int(
                threshold * 100
            )
        ).zfill(3)

        panel[
            f"demand_near_zero_{suffix}"
        ] = panel[
            "axis_score"
        ].abs().lt(
            threshold
        )

    return panel.sort_values(
        [
            "run_role",
            "geo_id",
            "date",
        ]
    ).reset_index(
        drop=True
    )


def _build_run_comparison(
    monthly_panel: pd.DataFrame,
) -> pd.DataFrame:
    baseline = monthly_panel[
        monthly_panel[
            "run_role"
        ].eq("baseline")
    ].copy()

    challenger = monthly_panel[
        monthly_panel[
            "run_role"
        ].eq("challenger")
    ].copy()

    baseline = baseline.drop(
        columns=[
            "run_role",
        ]
    )

    challenger = challenger.drop(
        columns=[
            "run_role",
        ]
    )

    merged = baseline.merge(
        challenger,
        on=[
            "geo_id",
            "date",
        ],
        how="outer",
        suffixes=(
            "_baseline",
            "_challenger",
        ),
        indicator=True,
        validate="one_to_one",
    )

    if not merged[
        "_merge"
    ].eq("both").all():
        raise AssertionError(
            "Baseline and challenger Demand panels "
            "do not have identical geo/date coverage"
        )

    delta_columns = [
        "axis_score",
        "price",
        "affordability",
        "price_affordability_contribution",
        "other_demand_contribution",
        "price_affordability_cancellation_amount",
        "price_affordability_cancellation_rate",
    ]

    for column in delta_columns:
        baseline_column = (
            f"{column}_baseline"
        )
        challenger_column = (
            f"{column}_challenger"
        )

        if (
            baseline_column
            in merged.columns
            and challenger_column
            in merged.columns
        ):
            merged[
                f"{column}_delta"
            ] = (
                merged[
                    challenger_column
                ]
                - merged[
                    baseline_column
                ]
            )

    merged[
        "price_affordability_explained_axis_delta"
    ] = (
        merged[
            "price_affordability_contribution_delta"
        ]
    )

    merged[
        "unexplained_axis_delta"
    ] = (
        merged[
            "axis_score_delta"
        ]
        - merged[
            "price_affordability_explained_axis_delta"
        ]
    )

    if (
        merged[
            "unexplained_axis_delta"
        ].abs().max()
        > 1e-12
    ):
        raise AssertionError(
            "Demand-axis delta is not fully explained "
            "by Price/Affordability contribution delta"
        )

    return merged.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(
        drop=True
    )


def _build_contribution_summary(
    contributions: pd.DataFrame,
) -> pd.DataFrame:
    return (
        contributions.groupby(
            [
                "run_role",
                "geo_id",
                "dimension",
                "dimension_weight",
                "is_price_family_dimension",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            mean_dimension_score=(
                "dimension_score",
                "mean",
            ),
            mean_weighted_contribution=(
                "weighted_contribution",
                "mean",
            ),
            mean_absolute_weighted_contribution=(
                "weighted_contribution",
                lambda values: values.abs().mean(),
            ),
            median_absolute_weighted_contribution=(
                "weighted_contribution",
                lambda values: values.abs().median(),
            ),
            positive_contribution_rate=(
                "weighted_contribution",
                lambda values: values.gt(
                    0
                ).mean(),
            ),
        )
        .reset_index()
    )


def _build_cancellation_summary(
    monthly_panel: pd.DataFrame,
) -> pd.DataFrame:
    return (
        monthly_panel.groupby(
            [
                "run_role",
                "geo_id",
            ]
        )
        .agg(
            rows=("date", "size"),
            opposite_sign_months=(
                "price_affordability_opposite_sign",
                "sum",
            ),
            opposite_sign_rate=(
                "price_affordability_opposite_sign",
                "mean",
            ),
            mean_gross_magnitude=(
                "price_affordability_gross_magnitude",
                "mean",
            ),
            mean_net_magnitude=(
                "price_affordability_net_magnitude",
                "mean",
            ),
            mean_cancellation_amount=(
                "price_affordability_cancellation_amount",
                "mean",
            ),
            mean_cancellation_rate=(
                "price_affordability_cancellation_rate",
                "mean",
            ),
            p90_cancellation_rate=(
                "price_affordability_cancellation_rate",
                lambda values: values.quantile(
                    0.90
                ),
            ),
            full_cancellation_rate=(
                "price_affordability_cancellation_rate",
                lambda values: values.ge(
                    0.90
                ).mean(),
            ),
        )
        .reset_index()
    )


def _build_near_zero_summary(
    monthly_panel: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[
        dict[str, object]
    ] = []

    contribution_columns = [
        column
        for column in monthly_panel.columns
        if column.endswith(
            "_dimension_score"
        )
    ]

    for (
        run_role,
        geo_id,
    ), frame in monthly_panel.groupby(
        [
            "run_role",
            "geo_id",
        ]
    ):
        for threshold in NEAR_ZERO_THRESHOLDS:
            suffix = str(
                int(
                    threshold * 100
                )
            ).zfill(3)

            near_zero = frame[
                frame[
                    f"demand_near_zero_{suffix}"
                ]
            ]

            row: dict[
                str,
                object,
            ] = {
                "run_role": run_role,
                "geo_id": geo_id,
                "threshold": threshold,
                "rows": len(frame),
                "near_zero_months": (
                    len(near_zero)
                ),
                "near_zero_rate": (
                    len(near_zero)
                    / len(frame)
                    if len(frame)
                    else np.nan
                ),
                (
                    "mean_absolute_price_"
                    "contribution_near_zero"
                ): (
                    near_zero[
                        "price"
                    ].abs().mean()
                ),
                (
                    "mean_absolute_affordability_"
                    "contribution_near_zero"
                ): (
                    near_zero[
                        "affordability"
                    ].abs().mean()
                ),
                (
                    "mean_absolute_other_"
                    "contribution_near_zero"
                ): (
                    near_zero[
                        "other_demand_contribution"
                    ].abs().mean()
                ),
                (
                    "mean_price_affordability_"
                    "cancellation_rate_near_zero"
                ): (
                    near_zero[
                        "price_affordability_cancellation_rate"
                    ].mean()
                ),
            }

            for column in contribution_columns:
                row[
                    f"mean_{column}_near_zero"
                ] = near_zero[
                    column
                ].mean()

                row[
                    f"mean_absolute_{column}_near_zero"
                ] = near_zero[
                    column
                ].abs().mean()

            rows.append(row)

    return pd.DataFrame(rows)


def _build_dominant_dimension_summary(
    contributions: pd.DataFrame,
    monthly_panel: pd.DataFrame,
) -> pd.DataFrame:
    near_zero_keys = monthly_panel[
        monthly_panel[
            "demand_near_zero_010"
        ]
    ][
        [
            "run_role",
            "geo_id",
            "date",
        ]
    ]

    focus = contributions.merge(
        near_zero_keys,
        on=[
            "run_role",
            "geo_id",
            "date",
        ],
        how="inner",
        validate="many_to_one",
    )

    if focus.empty:
        return pd.DataFrame(
            columns=[
                "run_role",
                "geo_id",
                "dimension",
                "dominant_months",
                "dominant_rate",
            ]
        )

    focus[
        "absolute_weighted_contribution"
    ] = focus[
        "weighted_contribution"
    ].abs()

    focus[
        "rank_within_month"
    ] = focus.groupby(
        [
            "run_role",
            "geo_id",
            "date",
        ]
    )[
        "absolute_weighted_contribution"
    ].rank(
        method="first",
        ascending=False,
    )

    dominant = focus[
        focus[
            "rank_within_month"
        ].eq(1)
    ]

    total_months = (
        near_zero_keys.groupby(
            [
                "run_role",
                "geo_id",
            ]
        )
        .size()
        .reset_index(
            name="near_zero_months"
        )
    )

    summary = (
        dominant.groupby(
            [
                "run_role",
                "geo_id",
                "dimension",
            ]
        )
        .size()
        .reset_index(
            name="dominant_months"
        )
    )

    summary = summary.merge(
        total_months,
        on=[
            "run_role",
            "geo_id",
        ],
        how="left",
        validate="many_to_one",
    )

    summary[
        "dominant_rate"
    ] = (
        summary[
            "dominant_months"
        ]
        / summary[
            "near_zero_months"
        ]
    )

    return summary.sort_values(
        [
            "geo_id",
            "run_role",
            "dominant_rate",
        ],
        ascending=[
            True,
            True,
            False,
        ],
    ).reset_index(
        drop=True
    )


def _build_historical_summary(
    run_comparison: pd.DataFrame,
) -> pd.DataFrame:
    periods = (
        (
            "2009_2012",
            pd.Timestamp(
                "2009-01-01"
            ),
            pd.Timestamp(
                "2012-12-31"
            ),
        ),
        (
            "2013_2019",
            pd.Timestamp(
                "2013-01-01"
            ),
            pd.Timestamp(
                "2019-12-31"
            ),
        ),
        (
            "2020_2021",
            pd.Timestamp(
                "2020-01-01"
            ),
            pd.Timestamp(
                "2021-12-31"
            ),
        ),
        (
            "2022_rate_shock",
            pd.Timestamp(
                "2022-01-01"
            ),
            pd.Timestamp(
                "2022-12-31"
            ),
        ),
        (
            "2023_2026",
            pd.Timestamp(
                "2023-01-01"
            ),
            pd.Timestamp(
                "2026-12-31"
            ),
        ),
    )

    rows: list[
        dict[str, object]
    ] = []

    for (
        period,
        start,
        end,
    ) in periods:
        focus = run_comparison[
            run_comparison[
                "date"
            ].between(
                start,
                end,
                inclusive="both",
            )
        ]

        if focus.empty:
            continue

        summary = (
            focus.groupby(
                "geo_id"
            )
            .agg(
                rows=("date", "size"),
                mean_axis_score_delta=(
                    "axis_score_delta",
                    "mean",
                ),
                mean_absolute_axis_score_delta=(
                    "axis_score_delta",
                    lambda values: values.abs().mean(),
                ),
                p90_absolute_axis_score_delta=(
                    "axis_score_delta",
                    lambda values: values.abs().quantile(
                        0.90
                    ),
                ),
                mean_price_contribution_delta=(
                    "price_delta",
                    "mean",
                ),
                mean_affordability_contribution_delta=(
                    "affordability_delta",
                    "mean",
                ),
                mean_cancellation_rate_delta=(
                    "price_affordability_cancellation_rate_delta",
                    "mean",
                ),
            )
            .reset_index()
        )

        summary["period"] = period

        rows.extend(
            summary.to_dict(
                orient="records"
            )
        )

    return pd.DataFrame(rows)


def _build_largest_change_months(
    run_comparison: pd.DataFrame,
    *,
    rows_per_geo: int = 20,
) -> pd.DataFrame:
    work = run_comparison.copy()

    work[
        "absolute_axis_score_delta"
    ] = work[
        "axis_score_delta"
    ].abs()

    return (
        work.sort_values(
            [
                "geo_id",
                "absolute_axis_score_delta",
                "date",
            ],
            ascending=[
                True,
                False,
                True,
            ],
        )
        .groupby(
            "geo_id",
            as_index=False,
        )
        .head(
            rows_per_geo
        )
        .reset_index(
            drop=True
        )
    )


def _build_complete_dimension_history(
    *,
    comparison: dict[str, pd.DataFrame],
    artifact_root: str | Path,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    """
    Build complete baseline and challenger dimension histories.

    The linked price-family comparison's focused dimension history
    contains only Price and Affordability. Demand reconstruction
    requires every enabled Demand dimension.
    """
    store = RegimeArtifactStore(
        artifact_root
    )

    baseline_dimensions = (
        store.read_dataframe(
            BASELINE_RUN_ID,
            "dimension_scores",
        )
        .copy()
    )

    baseline_dimensions["date"] = (
        pd.to_datetime(
            baseline_dimensions["date"],
            errors="coerce",
        )
    )

    baseline_dimensions = baseline_dimensions[
        baseline_dimensions[
            "geo_id"
        ].isin(geo_ids)
    ].copy()

    baseline_dimensions[
        "run_role"
    ] = "baseline"

    challenger_dimensions = comparison[
        "challenger_dimension_scores"
    ].copy()

    challenger_dimensions["date"] = (
        pd.to_datetime(
            challenger_dimensions["date"],
            errors="coerce",
        )
    )

    challenger_dimensions = (
        challenger_dimensions[
            challenger_dimensions[
                "geo_id"
            ].isin(geo_ids)
        ].copy()
    )

    challenger_dimensions[
        "run_role"
    ] = "challenger"

    required_columns = [
        "run_role",
        "geo_id",
        "date",
        "dimension",
        "dimension_score",
        "metric_count",
        "metric_weight_sum",
        "min_metric_score",
        "max_metric_score",
        "max_metric_age_days",
    ]

    missing_baseline = (
        set(required_columns)
        - set(
            baseline_dimensions.columns
        )
    )

    missing_challenger = (
        set(required_columns)
        - set(
            challenger_dimensions.columns
        )
    )

    if missing_baseline:
        raise ValueError(
            "Baseline dimensions are missing "
            f"columns: {sorted(missing_baseline)}"
        )

    if missing_challenger:
        raise ValueError(
            "Challenger dimensions are missing "
            f"columns: {sorted(missing_challenger)}"
        )

    history = pd.concat(
        [
            baseline_dimensions[
                required_columns
            ],
            challenger_dimensions[
                required_columns
            ],
        ],
        ignore_index=True,
    )

    duplicates = history.duplicated(
        subset=[
            "run_role",
            "geo_id",
            "date",
            "dimension",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Complete dimension history contains "
            "duplicate rows:\n"
            + history.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return history.sort_values(
        [
            "run_role",
            "geo_id",
            "date",
            "dimension",
        ]
    ).reset_index(
        drop=True
    )


def build_demand_contribution_audit(
    *,
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: tuple[str, ...] = (
        FOCUS_GEOS
    ),
) -> dict[str, pd.DataFrame]:
    comparison = (
        build_linked_price_family_comparison(
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    dimension_history = (
        _build_complete_dimension_history(
            comparison=comparison,
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    axis_history = comparison[
        "axis_score_history"
    ]

    contributions = (
        _build_weighted_contributions(
            dimension_history
        )
    )

    monthly_panel = (
        _build_monthly_panel(
            contributions,
            axis_history,
        )
    )

    run_comparison = (
        _build_run_comparison(
            monthly_panel
        )
    )

    contribution_summary = (
        _build_contribution_summary(
            contributions
        )
    )

    cancellation_summary = (
        _build_cancellation_summary(
            monthly_panel
        )
    )

    near_zero_summary = (
        _build_near_zero_summary(
            monthly_panel
        )
    )

    dominant_dimension_summary = (
        _build_dominant_dimension_summary(
            contributions,
            monthly_panel,
        )
    )

    historical_summary = (
        _build_historical_summary(
            run_comparison
        )
    )

    largest_change_months = (
        _build_largest_change_months(
            run_comparison
        )
    )

    return {
        "demand_weights": (
            _demand_weights()
        ),
        "dimension_contributions": (
            contributions
        ),
        "monthly_contribution_panel": (
            monthly_panel
        ),
        "run_comparison": (
            run_comparison
        ),
        "contribution_summary": (
            contribution_summary
        ),
        "cancellation_summary": (
            cancellation_summary
        ),
        "near_zero_summary": (
            near_zero_summary
        ),
        "dominant_dimension_summary": (
            dominant_dimension_summary
        ),
        "historical_summary": (
            historical_summary
        ),
        "largest_change_months": (
            largest_change_months
        ),
    }
