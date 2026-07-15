from __future__ import annotations
# regime/experiments/labor_demand_comparison.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)
from regime._02_feature_normalizer import normalize_features
from regime._03_metric_scorer import score_metrics
from regime._04_asof_aligner import align_metric_scores_asof
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes


BASELINE_RUN_ID = "macro_regime_v1_bps120_sources"

FOCUS_GEOS = (
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
)

LABOR_METRICS = (
    "employment",
    "labor_force",
    "laus_unemployment_rate",
)

LABOR_POLICIES = {
    "labor_ma3_momentum_lag3": 3,
    "labor_ma6_momentum_lag3": 6,
}

FEATURE_KEY_MAP = {
    "employment": {
        "level": "laus_employment_level",
        "short": "laus_employment_short",
        "long": "laus_employment_long",
    },
    "labor_force": {
        "level": "laus_labor_force_level",
        "short": "laus_labor_force_short",
        "long": "laus_labor_force_long",
    },
    "laus_unemployment_rate": {
        "level": "laus_unemployment_rate_level",
        "short": "laus_unemployment_rate_short",
        "long": "laus_unemployment_rate_long",
    },
}

TARGET_FEATURE_KEYS = {
    key
    for mapping in FEATURE_KEY_MAP.values()
    for key in mapping.values()
}

FEATURE_COLUMNS = [
    "geo_id",
    "date",
    "canonical_metric_key",
    "feature_key",
    "raw_feature_value",
]


def _safe_ratio_minus_one(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    output = (
        pd.to_numeric(numerator, errors="coerce")
        / pd.to_numeric(denominator, errors="coerce")
        - 1.0
    )

    return output.replace(
        [np.inf, -np.inf],
        np.nan,
    )


def _standardize_date(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    work = frame.copy()

    if "date" in work.columns:
        source = "date"
    elif "evaluation_date" in work.columns:
        source = "evaluation_date"
    elif "metric_date" in work.columns:
        source = "metric_date"
    else:
        raise ValueError(
            "Could not resolve date column from "
            f"{list(work.columns)}"
        )

    if source != "date":
        work = work.rename(
            columns={source: "date"}
        )

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    if work["date"].isna().any():
        raise ValueError(
            "Comparison frame contains invalid dates"
        )

    return work


def _load_labor_sources(
    source_metrics: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
    }

    missing = required - set(
        source_metrics.columns
    )

    if missing:
        raise ValueError(
            "source_metrics is missing columns: "
            f"{sorted(missing)}"
        )

    work = source_metrics[
        source_metrics[
            "canonical_metric_key"
        ].isin(LABOR_METRICS)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "value",
        ]
    ].copy()

    if work.empty:
        raise ValueError(
            "No labor source rows were found"
        )

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["value"] = pd.to_numeric(
        work["value"],
        errors="coerce",
    )

    invalid = work[
        work["date"].isna()
        | work["value"].isna()
        | ~np.isfinite(work["value"])
    ]

    if not invalid.empty:
        raise ValueError(
            "Labor source rows contain invalid values:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicates = work.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Labor source rows are not unique:\n"
            + work.loc[duplicates]
            .head(30)
            .to_string(index=False)
        )

    return work.sort_values(
        [
            "geo_id",
            "canonical_metric_key",
            "date",
        ]
    ).reset_index(drop=True)


def build_labor_policy_features(
    source_metrics: pd.DataFrame,
    *,
    policy_id: str,
    window: int,
) -> pd.DataFrame:
    if policy_id not in LABOR_POLICIES:
        raise ValueError(
            f"Unknown labor policy: {policy_id}"
        )

    if LABOR_POLICIES[policy_id] != window:
        raise ValueError(
            "Policy/window mismatch for "
            f"{policy_id}: {window}"
        )

    work = _load_labor_sources(
        source_metrics
    )

    grouped = work.groupby(
        [
            "geo_id",
            "canonical_metric_key",
        ],
        group_keys=False,
        sort=False,
    )

    work["level_value"] = (
        grouped["value"]
        .rolling(
            window=window,
            min_periods=window,
        )
        .mean()
        .reset_index(
            level=[0, 1],
            drop=True,
        )
    )

    work["short_reference"] = (
        work.groupby(
            [
                "geo_id",
                "canonical_metric_key",
            ]
        )["level_value"]
        .shift(3)
    )

    work["long_reference"] = (
        work.groupby(
            [
                "geo_id",
                "canonical_metric_key",
            ]
        )["level_value"]
        .shift(12)
    )

    work["short_value"] = (
        _safe_ratio_minus_one(
            work["level_value"],
            work["short_reference"],
        )
    )

    work["long_value"] = (
        _safe_ratio_minus_one(
            work["level_value"],
            work["long_reference"],
        )
    )

    frames: list[pd.DataFrame] = []

    for component, value_column in (
        ("level", "level_value"),
        ("short", "short_value"),
        ("long", "long_value"),
    ):
        frame = work[
            [
                "geo_id",
                "date",
                "canonical_metric_key",
            ]
        ].copy()

        frame["feature_key"] = [
            FEATURE_KEY_MAP[metric][component]
            for metric in frame[
                "canonical_metric_key"
            ]
        ]

        frame["raw_feature_value"] = (
            work[value_column]
        )

        frame["policy_id"] = policy_id
        frame["window"] = window
        frame["feature_component"] = component

        frames.append(frame)

    output = pd.concat(
        frames,
        ignore_index=True,
    )

    output = output.dropna(
        subset=["raw_feature_value"]
    )

    if not np.isfinite(
        output["raw_feature_value"]
    ).all():
        raise AssertionError(
            "Labor policy features contain "
            "non-finite values"
        )

    duplicates = output.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Labor policy features contain duplicates"
        )

    return output.sort_values(
        [
            "feature_key",
            "geo_id",
            "date",
        ]
    ).reset_index(drop=True)


def _replace_target_features(
    baseline_features: pd.DataFrame,
    override: pd.DataFrame,
) -> pd.DataFrame:
    missing = (
        set(FEATURE_COLUMNS)
        - set(baseline_features.columns)
    )

    if missing:
        raise ValueError(
            "Baseline features are missing columns: "
            f"{sorted(missing)}"
        )

    baseline = baseline_features[
        FEATURE_COLUMNS
    ].copy()

    target_mask = (
        baseline[
            "canonical_metric_key"
        ].isin(LABOR_METRICS)
        & baseline[
            "feature_key"
        ].isin(TARGET_FEATURE_KEYS)
    )

    retained = baseline[
        ~target_mask
    ].copy()

    challenger = pd.concat(
        [
            retained,
            override[FEATURE_COLUMNS],
        ],
        ignore_index=True,
    )

    duplicates = challenger.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Labor challenger created duplicate "
            "feature rows"
        )

    feature_order = (
        baseline[["feature_key"]]
        .drop_duplicates()
        .reset_index()
        .rename(
            columns={
                "index": "_feature_order",
            }
        )
    )

    challenger = challenger.merge(
        feature_order,
        on="feature_key",
        how="left",
        validate="many_to_one",
    )

    if challenger[
        "_feature_order"
    ].isna().any():
        raise AssertionError(
            "Labor challenger introduced unknown "
            "feature keys"
        )

    return (
        challenger.sort_values(
            [
                "_feature_order",
                "geo_id",
                "canonical_metric_key",
                "date",
            ],
            kind="mergesort",
        )
        .drop(columns=["_feature_order"])
        .reset_index(drop=True)
    )


def _sort_exact(
    frame: pd.DataFrame,
    keys: list[str],
) -> pd.DataFrame:
    valid = [
        key
        for key in keys
        if key in frame.columns
    ]

    if not valid:
        raise ValueError(
            "No valid sort keys for exact comparison"
        )

    return frame.sort_values(
        valid,
        kind="mergesort",
    ).reset_index(drop=True)


def _exact_match(
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
    *,
    artifact_name: str,
    scope: str,
    sort_keys: list[str],
) -> dict[str, object]:
    left = _sort_exact(
        baseline,
        sort_keys,
    )

    right = _sort_exact(
        challenger,
        sort_keys,
    )

    message = ""

    try:
        pd.testing.assert_frame_equal(
            left,
            right,
            check_dtype=True,
            check_exact=True,
            check_like=False,
        )
        matched = True
    except AssertionError as exc:
        matched = False
        message = str(exc)[:4000]

    return {
        "artifact_name": artifact_name,
        "scope": scope,
        "baseline_rows": len(left),
        "challenger_rows": len(right),
        "exact_match": matched,
        "error_message": message,
    }


def _add_monthly_change(
    frame: pd.DataFrame,
    *,
    value_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    work = _standardize_date(
        frame
    )

    work[value_column] = pd.to_numeric(
        work[value_column],
        errors="coerce",
    )

    work = work.sort_values(
        [
            *group_columns,
            "date",
        ]
    ).reset_index(drop=True)

    grouped = work.groupby(
        group_columns,
        group_keys=False,
    )

    work[
        f"{value_column}_change_1m"
    ] = grouped[
        value_column
    ].diff()

    work[
        f"absolute_{value_column}_change_1m"
    ] = work[
        f"{value_column}_change_1m"
    ].abs()

    previous = grouped[
        value_column
    ].shift(1)

    work[
        f"{value_column}_sign_flip"
    ] = (
        previous.notna()
        & np.sign(previous).ne(
            np.sign(
                work[value_column]
            )
        )
    )

    return work


def _stability_summary(
    frame: pd.DataFrame,
    *,
    value_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    return (
        frame.groupby(
            group_columns,
            dropna=False,
        )
        .agg(
            rows=(value_column, "count"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            mean_value=(value_column, "mean"),
            value_std=(value_column, "std"),
            mean_absolute_change_1m=(
                f"absolute_{value_column}_change_1m",
                "mean",
            ),
            p90_absolute_change_1m=(
                f"absolute_{value_column}_change_1m",
                lambda values: values.quantile(0.90),
            ),
            maximum_absolute_change_1m=(
                f"absolute_{value_column}_change_1m",
                "max",
            ),
            sign_flip_rate=(
                f"{value_column}_sign_flip",
                "mean",
            ),
            near_zero_rate=(
                value_column,
                lambda values: values.abs().lt(0.10).mean(),
            ),
        )
        .reset_index()
    )


def _build_core_demand_cancellation(
    metric_scores: pd.DataFrame,
    dimension_scores: pd.DataFrame,
    *,
    run_role: str,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    aligned = align_metric_scores_asof(
        metric_scores
    )

    required = {
        "geo_id",
        "evaluation_date",
        "canonical_metric_key",
        "metric_score",
    }

    missing = required - set(
        aligned.columns
    )

    if missing:
        raise ValueError(
            "Aligned metrics are missing columns: "
            f"{sorted(missing)}"
        )

    config_rows = dimension_scores[
        dimension_scores[
            "dimension"
        ].eq("demand")
        & dimension_scores[
            "geo_id"
        ].isin(geo_ids)
    ][
        [
            "geo_id",
            "date",
            "dimension_score",
        ]
    ].copy()

    config_rows["date"] = pd.to_datetime(
        config_rows["date"],
        errors="coerce",
    )

    # Reuse the registry-backed weights through the config loader indirectly
    # by deriving the available effective weight from the stored dimension
    # inputs. The metric registry in this project gives equal 1/6 weights for
    # the six core Demand metrics.
    demand_metrics = (
        "employment",
        "labor_force",
        "laus_unemployment_rate",
        "gdp_annual",
        "median_household_income",
        "population",
    )

    focus = aligned[
        aligned[
            "canonical_metric_key"
        ].isin(demand_metrics)
        & aligned[
            "geo_id"
        ].isin(geo_ids)
    ].copy()

    focus = focus.rename(
        columns={
            "evaluation_date": "date",
        }
    )

    focus["date"] = pd.to_datetime(
        focus["date"],
        errors="coerce",
    )

    focus["configured_weight"] = 1.0 / 6.0

    focus[
        "available_weight_sum"
    ] = focus.groupby(
        [
            "geo_id",
            "date",
        ]
    )[
        "configured_weight"
    ].transform("sum")

    focus[
        "effective_weight"
    ] = (
        focus["configured_weight"]
        / focus[
            "available_weight_sum"
        ]
    )

    focus[
        "weighted_contribution"
    ] = (
        focus["metric_score"]
        * focus[
            "effective_weight"
        ]
    )

    monthly = (
        focus.groupby(
            [
                "geo_id",
                "date",
            ]
        )
        .agg(
            gross_contribution_magnitude=(
                "weighted_contribution",
                lambda values: values.abs().sum(),
            ),
            reconstructed_dimension_score=(
                "weighted_contribution",
                "sum",
            ),
        )
        .reset_index()
    )

    monthly = monthly.merge(
        config_rows,
        on=[
            "geo_id",
            "date",
        ],
        how="inner",
        validate="one_to_one",
    )

    monthly[
        "reconstruction_error"
    ] = (
        monthly[
            "reconstructed_dimension_score"
        ]
        - monthly[
            "dimension_score"
        ]
    )

    if (
        monthly[
            "reconstruction_error"
        ].abs().max()
        > 1e-12
    ):
        raise AssertionError(
            "Labor comparison could not reconstruct "
            "the core Demand dimension"
        )

    monthly[
        "cancellation_amount"
    ] = (
        monthly[
            "gross_contribution_magnitude"
        ]
        - monthly[
            "dimension_score"
        ].abs()
    ).clip(
        lower=0.0
    )

    monthly[
        "cancellation_rate"
    ] = np.where(
        monthly[
            "gross_contribution_magnitude"
        ].gt(0),
        monthly[
            "cancellation_amount"
        ]
        / monthly[
            "gross_contribution_magnitude"
        ],
        0.0,
    )

    monthly["run_role"] = run_role

    return monthly


def build_labor_demand_comparison(
    *,
    baseline_run_id: str = BASELINE_RUN_ID,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: tuple[str, ...] = FOCUS_GEOS,
) -> dict[str, pd.DataFrame]:
    store = RegimeArtifactStore(
        artifact_root
    )

    source_metrics = store.read_dataframe(
        baseline_run_id,
        "source_metrics",
    )

    baseline_features = store.read_dataframe(
        baseline_run_id,
        "features",
    )

    baseline_normalized = store.read_dataframe(
        baseline_run_id,
        "normalized_features",
    )

    baseline_metrics = store.read_dataframe(
        baseline_run_id,
        "metric_scores",
    )

    baseline_dimensions = store.read_dataframe(
        baseline_run_id,
        "dimension_scores",
    )

    baseline_axes = store.read_dataframe(
        baseline_run_id,
        "axis_scores",
    )

    outputs: dict[str, pd.DataFrame] = {}

    normalized_histories = []
    metric_histories = []
    dimension_histories = []
    axis_histories = []
    cancellation_histories = []

    baseline_normalized_focus = baseline_normalized[
        baseline_normalized[
            "canonical_metric_key"
        ].isin(LABOR_METRICS)
        & baseline_normalized[
            "geo_id"
        ].isin(geo_ids)
    ].copy()

    baseline_normalized_focus[
        "run_role"
    ] = "baseline"

    normalized_histories.append(
        baseline_normalized_focus
    )

    baseline_metric_focus = baseline_metrics[
        baseline_metrics[
            "canonical_metric_key"
        ].isin(LABOR_METRICS)
        & baseline_metrics[
            "geo_id"
        ].isin(geo_ids)
    ].copy()

    baseline_metric_focus[
        "run_role"
    ] = "baseline"

    metric_histories.append(
        baseline_metric_focus
    )

    baseline_dimension_focus = (
        _standardize_date(
            baseline_dimensions
        )
    )

    baseline_dimension_focus = (
        baseline_dimension_focus[
            baseline_dimension_focus[
                "dimension"
            ].eq("demand")
            & baseline_dimension_focus[
                "geo_id"
            ].isin(geo_ids)
        ].copy()
    )

    baseline_dimension_focus[
        "run_role"
    ] = "baseline"

    dimension_histories.append(
        baseline_dimension_focus
    )

    baseline_axis_focus = (
        _standardize_date(
            baseline_axes
        )
    )

    baseline_axis_focus = (
        baseline_axis_focus[
            baseline_axis_focus[
                "axis"
            ].eq("demand")
            & baseline_axis_focus[
                "geo_id"
            ].isin(geo_ids)
        ].copy()
    )

    baseline_axis_focus[
        "run_role"
    ] = "baseline"

    axis_histories.append(
        baseline_axis_focus
    )

    cancellation_histories.append(
        _build_core_demand_cancellation(
            baseline_metrics,
            baseline_dimensions,
            run_role="baseline",
            geo_ids=geo_ids,
        )
    )

    isolation_rows = []

    for policy_id, window in (
        LABOR_POLICIES.items()
    ):
        override = build_labor_policy_features(
            source_metrics,
            policy_id=policy_id,
            window=window,
        )

        challenger_features = (
            _replace_target_features(
                baseline_features,
                override,
            )
        )

        challenger_normalized = normalize_features(
            challenger_features
        )

        challenger_metrics = score_metrics(
            challenger_normalized
        )

        challenger_aligned = (
            align_metric_scores_asof(
                challenger_metrics
            )
        )

        challenger_dimensions = score_dimensions(
            challenger_aligned
        )

        challenger_axes = score_axes(
            challenger_dimensions
        )

        outputs[
            f"{policy_id}__features"
        ] = challenger_features

        outputs[
            f"{policy_id}__normalized_features"
        ] = challenger_normalized

        outputs[
            f"{policy_id}__metric_scores"
        ] = challenger_metrics

        outputs[
            f"{policy_id}__dimension_scores"
        ] = challenger_dimensions

        outputs[
            f"{policy_id}__axis_scores"
        ] = challenger_axes

        isolation_rows.extend(
            [
                _exact_match(
                    baseline_normalized[
                        ~baseline_normalized[
                            "canonical_metric_key"
                        ].isin(LABOR_METRICS)
                    ].copy(),
                    challenger_normalized[
                        ~challenger_normalized[
                            "canonical_metric_key"
                        ].isin(LABOR_METRICS)
                    ].copy(),
                    artifact_name=(
                        "normalized_features"
                    ),
                    scope=(
                        f"{policy_id}__all_non_labor_metrics"
                    ),
                    sort_keys=[
                        "geo_id",
                        "date",
                        "canonical_metric_key",
                        "feature_key",
                    ],
                ),
                _exact_match(
                    baseline_metrics[
                        ~baseline_metrics[
                            "canonical_metric_key"
                        ].isin(LABOR_METRICS)
                    ].copy(),
                    challenger_metrics[
                        ~challenger_metrics[
                            "canonical_metric_key"
                        ].isin(LABOR_METRICS)
                    ].copy(),
                    artifact_name="metric_scores",
                    scope=(
                        f"{policy_id}__all_non_labor_metrics"
                    ),
                    sort_keys=[
                        "geo_id",
                        "date",
                        "canonical_metric_key",
                    ],
                ),
                _exact_match(
                    _standardize_date(
                        baseline_dimensions
                    )[
                        ~_standardize_date(
                            baseline_dimensions
                        )[
                            "dimension"
                        ].eq("demand")
                    ].copy(),
                    _standardize_date(
                        challenger_dimensions
                    )[
                        ~_standardize_date(
                            challenger_dimensions
                        )[
                            "dimension"
                        ].eq("demand")
                    ].copy(),
                    artifact_name="dimension_scores",
                    scope=(
                        f"{policy_id}__all_non_demand_dimensions"
                    ),
                    sort_keys=[
                        "geo_id",
                        "date",
                        "dimension",
                    ],
                ),
                _exact_match(
                    _standardize_date(
                        baseline_axes
                    )[
                        ~_standardize_date(
                            baseline_axes
                        )[
                            "axis"
                        ].eq("demand")
                    ].copy(),
                    _standardize_date(
                        challenger_axes
                    )[
                        ~_standardize_date(
                            challenger_axes
                        )[
                            "axis"
                        ].eq("demand")
                    ].copy(),
                    artifact_name="axis_scores",
                    scope=(
                        f"{policy_id}__all_non_demand_axes"
                    ),
                    sort_keys=[
                        "geo_id",
                        "date",
                        "axis",
                    ],
                ),
            ]
        )

        normalized_focus = challenger_normalized[
            challenger_normalized[
                "canonical_metric_key"
            ].isin(LABOR_METRICS)
            & challenger_normalized[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        normalized_focus[
            "run_role"
        ] = policy_id

        normalized_histories.append(
            normalized_focus
        )

        metric_focus = challenger_metrics[
            challenger_metrics[
                "canonical_metric_key"
            ].isin(LABOR_METRICS)
            & challenger_metrics[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        metric_focus["run_role"] = (
            policy_id
        )

        metric_histories.append(
            metric_focus
        )

        dimension_focus = _standardize_date(
            challenger_dimensions
        )

        dimension_focus = dimension_focus[
            dimension_focus[
                "dimension"
            ].eq("demand")
            & dimension_focus[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        dimension_focus[
            "run_role"
        ] = policy_id

        dimension_histories.append(
            dimension_focus
        )

        axis_focus = _standardize_date(
            challenger_axes
        )

        axis_focus = axis_focus[
            axis_focus[
                "axis"
            ].eq("demand")
            & axis_focus[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        axis_focus["run_role"] = (
            policy_id
        )

        axis_histories.append(
            axis_focus
        )

        cancellation_histories.append(
            _build_core_demand_cancellation(
                challenger_metrics,
                challenger_dimensions,
                run_role=policy_id,
                geo_ids=geo_ids,
            )
        )

    normalized_history = pd.concat(
        normalized_histories,
        ignore_index=True,
    )

    normalized_history[
        "feature_component"
    ] = (
        normalized_history[
            "feature_key"
        ]
        .str.rsplit("_", n=1)
        .str[-1]
    )

    normalized_history = _add_monthly_change(
        normalized_history,
        value_column="feature_score",
        group_columns=[
            "run_role",
            "geo_id",
            "canonical_metric_key",
            "feature_key",
        ],
    )

    metric_history = _add_monthly_change(
        pd.concat(
            metric_histories,
            ignore_index=True,
        ),
        value_column="metric_score",
        group_columns=[
            "run_role",
            "geo_id",
            "canonical_metric_key",
        ],
    )

    dimension_history = _add_monthly_change(
        pd.concat(
            dimension_histories,
            ignore_index=True,
        ),
        value_column="dimension_score",
        group_columns=[
            "run_role",
            "geo_id",
            "dimension",
        ],
    )

    axis_history = _add_monthly_change(
        pd.concat(
            axis_histories,
            ignore_index=True,
        ),
        value_column="axis_score",
        group_columns=[
            "run_role",
            "geo_id",
            "axis",
        ],
    )

    cancellation_history = pd.concat(
        cancellation_histories,
        ignore_index=True,
    )

    outputs.update(
        {
            "isolation_audit": pd.DataFrame(
                isolation_rows
            ),
            "normalized_feature_history": (
                normalized_history
            ),
            "metric_score_history": (
                metric_history
            ),
            "dimension_score_history": (
                dimension_history
            ),
            "axis_score_history": (
                axis_history
            ),
            "cancellation_history": (
                cancellation_history
            ),
            "feature_stability_summary": (
                _stability_summary(
                    normalized_history,
                    value_column="feature_score",
                    group_columns=[
                        "run_role",
                        "geo_id",
                        "canonical_metric_key",
                        "feature_component",
                    ],
                )
            ),
            "metric_stability_summary": (
                _stability_summary(
                    metric_history,
                    value_column="metric_score",
                    group_columns=[
                        "run_role",
                        "geo_id",
                        "canonical_metric_key",
                    ],
                )
            ),
            "dimension_stability_summary": (
                _stability_summary(
                    dimension_history,
                    value_column="dimension_score",
                    group_columns=[
                        "run_role",
                        "geo_id",
                        "dimension",
                    ],
                )
            ),
            "axis_stability_summary": (
                _stability_summary(
                    axis_history,
                    value_column="axis_score",
                    group_columns=[
                        "run_role",
                        "geo_id",
                        "axis",
                    ],
                )
            ),
            "cancellation_summary": (
                cancellation_history.groupby(
                    [
                        "run_role",
                        "geo_id",
                    ]
                )
                .agg(
                    rows=("date", "size"),
                    mean_cancellation_rate=(
                        "cancellation_rate",
                        "mean",
                    ),
                    p90_cancellation_rate=(
                        "cancellation_rate",
                        lambda values: values.quantile(
                            0.90
                        ),
                    ),
                    full_cancellation_rate=(
                        "cancellation_rate",
                        lambda values: values.ge(
                            0.90
                        ).mean(),
                    ),
                    near_zero_dimension_rate=(
                        "dimension_score",
                        lambda values: values.abs().lt(
                            0.10
                        ).mean(),
                    ),
                )
                .reset_index()
            ),
        }
    )

    return outputs
