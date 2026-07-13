from __future__ import annotations
# regime/experiments/metric_normalization_stability.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)
from regime._02_feature_normalizer import (
    normalize_features,
)
from regime._03_metric_scorer import (
    score_metrics,
)
from regime.experiments.smoothing_features import (
    build_smoothed_metric_features,
)
from regime.experiments.smoothing_policy import (
    load_smoothing_experiments,
)


DEFAULT_RUN_ID = (
    "macro_regime_v1_bps120_sources"
)

TARGET_METRIC = "active_inventory"

FOCUS_GEOS = (
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
)

BASELINE_POLICY_ID = "baseline_current"

CHALLENGER_IDS = (
    "inventory_ma3_deviation",
    "inventory_ma3_momentum",
)

PRODUCTION_FEATURE_KEY_MAP = {
    "level": "redfin_inventory_level",
    "short": "redfin_inventory_short",
    "long": "redfin_inventory_long",
}

TARGET_FEATURE_KEYS = set(
    PRODUCTION_FEATURE_KEY_MAP.values()
)


def _safe_correlation(
    left: pd.Series,
    right: pd.Series,
) -> float:
    aligned = pd.concat(
        [
            pd.to_numeric(
                left,
                errors="coerce",
            ),
            pd.to_numeric(
                right,
                errors="coerce",
            ),
        ],
        axis=1,
    ).dropna()

    if len(aligned) < 3:
        return np.nan

    if (
        aligned.iloc[:, 0].nunique()
        <= 1
        or aligned.iloc[:, 1].nunique()
        <= 1
    ):
        return np.nan

    return float(
        aligned.iloc[:, 0].corr(
            aligned.iloc[:, 1]
        )
    )


def _load_baseline_artifacts(
    store: RegimeArtifactStore,
    *,
    run_id: str,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    source_metrics = store.read_dataframe(
        run_id,
        "source_metrics",
    )

    features = store.read_dataframe(
        run_id,
        "features",
    )

    normalized_features = (
        store.read_dataframe(
            run_id,
            "normalized_features",
        )
    )

    return (
        source_metrics,
        features,
        normalized_features,
    )


def _load_raw_inventory(
    source_metrics: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
        "metric_origin",
    }

    missing = (
        required
        - set(source_metrics.columns)
    )

    if missing:
        raise ValueError(
            "source_metrics is missing columns: "
            f"{sorted(missing)}"
        )

    inventory = source_metrics[
        source_metrics[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "value",
            "metric_origin",
        ]
    ].copy()

    inventory = inventory.rename(
        columns={
            "value": "raw_value",
        }
    )

    inventory["date"] = pd.to_datetime(
        inventory["date"],
        errors="coerce",
    )

    inventory["raw_value"] = pd.to_numeric(
        inventory["raw_value"],
        errors="coerce",
    )

    invalid = inventory[
        inventory["date"].isna()
        | inventory["raw_value"].isna()
        | ~np.isfinite(
            inventory["raw_value"]
        )
    ]

    if not invalid.empty:
        raise ValueError(
            "Raw inventory contains invalid rows:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicates = inventory.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Raw inventory contains duplicate keys:\n"
            + inventory.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return (
        inventory.sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _build_challenger_inventory_features(
    raw_inventory: pd.DataFrame,
    *,
    experiment_id: str,
) -> pd.DataFrame:
    experiments = (
        load_smoothing_experiments(
            validate=True
        )
    )

    if experiment_id not in experiments:
        raise KeyError(
            f"Unknown smoothing experiment: "
            f"{experiment_id}"
        )

    policy = experiments[
        experiment_id
    ].policy_for(
        TARGET_METRIC
    )

    if policy is None:
        raise AssertionError(
            "Could not resolve policy for "
            f"{experiment_id}/{TARGET_METRIC}"
        )

    challenger = (
        build_smoothed_metric_features(
            raw_inventory,
            policy=policy,
            value_column="raw_value",
            preserve_columns=[
                "metric_origin",
            ],
        )
    )

    challenger["feature_key"] = (
        challenger[
            "feature_component"
        ].map(
            PRODUCTION_FEATURE_KEY_MAP
        )
    )

    missing_feature_keys = challenger[
        challenger[
            "feature_key"
        ].isna()
    ]

    if not missing_feature_keys.empty:
        raise AssertionError(
            "A smoothing component could not map "
            "to a production feature key:\n"
            + missing_feature_keys.head(
                30
            ).to_string(index=False)
        )

    challenger = challenger[
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
            "raw_feature_value",
        ]
    ].copy()

    challenger = challenger.dropna(
        subset=[
            "raw_feature_value",
        ]
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
            "Challenger inventory features contain "
            "duplicate production keys:\n"
            + challenger.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return challenger


def _replace_inventory_features(
    baseline_features: pd.DataFrame,
    challenger_inventory: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
        "raw_feature_value",
    }

    missing = (
        required
        - set(baseline_features.columns)
    )

    if missing:
        raise ValueError(
            "Baseline feature artifact is missing "
            f"columns: {sorted(missing)}"
        )

    retained = baseline_features[
        ~(
            baseline_features[
                "canonical_metric_key"
            ].eq(TARGET_METRIC)
            & baseline_features[
                "feature_key"
            ].isin(TARGET_FEATURE_KEYS)
        )
    ].copy()

    combined = pd.concat(
        [
            retained,
            challenger_inventory,
        ],
        ignore_index=True,
    )

    duplicates = combined.duplicated(
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
            "Feature replacement created duplicate keys:\n"
            + combined.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    # Preserve production feature-definition ordering as much as
    # possible. The normalizer should eventually be order-invariant,
    # but current pipeline parity has shown order can matter.
    feature_order = (
        baseline_features[
            [
                "feature_key",
            ]
        ]
        .drop_duplicates()
        .reset_index()
        .rename(
            columns={
                "index": "_feature_order",
            }
        )
    )

    combined = combined.merge(
        feature_order,
        on="feature_key",
        how="left",
        validate="many_to_one",
    )

    missing_order = combined[
        combined[
            "_feature_order"
        ].isna()
    ]

    if not missing_order.empty:
        raise AssertionError(
            "Combined features contain unknown "
            "production feature keys:\n"
            + missing_order[
                [
                    "feature_key",
                ]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

    combined = (
        combined.sort_values(
            [
                "_feature_order",
                "geo_id",
                "canonical_metric_key",
                "date",
            ],
            kind="mergesort",
        )
        .drop(
            columns=[
                "_feature_order",
            ]
        )
        .reset_index(drop=True)
    )

    return combined


def _run_policy_pipeline(
    *,
    policy_id: str,
    baseline_features: pd.DataFrame,
    baseline_normalized_features: (
        pd.DataFrame
    ),
    raw_inventory: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    if policy_id == BASELINE_POLICY_ID:
        normalized = (
            baseline_normalized_features.copy()
        )

    else:
        challenger_inventory = (
            _build_challenger_inventory_features(
                raw_inventory,
                experiment_id=policy_id,
            )
        )

        challenger_features = (
            _replace_inventory_features(
                baseline_features,
                challenger_inventory,
            )
        )

        normalized = normalize_features(
            challenger_features
        )

    metric_scores = score_metrics(
        normalized
    )

    normalized_inventory = normalized[
        normalized[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
        & normalized[
            "feature_key"
        ].isin(TARGET_FEATURE_KEYS)
    ].copy()

    normalized_inventory[
        "policy_id"
    ] = policy_id

    inventory_scores = metric_scores[
        metric_scores[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
    ].copy()

    inventory_scores[
        "policy_id"
    ] = policy_id

    return (
        normalized_inventory,
        inventory_scores,
    )


def _add_feature_diagnostics(
    normalized_features: pd.DataFrame,
) -> pd.DataFrame:
    work = normalized_features.copy()

    work["date"] = pd.to_datetime(
        work["date"]
    )

    work[
        "feature_component"
    ] = work[
        "feature_key"
    ].map(
        {
            value: key
            for key, value
            in PRODUCTION_FEATURE_KEY_MAP.items()
        }
    )

    work = work.sort_values(
        [
            "policy_id",
            "geo_id",
            "feature_component",
            "date",
        ]
    ).reset_index(drop=True)

    grouped = work.groupby(
        [
            "policy_id",
            "geo_id",
            "feature_component",
        ],
        group_keys=False,
    )

    for value_column in (
        "percentile",
        "feature_score",
    ):
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

    work[
        "previous_feature_score"
    ] = grouped[
        "feature_score"
    ].shift(1)

    work[
        "score_sign_flip_flag"
    ] = (
        work["feature_score"].notna()
        & work[
            "previous_feature_score"
        ].notna()
        & work["feature_score"].ne(0)
        & work[
            "previous_feature_score"
        ].ne(0)
        & (
            np.sign(
                work["feature_score"]
            )
            != np.sign(
                work[
                    "previous_feature_score"
                ]
            )
        )
    )

    work["calendar_month"] = (
        work["date"].dt.month
    )

    return work


def _add_metric_diagnostics(
    metric_scores: pd.DataFrame,
) -> pd.DataFrame:
    work = metric_scores.copy()

    work["date"] = pd.to_datetime(
        work["date"]
    )

    work = work.sort_values(
        [
            "policy_id",
            "geo_id",
            "date",
        ]
    ).reset_index(drop=True)

    grouped = work.groupby(
        [
            "policy_id",
            "geo_id",
        ],
        group_keys=False,
    )

    work[
        "metric_score_change_1m"
    ] = grouped[
        "metric_score"
    ].diff()

    work[
        "absolute_metric_score_change_1m"
    ] = work[
        "metric_score_change_1m"
    ].abs()

    work[
        "previous_metric_score"
    ] = grouped[
        "metric_score"
    ].shift(1)

    work[
        "metric_sign_flip_flag"
    ] = (
        work["metric_score"].notna()
        & work[
            "previous_metric_score"
        ].notna()
        & work["metric_score"].ne(0)
        & work[
            "previous_metric_score"
        ].ne(0)
        & (
            np.sign(
                work["metric_score"]
            )
            != np.sign(
                work[
                    "previous_metric_score"
                ]
            )
        )
    )

    work["calendar_month"] = (
        work["date"].dt.month
    )

    return work


def _feature_stability_summary(
    feature_history: pd.DataFrame,
) -> pd.DataFrame:
    return (
        feature_history.groupby(
            [
                "policy_id",
                "geo_id",
                "feature_component",
            ],
            dropna=False,
        )
        .agg(
            rows=("percentile", "count"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            percentile_mean=(
                "percentile",
                "mean",
            ),
            percentile_std=(
                "percentile",
                "std",
            ),
            percentile_p05=(
                "percentile",
                lambda values: values.quantile(
                    0.05
                ),
            ),
            percentile_p95=(
                "percentile",
                lambda values: values.quantile(
                    0.95
                ),
            ),
            percentile_range=(
                "percentile",
                lambda values: (
                    values.max()
                    - values.min()
                ),
            ),
            tail_low_rate=(
                "percentile",
                lambda values: values.le(
                    0.10
                ).mean(),
            ),
            tail_high_rate=(
                "percentile",
                lambda values: values.ge(
                    0.90
                ).mean(),
            ),
            mean_absolute_percentile_change=(
                (
                    "absolute_percentile_"
                    "change_1m"
                ),
                "mean",
            ),
            p90_absolute_percentile_change=(
                (
                    "absolute_percentile_"
                    "change_1m"
                ),
                lambda values: values.quantile(
                    0.90
                ),
            ),
            maximum_absolute_percentile_change=(
                (
                    "absolute_percentile_"
                    "change_1m"
                ),
                "max",
            ),
            feature_score_std=(
                "feature_score",
                "std",
            ),
            mean_absolute_feature_score_change=(
                (
                    "absolute_feature_score_"
                    "change_1m"
                ),
                "mean",
            ),
            p90_absolute_feature_score_change=(
                (
                    "absolute_feature_score_"
                    "change_1m"
                ),
                lambda values: values.quantile(
                    0.90
                ),
            ),
            maximum_absolute_feature_score_change=(
                (
                    "absolute_feature_score_"
                    "change_1m"
                ),
                "max",
            ),
            score_sign_flip_rate=(
                "score_sign_flip_flag",
                "mean",
            ),
        )
        .reset_index()
    )


def _metric_stability_summary(
    metric_history: pd.DataFrame,
) -> pd.DataFrame:
    return (
        metric_history.groupby(
            [
                "policy_id",
                "geo_id",
            ],
            dropna=False,
        )
        .agg(
            rows=("metric_score", "count"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            metric_score_mean=(
                "metric_score",
                "mean",
            ),
            metric_score_std=(
                "metric_score",
                "std",
            ),
            metric_score_p05=(
                "metric_score",
                lambda values: values.quantile(
                    0.05
                ),
            ),
            metric_score_p95=(
                "metric_score",
                lambda values: values.quantile(
                    0.95
                ),
            ),
            mean_absolute_metric_score_change=(
                (
                    "absolute_metric_score_"
                    "change_1m"
                ),
                "mean",
            ),
            p90_absolute_metric_score_change=(
                (
                    "absolute_metric_score_"
                    "change_1m"
                ),
                lambda values: values.quantile(
                    0.90
                ),
            ),
            maximum_absolute_metric_score_change=(
                (
                    "absolute_metric_score_"
                    "change_1m"
                ),
                "max",
            ),
            metric_sign_flip_rate=(
                "metric_sign_flip_flag",
                "mean",
            ),
            mean_feature_count=(
                "feature_count",
                "mean",
            ),
            minimum_feature_count=(
                "feature_count",
                "min",
            ),
            mean_feature_weight_sum=(
                "feature_weight_sum",
                "mean",
            ),
        )
        .reset_index()
    )


def _seasonality_summary(
    feature_history: pd.DataFrame,
    metric_history: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    feature_seasonality = (
        feature_history.groupby(
            [
                "policy_id",
                "geo_id",
                "feature_component",
                "calendar_month",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            mean_absolute_percentile_change=(
                (
                    "absolute_percentile_"
                    "change_1m"
                ),
                "mean",
            ),
            mean_absolute_feature_score_change=(
                (
                    "absolute_feature_score_"
                    "change_1m"
                ),
                "mean",
            ),
            score_sign_flip_rate=(
                "score_sign_flip_flag",
                "mean",
            ),
        )
        .reset_index()
    )

    metric_seasonality = (
        metric_history.groupby(
            [
                "policy_id",
                "geo_id",
                "calendar_month",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            mean_absolute_metric_score_change=(
                (
                    "absolute_metric_score_"
                    "change_1m"
                ),
                "mean",
            ),
            metric_sign_flip_rate=(
                "metric_sign_flip_flag",
                "mean",
            ),
        )
        .reset_index()
    )

    return (
        feature_seasonality,
        metric_seasonality,
    )


def _baseline_correlations(
    feature_history: pd.DataFrame,
    metric_history: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    baseline_features = feature_history[
        feature_history[
            "policy_id"
        ].eq(BASELINE_POLICY_ID)
    ][
        [
            "geo_id",
            "date",
            "feature_component",
            "percentile",
            "feature_score",
        ]
    ].rename(
        columns={
            "percentile": (
                "baseline_percentile"
            ),
            "feature_score": (
                "baseline_feature_score"
            ),
        }
    )

    feature_rows: list[
        dict[str, object]
    ] = []

    challengers = feature_history[
        ~feature_history[
            "policy_id"
        ].eq(BASELINE_POLICY_ID)
    ]

    for (
        policy_id,
        geo_id,
        feature_component,
    ), frame in challengers.groupby(
        [
            "policy_id",
            "geo_id",
            "feature_component",
        ]
    ):
        merged = frame.merge(
            baseline_features,
            on=[
                "geo_id",
                "date",
                "feature_component",
            ],
            how="inner",
            validate="one_to_one",
        )

        feature_rows.append(
            {
                "policy_id": policy_id,
                "geo_id": geo_id,
                "feature_component": (
                    feature_component
                ),
                "overlap_rows": int(
                    merged[
                        [
                            "feature_score",
                            (
                                "baseline_"
                                "feature_score"
                            ),
                        ]
                    ]
                    .dropna()
                    .shape[0]
                ),
                "percentile_correlation": (
                    _safe_correlation(
                        merged["percentile"],
                        merged[
                            "baseline_percentile"
                        ],
                    )
                ),
                "feature_score_correlation": (
                    _safe_correlation(
                        merged[
                            "feature_score"
                        ],
                        merged[
                            (
                                "baseline_"
                                "feature_score"
                            )
                        ],
                    )
                ),
                "mean_absolute_percentile_difference": (
                    (
                        merged["percentile"]
                        - merged[
                            "baseline_percentile"
                        ]
                    )
                    .abs()
                    .mean()
                ),
                "mean_absolute_feature_score_difference": (
                    (
                        merged[
                            "feature_score"
                        ]
                        - merged[
                            (
                                "baseline_"
                                "feature_score"
                            )
                        ]
                    )
                    .abs()
                    .mean()
                ),
            }
        )

    baseline_metric = metric_history[
        metric_history[
            "policy_id"
        ].eq(BASELINE_POLICY_ID)
    ][
        [
            "geo_id",
            "date",
            "metric_score",
        ]
    ].rename(
        columns={
            "metric_score": (
                "baseline_metric_score"
            ),
        }
    )

    metric_rows: list[
        dict[str, object]
    ] = []

    challenger_metric = metric_history[
        ~metric_history[
            "policy_id"
        ].eq(BASELINE_POLICY_ID)
    ]

    for (
        policy_id,
        geo_id,
    ), frame in challenger_metric.groupby(
        [
            "policy_id",
            "geo_id",
        ]
    ):
        merged = frame.merge(
            baseline_metric,
            on=[
                "geo_id",
                "date",
            ],
            how="inner",
            validate="one_to_one",
        )

        metric_rows.append(
            {
                "policy_id": policy_id,
                "geo_id": geo_id,
                "overlap_rows": int(
                    merged[
                        [
                            "metric_score",
                            (
                                "baseline_"
                                "metric_score"
                            ),
                        ]
                    ]
                    .dropna()
                    .shape[0]
                ),
                "metric_score_correlation": (
                    _safe_correlation(
                        merged["metric_score"],
                        merged[
                            "baseline_metric_score"
                        ],
                    )
                ),
                "mean_absolute_metric_score_difference": (
                    (
                        merged["metric_score"]
                        - merged[
                            "baseline_metric_score"
                        ]
                    )
                    .abs()
                    .mean()
                ),
            }
        )

    return (
        pd.DataFrame(feature_rows),
        pd.DataFrame(metric_rows),
    )


def _comparison_vs_baseline(
    metric_summary: pd.DataFrame,
) -> pd.DataFrame:
    baseline = metric_summary[
        metric_summary[
            "policy_id"
        ].eq(BASELINE_POLICY_ID)
    ][
        [
            "geo_id",
            "metric_score_std",
            (
                "mean_absolute_metric_"
                "score_change"
            ),
            (
                "p90_absolute_metric_"
                "score_change"
            ),
            "metric_sign_flip_rate",
        ]
    ].rename(
        columns={
            "metric_score_std": (
                "baseline_metric_score_std"
            ),
            (
                "mean_absolute_metric_"
                "score_change"
            ): (
                "baseline_mean_absolute_"
                "metric_score_change"
            ),
            (
                "p90_absolute_metric_"
                "score_change"
            ): (
                "baseline_p90_absolute_"
                "metric_score_change"
            ),
            "metric_sign_flip_rate": (
                "baseline_metric_sign_flip_rate"
            ),
        }
    )

    challengers = metric_summary[
        ~metric_summary[
            "policy_id"
        ].eq(BASELINE_POLICY_ID)
    ].copy()

    output = challengers.merge(
        baseline,
        on="geo_id",
        how="left",
        validate="many_to_one",
    )

    for metric in (
        "metric_score_std",
        (
            "mean_absolute_metric_"
            "score_change"
        ),
        (
            "p90_absolute_metric_"
            "score_change"
        ),
    ):
        baseline_column = (
            f"baseline_{metric}"
        )

        output[
            f"{metric}_pct_vs_baseline"
        ] = np.where(
            output[
                baseline_column
            ].ne(0),
            (
                output[metric]
                / output[
                    baseline_column
                ]
                - 1.0
            ),
            np.nan,
        )

    output[
        (
            "metric_sign_flip_rate_"
            "delta_vs_baseline"
        )
    ] = (
        output[
            "metric_sign_flip_rate"
        ]
        - output[
            "baseline_metric_sign_flip_rate"
        ]
    )

    return output


def build_metric_normalization_stability_audit(
    *,
    run_id: str = DEFAULT_RUN_ID,
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: tuple[str, ...] = (
        FOCUS_GEOS
    ),
) -> dict[str, pd.DataFrame]:
    store = RegimeArtifactStore(
        artifact_root
    )

    (
        source_metrics,
        baseline_features,
        baseline_normalized_features,
    ) = _load_baseline_artifacts(
        store,
        run_id=run_id,
    )

    raw_inventory = _load_raw_inventory(
        source_metrics
    )

    normalized_frames: list[
        pd.DataFrame
    ] = []

    metric_frames: list[
        pd.DataFrame
    ] = []

    for policy_id in (
        BASELINE_POLICY_ID,
        *CHALLENGER_IDS,
    ):
        (
            normalized_inventory,
            inventory_scores,
        ) = _run_policy_pipeline(
            policy_id=policy_id,
            baseline_features=(
                baseline_features
            ),
            baseline_normalized_features=(
                baseline_normalized_features
            ),
            raw_inventory=raw_inventory,
        )

        normalized_frames.append(
            normalized_inventory
        )

        metric_frames.append(
            inventory_scores
        )

    normalized_history = pd.concat(
        normalized_frames,
        ignore_index=True,
    )

    metric_history = pd.concat(
        metric_frames,
        ignore_index=True,
    )

    normalized_history = (
        normalized_history[
            normalized_history[
                "geo_id"
            ].isin(geo_ids)
        ].copy()
    )

    metric_history = (
        metric_history[
            metric_history[
                "geo_id"
            ].isin(geo_ids)
        ].copy()
    )

    feature_history = (
        _add_feature_diagnostics(
            normalized_history
        )
    )

    metric_history = (
        _add_metric_diagnostics(
            metric_history
        )
    )

    feature_summary = (
        _feature_stability_summary(
            feature_history
        )
    )

    metric_summary = (
        _metric_stability_summary(
            metric_history
        )
    )

    (
        feature_seasonality,
        metric_seasonality,
    ) = _seasonality_summary(
        feature_history,
        metric_history,
    )

    (
        feature_correlations,
        metric_correlations,
    ) = _baseline_correlations(
        feature_history,
        metric_history,
    )

    comparison = (
        _comparison_vs_baseline(
            metric_summary
        )
    )

    return {
        "normalized_feature_history": (
            feature_history
        ),
        "metric_score_history": (
            metric_history
        ),
        "feature_stability_summary": (
            feature_summary
        ),
        "metric_stability_summary": (
            metric_summary
        ),
        "feature_seasonality": (
            feature_seasonality
        ),
        "metric_seasonality": (
            metric_seasonality
        ),
        "feature_baseline_correlations": (
            feature_correlations
        ),
        "metric_baseline_correlations": (
            metric_correlations
        ),
        "metric_comparison_vs_baseline": (
            comparison
        ),
    }
