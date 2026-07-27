from __future__ import annotations
# regime/experiments/active_inventory_comparison.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)
from regime.smoothing_features import (
    build_smoothed_metric_features,
)
from regime.smoothing_policy import (
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

CURRENT_FEATURE_MAP = {
    "redfin_inventory_level": "level",
    "redfin_inventory_short": "short",
    "redfin_inventory_long": "long",
}

CHALLENGER_IDS = (
    "inventory_ma3_momentum",
    "inventory_ma3_deviation",
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


def _load_raw_inventory(
    store: RegimeArtifactStore,
    *,
    run_id: str,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    source_metrics = (
        store.read_dataframe(
            run_id,
            "source_metrics",
        )
    )

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
            "source_metrics artifact is missing "
            f"required columns: {sorted(missing)}"
        )

    inventory = source_metrics[
        source_metrics[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
        & source_metrics[
            "geo_id"
        ].isin(geo_ids)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "value",
            "metric_origin",
        ]
    ].copy()

    inventory["date"] = pd.to_datetime(
        inventory["date"],
        errors="coerce",
    )

    inventory["value"] = pd.to_numeric(
        inventory["value"],
        errors="coerce",
    )

    inventory = inventory.rename(
        columns={
            "value": "raw_value",
        }
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
            "Raw active-inventory observations "
            "contain invalid dates or values:\n"
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
            "Raw active-inventory observations "
            "contain duplicate keys:\n"
            + inventory.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    if (
        inventory["metric_origin"]
        .ne("canonical_source")
        .any()
    ):
        raise AssertionError(
            "Active inventory must come from "
            "canonical_source observations"
        )

    found_geos = set(
        inventory["geo_id"].unique()
    )

    missing_geos = (
        set(geo_ids)
        - found_geos
    )

    if missing_geos:
        raise ValueError(
            "Raw active inventory is missing "
            f"focus geographies: {sorted(missing_geos)}"
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


def _load_current_features(
    store: RegimeArtifactStore,
    *,
    run_id: str,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    features = store.read_dataframe(
        run_id,
        "features",
    )

    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
        "raw_feature_value",
    }

    missing = (
        required
        - set(features.columns)
    )

    if missing:
        raise ValueError(
            "features artifact is missing "
            f"required columns: {sorted(missing)}"
        )

    current = features[
        features[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
        & features[
            "geo_id"
        ].isin(geo_ids)
        & features[
            "feature_key"
        ].isin(
            CURRENT_FEATURE_MAP
        )
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
            "raw_feature_value",
        ]
    ].copy()

    current["date"] = pd.to_datetime(
        current["date"],
        errors="coerce",
    )

    current[
        "feature_component"
    ] = current[
        "feature_key"
    ].map(
        CURRENT_FEATURE_MAP
    )

    current["policy_id"] = (
        "baseline_current"
    )

    current[
        "transform_strategy"
    ] = "current"

    current = current.rename(
        columns={
            "raw_feature_value": (
                "feature_value"
            ),
        }
    )

    duplicates = current.duplicated(
        subset=[
            "geo_id",
            "date",
            "feature_component",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Current active-inventory features "
            "contain duplicate component rows:\n"
            + current.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return (
        current.sort_values(
            [
                "geo_id",
                "date",
                "feature_component",
            ]
        )
        .reset_index(drop=True)
    )


def _build_challenger_features(
    raw_inventory: pd.DataFrame,
) -> pd.DataFrame:
    experiments = (
        load_smoothing_experiments(
            validate=True
        )
    )

    frames: list[pd.DataFrame] = []

    for experiment_id in (
        CHALLENGER_IDS
    ):
        experiment = experiments[
            experiment_id
        ]

        policy = experiment.policy_for(
            TARGET_METRIC
        )

        if policy is None:
            raise AssertionError(
                "Could not resolve smoothing policy "
                f"for {experiment_id}/{TARGET_METRIC}"
            )

        features = (
            build_smoothed_metric_features(
                raw_inventory,
                policy=policy,
                value_column="raw_value",
                preserve_columns=[
                    "metric_origin",
                ],
            )
        )

        features = features.rename(
            columns={
                "raw_feature_value": (
                    "feature_value"
                ),
                "smoothing_experiment_id": (
                    "policy_id"
                ),
                "smoothing_strategy": (
                    "transform_strategy"
                ),
            }
        )

        frames.append(
            features[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "feature_key",
                    "feature_component",
                    "feature_value",
                    "policy_id",
                    "transform_strategy",
                    "raw_value",
                    "metric_origin",
                ]
            ]
        )

    output = pd.concat(
        frames,
        ignore_index=True,
    )

    duplicates = output.duplicated(
        subset=[
            "geo_id",
            "date",
            "feature_component",
            "policy_id",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Challenger features contain "
            "duplicate rows:\n"
            + output.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return output


def _add_series_diagnostics(
    features: pd.DataFrame,
) -> pd.DataFrame:
    work = features.copy()

    work["feature_value"] = pd.to_numeric(
        work["feature_value"],
        errors="coerce",
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

    work[
        "feature_change_1m"
    ] = grouped[
        "feature_value"
    ].diff()

    work[
        "absolute_feature_change_1m"
    ] = work[
        "feature_change_1m"
    ].abs()

    work[
        "previous_feature_value"
    ] = grouped[
        "feature_value"
    ].shift(1)

    work[
        "sign_flip_flag"
    ] = (
        work["feature_value"].notna()
        & work[
            "previous_feature_value"
        ].notna()
        & work["feature_value"].ne(0)
        & work[
            "previous_feature_value"
        ].ne(0)
        & (
            np.sign(
                work["feature_value"]
            )
            != np.sign(
                work[
                    "previous_feature_value"
                ]
            )
        )
    )

    work["calendar_month"] = (
        work["date"].dt.month
    )

    return work


def _build_coverage_summary(
    feature_history: pd.DataFrame,
) -> pd.DataFrame:
    return (
        feature_history.groupby(
            [
                "policy_id",
                "transform_strategy",
                "geo_id",
                "feature_component",
            ],
            dropna=False,
        )
        .agg(
            total_rows=(
                "feature_value",
                "size",
            ),
            valid_rows=(
                "feature_value",
                "count",
            ),
            first_date=(
                "date",
                "min",
            ),
            last_date=(
                "date",
                "max",
            ),
            first_valid_date=(
                "date",
                lambda values: values[
                    feature_history.loc[
                        values.index,
                        "feature_value",
                    ].notna()
                ].min(),
            ),
            last_valid_date=(
                "date",
                lambda values: values[
                    feature_history.loc[
                        values.index,
                        "feature_value",
                    ].notna()
                ].max(),
            ),
        )
        .reset_index()
    )


def _build_volatility_summary(
    feature_history: pd.DataFrame,
) -> pd.DataFrame:
    return (
        feature_history.groupby(
            [
                "policy_id",
                "transform_strategy",
                "geo_id",
                "feature_component",
            ],
            dropna=False,
        )
        .agg(
            rows=(
                "feature_value",
                "count",
            ),
            mean_feature_value=(
                "feature_value",
                "mean",
            ),
            feature_value_std=(
                "feature_value",
                "std",
            ),
            mean_absolute_change_1m=(
                "absolute_feature_change_1m",
                "mean",
            ),
            median_absolute_change_1m=(
                "absolute_feature_change_1m",
                "median",
            ),
            p90_absolute_change_1m=(
                "absolute_feature_change_1m",
                lambda values: values.quantile(
                    0.90
                ),
            ),
            p95_absolute_change_1m=(
                "absolute_feature_change_1m",
                lambda values: values.quantile(
                    0.95
                ),
            ),
            maximum_absolute_change_1m=(
                "absolute_feature_change_1m",
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
            minimum_feature_value=(
                "feature_value",
                "min",
            ),
            maximum_feature_value=(
                "feature_value",
                "max",
            ),
        )
        .reset_index()
    )


def _build_seasonality_summary(
    feature_history: pd.DataFrame,
) -> pd.DataFrame:
    return (
        feature_history.groupby(
            [
                "policy_id",
                "transform_strategy",
                "geo_id",
                "feature_component",
                "calendar_month",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            valid_rows=(
                "feature_value",
                "count",
            ),
            mean_absolute_change_1m=(
                "absolute_feature_change_1m",
                "mean",
            ),
            median_absolute_change_1m=(
                "absolute_feature_change_1m",
                "median",
            ),
            p90_absolute_change_1m=(
                "absolute_feature_change_1m",
                lambda values: values.quantile(
                    0.90
                ),
            ),
            sign_flip_rate=(
                "sign_flip_flag",
                "mean",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "policy_id",
                "geo_id",
                "feature_component",
                "calendar_month",
            ]
        )
        .reset_index(drop=True)
    )


def _build_correlations(
    feature_history: pd.DataFrame,
) -> pd.DataFrame:
    baseline = feature_history[
        feature_history[
            "policy_id"
        ].eq("baseline_current")
    ][
        [
            "geo_id",
            "date",
            "feature_component",
            "feature_value",
        ]
    ].rename(
        columns={
            "feature_value": (
                "baseline_feature_value"
            ),
        }
    )

    rows: list[dict[str, object]] = []

    challengers = feature_history[
        ~feature_history[
            "policy_id"
        ].eq("baseline_current")
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
        ],
        dropna=False,
    ):
        merged = frame[
            [
                "geo_id",
                "date",
                "feature_component",
                "feature_value",
            ]
        ].merge(
            baseline,
            on=[
                "geo_id",
                "date",
                "feature_component",
            ],
            how="inner",
            validate="one_to_one",
        )

        rows.append(
            {
                "policy_id": policy_id,
                "geo_id": geo_id,
                "feature_component": (
                    feature_component
                ),
                "overlap_rows": len(
                    merged.dropna(
                        subset=[
                            "feature_value",
                            (
                                "baseline_"
                                "feature_value"
                            ),
                        ]
                    )
                ),
                "correlation_with_baseline": (
                    _safe_correlation(
                        merged[
                            "feature_value"
                        ],
                        merged[
                            (
                                "baseline_"
                                "feature_value"
                            )
                        ],
                    )
                ),
                "mean_absolute_difference": (
                    (
                        merged[
                            "feature_value"
                        ]
                        - merged[
                            (
                                "baseline_"
                                "feature_value"
                            )
                        ]
                    )
                    .abs()
                    .mean()
                ),
            }
        )

    return pd.DataFrame(rows)


def _build_policy_comparison(
    volatility_summary: pd.DataFrame,
) -> pd.DataFrame:
    baseline = volatility_summary[
        volatility_summary[
            "policy_id"
        ].eq("baseline_current")
    ][
        [
            "geo_id",
            "feature_component",
            "mean_absolute_change_1m",
            "p90_absolute_change_1m",
            "sign_flip_rate",
        ]
    ].rename(
        columns={
            "mean_absolute_change_1m": (
                "baseline_mean_absolute_"
                "change_1m"
            ),
            "p90_absolute_change_1m": (
                "baseline_p90_absolute_"
                "change_1m"
            ),
            "sign_flip_rate": (
                "baseline_sign_flip_rate"
            ),
        }
    )

    challengers = volatility_summary[
        ~volatility_summary[
            "policy_id"
        ].eq("baseline_current")
    ].copy()

    comparison = challengers.merge(
        baseline,
        on=[
            "geo_id",
            "feature_component",
        ],
        how="left",
        validate="many_to_one",
    )

    comparison[
        "mean_absolute_change_pct_vs_baseline"
    ] = np.where(
        comparison[
            (
                "baseline_mean_absolute_"
                "change_1m"
            )
        ].ne(0),
        (
            comparison[
                "mean_absolute_change_1m"
            ]
            / comparison[
                (
                    "baseline_mean_absolute_"
                    "change_1m"
                )
            ]
            - 1.0
        ),
        np.nan,
    )

    comparison[
        "p90_absolute_change_pct_vs_baseline"
    ] = np.where(
        comparison[
            (
                "baseline_p90_absolute_"
                "change_1m"
            )
        ].ne(0),
        (
            comparison[
                "p90_absolute_change_1m"
            ]
            / comparison[
                (
                    "baseline_p90_absolute_"
                    "change_1m"
                )
            ]
            - 1.0
        ),
        np.nan,
    )

    comparison[
        "sign_flip_rate_delta_vs_baseline"
    ] = (
        comparison[
            "sign_flip_rate"
        ]
        - comparison[
            "baseline_sign_flip_rate"
        ]
    )

    return comparison.sort_values(
        [
            "geo_id",
            "feature_component",
            "policy_id",
        ]
    ).reset_index(drop=True)


def build_active_inventory_comparison(
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

    raw_inventory = _load_raw_inventory(
        store,
        run_id=run_id,
        geo_ids=geo_ids,
    )

    current_features = (
        _load_current_features(
            store,
            run_id=run_id,
            geo_ids=geo_ids,
        )
    )

    challenger_features = (
        _build_challenger_features(
            raw_inventory
        )
    )

    combined_features = pd.concat(
        [
            current_features[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "feature_key",
                    "feature_component",
                    "feature_value",
                    "policy_id",
                    "transform_strategy",
                ]
            ],
            challenger_features[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "feature_key",
                    "feature_component",
                    "feature_value",
                    "policy_id",
                    "transform_strategy",
                ]
            ],
        ],
        ignore_index=True,
    )

    feature_history = (
        _add_series_diagnostics(
            combined_features
        )
    )

    coverage_summary = (
        _build_coverage_summary(
            feature_history
        )
    )

    volatility_summary = (
        _build_volatility_summary(
            feature_history
        )
    )

    seasonality_summary = (
        _build_seasonality_summary(
            feature_history
        )
    )

    correlations = _build_correlations(
        feature_history
    )

    policy_comparison = (
        _build_policy_comparison(
            volatility_summary
        )
    )

    return {
        "raw_inventory": raw_inventory,
        "feature_history": feature_history,
        "coverage_summary": coverage_summary,
        "volatility_summary": volatility_summary,
        "seasonality_summary": seasonality_summary,
        "baseline_correlations": correlations,
        "policy_comparison": policy_comparison,
    }
