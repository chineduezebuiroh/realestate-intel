from __future__ import annotations
# regime/experiments/inventory_structural_window_diagnostic.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore
from regime.experiments.smoothing_features import (
    build_smoothed_metric_features_wide,
)
from regime.experiments.smoothing_policy import load_smoothing_experiments


DEFAULT_RUN_ID = "macro_regime_v1_bps120_sources"
TARGET_METRIC = "active_inventory"
POLICY_IDS = (
    "inventory_ma3_momentum",
    "inventory_ma3_deviation",
    "inventory_ma6_structural",
    "inventory_ma12_structural",
)
FOCUS_GEOS = (
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
)
FEATURE_COLUMNS = {
    "level": "smoothed_level_value",
    "short": "smoothed_short_value",
    "long": "smoothed_long_value",
}
TURNING_POINT_MIN_ABS_CHANGE = 0.02
SHOCK_QUANTILE = 0.90


def _safe_correlation(
    left: pd.Series,
    right: pd.Series,
    *,
    method: str,
) -> float:
    aligned = pd.concat(
        [
            pd.to_numeric(left, errors="coerce"),
            pd.to_numeric(right, errors="coerce"),
        ],
        axis=1,
    ).dropna()

    if len(aligned) < 3:
        return np.nan

    if aligned.iloc[:, 0].nunique() <= 1 or aligned.iloc[:, 1].nunique() <= 1:
        return np.nan

    return float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1], method=method))


def _load_raw_inventory(
    store: RegimeArtifactStore,
    *,
    run_id: str,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    frame = store.read_dataframe(run_id, "source_metrics")
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
        "metric_origin",
    }
    missing = required - set(frame.columns)

    if missing:
        raise ValueError(
            "source_metrics is missing required columns: "
            f"{sorted(missing)}"
        )

    inventory = frame[
        frame["canonical_metric_key"].eq(TARGET_METRIC)
        & frame["geo_id"].isin(geo_ids)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "value",
            "metric_origin",
        ]
    ].copy()

    inventory = inventory.rename(columns={"value": "raw_value"})
    inventory["date"] = pd.to_datetime(inventory["date"], errors="coerce")
    inventory["raw_value"] = pd.to_numeric(
        inventory["raw_value"],
        errors="coerce",
    )

    invalid = inventory[
        inventory["date"].isna()
        | inventory["raw_value"].isna()
        | ~np.isfinite(inventory["raw_value"])
    ]

    if not invalid.empty:
        raise ValueError(
            "Raw inventory contains invalid rows:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicates = inventory.duplicated(
        subset=["geo_id", "date", "canonical_metric_key"],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Raw inventory contains duplicate rows:\n"
            + inventory.loc[duplicates].head(30).to_string(index=False)
        )

    return inventory.sort_values(["geo_id", "date"]).reset_index(drop=True)


def _build_feature_history(raw_inventory: pd.DataFrame) -> pd.DataFrame:
    experiments = load_smoothing_experiments(validate=True)
    frames: list[pd.DataFrame] = []

    for policy_id in POLICY_IDS:
        policy = experiments[policy_id].policy_for(TARGET_METRIC)

        if policy is None:
            raise AssertionError(
                f"Could not resolve {policy_id}/{TARGET_METRIC}"
            )

        wide = build_smoothed_metric_features_wide(
            raw_inventory,
            policy=policy,
            value_column="raw_value",
        )

        keep = [
            "geo_id",
            "date",
            "canonical_metric_key",
            "raw_value",
            "metric_origin",
            "smoothed_level_value",
            "smoothed_short_value",
            "smoothed_long_value",
            "level_ma_value",
            "short_ma_value",
            "long_ma_value",
            "short_reference_value",
            "long_reference_value",
        ]
        work = wide[keep].copy()
        work["policy_id"] = policy_id
        work["transform_strategy"] = policy.transform_strategy
        work["level_window"] = policy.level_window
        work["short_window"] = policy.short_window
        work["long_window"] = policy.long_window
        work["long_lag_periods"] = policy.long_lag_periods
        frames.append(work)

    history = pd.concat(frames, ignore_index=True)
    history["calendar_month"] = history["date"].dt.month

    return history.sort_values(
        ["policy_id", "geo_id", "date"]
    ).reset_index(drop=True)


def _build_long_history(feature_history: pd.DataFrame) -> pd.DataFrame:
    id_columns = [
        "policy_id",
        "transform_strategy",
        "level_window",
        "short_window",
        "long_window",
        "long_lag_periods",
        "geo_id",
        "date",
        "calendar_month",
        "raw_value",
    ]
    reverse_map = {value: key for key, value in FEATURE_COLUMNS.items()}

    long = feature_history.melt(
        id_vars=id_columns,
        value_vars=list(FEATURE_COLUMNS.values()),
        var_name="feature_value_column",
        value_name="feature_value",
    )
    long["feature_component"] = long["feature_value_column"].map(reverse_map)
    long = long.sort_values(
        ["policy_id", "geo_id", "feature_component", "date"]
    ).reset_index(drop=True)

    grouped = long.groupby(
        ["policy_id", "geo_id", "feature_component"],
        group_keys=False,
    )
    long["feature_change_1m"] = grouped["feature_value"].diff()
    long["absolute_feature_change_1m"] = long["feature_change_1m"].abs()
    long["change_sign"] = np.sign(long["feature_change_1m"])
    long["previous_change_sign"] = grouped["change_sign"].shift(1)
    long["turning_point_flag"] = (
        long["feature_change_1m"].notna()
        & long["previous_change_sign"].notna()
        & long["feature_change_1m"].abs().ge(TURNING_POINT_MIN_ABS_CHANGE)
        & long["change_sign"].ne(0)
        & long["previous_change_sign"].ne(0)
        & long["change_sign"].ne(long["previous_change_sign"])
    )

    return long


def _coverage_summary(long: pd.DataFrame) -> pd.DataFrame:
    summary = (
        long.groupby(
            ["policy_id", "geo_id", "feature_component"],
            dropna=False,
        )
        .agg(
            total_rows=("date", "size"),
            valid_rows=("feature_value", "count"),
            first_source_date=("date", "min"),
            first_valid_date=(
                "date",
                lambda values: values[
                    long.loc[values.index, "feature_value"].notna()
                ].min(),
            ),
            last_valid_date=(
                "date",
                lambda values: values[
                    long.loc[values.index, "feature_value"].notna()
                ].max(),
            ),
        )
        .reset_index()
    )
    summary["warmup_rows"] = summary["total_rows"] - summary["valid_rows"]
    return summary


def _stability_summary(long: pd.DataFrame) -> pd.DataFrame:
    return (
        long.groupby(
            ["policy_id", "geo_id", "feature_component"],
            dropna=False,
        )
        .agg(
            rows=("feature_value", "count"),
            feature_mean=("feature_value", "mean"),
            feature_std=("feature_value", "std"),
            mean_absolute_change_1m=("absolute_feature_change_1m", "mean"),
            p90_absolute_change_1m=(
                "absolute_feature_change_1m",
                lambda values: values.quantile(0.90),
            ),
            maximum_absolute_change_1m=("absolute_feature_change_1m", "max"),
            turning_points=("turning_point_flag", "sum"),
        )
        .reset_index()
    )


def _calendar_month_summary(long: pd.DataFrame) -> pd.DataFrame:
    return (
        long.groupby(
            [
                "policy_id",
                "geo_id",
                "feature_component",
                "calendar_month",
            ],
            dropna=False,
        )
        .agg(
            rows=("feature_value", "count"),
            mean_feature_value=("feature_value", "mean"),
            median_feature_value=("feature_value", "median"),
            positive_rate=(
                "feature_value",
                lambda values: values.gt(0).mean(),
            ),
            mean_absolute_change_1m=("absolute_feature_change_1m", "mean"),
            turning_point_rate=("turning_point_flag", "mean"),
        )
        .reset_index()
    )


def _calendar_month_variance_share(frame: pd.DataFrame) -> float:
    work = frame.dropna(subset=["feature_value"])

    if len(work) < 12:
        return np.nan

    total_variance = float(work["feature_value"].var(ddof=0))

    if not np.isfinite(total_variance) or total_variance <= 0:
        return np.nan

    month_means = work.groupby("calendar_month")["feature_value"].transform(
        "mean"
    )
    return float(month_means.var(ddof=0) / total_variance)


def _seasonal_dependence_summary(long: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for keys, frame in long.groupby(
        ["policy_id", "geo_id", "feature_component"]
    ):
        policy_id, geo_id, component = keys
        valid = frame.dropna(subset=["feature_value"])
        month_means = valid.groupby("calendar_month")["feature_value"].mean()
        month_positive = valid.groupby("calendar_month")["feature_value"].apply(
            lambda values: values.gt(0).mean()
        )

        rows.append(
            {
                "policy_id": policy_id,
                "geo_id": geo_id,
                "feature_component": component,
                "rows": len(valid),
                "calendar_month_variance_share": (
                    _calendar_month_variance_share(valid)
                ),
                "calendar_month_mean_range": (
                    month_means.max() - month_means.min()
                ),
                "calendar_month_positive_rate_range": (
                    month_positive.max() - month_positive.min()
                ),
            }
        )

    return pd.DataFrame(rows)


def _feature_redundancy_summary(
    feature_history: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    pairs = (("level", "short"), ("level", "long"), ("short", "long"))

    for (policy_id, geo_id), frame in feature_history.groupby(
        ["policy_id", "geo_id"]
    ):
        for left, right in pairs:
            left_values = frame[FEATURE_COLUMNS[left]]
            right_values = frame[FEATURE_COLUMNS[right]]
            rows.append(
                {
                    "policy_id": policy_id,
                    "geo_id": geo_id,
                    "left_component": left,
                    "right_component": right,
                    "pearson_correlation": _safe_correlation(
                        left_values,
                        right_values,
                        method="pearson",
                    ),
                    "spearman_correlation": _safe_correlation(
                        left_values,
                        right_values,
                        method="spearman",
                    ),
                }
            )

    return pd.DataFrame(rows)


def _policy_pair_correlations(long: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    policy_ids = sorted(long["policy_id"].unique())

    for left_index, left_policy in enumerate(policy_ids):
        for right_policy in policy_ids[left_index + 1 :]:
            left = long[long["policy_id"].eq(left_policy)]
            right = long[long["policy_id"].eq(right_policy)]
            merged = left.merge(
                right,
                on=["geo_id", "date", "feature_component"],
                how="inner",
                suffixes=("_left", "_right"),
                validate="one_to_one",
            )

            for (geo_id, component), frame in merged.groupby(
                ["geo_id", "feature_component"]
            ):
                rows.append(
                    {
                        "left_policy_id": left_policy,
                        "right_policy_id": right_policy,
                        "geo_id": geo_id,
                        "feature_component": component,
                        "overlap_rows": int(
                            frame[
                                ["feature_value_left", "feature_value_right"]
                            ]
                            .dropna()
                            .shape[0]
                        ),
                        "pearson_correlation": _safe_correlation(
                            frame["feature_value_left"],
                            frame["feature_value_right"],
                            method="pearson",
                        ),
                        "spearman_correlation": _safe_correlation(
                            frame["feature_value_left"],
                            frame["feature_value_right"],
                            method="spearman",
                        ),
                        "mean_absolute_difference": (
                            frame["feature_value_left"]
                            - frame["feature_value_right"]
                        )
                        .abs()
                        .mean(),
                    }
                )

    return pd.DataFrame(rows)


def _ma6_momentum_equivalence(pair_correlations: pd.DataFrame) -> pd.DataFrame:
    policies = {
        "inventory_ma3_momentum",
        "inventory_ma6_structural",
    }
    output = pair_correlations[
        pair_correlations["left_policy_id"].isin(policies)
        & pair_correlations["right_policy_id"].isin(policies)
        & pair_correlations["feature_component"].eq("short")
    ].copy()

    if output.empty:
        raise AssertionError(
            "Could not resolve MA3 momentum versus MA6 structural short"
        )

    output["rank_equivalent"] = np.isclose(
        output["spearman_correlation"],
        1.0,
        rtol=0.0,
        atol=1e-12,
        equal_nan=False,
    )

    if not output["rank_equivalent"].all():
        raise AssertionError(
            "MA3 momentum and MA6 structural short are not rank-equivalent"
        )

    return output


def _turning_point_summary(long: pd.DataFrame) -> pd.DataFrame:
    events = long[long["turning_point_flag"]][
        ["policy_id", "geo_id", "feature_component", "date"]
    ].copy()
    reference_id = "inventory_ma3_deviation"
    rows: list[dict[str, object]] = []

    for (policy_id, geo_id, component), frame in events.groupby(
        ["policy_id", "geo_id", "feature_component"]
    ):
        if policy_id == reference_id:
            continue

        reference_dates = events[
            events["policy_id"].eq(reference_id)
            & events["geo_id"].eq(geo_id)
            & events["feature_component"].eq(component)
        ]["date"].tolist()

        lags: list[int] = []
        for date in frame["date"]:
            if not reference_dates:
                continue
            signed = [
                (date.year - candidate.year) * 12
                + (date.month - candidate.month)
                for candidate in reference_dates
            ]
            lags.append(min(signed, key=abs))

        if lags:
            rows.append(
                {
                    "policy_id": policy_id,
                    "reference_policy_id": reference_id,
                    "geo_id": geo_id,
                    "feature_component": component,
                    "events": len(lags),
                    "mean_lag_months": float(np.mean(lags)),
                    "median_lag_months": float(np.median(lags)),
                    "mean_absolute_lag_months": float(
                        np.mean(np.abs(lags))
                    ),
                    "p90_absolute_lag_months": float(
                        np.quantile(np.abs(lags), 0.90)
                    ),
                }
            )

    return pd.DataFrame(rows)


def _shock_summary(feature_history: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for geo_id, geo_frame in feature_history[
        feature_history["policy_id"].eq("inventory_ma3_deviation")
    ].groupby("geo_id"):
        raw = (
            geo_frame[["date", "raw_value"]]
            .drop_duplicates()
            .sort_values("date")
        )
        raw["raw_pct_change_1m"] = raw["raw_value"].pct_change()
        threshold = raw["raw_pct_change_1m"].abs().quantile(SHOCK_QUANTILE)
        shock_dates = set(
            raw.loc[
                raw["raw_pct_change_1m"].abs().ge(threshold),
                "date",
            ]
        )

        for policy_id, policy_frame in feature_history[
            feature_history["geo_id"].eq(geo_id)
        ].groupby("policy_id"):
            shocks = policy_frame[policy_frame["date"].isin(shock_dates)]
            rows.append(
                {
                    "policy_id": policy_id,
                    "geo_id": geo_id,
                    "shock_events": shocks["date"].nunique(),
                    "mean_absolute_short_on_shock": (
                        shocks["smoothed_short_value"].abs().mean()
                    ),
                    "p90_absolute_short_on_shock": (
                        shocks["smoothed_short_value"].abs().quantile(0.90)
                    ),
                    "mean_absolute_level_on_shock": (
                        shocks["smoothed_level_value"].abs().mean()
                    ),
                    "mean_absolute_long_on_shock": (
                        shocks["smoothed_long_value"].abs().mean()
                    ),
                }
            )

    return pd.DataFrame(rows)


def build_inventory_structural_window_diagnostic(
    *,
    run_id: str = DEFAULT_RUN_ID,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: tuple[str, ...] = FOCUS_GEOS,
) -> dict[str, pd.DataFrame]:
    store = RegimeArtifactStore(artifact_root)
    raw_inventory = _load_raw_inventory(
        store,
        run_id=run_id,
        geo_ids=geo_ids,
    )
    feature_history = _build_feature_history(raw_inventory)
    long = _build_long_history(feature_history)
    pair_correlations = _policy_pair_correlations(long)

    return {
        "feature_history": feature_history,
        "long_feature_history": long,
        "coverage_summary": _coverage_summary(long),
        "stability_summary": _stability_summary(long),
        "calendar_month_summary": _calendar_month_summary(long),
        "seasonal_dependence_summary": _seasonal_dependence_summary(long),
        "feature_redundancy_summary": _feature_redundancy_summary(
            feature_history
        ),
        "policy_pair_correlations": pair_correlations,
        "ma6_momentum_equivalence": _ma6_momentum_equivalence(
            pair_correlations
        ),
        "turning_point_summary": _turning_point_summary(long),
        "shock_summary": _shock_summary(feature_history),
    }
