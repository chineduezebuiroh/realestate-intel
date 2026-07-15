from __future__ import annotations
# regime/experiments/core_demand_dimension_diagnostic.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._04_asof_aligner import align_metric_scores_asof
from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore


BASELINE_RUN_ID = "macro_regime_v1_bps120_sources"
TARGET_DIMENSION = "demand"
FOCUS_GEOS = (
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
)
NEAR_ZERO_THRESHOLD = 0.10


def _truthy(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "1", "yes", "y"})
    )


def _safe_correlation(
    left: pd.Series,
    right: pd.Series,
    *,
    method: str = "pearson",
) -> float:
    work = pd.concat(
        [
            pd.to_numeric(left, errors="coerce"),
            pd.to_numeric(right, errors="coerce"),
        ],
        axis=1,
    ).dropna()

    if len(work) < 3:
        return np.nan

    if work.iloc[:, 0].nunique() <= 1 or work.iloc[:, 1].nunique() <= 1:
        return np.nan

    return float(work.iloc[:, 0].corr(work.iloc[:, 1], method=method))


def _calendar_month_variance_share(
    values: pd.Series,
    months: pd.Series,
) -> float:
    work = pd.DataFrame(
        {
            "value": pd.to_numeric(values, errors="coerce"),
            "calendar_month": months,
        }
    ).dropna()

    if len(work) < 24:
        return np.nan

    total_variance = float(work["value"].var(ddof=0))

    if not np.isfinite(total_variance) or total_variance <= 0:
        return np.nan

    month_means = work.groupby("calendar_month")["value"].transform("mean")
    between_variance = float(month_means.var(ddof=0))

    return between_variance / total_variance


def _load_demand_metric_registry() -> pd.DataFrame:
    config = load_regime_config(validate=True)
    registry = config.metric_dimensions.copy()

    required = {
        "canonical_metric_key",
        "dimension",
        "metric_weight",
        "enabled",
        "diagnostic_only",
        "macro_enabled",
    }
    missing = required - set(registry.columns)

    if missing:
        raise ValueError(
            "Metric-dimension registry is missing "
            f"columns: {sorted(missing)}"
        )

    registry = registry[
        _truthy(registry["enabled"])
        & ~_truthy(registry["diagnostic_only"])
        & _truthy(registry["macro_enabled"])
        & registry["dimension"].eq(TARGET_DIMENSION)
    ].copy()

    registry["metric_weight"] = pd.to_numeric(
        registry["metric_weight"],
        errors="coerce",
    )

    if registry.empty:
        raise ValueError(
            "No enabled metrics were found for the core Demand dimension"
        )

    if registry["metric_weight"].isna().any():
        raise ValueError(
            "Core Demand registry contains non-numeric metric weights"
        )

    conflicts = (
        registry[["canonical_metric_key", "metric_weight"]]
        .drop_duplicates()
        .groupby("canonical_metric_key")["metric_weight"]
        .nunique()
        .reset_index(name="weight_count")
    )
    conflicts = conflicts[conflicts["weight_count"].gt(1)]

    if not conflicts.empty:
        raise ValueError(
            "Core Demand metrics have conflicting weights:\n"
            + conflicts.to_string(index=False)
        )

    output = (
        registry[["canonical_metric_key", "metric_weight"]]
        .drop_duplicates(subset=["canonical_metric_key"])
        .sort_values(
            ["metric_weight", "canonical_metric_key"],
            ascending=[False, True],
        )
        .reset_index(drop=True)
    )

    total_weight = output["metric_weight"].sum()

    if not np.isfinite(total_weight) or total_weight <= 0:
        raise AssertionError(
            "Core Demand metric weights have a non-positive total"
        )

    return output


def _load_feature_registry(metric_keys: set[str]) -> pd.DataFrame:
    config = load_regime_config(validate=True)
    features = config.features.copy()

    required = {
        "feature_key",
        "canonical_metric_key",
        "feature_weight",
    }
    missing = required - set(features.columns)

    if missing:
        raise ValueError(
            "Feature registry is missing "
            f"columns: {sorted(missing)}"
        )

    features = features[
        features["canonical_metric_key"].isin(metric_keys)
    ][
        ["canonical_metric_key", "feature_key", "feature_weight"]
    ].copy()

    features["feature_weight"] = pd.to_numeric(
        features["feature_weight"],
        errors="coerce",
    )

    if features.empty:
        raise ValueError(
            "No production features were found for core Demand metrics"
        )

    if features["feature_weight"].isna().any():
        raise ValueError(
            "Core Demand features contain non-numeric weights"
        )

    return (
        features.drop_duplicates(
            subset=["canonical_metric_key", "feature_key"]
        )
        .sort_values(["canonical_metric_key", "feature_key"])
        .reset_index(drop=True)
    )


def _prepare_metric_history(
    aligned_metrics: pd.DataFrame,
    *,
    metric_keys: set[str],
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    required = {
        "geo_id",
        "evaluation_date",
        "metric_date",
        "canonical_metric_key",
        "metric_score",
        "metric_age_days",
    }
    missing = required - set(aligned_metrics.columns)

    if missing:
        raise ValueError(
            "Aligned metric scores are missing "
            f"columns: {sorted(missing)}"
        )

    work = aligned_metrics[
        aligned_metrics["canonical_metric_key"].isin(metric_keys)
        & aligned_metrics["geo_id"].isin(geo_ids)
    ].copy()

    if work.empty:
        raise ValueError(
            "No aligned core Demand metric scores were found"
        )

    work = work.rename(columns={"evaluation_date": "date"})
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work["metric_date"] = pd.to_datetime(
        work["metric_date"],
        errors="coerce",
    )
    work["metric_score"] = pd.to_numeric(
        work["metric_score"],
        errors="coerce",
    )
    work["metric_age_days"] = pd.to_numeric(
        work["metric_age_days"],
        errors="coerce",
    )

    invalid = work[
        work["date"].isna()
        | work["metric_date"].isna()
        | work["metric_score"].isna()
        | ~np.isfinite(work["metric_score"])
    ]

    if not invalid.empty:
        raise ValueError(
            "Core Demand metric history contains invalid rows:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicates = work.duplicated(
        subset=["geo_id", "date", "canonical_metric_key"],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Core Demand metric history is not unique by geo/date/metric:\n"
            + work.loc[duplicates].head(30).to_string(index=False)
        )

    work = work.sort_values(
        ["geo_id", "canonical_metric_key", "date"]
    ).reset_index(drop=True)

    grouped = work.groupby(
        ["geo_id", "canonical_metric_key"],
        group_keys=False,
    )

    work["metric_score_change_1m"] = grouped["metric_score"].diff()
    work["absolute_metric_score_change_1m"] = work[
        "metric_score_change_1m"
    ].abs()

    previous = grouped["metric_score"].shift(1)

    work["metric_sign_flip"] = (
        previous.notna()
        & np.sign(previous).ne(np.sign(work["metric_score"]))
    )

    work["calendar_month"] = work["date"].dt.month

    return work


def _prepare_feature_history(
    normalized_features: pd.DataFrame,
    *,
    metric_keys: set[str],
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
        "raw_feature_value",
        "percentile",
        "feature_score",
    }
    missing = required - set(normalized_features.columns)

    if missing:
        raise ValueError(
            "Normalized features are missing "
            f"columns: {sorted(missing)}"
        )

    work = normalized_features[
        normalized_features["canonical_metric_key"].isin(metric_keys)
        & normalized_features["geo_id"].isin(geo_ids)
    ].copy()

    if work.empty:
        raise ValueError(
            "No normalized features were found for core Demand metrics"
        )

    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work["raw_feature_value"] = pd.to_numeric(
        work["raw_feature_value"],
        errors="coerce",
    )
    work["feature_score"] = pd.to_numeric(
        work["feature_score"],
        errors="coerce",
    )

    work = work.sort_values(
        ["geo_id", "canonical_metric_key", "feature_key", "date"]
    ).reset_index(drop=True)

    grouped = work.groupby(
        ["geo_id", "canonical_metric_key", "feature_key"],
        group_keys=False,
    )

    work["feature_score_change_1m"] = grouped["feature_score"].diff()
    work["absolute_feature_score_change_1m"] = work[
        "feature_score_change_1m"
    ].abs()

    previous = grouped["feature_score"].shift(1)

    work["feature_sign_flip"] = (
        previous.notna()
        & np.sign(previous).ne(np.sign(work["feature_score"]))
    )

    work["calendar_month"] = work["date"].dt.month

    return work


def _build_metric_summary(metric_history: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for (geo_id, metric_key), frame in metric_history.groupby(
        ["geo_id", "canonical_metric_key"]
    ):
        rows.append(
            {
                "geo_id": geo_id,
                "canonical_metric_key": metric_key,
                "rows": len(frame),
                "first_date": frame["date"].min(),
                "last_date": frame["date"].max(),
                "mean_metric_score": frame["metric_score"].mean(),
                "metric_score_std": frame["metric_score"].std(),
                "mean_absolute_change_1m": frame[
                    "absolute_metric_score_change_1m"
                ].mean(),
                "p90_absolute_change_1m": frame[
                    "absolute_metric_score_change_1m"
                ].quantile(0.90),
                "maximum_absolute_change_1m": frame[
                    "absolute_metric_score_change_1m"
                ].max(),
                "sign_flip_rate": frame["metric_sign_flip"].mean(),
                "near_zero_rate": frame["metric_score"].abs().lt(
                    NEAR_ZERO_THRESHOLD
                ).mean(),
                "mean_metric_age_days": frame["metric_age_days"].mean(),
                "p90_metric_age_days": frame["metric_age_days"].quantile(
                    0.90
                ),
                "maximum_metric_age_days": frame[
                    "metric_age_days"
                ].max(),
                "calendar_month_variance_share": (
                    _calendar_month_variance_share(
                        frame["metric_score"],
                        frame["calendar_month"],
                    )
                ),
            }
        )

    return pd.DataFrame(rows)


def _build_feature_summary(feature_history: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for (geo_id, metric_key, feature_key), frame in feature_history.groupby(
        ["geo_id", "canonical_metric_key", "feature_key"]
    ):
        rows.append(
            {
                "geo_id": geo_id,
                "canonical_metric_key": metric_key,
                "feature_key": feature_key,
                "rows": len(frame),
                "first_date": frame["date"].min(),
                "last_date": frame["date"].max(),
                "feature_score_std": frame["feature_score"].std(),
                "mean_absolute_change_1m": frame[
                    "absolute_feature_score_change_1m"
                ].mean(),
                "p90_absolute_change_1m": frame[
                    "absolute_feature_score_change_1m"
                ].quantile(0.90),
                "sign_flip_rate": frame["feature_sign_flip"].mean(),
                "near_zero_rate": frame["feature_score"].abs().lt(
                    NEAR_ZERO_THRESHOLD
                ).mean(),
                "calendar_month_variance_share": (
                    _calendar_month_variance_share(
                        frame["feature_score"],
                        frame["calendar_month"],
                    )
                ),
            }
        )

    return pd.DataFrame(rows)


def _build_metric_contributions(
    metric_history: pd.DataFrame,
    metric_registry: pd.DataFrame,
) -> pd.DataFrame:
    work = metric_history.merge(
        metric_registry,
        on="canonical_metric_key",
        how="left",
        validate="many_to_one",
    )

    if work["metric_weight"].isna().any():
        raise AssertionError(
            "Core Demand metric history contains unmapped metric weights"
        )

    work["available_metric_weight_sum"] = (
        work.groupby(["geo_id", "date"])["metric_weight"]
        .transform("sum")
    )

    if work["available_metric_weight_sum"].le(0).any():
        raise AssertionError(
            "Core Demand available metric weight sum is non-positive"
        )

    work["effective_metric_weight"] = (
        work["metric_weight"]
        / work["available_metric_weight_sum"]
    )

    work["weighted_metric_contribution"] = (
        work["metric_score"]
        * work["effective_metric_weight"]
    )

    return work


def _load_demand_dimension_history(
    store: RegimeArtifactStore,
    *,
    run_id: str,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    dimensions = store.read_dataframe(
        run_id,
        "dimension_scores",
    )

    dimensions["date"] = pd.to_datetime(
        dimensions["date"],
        errors="coerce",
    )

    focus = dimensions[
        dimensions["dimension"].eq(TARGET_DIMENSION)
        & dimensions["geo_id"].isin(geo_ids)
    ].copy()

    if focus.empty:
        raise ValueError(
            "No core Demand dimension history was found"
        )

    return focus


def _build_monthly_contribution_panel(
    contributions: pd.DataFrame,
    dimension_history: pd.DataFrame,
) -> pd.DataFrame:
    contribution_wide = (
        contributions.pivot(
            index=["geo_id", "date"],
            columns="canonical_metric_key",
            values="weighted_metric_contribution",
        )
        .reset_index()
    )

    score_wide = (
        contributions.pivot(
            index=["geo_id", "date"],
            columns="canonical_metric_key",
            values="metric_score",
        )
        .reset_index()
    )

    score_wide = score_wide.rename(
        columns={
            column: f"{column}_metric_score"
            for column in score_wide.columns
            if column not in {"geo_id", "date"}
        }
    )

    panel = contribution_wide.merge(
        score_wide,
        on=["geo_id", "date"],
        how="inner",
        validate="one_to_one",
    )

    panel = panel.merge(
        dimension_history[
            ["geo_id", "date", "dimension_score"]
        ],
        on=["geo_id", "date"],
        how="inner",
        validate="one_to_one",
    )

    contribution_columns = [
        metric_key
        for metric_key in contributions["canonical_metric_key"].unique()
        if metric_key in panel.columns
    ]

    panel["reconstructed_dimension_score"] = panel[
        contribution_columns
    ].sum(axis=1, min_count=1)

    panel["reconstruction_error"] = (
        panel["reconstructed_dimension_score"]
        - panel["dimension_score"]
    )

    maximum_error = panel["reconstruction_error"].abs().max()

    if (
        not np.isfinite(maximum_error)
        or maximum_error > 1e-12
    ):
        raise AssertionError(
            "Core Demand metric contributions do not reconstruct "
            f"the dimension exactly. max_error={maximum_error}"
        )

    panel["gross_contribution_magnitude"] = panel[
        contribution_columns
    ].abs().sum(axis=1, min_count=1)

    panel["net_contribution_magnitude"] = panel[
        "dimension_score"
    ].abs()

    panel["cancellation_amount"] = (
        panel["gross_contribution_magnitude"]
        - panel["net_contribution_magnitude"]
    )

    panel["cancellation_rate"] = np.where(
        panel["gross_contribution_magnitude"].gt(0),
        panel["cancellation_amount"]
        / panel["gross_contribution_magnitude"],
        0.0,
    )

    panel["near_zero_dimension"] = panel[
        "dimension_score"
    ].abs().lt(NEAR_ZERO_THRESHOLD)

    return panel.sort_values(
        ["geo_id", "date"]
    ).reset_index(drop=True)


def _build_contribution_summary(
    contributions: pd.DataFrame,
) -> pd.DataFrame:
    return (
        contributions.groupby(
            ["geo_id", "canonical_metric_key", "metric_weight"],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            mean_available_metric_weight_sum=(
                "available_metric_weight_sum",
                "mean",
            ),
            mean_effective_metric_weight=(
                "effective_metric_weight",
                "mean",
            ),
            mean_metric_score=("metric_score", "mean"),
            mean_weighted_contribution=(
                "weighted_metric_contribution",
                "mean",
            ),
            mean_absolute_weighted_contribution=(
                "weighted_metric_contribution",
                lambda values: values.abs().mean(),
            ),
            positive_contribution_rate=(
                "weighted_metric_contribution",
                lambda values: values.gt(0).mean(),
            ),
        )
        .reset_index()
    )


def _build_cancellation_summary(
    monthly_panel: pd.DataFrame,
) -> pd.DataFrame:
    return (
        monthly_panel.groupby("geo_id")
        .agg(
            rows=("date", "size"),
            mean_gross_magnitude=(
                "gross_contribution_magnitude",
                "mean",
            ),
            mean_net_magnitude=(
                "net_contribution_magnitude",
                "mean",
            ),
            mean_cancellation_amount=(
                "cancellation_amount",
                "mean",
            ),
            mean_cancellation_rate=(
                "cancellation_rate",
                "mean",
            ),
            p90_cancellation_rate=(
                "cancellation_rate",
                lambda values: values.quantile(0.90),
            ),
            full_cancellation_rate=(
                "cancellation_rate",
                lambda values: values.ge(0.90).mean(),
            ),
            near_zero_dimension_rate=(
                "near_zero_dimension",
                "mean",
            ),
        )
        .reset_index()
    )


def _build_dominant_metric_summary(
    contributions: pd.DataFrame,
    monthly_panel: pd.DataFrame,
) -> pd.DataFrame:
    near_zero_keys = monthly_panel[
        monthly_panel["near_zero_dimension"]
    ][["geo_id", "date"]]

    focus = contributions.merge(
        near_zero_keys,
        on=["geo_id", "date"],
        how="inner",
        validate="many_to_one",
    )

    focus["absolute_contribution"] = focus[
        "weighted_metric_contribution"
    ].abs()

    focus["rank_within_month"] = focus.groupby(
        ["geo_id", "date"]
    )["absolute_contribution"].rank(
        method="first",
        ascending=False,
    )

    dominant = focus[
        focus["rank_within_month"].eq(1)
    ]

    totals = (
        near_zero_keys.groupby("geo_id")
        .size()
        .reset_index(name="near_zero_months")
    )

    summary = (
        dominant.groupby(
            ["geo_id", "canonical_metric_key"]
        )
        .size()
        .reset_index(name="dominant_months")
    )

    summary = summary.merge(
        totals,
        on="geo_id",
        how="left",
        validate="many_to_one",
    )

    summary["dominant_rate"] = (
        summary["dominant_months"]
        / summary["near_zero_months"]
    )

    return summary.sort_values(
        ["geo_id", "dominant_rate"],
        ascending=[True, False],
    ).reset_index(drop=True)


def _build_pairwise_correlations(
    metric_history: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for geo_id, frame in metric_history.groupby("geo_id"):
        wide = frame.pivot(
            index="date",
            columns="canonical_metric_key",
            values="metric_score",
        )

        metrics = sorted(wide.columns)

        for left_index, left_metric in enumerate(metrics):
            for right_metric in metrics[left_index + 1:]:
                rows.append(
                    {
                        "geo_id": geo_id,
                        "left_metric": left_metric,
                        "right_metric": right_metric,
                        "pearson_correlation": (
                            _safe_correlation(
                                wide[left_metric],
                                wide[right_metric],
                                method="pearson",
                            )
                        ),
                        "spearman_correlation": (
                            _safe_correlation(
                                wide[left_metric],
                                wide[right_metric],
                                method="spearman",
                            )
                        ),
                    }
                )

    return pd.DataFrame(rows)


def _build_latest_panel(
    monthly_panel: pd.DataFrame,
    *,
    rows_per_geo: int = 36,
) -> pd.DataFrame:
    return (
        monthly_panel.sort_values(["geo_id", "date"])
        .groupby("geo_id", as_index=False)
        .tail(rows_per_geo)
        .reset_index(drop=True)
    )


def build_core_demand_dimension_diagnostic(
    *,
    run_id: str = BASELINE_RUN_ID,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: tuple[str, ...] = FOCUS_GEOS,
) -> dict[str, pd.DataFrame]:
    store = RegimeArtifactStore(artifact_root)

    metric_registry = _load_demand_metric_registry()
    metric_keys = set(metric_registry["canonical_metric_key"])
    feature_registry = _load_feature_registry(metric_keys)

    metric_scores = store.read_dataframe(
        run_id,
        "metric_scores",
    )
    aligned_metrics = align_metric_scores_asof(metric_scores)
    normalized_features = store.read_dataframe(
        run_id,
        "normalized_features",
    )

    metric_history = _prepare_metric_history(
        aligned_metrics,
        metric_keys=metric_keys,
        geo_ids=geo_ids,
    )
    feature_history = _prepare_feature_history(
        normalized_features,
        metric_keys=metric_keys,
        geo_ids=geo_ids,
    )
    dimension_history = _load_demand_dimension_history(
        store,
        run_id=run_id,
        geo_ids=geo_ids,
    )
    contributions = _build_metric_contributions(
        metric_history,
        metric_registry,
    )
    monthly_panel = _build_monthly_contribution_panel(
        contributions,
        dimension_history,
    )

    return {
        "metric_registry": metric_registry,
        "feature_registry": feature_registry,
        "metric_history": metric_history,
        "feature_history": feature_history,
        "dimension_history": dimension_history,
        "metric_summary": _build_metric_summary(metric_history),
        "feature_summary": _build_feature_summary(feature_history),
        "metric_contributions": contributions,
        "monthly_contribution_panel": monthly_panel,
        "contribution_summary": _build_contribution_summary(
            contributions
        ),
        "cancellation_summary": _build_cancellation_summary(
            monthly_panel
        ),
        "dominant_metric_summary": _build_dominant_metric_summary(
            contributions,
            monthly_panel,
        ),
        "pairwise_metric_correlations": _build_pairwise_correlations(
            metric_history
        ),
        "latest_monthly_panel": _build_latest_panel(
            monthly_panel
        ),
    }
