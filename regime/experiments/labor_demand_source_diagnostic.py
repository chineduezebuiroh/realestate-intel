from __future__ import annotations
# regime/experiments/labor_demand_source_diagnostic.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore

BASELINE_RUN_ID = "macro_regime_v1_bps120_sources"
FOCUS_GEOS = (
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
)
LAUS_METRICS = (
    "employment",
    "labor_force",
    "laus_unemployment_rate",
)
ROLLING_WINDOWS = (3, 6, 12)
LAGS = (1, 3, 12)


def _safe_ratio_minus_one(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    values = (
        pd.to_numeric(numerator, errors="coerce")
        / pd.to_numeric(denominator, errors="coerce")
        - 1.0
    )
    return values.replace([np.inf, -np.inf], np.nan)


def _calendar_month_variance_share(values: pd.Series, months: pd.Series) -> float:
    work = pd.DataFrame(
        {
            "value": pd.to_numeric(values, errors="coerce"),
            "month": months,
        }
    ).dropna()
    if len(work) < 24:
        return np.nan
    total_variance = float(work["value"].var(ddof=0))
    if not np.isfinite(total_variance) or total_variance <= 0:
        return np.nan
    month_means = work.groupby("month")["value"].transform("mean")
    between_variance = float(month_means.var(ddof=0))
    return between_variance / total_variance


def _safe_corr(left: pd.Series, right: pd.Series) -> float:
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
    return float(work.iloc[:, 0].corr(work.iloc[:, 1]))


def _load_laus_sources(
    store: RegimeArtifactStore,
    *,
    run_id: str,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    source = store.read_dataframe(run_id, "source_metrics")
    required = {"geo_id", "date", "canonical_metric_key", "value"}
    missing = required - set(source.columns)
    if missing:
        raise ValueError(
            "source_metrics is missing required "
            f"columns: {sorted(missing)}"
        )

    frame = source[
        source["geo_id"].isin(geo_ids)
        & source["canonical_metric_key"].isin(LAUS_METRICS)
    ].copy()
    if frame.empty:
        raise ValueError("No LAUS source observations were found")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    invalid = frame[
        frame["date"].isna()
        | frame["value"].isna()
        | ~np.isfinite(frame["value"])
    ]
    if not invalid.empty:
        raise ValueError(
            "LAUS source observations contain invalid rows:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicates = frame.duplicated(
        subset=["geo_id", "date", "canonical_metric_key"],
        keep=False,
    )
    if duplicates.any():
        raise ValueError(
            "LAUS source observations are not unique by geo/date/metric:\n"
            + frame.loc[duplicates].head(30).to_string(index=False)
        )

    return frame.sort_values(
        ["geo_id", "canonical_metric_key", "date"]
    ).reset_index(drop=True)


def _build_source_feature_panel(source: pd.DataFrame) -> pd.DataFrame:
    panel = source.copy()
    grouped = panel.groupby(
        ["geo_id", "canonical_metric_key"],
        group_keys=False,
        sort=False,
    )

    for lag in LAGS:
        panel[f"raw_change_{lag}m"] = grouped["value"].pct_change(
            periods=lag,
            fill_method=None,
        )

    for window in ROLLING_WINDOWS:
        level_column = f"ma{window}"
        panel[level_column] = (
            grouped["value"]
            .rolling(window=window, min_periods=window)
            .mean()
            .reset_index(level=[0, 1], drop=True)
        )
        for lag in LAGS:
            reference_column = f"ma{window}_lag{lag}"
            panel[reference_column] = panel.groupby(
                ["geo_id", "canonical_metric_key"]
            )[level_column].shift(lag)
            panel[f"ma{window}_momentum_lag{lag}"] = _safe_ratio_minus_one(
                panel[level_column],
                panel[reference_column],
            )

    panel["calendar_month"] = panel["date"].dt.month
    return panel


def _load_normalized_laus_features(
    store: RegimeArtifactStore,
    *,
    run_id: str,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    normalized = store.read_dataframe(run_id, "normalized_features")
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
        "raw_feature_value",
        "feature_score",
    }
    missing = required - set(normalized.columns)
    if missing:
        raise ValueError(
            "normalized_features is missing "
            f"columns: {sorted(missing)}"
        )

    frame = normalized[
        normalized["geo_id"].isin(geo_ids)
        & normalized["feature_key"].str.startswith(
            (
                "laus_employment_",
                "laus_labor_force_",
                "laus_unemployment_rate_",
            )
        )
    ].copy()
    if frame.empty:
        raise ValueError("No normalized LAUS features were found")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["raw_feature_value"] = pd.to_numeric(
        frame["raw_feature_value"], errors="coerce"
    )
    frame["feature_score"] = pd.to_numeric(
        frame["feature_score"], errors="coerce"
    )
    frame["calendar_month"] = frame["date"].dt.month
    frame["feature_component"] = (
        frame["feature_key"].str.rsplit("_", n=1).str[-1]
    )

    grouped = frame.groupby(["geo_id", "feature_key"], group_keys=False)
    frame["raw_feature_change_1m"] = grouped["raw_feature_value"].diff()
    frame["feature_score_change_1m"] = grouped["feature_score"].diff()
    return frame


def _build_raw_summary(panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (geo_id, metric_key), frame in panel.groupby(
        ["geo_id", "canonical_metric_key"]
    ):
        rows.append(
            {
                "geo_id": geo_id,
                "canonical_metric_key": metric_key,
                "rows": len(frame),
                "first_date": frame["date"].min(),
                "last_date": frame["date"].max(),
                "mean_abs_raw_change_1m": frame["raw_change_1m"].abs().mean(),
                "p90_abs_raw_change_1m": frame["raw_change_1m"].abs().quantile(0.90),
                "mean_abs_raw_change_3m": frame["raw_change_3m"].abs().mean(),
                "mean_abs_raw_change_12m": frame["raw_change_12m"].abs().mean(),
                "raw_change_1m_calendar_month_variance_share": _calendar_month_variance_share(
                    frame["raw_change_1m"], frame["calendar_month"]
                ),
                "raw_change_3m_calendar_month_variance_share": _calendar_month_variance_share(
                    frame["raw_change_3m"], frame["calendar_month"]
                ),
                "raw_change_12m_calendar_month_variance_share": _calendar_month_variance_share(
                    frame["raw_change_12m"], frame["calendar_month"]
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_candidate_summary(panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (geo_id, metric_key), frame in panel.groupby(
        ["geo_id", "canonical_metric_key"]
    ):
        for window in ROLLING_WINDOWS:
            level_column = f"ma{window}"
            for lag in LAGS:
                candidate_column = f"ma{window}_momentum_lag{lag}"
                valid = frame[candidate_column].notna()
                rows.append(
                    {
                        "geo_id": geo_id,
                        "canonical_metric_key": metric_key,
                        "window": window,
                        "lag_periods": lag,
                        "valid_rows": int(valid.sum()),
                        "first_valid_date": frame.loc[valid, "date"].min(),
                        "last_valid_date": frame.loc[valid, "date"].max(),
                        "mean_abs_movement": frame[candidate_column].abs().mean(),
                        "p90_abs_movement": frame[candidate_column].abs().quantile(0.90),
                        "calendar_month_variance_share": _calendar_month_variance_share(
                            frame[candidate_column], frame["calendar_month"]
                        ),
                        "raw_level_correlation": _safe_corr(
                            frame["value"], frame[level_column]
                        ),
                        "raw_change_3m_correlation": _safe_corr(
                            frame["raw_change_3m"], frame[candidate_column]
                        ),
                    }
                )
    return pd.DataFrame(rows)


def _build_current_feature_summary(normalized: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (geo_id, metric_key, feature_key, component), frame in normalized.groupby(
        [
            "geo_id",
            "canonical_metric_key",
            "feature_key",
            "feature_component",
        ]
    ):
        rows.append(
            {
                "geo_id": geo_id,
                "canonical_metric_key": metric_key,
                "feature_key": feature_key,
                "feature_component": component,
                "rows": len(frame),
                "first_date": frame["date"].min(),
                "last_date": frame["date"].max(),
                "raw_feature_std": frame["raw_feature_value"].std(),
                "feature_score_std": frame["feature_score"].std(),
                "mean_abs_raw_feature_change_1m": frame[
                    "raw_feature_change_1m"
                ].abs().mean(),
                "mean_abs_feature_score_change_1m": frame[
                    "feature_score_change_1m"
                ].abs().mean(),
                "raw_feature_calendar_month_variance_share": _calendar_month_variance_share(
                    frame["raw_feature_value"], frame["calendar_month"]
                ),
                "feature_score_calendar_month_variance_share": _calendar_month_variance_share(
                    frame["feature_score"], frame["calendar_month"]
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_month_of_year_summary(panel: pd.DataFrame) -> pd.DataFrame:
    long = panel.melt(
        id_vars=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "calendar_month",
        ],
        value_vars=["raw_change_1m", "raw_change_3m", "raw_change_12m"],
        var_name="measure",
        value_name="measure_value",
    )
    return (
        long.groupby(
            [
                "geo_id",
                "canonical_metric_key",
                "measure",
                "calendar_month",
            ],
            dropna=False,
        )
        .agg(
            rows=("measure_value", "count"),
            mean_value=("measure_value", "mean"),
            median_value=("measure_value", "median"),
            positive_rate=("measure_value", lambda values: values.gt(0).mean()),
        )
        .reset_index()
    )


def build_labor_demand_source_diagnostic(
    *,
    run_id: str = BASELINE_RUN_ID,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: tuple[str, ...] = FOCUS_GEOS,
) -> dict[str, pd.DataFrame]:
    store = RegimeArtifactStore(artifact_root)
    source = _load_laus_sources(store, run_id=run_id, geo_ids=geo_ids)
    source_panel = _build_source_feature_panel(source)
    normalized = _load_normalized_laus_features(
        store, run_id=run_id, geo_ids=geo_ids
    )
    return {
        "source_panel": source_panel,
        "raw_summary": _build_raw_summary(source_panel),
        "candidate_summary": _build_candidate_summary(source_panel),
        "current_feature_history": normalized,
        "current_feature_summary": _build_current_feature_summary(normalized),
        "month_of_year_summary": _build_month_of_year_summary(source_panel),
    }
