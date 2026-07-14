from __future__ import annotations
# regime/experiments/price_family_source_diagnostic.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore

DEFAULT_RUN_ID = "macro_regime_v1_bps120_sources"
FOCUS_GEOS = (
    "alameda_county_ca__county",
    "district_of_columbia_dc__county",
)
PRICE_FAMILY_METRICS = (
    "median_sale_price",
    "median_ppsf",
    "price_to_income",
    "payment_burden",
)
ROLLING_WINDOWS = (3, 6, 9, 12)
LAGS = (1, 3, 12)


def _safe_corr(left: pd.Series, right: pd.Series, method: str) -> float:
    work = pd.concat(
        [
            pd.to_numeric(left, errors="coerce"),
            pd.to_numeric(right, errors="coerce"),
        ],
        axis=1,
    ).dropna()
    if len(work) < 3 or work.iloc[:, 0].nunique() <= 1 or work.iloc[:, 1].nunique() <= 1:
        return np.nan
    return float(work.iloc[:, 0].corr(work.iloc[:, 1], method=method))


def _calendar_month_variance_share(values: pd.Series, months: pd.Series) -> float:
    work = pd.DataFrame(
        {
            "value": pd.to_numeric(values, errors="coerce"),
            "month": months,
        }
    ).dropna()
    if len(work) < 24:
        return np.nan
    total = float(work["value"].var(ddof=0))
    if not np.isfinite(total) or total <= 0:
        return np.nan
    between = float(
        work.groupby("month")["value"].transform("mean").var(ddof=0)
    )
    return between / total


def _trend_stats(values: pd.Series) -> tuple[float, float]:
    y = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=float)
    if len(y) < 12:
        return np.nan, np.nan
    x = np.arange(len(y), dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    fitted = slope * x + intercept
    total_ss = float(np.sum((y - y.mean()) ** 2))
    residual_ss = float(np.sum((y - fitted) ** 2))
    r_squared = np.nan if total_ss <= 0 else 1.0 - residual_ss / total_ss
    return float(slope), float(r_squared)


def _load_source(
    *,
    run_id: str,
    artifact_root: str | Path,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    store = RegimeArtifactStore(artifact_root)
    source = store.read_dataframe(run_id, "source_metrics")
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
        "metric_origin",
    }
    missing = required - set(source.columns)
    if missing:
        raise ValueError(f"source_metrics missing columns: {sorted(missing)}")

    frame = source[
        source["geo_id"].isin(geo_ids)
        & source["canonical_metric_key"].isin(PRICE_FAMILY_METRICS)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "value",
            "metric_origin",
        ]
    ].copy()

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")

    invalid = frame[
        frame["date"].isna()
        | frame["value"].isna()
        | ~np.isfinite(frame["value"])
    ]
    if not invalid.empty:
        raise ValueError(
            "Invalid price-family source rows:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicates = frame.duplicated(
        ["geo_id", "date", "canonical_metric_key"],
        keep=False,
    )
    if duplicates.any():
        raise ValueError(
            "Duplicate price-family source rows:\n"
            + frame.loc[duplicates].head(30).to_string(index=False)
        )

    actual = set(frame["canonical_metric_key"].unique())
    expected = set(PRICE_FAMILY_METRICS)
    if actual != expected:
        raise AssertionError(
            f"Expected metrics {sorted(expected)}, found {sorted(actual)}"
        )

    return frame.sort_values(
        ["geo_id", "canonical_metric_key", "date"]
    ).reset_index(drop=True)


def _build_panel(source: pd.DataFrame) -> pd.DataFrame:
    panel = source.copy()
    keys = ["geo_id", "canonical_metric_key"]
    grouped = panel.groupby(keys, group_keys=False)

    for lag in LAGS:
        panel[f"raw_change_{lag}m"] = grouped["value"].pct_change(
            periods=lag,
            fill_method=None,
        )

    panel["log_value"] = np.log(panel["value"].where(panel["value"] > 0))

    for window in ROLLING_WINDOWS:
        rolling = grouped["value"].rolling(
            window=window,
            min_periods=window,
        ).mean()
        column = f"ma{window}"
        panel[column] = rolling.reset_index(
            level=[0, 1],
            drop=True,
        )
        ma_grouped = panel.groupby(keys, group_keys=False)
        for lag in LAGS:
            panel[f"{column}_change_{lag}m"] = ma_grouped[column].pct_change(
                periods=lag,
                fill_method=None,
            )

    panel["calendar_month"] = panel["date"].dt.month
    return panel


def _source_summary(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (geo_id, metric), frame in panel.groupby(
        ["geo_id", "canonical_metric_key"]
    ):
        frame = frame.sort_values("date")
        slope, r_squared = _trend_stats(frame["log_value"])
        row = {
            "geo_id": geo_id,
            "canonical_metric_key": metric,
            "rows": len(frame),
            "first_date": frame["date"].min(),
            "last_date": frame["date"].max(),
            "first_value": frame["value"].iloc[0],
            "last_value": frame["value"].iloc[-1],
            "total_change": frame["value"].iloc[-1] / frame["value"].iloc[0] - 1.0,
            "log_trend_slope_per_month": slope,
            "log_trend_r_squared": r_squared,
        }
        for lag in LAGS:
            change = frame[f"raw_change_{lag}m"]
            row[f"mean_abs_raw_change_{lag}m"] = change.abs().mean()
            row[f"p90_abs_raw_change_{lag}m"] = change.abs().quantile(0.90)
            row[f"raw_change_{lag}m_calendar_variance_share"] = (
                _calendar_month_variance_share(change, frame["calendar_month"])
            )
        row["level_calendar_variance_share"] = _calendar_month_variance_share(
            frame["value"],
            frame["calendar_month"],
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _window_summary(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (geo_id, metric), frame in panel.groupby(
        ["geo_id", "canonical_metric_key"]
    ):
        for window in ROLLING_WINDOWS:
            level = f"ma{window}"
            for lag in LAGS:
                change = f"{level}_change_{lag}m"
                rows.append(
                    {
                        "geo_id": geo_id,
                        "canonical_metric_key": metric,
                        "window": window,
                        "lag_months": lag,
                        "valid_rows": int(frame[change].notna().sum()),
                        "raw_level_pearson": _safe_corr(
                            frame["value"], frame[level], "pearson"
                        ),
                        "raw_level_spearman": _safe_corr(
                            frame["value"], frame[level], "spearman"
                        ),
                        "mean_abs_change": frame[change].abs().mean(),
                        "p90_abs_change": frame[change].abs().quantile(0.90),
                        "calendar_month_variance_share": (
                            _calendar_month_variance_share(
                                frame[change],
                                frame["calendar_month"],
                            )
                        ),
                    }
                )
    return pd.DataFrame(rows)


def _cross_metric_correlations(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    measures = ["value"] + [f"raw_change_{lag}m" for lag in LAGS]
    for geo_id, geo_frame in panel.groupby("geo_id"):
        for measure in measures:
            wide = geo_frame.pivot(
                index="date",
                columns="canonical_metric_key",
                values=measure,
            )
            metrics = sorted(wide.columns)
            for i, left in enumerate(metrics):
                for right in metrics[i + 1 :]:
                    rows.append(
                        {
                            "geo_id": geo_id,
                            "measure": measure,
                            "left_metric": left,
                            "right_metric": right,
                            "pearson_correlation": _safe_corr(
                                wide[left], wide[right], "pearson"
                            ),
                            "spearman_correlation": _safe_corr(
                                wide[left], wide[right], "spearman"
                            ),
                        }
                    )
    return pd.DataFrame(rows)


def _latest_panel(panel: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
        "raw_change_1m",
        "raw_change_3m",
        "raw_change_12m",
        "ma3",
        "ma6",
        "ma9",
        "ma12",
    ]
    return (
        panel[columns]
        .sort_values(["geo_id", "canonical_metric_key", "date"])
        .groupby(["geo_id", "canonical_metric_key"], as_index=False)
        .tail(12)
        .reset_index(drop=True)
    )


def build_price_family_source_diagnostic(
    *,
    run_id: str = DEFAULT_RUN_ID,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: tuple[str, ...] = FOCUS_GEOS,
) -> dict[str, pd.DataFrame]:
    source = _load_source(
        run_id=run_id,
        artifact_root=artifact_root,
        geo_ids=geo_ids,
    )
    panel = _build_panel(source)
    return {
        "source_panel": panel,
        "source_summary": _source_summary(panel),
        "window_summary": _window_summary(panel),
        "cross_metric_correlations": _cross_metric_correlations(panel),
        "latest_panel": _latest_panel(panel),
    }
