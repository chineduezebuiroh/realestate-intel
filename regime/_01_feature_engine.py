from __future__ import annotations
# regime/_01_feature_engine.py

from pathlib import Path

import duckdb
import pandas as pd

from regime._00_config_loader import RegimeConfig, load_regime_config
from regime.derived_metrics import build_derived_metrics
from regime.canonical_metrics import resolve_canonical_metrics


SERVING_DB = Path("data/market_serving.duckdb")


def _zscore(s: pd.Series, min_obs: int = 12) -> pd.Series:
    expanding_mean = s.expanding(min_periods=min_obs).mean()
    expanding_std = s.expanding(min_periods=min_obs).std()
    return ((s - expanding_mean) / expanding_std).clip(-3, 3) / 3


def _window_to_periods(feature_window: str, default: int) -> int:
    value = str(feature_window or "").strip().lower()

    if not value:
        return default

    if value.endswith(("m", "q", "y")):
        value = value[:-1]

    try:
        periods = int(value)
    except ValueError:
        return default

    return max(periods, 1)


def _compute_feature(
    group: pd.DataFrame,
    transform: str,
    feature_window: str = "",
) -> pd.Series:
    group = group.sort_values("date")
    value = group["value"].astype(float)

    if transform == "level_zscore":
        return value

    if transform == "mom_zscore":
        periods = _window_to_periods(feature_window, default=1)
        return value.pct_change(periods)

    if transform == "qoq_zscore":
        periods = _window_to_periods(feature_window, default=1)
        return value.pct_change(periods)

    if transform == "yoy_zscore":
        periods = _window_to_periods(feature_window, default=12)
        return value.pct_change(periods)

    if transform == "rolling_yoy_zscore":
        periods = _window_to_periods(feature_window, default=3)
        return value.pct_change(periods)

    if transform == "none":
        return value

    raise ValueError(f"Unsupported transform: {transform}")


def load_raw_metric_series(
    config: RegimeConfig,
    db_path: str | Path = SERVING_DB,
) -> pd.DataFrame:
    source_metrics = config.source_metrics[["metric_key", "source_id", "metric_id"]]

    db_path = Path(db_path)
    
    if not db_path.is_file():
        raise FileNotFoundError(f"Serving database not found: {db_path}")
    
    con = duckdb.connect(str(db_path), read_only=True)

    facts = con.execute("""
        SELECT geo_id, date, source_id, metric_id, value
        FROM fact_timeseries
        WHERE value IS NOT NULL
    """).fetchdf()
    con.close()

    facts["date"] = pd.to_datetime(facts["date"])

    raw = facts.merge(
        source_metrics,
        on=["source_id", "metric_id"],
        how="inner",
    )

    return raw[["geo_id", "date", "metric_key", "value"]]


def build_feature_matrix(
    config: RegimeConfig | None = None,
    db_path: str | Path = SERVING_DB,
) -> pd.DataFrame:
    if config is None:
        config = load_regime_config(validate=True)

    raw_source = load_raw_metric_series(
        config,
        db_path=db_path,
    )
    raw = resolve_canonical_metrics(raw_source, config)
    derived = build_derived_metrics(raw)

    if not derived.empty:
        raw = pd.concat([raw, derived], ignore_index=True)

    feature_defs = (
        config.features
        .merge(
            config.metric_dimensions[["metric_key", "canonical_metric_key"]],
            on="metric_key",
            how="left",
        )
        .drop_duplicates(
            subset=[
                "canonical_metric_key",
                "feature_type",
                "transform",
                "feature_window",
                "dimension_context",
            ]
        )
    )

    rows = []
    for _, f in feature_defs.iterrows():
        metric_key = f["canonical_metric_key"]
        feature_key = f["feature_key"]
        transform = f["transform"]

        metric_df = raw[raw["canonical_metric_key"] == metric_key].copy()
        if metric_df.empty:
            continue

        metric_df["feature_key"] = feature_key
        metric_df["transform"] = transform

        metric_df = metric_df.sort_values(["geo_id", "canonical_metric_key", "date"]).copy()

        feature_window = f.get("feature_window", "")
        
        metric_df["raw_feature_value"] = (
            metric_df
            .groupby(["geo_id", "canonical_metric_key"], group_keys=False)["value"]
            .transform(
                lambda s: _compute_feature(
                    pd.DataFrame({
                        "date": metric_df.loc[s.index, "date"],
                        "value": s,
                    }),
                    transform,
                    feature_window,
                )
            )
        )

        rows.append(
            metric_df[["geo_id", "date", "canonical_metric_key", "feature_key", "raw_feature_value"]]
        )

    if not rows:
        return pd.DataFrame(
            columns=["geo_id", "date", "canonical_metric_key", "feature_key", "raw_feature_value"]
        )

    out = pd.concat(rows, ignore_index=True)
    out = out.dropna(subset=["raw_feature_value"])

    return out
