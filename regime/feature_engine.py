from __future__ import annotations
# regime/feature_engine.py

from pathlib import Path

import duckdb
import pandas as pd

from regime.config_loader import RegimeConfig, load_regime_config
from regime.derived_metrics import build_derived_metrics


SERVING_DB = Path("data/market_serving.duckdb")


def _zscore(s: pd.Series, min_obs: int = 12) -> pd.Series:
    expanding_mean = s.expanding(min_periods=min_obs).mean()
    expanding_std = s.expanding(min_periods=min_obs).std()
    return ((s - expanding_mean) / expanding_std).clip(-3, 3) / 3


def _compute_feature(group: pd.DataFrame, transform: str) -> pd.Series:
    group = group.sort_values("date")
    value = group["value"].astype(float)

    if transform == "level_zscore":
        return _zscore(value)

    if transform == "mom_zscore":
        return _zscore(value.pct_change(1))

    if transform == "qoq_zscore":
        return _zscore(value.pct_change(1))

    if transform == "yoy_zscore":
        return _zscore(value.pct_change(12))

    if transform == "rolling_yoy_zscore":
        return _zscore(value.pct_change(3))

    if transform == "none":
        return value

    raise ValueError(f"Unsupported transform: {transform}")


def load_raw_metric_series(config: RegimeConfig) -> pd.DataFrame:
    source_metrics = config.source_metrics[["metric_key", "source_id", "metric_id"]]

    con = duckdb.connect(str(SERVING_DB))
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


def build_feature_matrix(config: RegimeConfig | None = None) -> pd.DataFrame:
    if config is None:
        config = load_regime_config(validate=True)

    raw = load_raw_metric_series(config)
    derived = build_derived_metrics(raw)

    if not derived.empty:
        raw = pd.concat([raw, derived], ignore_index=True)

    feature_defs = config.features.copy()

    rows = []
    for _, f in feature_defs.iterrows():
        metric_key = f["metric_key"]
        feature_key = f["feature_key"]
        transform = f["transform"]

        metric_df = raw[raw["metric_key"] == metric_key].copy()
        if metric_df.empty:
            continue

        metric_df["feature_key"] = feature_key
        metric_df["transform"] = transform

        metric_df = metric_df.sort_values(["geo_id", "metric_key", "date"]).copy()
        
        metric_df["feature_value"] = (
            metric_df
            .groupby(["geo_id", "metric_key"], group_keys=False)["value"]
            .transform(lambda s: _compute_feature(pd.DataFrame({"date": metric_df.loc[s.index, "date"], "value": s}), transform))
        )

        rows.append(
            metric_df[["geo_id", "date", "metric_key", "feature_key", "feature_value"]]
        )

    if not rows:
        return pd.DataFrame(
            columns=["geo_id", "date", "metric_key", "feature_key", "feature_value"]
        )

    out = pd.concat(rows, ignore_index=True)
    out = out.dropna(subset=["feature_value"])

    return out
