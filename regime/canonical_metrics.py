from __future__ import annotations
# regime/canonical_metrics.py

import pandas as pd

from regime.config_loader import RegimeConfig, load_regime_config


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def resolve_canonical_metrics(
    raw: pd.DataFrame,
    config: RegimeConfig | None = None,
) -> pd.DataFrame:
    """
    Convert physical/source metric series into canonical metric series.

    Input columns:
      geo_id, date, metric_key, value

    Output columns:
      geo_id, date, canonical_metric_key, value

    Resolution rules come from metric_dimension_registry.csv:
      - canonical_metric_key
      - source_priority
      - merge_strategy
      - enabled
      - diagnostic_only

    For primary_else_fallback, lower source_priority wins.
    """
    if config is None:
        config = load_regime_config(validate=True)

    dim = config.metric_dimensions.copy()

    dim = dim[_truthy(dim["enabled"]) & ~_truthy(dim["diagnostic_only"])].copy()

    keep_cols = [
        "metric_key",
        "canonical_metric_key",
        "source_priority",
        "merge_strategy",
    ]

    dim = dim[keep_cols].drop_duplicates()
    dim["source_priority"] = pd.to_numeric(dim["source_priority"], errors="coerce").fillna(9999)

    merged = raw.merge(dim, on="metric_key", how="inner")

    if merged.empty:
        return pd.DataFrame(columns=["geo_id", "date", "canonical_metric_key", "value"])

    # Strategy v1:
    # - primary_else_fallback: pick lowest source_priority per geo/date/canonical metric
    # - direct: same behavior, but normally only one metric exists
    # - diagnostic_only rows are excluded above
    merged = merged.sort_values(
        ["geo_id", "date", "canonical_metric_key", "source_priority", "metric_key"]
    )

    resolved = (
        merged
        .drop_duplicates(
            subset=["geo_id", "date", "canonical_metric_key"],
            keep="first",
        )
        [["geo_id", "date", "canonical_metric_key", "value"]]
        .copy()
    )

    return resolved
