from __future__ import annotations
# forecast/matrix/hashing.py

from typing import List, Tuple

import pandas as pd

from forecast.core.backtest_utils import month_end_index
from forecast.features.feature_loader import FeatureSpec


def base_key_from_lagged_col(col: str) -> str:
    # "metric__geo__pt_lag12" -> "metric__geo__pt"
    if "_lag" not in col:
        return col
    return col.rsplit("_lag", 1)[0]


def specs_from_selected_base_keys(all_specs: List[FeatureSpec], selected_base_keys: List[str]) -> List[FeatureSpec]:
    sel = set(selected_base_keys)
    return [spec for spec in all_specs if spec.name in sel]


def normalize_month_end_series(s: pd.Series) -> pd.Series:
    """
    Standard month-end normalization:
      - month-end index
      - drop duplicate months (keep last)
      - sort
    """
    s = s.copy()
    s.index = month_end_index(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def parse_base_key_from_spec_name(spec_name: str) -> Tuple[str, str, str]:
    """
    spec.name is built as f"{metric_id}__{geo_id}__{pt_id}"
    """
    parts = spec_name.split("__")
    if len(parts) != 3:
        raise ValueError(f"Bad FeatureSpec.name format: {spec_name}")
    return parts[0], parts[1], parts[2]


def base_key(metric_id: str, geo_id: str, pt_id: str) -> str:
    return f"{metric_id}__{geo_id}__{pt_id}"


def lagged_col_name(base_key_str: str, lag: int) -> str:
    # IMPORTANT: single lag suffix, never double lag.
    return f"{base_key_str}_lag{lag}"


def build_lagged_X_from_base(base_df: pd.DataFrame, feature_specs: List[FeatureSpec]) -> pd.DataFrame:
    """
    base_df columns are base series keyed as "{metric}__{geo}__{pt}".
    Returns lagged feature DataFrame with columns "{base}_lag{lag}".
    """
    cols = {}
    for spec in feature_specs:
        basek = spec.name  # already metric__geo__pt
        if basek not in base_df.columns:
            raise KeyError(f"Missing base series in base_df: {basek}")
        for lag in spec.lags:
            cols[lagged_col_name(basek, lag)] = base_df[basek].shift(lag)
    return pd.DataFrame(cols, index=base_df.index)


def parse_feature_col(col: str) -> Tuple[str, str, str, int]:
    """
    Parse a canonical lagged feature column:
      "{metric_id}__{geo_id}__{pt_id}_lag{L}"

    Returns: (metric_id, geo_id, pt_id, L)
    """
    if "_lag" not in col:
        raise ValueError(f"Not a lagged feature column: {col}")

    base, lag_part = col.rsplit("_lag", 1)
    lag = int(lag_part)

    parts = base.split("__")
    if len(parts) != 3:
        raise ValueError(f"Bad feature base format (expected 3 parts): {col}")

    metric_id, geo_id, pt_id = parts
    return metric_id, geo_id, pt_id, lag
