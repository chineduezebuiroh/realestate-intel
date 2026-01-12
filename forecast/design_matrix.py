from __future__ import annotations

# forecast/design_matrix.py

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .backtest_utils import month_end_index, month_ends_after
from .feature_loader import FeatureSpec, TargetSpec, load_series_from_fact
from .exog_forecast import forecast_exog_seasonal_naive

def _normalize_me(s: pd.Series) -> pd.Series:
    s = s.copy()
    s.index = month_end_index(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s

def _parse_base_key_from_spec_name(spec_name: str) -> Tuple[str, str, str]:
    # spec.name is built as f"{metric_id}__{geo_id}__{pt_id}"
    parts = spec_name.split("__")
    if len(parts) != 3:
        raise ValueError(f"Bad FeatureSpec.name format: {spec_name}")
    return parts[0], parts[1], parts[2]

def _base_key(metric_id: str, geo_id: str, pt_id: str) -> str:
    return f"{metric_id}__{geo_id}__{pt_id}"

def _lagged_col_name(base_key: str, lag: int) -> str:
    # IMPORTANT: single lag suffix, never double lag.
    return f"{base_key}_lag{lag}"

def build_lagged_X_from_base(
    base_df: pd.DataFrame,
    feature_specs: List[FeatureSpec],
) -> pd.DataFrame:
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
            cols[_lagged_col_name(basek, lag)] = base_df[basek].shift(lag)
    return pd.DataFrame(cols, index=base_df.index)

def build_train_and_future_exog_forecasted(
    target: TargetSpec,
    feature_specs: List[FeatureSpec],
    anchor_date: pd.Timestamp,
    horizon: int,
) -> Tuple[pd.Series, pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    """
    Forecasted-exog mode:
      - Loads base exog series through anchor from facts.
      - Forecasts base exog through horizon using seasonal naive.
      - Builds lagged X for BOTH train and future from concatenated base.
    Returns:
      y_train_raw (month-end, through anchor, may contain NaNs)
      X_train (lagged, aligned to y timeline)
      X_future (lagged, index = horizon month-ends after anchor)
      test_idx (horizon month-ends)
    """
    anchor_date = pd.Timestamp(anchor_date)
    test_idx = month_ends_after(anchor_date, horizon)  # month-end timestamps

    # 1) Load y (target defines truth timeline, but we only need train y values)
    y_raw = _normalize_me(load_series_from_fact(
        metric_id=target.metric_id,
        geo_id=target.geo_id,
        property_type_id=target.property_type_id,
    ))
    y_train_raw = y_raw.loc[:anchor_date]

    # 2) Load base exog series (through anchor), normalize
    base_hist: Dict[str, pd.Series] = {}
    for spec in feature_specs:
        metric_id, geo_id, pt_id = _parse_base_key_from_spec_name(spec.name)
        s = _normalize_me(load_series_from_fact(metric_id, geo_id, pt_id)).loc[:anchor_date]
        s.name = spec.name
        base_hist[spec.name] = s

    # 3) Forecast base exog into horizon
    base_fc = {}
    for k, s_hist in base_hist.items():
        base_fc[k] = forecast_exog_seasonal_naive(s_hist, horizon_idx=test_idx, season=12)

    # 4) Build a combined base_df over train+future index
    # Use y_train timeline union horizon so lags can “reach” into train portion.
    base_index = y_train_raw.index.union(test_idx).sort_values()
    base_df = pd.DataFrame(index=base_index)

    for k, s_hist in base_hist.items():
        base_df[k] = s_hist.reindex(base_index)
        # fill the future part with forecast
        base_df.loc[test_idx, k] = base_fc[k].reindex(test_idx)

    # 5) Build lagged X
    X_all = build_lagged_X_from_base(base_df, feature_specs)

    # 6) Split train/future portions
    X_train = X_all.loc[:anchor_date]
    X_future = X_all.loc[test_idx]

    return y_train_raw, X_train, X_future, test_idx
