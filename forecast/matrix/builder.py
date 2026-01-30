from __future__ import annotations
# forecast/matrix/builder.py

from dataclasses import replace
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from forecast.core.backtest_utils import month_end_index, month_ends_after
from forecast.features.feature_loader import (
    FeatureSpec,
    TargetSpec,
    load_series_from_fact,
    load_series_from_fact_with_source,
)

from forecast.matrix.hashing import normalize_month_end_series  # small reuse


def _forecast_base_seasonal_naive_else_last(
    s_base: pd.Series,
    idx_future: pd.DatetimeIndex,
    season_lag: int = 12,
) -> pd.Series:
    """
    Seasonal naive: s[t] = s[t-season_lag] if available else last observed value.

    s_base must be month-end indexed and sorted.
    """
    s_base = s_base.dropna()
    if len(s_base) == 0:
        return pd.Series(index=idx_future, dtype=float)

    last_val = float(s_base.iloc[-1])

    # Construct output one step at a time because future depends on prior future when missing.
    out = pd.Series(index=idx_future, dtype=float)

    # We'll create a lookup that includes history + generated future
    lookup = s_base.copy()

    for t in idx_future:
        t_season = t - pd.DateOffset(months=season_lag)
        val = lookup.get(t_season, np.nan)
        if pd.isna(val):
            val = last_val
        out.loc[t] = float(val)
        lookup.loc[t] = float(val)

    return out


def build_train_and_future_exog_forecasted(
    target: TargetSpec,
    feature_specs: List[FeatureSpec],
    anchor_date,
    horizon: int,
    method: str = "seasonal_naive_else_last",
    *,
    data_asof: Optional[date] = None,
    asof_by_source: Optional[Dict[str, date]] = None,
) -> Tuple[pd.Series, pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    """
    Build:
      - y_full_raw: target on full month-end timeline
      - X_train_raw: lagged exog features aligned to y_full_raw index (NaNs allowed)
      - X_future_fc: lagged exog features for horizon months using forecasted base exog
      - test_idx_full: horizon month-end index after anchor_date
    """

    # If caller provided overrides, push them into TargetSpec
    if data_asof is not None:
        target = replace(target, data_asof=data_asof)
    if asof_by_source is not None:
        target = replace(target, asof_by_source=asof_by_source)

    # Canonicalize anchor_date to platform month-end convention
    anchor_date = pd.Timestamp(anchor_date)
    anchor_date = pd.DatetimeIndex(month_end_index(pd.DatetimeIndex([anchor_date])))[0]

    # -------------------------
    # 1) Load & normalize target (defines the timeline)
    # -------------------------
    y_raw = load_series_from_fact(
        metric_id=target.metric_id,
        geo_id=target.geo_id,
        property_type_id=target.property_type_id,
        data_asof=target.data_asof,
        asof_by_source=target.asof_by_source,
    ).copy()

    y_raw = normalize_month_end_series(y_raw)
    y_raw.name = "y"

    # -------------------------
    # 2) Load & normalize base exog series (UNLAGGED)
    # -------------------------
    base_exog: Dict[str, pd.Series] = {}
    for spec in feature_specs:
        s = load_series_from_fact_with_source(
            metric_id=spec.metric_id,
            geo_id=spec.geo_id,
            property_type_id=spec.property_type_id,
            source_id=spec.source_id,   # critical: disambiguates
            data_asof=target.data_asof,
            asof_by_source=target.asof_by_source,
        ).copy()

        s = normalize_month_end_series(s)

        # Ensure continuous month-end index within observed span (fixes alignment holes)
        obs_start = s.index.min()
        obs_end = s.index.max()
        full_obs_idx = pd.date_range(obs_start, obs_end, freq="ME")
        full_obs_idx = pd.DatetimeIndex(month_end_index(pd.DatetimeIndex(full_obs_idx)))
        s = s.reindex(full_obs_idx).ffill()

        base_exog[spec.name] = s

    # -------------------------
    # 3) Build TRAIN base exog on the target timeline (no shrinking)
    # -------------------------
    df_base_train = pd.DataFrame(
        {name: s.reindex(y_raw.index) for name, s in base_exog.items()},
        index=y_raw.index,
    )

    feature_cols_train: Dict[str, pd.Series] = {}
    for spec in feature_specs:
        for lag in spec.lags:
            col = f"{spec.name}_lag{lag}"
            feature_cols_train[col] = df_base_train[spec.name].shift(lag)

    X_train_raw = pd.DataFrame(feature_cols_train, index=y_raw.index)

    # -------------------------
    # 4) Build FUTURE base exog by forecasting UNLAGGED series (Type 2 backtest)
    # -------------------------
    test_idx_full = pd.DatetimeIndex(month_ends_after(anchor_date, horizon))
    test_idx_full = pd.DatetimeIndex(month_end_index(test_idx_full))
    test_idx_full = test_idx_full[~test_idx_full.duplicated()].sort_values()

    max_lag = max((lag for spec in feature_specs for lag in spec.lags), default=0)

    train_end = pd.Timestamp(anchor_date)
    train_idx = y_raw.index[y_raw.index <= train_end]

    base_future_idx = pd.DatetimeIndex(month_ends_after(anchor_date, horizon + max_lag))
    base_future_idx = pd.DatetimeIndex(month_end_index(base_future_idx))
    base_future_idx = base_future_idx[~base_future_idx.duplicated()].sort_values()

    full_idx = pd.DatetimeIndex(train_idx.append(base_future_idx))
    full_idx = pd.DatetimeIndex(month_end_index(full_idx))
    full_idx = full_idx[~full_idx.duplicated()].sort_values()

    # realized base series on full_idx (includes actual values in the future if they exist)
    df_base_realized = pd.DataFrame(
        {name: s.reindex(full_idx) for name, s in base_exog.items()},
        index=full_idx,
    )

    # Enforce "unknown future exog" for forecasted modes
    if method != "perfect_future":
        df_base_realized.loc[df_base_realized.index > train_end, :] = np.nan

    if method not in ("seasonal_naive_else_last", "perfect_future"):
        raise ValueError(f"Unknown exog forecast method: {method}")

    if method == "perfect_future":
        df_base_future = df_base_realized
    else:
        base_exog_fc: Dict[str, pd.Series] = {}

        for name, s in base_exog.items():
            s_full = s.reindex(full_idx)

            # ensure defined through anchor within TRAIN window
            train_mask = (s_full.index <= train_end)
            if train_mask.any():
                s_train = s_full.loc[train_mask]
                if s_train.isna().any():
                    s_full.loc[train_mask] = s_train.ffill()

            # fill only on the future horizon months (but the index includes +max_lag)
            for t in test_idx_full:
                if pd.notna(s_full.loc[t]):
                    continue

                t12 = pd.Timestamp(t) - pd.DateOffset(months=12)
                t12 = pd.DatetimeIndex(month_end_index(pd.DatetimeIndex([t12])))[0]
                if t12 in s_full.index and pd.notna(s_full.loc[t12]):
                    s_full.loc[t] = s_full.loc[t12]
                else:
                    prev = s_full.loc[:t].dropna()
                    if len(prev) > 0:
                        s_full.loc[t] = prev.iloc[-1]

            base_exog_fc[name] = s_full

        df_base_future = pd.DataFrame(
            {name: s.reindex(full_idx) for name, s in base_exog_fc.items()},
            index=full_idx,
        )

    # -------------------------
    # 5) Build FUTURE lagged features from the base exog (forecasted or perfect_future)
    # -------------------------
    feature_cols_future: Dict[str, pd.Series] = {}
    for spec in feature_specs:
        for lag in spec.lags:
            col = f"{spec.name}_lag{lag}"
            feature_cols_future[col] = df_base_future[spec.name].shift(lag)

    X_full_future = pd.DataFrame(feature_cols_future, index=full_idx)
    X_future_fc = X_full_future.reindex(test_idx_full)

    return y_raw, X_train_raw, X_future_fc, test_idx_full
