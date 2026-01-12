from __future__ import annotations

# forecast/design_matrix.py

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .backtest_utils import month_end_index, month_ends_after
from .feature_loader import FeatureSpec, TargetSpec, load_series_from_fact
from .exog_forecast import forecast_exog_seasonal_naive


# ================================================================
# Helpers
# ================================================================
def base_key_from_lagged_col(col: str) -> str:
    # "metric__geo__pt_lag12" -> "metric__geo__pt"
    if "_lag" not in col:
        return col
    return col.rsplit("_lag", 1)[0]


def specs_from_selected_base_keys(all_specs, selected_base_keys):
    out = []
    sel = set(selected_base_keys)
    for spec in all_specs:
        if spec.name in sel:
            out.append(spec)
    return out


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


def _parse_feature_col(col: str) -> Tuple[str, str, str, int]:
    """
    Parse a canonical lagged feature column:
      "{metric_id}__{geo_id}__{pt_id}_lag{L}"

    Returns: (metric_id, geo_id, pt_id, L)

    Raises ValueError if format doesn't match.
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
        lookup.loc[t] = float(val)  # allow chaining if needed

    return out


# ================================================================
# Primary Function
# ================================================================
def build_train_and_future_exog_forecasted(
    target: TargetSpec,
    feature_specs: List[FeatureSpec],
    anchor_date: pd.Timestamp,
    horizon: int,
) -> Tuple[pd.Series, pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    """
    Build:
      - y_full_raw: target on full month-end timeline
      - X_train_raw: lagged exog features aligned to y_full_raw index (NaNs allowed)
      - X_future_fc: lagged exog features for horizon months using forecasted base exog
      - test_idx_full: horizon month-end index after anchor_date
    """
    # -------------------------
    # 1) Load & normalize target (defines the timeline)
    # -------------------------
    y_raw = load_series_from_fact(
        metric_id=target.metric_id,
        geo_id=target.geo_id,
        property_type_id=target.property_type_id,
    ).copy()

    y_raw.index = month_end_index(y_raw.index)
    y_raw = y_raw[~y_raw.index.duplicated(keep="last")].sort_index()
    y_raw.name = "y"

    # -------------------------
    # 2) Load & normalize base exog series (UNLAGGED)
    # -------------------------
    base_exog: Dict[str, pd.Series] = {}
    for spec in feature_specs:
        s = load_series_from_fact(
            metric_id=spec.metric_id,
            geo_id=spec.geo_id,
            property_type_id=spec.property_type_id,
        ).copy()
        s.index = month_end_index(s.index)
        s = s[~s.index.duplicated(keep="last")].sort_index()
        base_exog[spec.name] = s

    # -------------------------
    # 3) Build TRAIN lagged features on the target timeline (no shrinking)
    # -------------------------
    # Training uses y_raw.index timeline; exog series are reindexed to it.
    df_base_train = pd.DataFrame(index=y_raw.index)
    for name, s in base_exog.items():
        df_base_train[name] = s.reindex(df_base_train.index)

    feature_cols_train = {}
    for spec in feature_specs:
        for lag in spec.lags:
            col = f"{spec.name}_lag{lag}"
            feature_cols_train[col] = df_base_train[spec.name].shift(lag)

    X_train_raw = pd.DataFrame(feature_cols_train, index=y_raw.index)

    # -------------------------
    # 4) Build FUTURE month grid and forecast UNLAGGED exog into it
    # -------------------------
    anchor_date = pd.to_datetime(anchor_date)
    anchor_date = month_end_index(pd.DatetimeIndex([anchor_date]))[0]

    test_idx_full = month_ends_after(anchor_date, horizon)

    # We need base values covering both:
    # - history up to anchor (for lagging into early future months)
    # - forecasted base values for the future months
    #
    # So create a "base panel" on (history ∪ future) and then lag it.
    idx_hist = y_raw.index[y_raw.index <= anchor_date]
    idx_all = idx_hist.union(test_idx_full)

    df_base_all = pd.DataFrame(index=idx_all)

    for spec in feature_specs:
        s_hist = base_exog[spec.name].reindex(idx_all)

        # Forecast only for future months; keep actuals where present
        s_fc = _forecast_base_seasonal_naive_else_last(
            s_base=base_exog[spec.name].loc[:anchor_date],
            idx_future=test_idx_full,
            season_lag=12,
        )

        # Combine: prefer actual values wherever present, else forecast for future
        s_comb = s_hist.copy()
        s_comb.loc[test_idx_full] = s_comb.loc[test_idx_full].combine_first(s_fc)

        df_base_all[spec.name] = s_comb

    # -------------------------
    # 5) Lag the combined base to create FUTURE lagged features
    # -------------------------
    feature_cols_all = {}
    for spec in feature_specs:
        for lag in spec.lags:
            col = f"{spec.name}_lag{lag}"
            feature_cols_all[col] = df_base_all[spec.name].shift(lag)

    X_all = pd.DataFrame(feature_cols_all, index=idx_all)
    X_future_fc = X_all.reindex(test_idx_full)

    # Return target (full), train features (full timeline), future features (horizon), and horizon index
    return y_raw, X_train_raw, X_future_fc, test_idx_full
