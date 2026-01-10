# forecast/backtest_utils.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import pandas as pd


def _month_end(ts: pd.Timestamp) -> pd.Timestamp:
    """Force timestamp to month-end (consistent with your series)."""
    p = ts.to_period("M")
    return p.to_timestamp(how="end")


def choose_anchor_dates(
    y: pd.Series,
    horizon: int,
    min_train_len: int = 60,
    step_months: int = 12,
    max_anchors: int = 3,
    latest_anchor_offset_months: Optional[int] = None,
) -> List[pd.Timestamp]:
    """
    Choose backtest anchors by calendar date (not by index).

    - last_date = max(y.index)
    - latest anchor defaults to (last_date - horizon months)
      OR you can override with latest_anchor_offset_months (e.g. 12 months)
    - then step backwards by step_months for additional anchors

    Ensures:
      - anchor exists on/within y index (snaps to month-end)
      - at least min_train_len observations exist up to anchor
    """
    if y.empty:
        return []

    y_idx = pd.DatetimeIndex(y.index).sort_values()
    last_date = _month_end(pd.Timestamp(y_idx.max()))

    if latest_anchor_offset_months is not None:
        latest_anchor = _month_end(last_date - pd.DateOffset(months=latest_anchor_offset_months))
    else:
        latest_anchor = _month_end(last_date - pd.DateOffset(months=horizon))

    anchors: List[pd.Timestamp] = []
    anchor = latest_anchor

    # precompute for fast train-length check
    # (count obs up to date)
    y_df = pd.DataFrame({"y": y.values}, index=y_idx)

    while len(anchors) < max_anchors:
        # stop if anchor before series start
        if anchor < _month_end(pd.Timestamp(y_idx.min())):
            break

        # ensure at least min_train_len observations up to anchor
        n_train = int((y_df.loc[:anchor]).shape[0])
        if n_train >= min_train_len:
            anchors.append(anchor)

        anchor = _month_end(anchor - pd.DateOffset(months=step_months))

    anchors = sorted(set(anchors))
    return anchors
