from __future__ import annotations
# forecast/backtest_utils.py

from typing import List, Optional
import pandas as pd
from pandas.tseries.offsets import MonthEnd

from forecast.core.anchors import AnchorPolicy, choose_anchors, month_end_index

# ========================================================
# Constants
# ========================================================
DEFAULT_MIN_TRAIN_LEN = 72          # your chosen standard
DEFAULT_ANCHOR_STEP_MONTHS = 12
DEFAULT_MAX_ANCHORS = 4
DEFAULT_ANCHOR_BUFFER_MONTHS = 12   # extra slack beyond horizon


def _month_end(ts: pd.Timestamp) -> pd.Timestamp: #<-- Delete??
    """Force timestamp to month-end (consistent with your series)."""
    p = pd.Timestamp(ts).to_period("M")
    return p.to_timestamp(how="end")


def month_ends_after(anchor: pd.Timestamp, steps: int) -> pd.DatetimeIndex:
    anchor = pd.Timestamp(anchor).to_period("M").to_timestamp(how="end")
    start = anchor + MonthEnd(1)
    return pd.date_range(start=start, periods=steps, freq="ME")

"""
def choose_anchor_dates(
    y: pd.Series,
    horizon: int,
    min_train_len: int = 60,
    step_months: int = 12,
    max_anchors: int = 3,
    latest_anchor_offset_months: Optional[int] = None,
) -> List[pd.Timestamp]:
"""
"""
    Date-based anchor selection.

    Default behavior:
      latest anchor = last_date - horizon months
      then step back by step_months

    If latest_anchor_offset_months is set:
      latest anchor = last_date - latest_anchor_offset_months
"""
"""
    if y is None or len(y) == 0:
        return []

    # normalize index to month-end
    y2 = y.copy()
    y2.index = pd.DatetimeIndex(y2.index)
    y2.index = month_end_index(y2.index)          # your canonical function
    y2 = y2[~y2.index.duplicated(keep="last")].sort_index()
    
    idx = pd.DatetimeIndex(y2.index)
    last_date = _month_end(idx.max())
    min_date = _month_end(idx.min())

    y_df = pd.DataFrame({"y": y2.values}, index=idx)


    # ALWAYS define anchor
    offset = latest_anchor_offset_months if latest_anchor_offset_months is not None else horizon
    anchor = _month_end(last_date - pd.DateOffset(months=int(offset)))
    # If anchor isn't in the timeline, move to the nearest prior available month in idx
    if anchor not in idx:
        prior = idx[idx <= anchor]
        if len(prior) == 0:
            return []
        anchor = _month_end(prior.max())


    print(f"[anchors] y_min={min_date.date()} y_max={last_date.date()} offset={offset} first_anchor={anchor.date()}")

    anchors: List[pd.Timestamp] = []


    while anchor >= min_date and len(anchors) < max_anchors:
        n_train = int(y_df.loc[:anchor, "y"].dropna().shape[0])
        if n_train >= min_train_len:
            anchors.append(anchor)
        anchor = _month_end(anchor - pd.DateOffset(months=int(step_months)))

    # de-dupe + sorted
    anchors = sorted(set(anchors))
    return anchors[-max_anchors:]  # ascending, keep newest max_anchors
"""

def choose_anchor_dates(
    y: pd.Series,
    horizon: int,
    min_train_len: int = 60,
    step_months: int = 12,
    max_anchors: int = 3,
    latest_anchor_offset_months: Optional[int] = None,
) -> List[pd.Timestamp]:
    """
    Back-compat shim. Prefer forecast.core.anchors.choose_anchors().
    """
    if y is None or len(y) == 0:
        return []

    policy = AnchorPolicy(
        horizon=int(horizon),
        min_train_len=int(min_train_len),
        step_months=int(step_months),
        max_anchors=int(max_anchors),
        latest_anchor_offset_months=int(latest_anchor_offset_months) if latest_anchor_offset_months is not None else None,
    )
    # This is “historical behavior”: anchors must have full horizon available.
    return choose_anchors(y, policy, require_full_horizon=True)
