from __future__ import annotations
# forecast/backtest_utils.py

#from dataclasses import dataclass
from typing import List, Optional
import pandas as pd


# ========================================================
# Constants
# ========================================================
DEFAULT_MIN_TRAIN_LEN = 72          # your chosen standard
DEFAULT_ANCHOR_STEP_MONTHS = 12
DEFAULT_MAX_ANCHORS = 4
DEFAULT_ANCHOR_BUFFER_MONTHS = 12   # extra slack beyond horizon


def _month_end(ts: pd.Timestamp) -> pd.Timestamp:
    """Force timestamp to month-end (consistent with your series)."""
    p = pd.Timestamp(ts).to_period("M")
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
    Date-based anchor selection.

    Default behavior:
      latest anchor = last_date - horizon months
      then step back by step_months

    If latest_anchor_offset_months is set:
      latest anchor = last_date - latest_anchor_offset_months
    """
    if y is None or len(y) == 0:
        return []

    idx = pd.DatetimeIndex(y.index).sort_values()
    last_date = _month_end(idx.max())

    if latest_anchor_offset_months is not None:
        anchor = _month_end(last_date - pd.DateOffset(months=latest_anchor_offset_months))
    else:
        anchor = _month_end(last_date - pd.DateOffset(months=horizon))

    anchors: List[pd.Timestamp] = []
    min_date = _month_end(idx.min())

    # fast train length check
    y_df = pd.DataFrame({"y": y.values}, index=idx)

    while anchor >= min_date and len(anchors) < max_anchors:
        n_train = int(y_df.loc[:anchor].shape[0])
        if n_train >= min_train_len:
            anchors.append(anchor)
        anchor = _month_end(anchor - pd.DateOffset(months=step_months))

    return sorted(set(anchors))
