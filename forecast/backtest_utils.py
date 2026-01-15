from __future__ import annotations
# forecast/backtest_utils.py

#from dataclasses import dataclass
from typing import List, Optional
import pandas as pd
from pandas.tseries.offsets import MonthEnd


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


def month_end_index(idx) -> pd.DatetimeIndex:
    """Convert any datetime-like index to month-end DatetimeIndex."""
    return pd.DatetimeIndex([_month_end(x) for x in idx])


def month_ends_after(anchor: pd.Timestamp, steps: int) -> pd.DatetimeIndex:
    # next month-end after anchor, steps times
    start = (pd.Timestamp(anchor) + MonthEnd(1))
    return pd.date_range(start=start, periods=steps, freq="ME")


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

    print(f"[anchors] y_min={min_date.date()} y_max={last_date.date()} offset={offset} first_anchor={anchor.date()}")

    anchors: List[pd.Timestamp] = []

    # train-length check uses counts up to anchor
    y_df = pd.DataFrame({"y": y.values}, index=idx)

    while anchor >= min_date and len(anchors) < max_anchors:
        n_train = int(y_df.loc[:anchor].shape[0])
        if n_train >= min_train_len:
            anchors.append(anchor)
        anchor = _month_end(anchor - pd.DateOffset(months=int(step_months)))

    # de-dupe + sorted
    return sorted(anchors)
