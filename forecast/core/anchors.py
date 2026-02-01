from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Sequence
import pandas as pd

@dataclass(frozen=True)
class AnchorPolicy:
    horizon: int
    min_train_len: int
    step_months: int
    max_anchors: int
    latest_anchor_offset_months: Optional[int] = None

def month_end_index(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    # if you already have this elsewhere, import it instead of redefining
    return pd.DatetimeIndex([pd.Timestamp(d).to_period("M").to_timestamp(how="end") for d in idx])

def choose_anchors(
    y: pd.Series,
    policy: AnchorPolicy,
    *,
    data_asof: Optional[pd.Timestamp] = None,
    anchors_csv: Optional[str] = None,
    require_full_horizon: bool = True,
) -> List[pd.Timestamp]:
    """
    Returns month-end timestamps.

    Rules:
      - Anchors are <= data_asof (if provided)
      - Need min_train_len observations before anchor
      - If require_full_horizon: must have y through anchor+horizon months
      - If anchors_csv provided: parse and validate against rules (still validate!)
    """
    y = y.dropna().copy()
    y.index = month_end_index(pd.DatetimeIndex(y.index))
    y = y[~y.index.duplicated(keep="last")].sort_index()

    if data_asof is not None:
        data_asof = pd.Timestamp(data_asof).to_period("M").to_timestamp(how="end")
        y = y.loc[:data_asof]

    if anchors_csv:
        anchors = [
            pd.Timestamp(s.strip()).to_period("M").to_timestamp(how="end")
            for s in anchors_csv.split(",")
            if s.strip()
        ]
        # validate anchors exist in y timeline
        for a in anchors:
            if a < y.index.min() or a > y.index.max():
                raise ValueError(f"[anchors] anchor {a.date()} outside y range [{y.index.min().date()}..{y.index.max().date()}]")
        # validate train len + horizon availability
        _validate_anchors(y, anchors, policy, require_full_horizon=require_full_horizon)
        return anchors

    # default behavior: generate anchors backwards from freshest eligible
    latest_offset = policy.latest_anchor_offset_months
    if latest_offset is None:
        latest_offset = policy.horizon  # selector/backtest default safety

    y_max = y.index.max()
    latest_anchor = (y_max - pd.offsets.MonthEnd(latest_offset)).to_period("M").to_timestamp(how="end")

    # walk backwards by step_months
    anchors: List[pd.Timestamp] = []
    cur = latest_anchor
    while True:
        anchors.append(cur)
        if len(anchors) >= policy.max_anchors:
            break
        cur = (cur - pd.DateOffset(months=policy.step_months)).to_period("M").to_timestamp(how="end")
        if cur < y.index.min():
            break

    anchors = sorted(set(anchors))
    _validate_anchors(y, anchors, policy, require_full_horizon=require_full_horizon)
    return anchors

def _validate_anchors(
    y: pd.Series,
    anchors: Sequence[pd.Timestamp],
    policy: AnchorPolicy,
    *,
    require_full_horizon: bool,
) -> None:
    # train len: require at least min_train_len non-null observations up to anchor
    for a in anchors:
        y_train = y.loc[:a].dropna()
        if len(y_train) < int(policy.min_train_len):
            raise ValueError(f"[anchors] anchor {a.date()} violates min_train_len={policy.min_train_len} (have={len(y_train)})")

        if require_full_horizon:
            end_needed = (a + pd.offsets.MonthEnd(policy.horizon)).to_period("M").to_timestamp(how="end")
            if end_needed > y.index.max():
                raise ValueError(
                    f"[anchors] anchor {a.date()} violates horizon={policy.horizon}: "
                    f"need through {end_needed.date()} but y_max={y.index.max().date()}"
                )
