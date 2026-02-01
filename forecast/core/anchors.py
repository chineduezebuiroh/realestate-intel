from __future__ import annotations
from dataclasses import dataclass
import pandas as pd

@dataclass(frozen=True)
class AnchorPolicy:
    horizon_months: int
    latest_anchor_offset_months: int
    anchor_step_months: int
    max_anchors: int

def choose_anchors(y_index: pd.DatetimeIndex, *, asof: pd.Timestamp, policy: AnchorPolicy) -> list[pd.Timestamp]:
    # Canonical month-end, sorted
    idx = pd.DatetimeIndex(y_index).sort_values().unique()
    idx = idx[idx <= asof]

    # Latest anchor must allow full horizon y availability
    # anchor <= asof - offset
    latest_allowed = (asof - pd.offsets.MonthEnd(policy.latest_anchor_offset_months))

    # anchors are month-ends present in idx
    candidates = idx[idx <= latest_allowed]
    if len(candidates) == 0:
        return []

    anchors = []
    cur = candidates[-1]
    while True:
        anchors.append(cur)
        if len(anchors) >= policy.max_anchors:
            break
        cur = cur - pd.offsets.MonthEnd(policy.anchor_step_months)
        if cur < candidates[0]:
            break

    return sorted(set(anchors))
