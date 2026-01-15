from __future__ import annotations
# forecast/xgb_shortlist.py

from pathlib import Path
import pandas as pd
from typing import List, Optional


def load_xgb_selected_feature_ids(
    *,
    artifact_root: str,
    xgb_batch_id: str,
    anchor_date: pd.Timestamp,
    top_k: int,
) -> List[str]:
    """
    EXACT copy of your current _load_xgb_selected_feature_ids implementation,
    just moved to a shared module so live + backtest can reuse it.
    """
    # >>> PASTE YOUR CURRENT _load_xgb_selected_feature_ids BODY HERE <<<
    raise NotImplementedError("Paste the existing implementation here.")


def resolve_anchor_for_live(
    *,
    artifact_root: str,
    xgb_batch_id: str,
    preferred_anchor: pd.Timestamp,
) -> pd.Timestamp:
    """
    Live runs often won't have an exact anchor artifact for preferred_anchor.
    We pick the latest available anchor on disk <= preferred_anchor if possible,
    else the latest available anchor overall.

    This prevents 'file not found' + avoids hallucinating a shortlist.
    """
    base = Path(artifact_root) / xgb_batch_id

    # IMPORTANT:
    # This depends on how your backtest wrote files. You already have it working in backtests,
    # so match that directory + filename convention here.
    #
    # If your _load_xgb_selected_feature_ids reads from a specific folder, point to that same folder.
    shortlist_dir = base  # <- CHANGE THIS to the actual shortlist directory used by your loader

    if not shortlist_dir.exists():
        return preferred_anchor

    # Heuristic: scan for dates in filenames like YYYY-MM-DD
    candidates: List[pd.Timestamp] = []
    for p in shortlist_dir.rglob("*"):
        if not p.is_file():
            continue
        s = p.stem
        try:
            dt = pd.Timestamp(s)
            candidates.append(dt)
        except Exception:
            continue

    if not candidates:
        return preferred_anchor

    candidates = sorted(set(candidates))
    leq = [d for d in candidates if d <= preferred_anchor]
    return leq[-1] if leq else candidates[-1]
