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
    anchor_key = anchor_date.date().isoformat()
    p = (
        Path(artifact_root)
        / xgb_batch_id
        / "xgb"
        / f"selected_features__anchor={anchor_key}.parquet"
    )

    if not p.exists():
        raise FileNotFoundError(
            f"Missing XGB shortlist parquet for anchor={anchor_key}: {p}"
        )

    df = pd.read_parquet(p)

    required = {"feature_id", "rank"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Shortlist parquet missing columns {sorted(missing)}: {p}"
        )

    df = df.sort_values("rank", ascending=True).head(int(top_k))
    feats = df["feature_id"].astype(str).tolist()

    if not feats:
        raise ValueError(
            f"XGB shortlist empty after top_k={top_k} for anchor={anchor_key}: {p}"
        )

    return feats


def resolve_anchor_for_live(
    *,
    artifact_root: str,
    xgb_batch_id: str,
    preferred_anchor: pd.Timestamp,
) -> pd.Timestamp:
    """
    Live runs often occur after the last backtest anchor.
    We select the latest available anchor <= preferred_anchor.
    """
    base = Path(artifact_root) / xgb_batch_id / "xgb"
    if not base.exists():
        return preferred_anchor

    anchors = []
    for p in base.glob("selected_features__anchor=*.parquet"):
        s = p.stem.replace("selected_features__anchor=", "")
        try:
            anchors.append(pd.Timestamp(s))
        except Exception:
            continue

    if not anchors:
        return preferred_anchor

    anchors = sorted(set(anchors))
    leq = [a for a in anchors if a <= preferred_anchor]
    return leq[-1] if leq else anchors[-1]
