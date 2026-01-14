from __future__ import annotations

# forecast/feature_selection.py

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from .feature_loader import FeatureSpec, TargetSpec, load_series_from_fact
from .backtest_utils import month_end_index


@dataclass(frozen=True)
class ScoredCandidate:
    spec: FeatureSpec
    score: float
    best_lead: int  # months x is shifted forward to align with y (x leads y)
    n_eff: int


def _to_yoy(s: pd.Series) -> pd.Series:
    s = s.copy()
    s.index = month_end_index(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s.pct_change(12)


def score_candidates_yoy_xcorr(
    target: TargetSpec,
    candidates: List[FeatureSpec],
    *,
    train_end: Optional[pd.Timestamp] = None,
    min_eff: int = 60,
    lead_months: Tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6),
) -> List[ScoredCandidate]:
    """
    Score each base series using max abs corr between YoY(target) and YoY(feature shifted by lead).
    - lead=3 means feature leads target by 3 months (x(t-3) helps predict y(t)).
    """
    y = load_series_from_fact(target.metric_id, target.geo_id, target.property_type_id)
    y_yoy = _to_yoy(y)
    if train_end is not None:
        y_yoy = y_yoy.loc[:train_end]

    scored: List[ScoredCandidate] = []

    for spec in candidates:
        try:
            x = load_series_from_fact(spec.metric_id, spec.geo_id, spec.property_type_id)
        except Exception:
            continue

        x_yoy = _to_yoy(x)
        if train_end is not None:
            x_yoy = x_yoy.loc[:train_end]

        # align on common index AFTER transforms
        best = (0.0, 0, 0)  # score, lead, n_eff
        for lead in lead_months:
            xx = x_yoy.shift(lead)
            df = pd.concat([y_yoy, xx], axis=1, join="inner").dropna()
            if len(df) < min_eff:
                continue
            c = float(df.iloc[:, 0].corr(df.iloc[:, 1]))
            if np.isnan(c):
                continue
            s_abs = abs(c)
            if s_abs > best[0]:
                best = (s_abs, lead, int(len(df)))

        if best[2] >= min_eff and best[0] > 0:
            scored.append(
                ScoredCandidate(
                    spec=spec,
                    score=best[0],
                    best_lead=best[1],
                    n_eff=best[2],
                )
            )

    # deterministic: score desc, then name
    scored.sort(key=lambda r: (-r.score, r.spec.name))
    return scored


def select_with_caps_and_buckets(
    scored: List[ScoredCandidate],
    *,
    # category caps (post-scoring)
    category_caps: Dict[str, int],
    # required minimum picks per category before filling others (optional)
    category_minimums: Optional[Dict[str, int]] = None,
    # geo bucket caps (optional)
    bucket_caps: Optional[Dict[str, int]] = None,
    bucket_fn=None,  # function(FeatureSpec)->bucket str
    # overall cap on BASE SERIES (before lag expansion)
    max_base_series: int = 250,
    # access to category on FeatureSpec: we’ll parse from name? no — you already have category upstream
) -> List[ScoredCandidate]:
    """
    Greedy deterministic selector:
      1) satisfy per-category minimums using highest scores (respect bucket caps)
      2) fill remaining up to category caps (respect bucket caps)
      3) stop at max_base_series
    """
    if category_minimums is None:
        category_minimums = {}
    if bucket_caps is None:
        bucket_caps = {}
    if bucket_fn is None:
        bucket_fn = lambda spec: "all"

    used_cat: Dict[str, int] = {k: 0 for k in category_caps.keys()}
    used_bucket: Dict[str, int] = {k: 0 for k in bucket_caps.keys()}
    picked: List[ScoredCandidate] = []

    def can_take(item: ScoredCandidate, cat: str, bucket: str) -> bool:
        cap = category_caps.get(cat)
        if cap is not None and used_cat.get(cat, 0) >= int(cap):
            return False
        bcap = bucket_caps.get(bucket)
        if bcap is not None and used_bucket.get(bucket, 0) >= int(bcap):
            return False
        return True

    # You need category on each candidate. We expect you to attach it to FeatureSpec.name? Nope.
    # So: require category to be embedded into spec.source_id? also no.
    # Practical fix: pass a cat_lookup dict into this function OR store category on FeatureSpec later.
    raise NotImplementedError("Pass a category lookup; see integration section below.")
