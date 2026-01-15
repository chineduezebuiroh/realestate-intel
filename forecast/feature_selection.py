from __future__ import annotations
# forecast/feature_selection.py

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from .backtest_utils import month_end_index
from .feature_loader import FeatureSpec, TargetSpec, load_series_from_fact


DC_EQUIV = {"dc_city", "dc_county", "dc_state"}


@dataclass(frozen=True)
class ScoredCandidate:
    spec: FeatureSpec
    score: float
    best_lead: int   # months x is shifted forward to align with y (x leads y)
    n_eff: int


# ===================================================
# Helpers
# ===================================================
def _canon_monthly(s: pd.Series) -> pd.Series:
    s = s.copy()
    s.index = month_end_index(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def _to_yoy(s: pd.Series) -> pd.Series:
    # YoY percent change on month-end index
    s = _canon_monthly(s)
    return s.pct_change(12)


def _score_pair_corr(a: pd.Series, b: pd.Series) -> float:
    """
    Returns abs Pearson correlation, or 0.0 if undefined.
    Bulletproof against NaN/inf/constant series.
    """
    aa = pd.to_numeric(a, errors="coerce").to_numpy(dtype=float)
    bb = pd.to_numeric(b, errors="coerce").to_numpy(dtype=float)

    mask = np.isfinite(aa) & np.isfinite(bb)
    aa = aa[mask]
    bb = bb[mask]

    if aa.size < 3:
        return 0.0

    # constant => corr undefined
    if np.std(aa) == 0.0 or np.std(bb) == 0.0:
        return 0.0

    r = np.corrcoef(aa, bb)[0, 1]
    if not np.isfinite(r):
        return 0.0
    return float(abs(r))


def _prepare_xy(y: pd.Series, x: pd.Series, min_n: int = 36):
    # align
    df = pd.concat([y, x], axis=1, join="inner")
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    if len(df) < min_n:
        return None

    yv = df.iloc[:, 0].astype(float).to_numpy()
    xv = df.iloc[:, 1].astype(float).to_numpy()

    # reject constant series (std==0) -> corr undefined
    if np.nanstd(yv) == 0 or np.nanstd(xv) == 0:
        return None

    return yv, xv, df.index

# ===================================================
# Main Logic
# ===================================================
def score_candidates(
    target: TargetSpec,
    candidates: List[FeatureSpec],
    *,
    train_end: Optional[pd.Timestamp] = None,
    min_eff: int = 60,
    lead_months: Tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6),
    score_mode: str = "yoy_xcorr",  # "yoy_xcorr" | "yoy_corr0" | "level_xcorr"
) -> List[ScoredCandidate]:
    """
    Deterministic scoring.
    - yoy_xcorr: YoY transform, then max abs corr across lead window
    - yoy_corr0: YoY transform, lead=0 only
    - level_xcorr: raw levels, then max abs corr across lead window (riskier)
    """
    y = load_series_from_fact(target.metric_id, target.geo_id, target.property_type_id)

    if score_mode.startswith("yoy"):
        y_s = _to_yoy(y)
    elif score_mode == "level_xcorr":
        y_s = _canon_monthly(y)
    else:
        raise ValueError(f"Unknown score_mode: {score_mode}")

    if train_end is not None:
        y_s = y_s.loc[:train_end]

    if score_mode == "yoy_corr0":
        lead_months = (0,)

    scored: List[ScoredCandidate] = []

    for spec in candidates:
        try:
            x = load_series_from_fact(spec.metric_id, spec.geo_id, spec.property_type_id)
        except Exception:
            continue

        if score_mode.startswith("yoy"):
            x_s = _to_yoy(x)
        else:
            x_s = _canon_monthly(x)

        if train_end is not None:
            x_s = x_s.loc[:train_end]

        best_score = 0.0
        best_lead = 0
        best_n = 0

        for lead in lead_months:
            xx = x_s.shift(lead)
            #df = pd.concat([y_s, xx], axis=1, join="inner").dropna()
            df = pd.concat({"y": y_s, "x": xx}, axis=1).dropna()
            if len(df) < min_eff:
                continue

            #yv = df.iloc[:, 0]
            #xv = df.iloc[:, 1]
            yv, xv = df["y"], df["x"]
            if yv.nunique() < 2 or xv.nunique() < 2:
                continue
            s_abs = _score_pair_corr(yv, xv)

            if s_abs > best_score:
                best_score = s_abs
                best_lead = int(lead)
                best_n = int(len(df))

        if best_n >= min_eff and best_score > 0:
            scored.append(ScoredCandidate(spec=spec, score=best_score, best_lead=best_lead, n_eff=best_n))

    scored.sort(key=lambda r: (-r.score, r.spec.name))
    return scored


def default_bucket(spec: FeatureSpec, target: TargetSpec) -> str:
    """
    Simple geo diversity buckets + DC equivalence.
    You can refine later.
    """
    g = spec.geo_id
    if g in DC_EQUIV:
        g = "dc_equiv"

    if g == target.geo_id or (target.geo_id in DC_EQUIV and g == "dc_equiv"):
        return "geo:target_equiv"

    # coarse geography types by naming convention
    if g.endswith("_dc") and len(g.split("_")) == 2 and g.split("_")[0].isdigit():
        return "geo:zipcode_dc"
    if g.endswith("_city") or "city" in g:
        return "geo:city"
    if "county" in g:
        return "geo:county"
    if g.endswith("_msa") or "msa" in g:
        return "geo:msa"
    if g.endswith("_state") or "state" in g:
        return "geo:state"
    if g in ("us_nation", "us"):
        return "geo:national"

    return "geo:other"


def select_scored_candidates(
    scored: List[ScoredCandidate],
    *,
    max_base_series: int,
    category_caps: Dict[str, int],
    category_minimums: Optional[Dict[str, int]] = None,
    bucket_caps: Optional[Dict[str, int]] = None,
    bucket_fn=None,
) -> List[ScoredCandidate]:
    """
    Greedy deterministic selection on scored base series, BEFORE lag expansion.
    - First satisfy category_minimums (if any)
    - Then fill remaining while respecting category_caps and bucket_caps
    """
    category_minimums = category_minimums or {}
    bucket_caps = bucket_caps or {}
    bucket_fn = bucket_fn or (lambda spec, target=None: "all")  # overwritten by wrapper below

    used_cat: Dict[str, int] = {}
    used_bucket: Dict[str, int] = {}
    picked: List[ScoredCandidate] = []

    def cat_of(item: ScoredCandidate) -> str:
        return (item.spec.category or "uncategorized").lower()

    def bucket_of(item: ScoredCandidate) -> str:
        return bucket_fn(item.spec)

    def can_take(item: ScoredCandidate) -> bool:
        cat = cat_of(item)
        cap = category_caps.get(cat)
        if cap is not None and used_cat.get(cat, 0) >= int(cap):
            return False

        b = bucket_of(item)
        bcap = bucket_caps.get(b)
        if bcap is not None and used_bucket.get(b, 0) >= int(bcap):
            return False

        return True

    def take(item: ScoredCandidate):
        cat = cat_of(item)
        b = bucket_of(item)
        picked.append(item)
        used_cat[cat] = used_cat.get(cat, 0) + 1
        used_bucket[b] = used_bucket.get(b, 0) + 1

    # 1) satisfy minimums
    for cat, min_n in category_minimums.items():
        need = int(min_n)
        if need <= 0:
            continue
        for item in scored:
            if len(picked) >= max_base_series:
                break
            if cat_of(item) != cat:
                continue
            if item in picked:
                continue
            if can_take(item):
                take(item)
                need -= 1
                if need <= 0:
                    break

    # 2) fill remainder
    for item in scored:
        if len(picked) >= max_base_series:
            break
        if item in picked:
            continue
        if can_take(item):
            take(item)

    return picked


def scored_to_feature_specs(scored: List[ScoredCandidate]) -> List[FeatureSpec]:
    return [s.spec for s in scored]

def score_corr0(y: pd.Series, x: pd.Series, min_n: int = 36) -> float:
    prep = _prepare_xy(y, x, min_n=min_n)
    if prep is None:
        return float("-inf")
    yv, xv, _ = prep
    c = np.corrcoef(yv, xv)[0, 1]
    if not np.isfinite(c):
        return float("-inf")
    return float(abs(c))

def score_xcorr(y: pd.Series, x: pd.Series, lead_months=(0,1,2,3,6,12), min_n: int = 36):
    best = float("-inf")
    best_lead = None

    for L in lead_months:
        # shift x forward so that x at time t corresponds to original x at time t-L
        xs = x.shift(L)

        prep = _prepare_xy(y, xs, min_n=min_n)
        if prep is None:
            continue

        yv, xv, _ = prep
        c = np.corrcoef(yv, xv)[0, 1]
        if not np.isfinite(c):
            continue

        s = float(abs(c))
        if s > best:
            best = s
            best_lead = int(L)

    if best == float("-inf"):
        return best, None
    return best, best_lead
