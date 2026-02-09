from __future__ import annotations
# forecast/selection/scoring.py

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import duckdb
import time

import numpy as np
import pandas as pd

from forecast.core.backtest_utils import month_end_index

from forecast.features.specs import FeatureSpec, TargetSpec
#from forecast.features.feature_loader import load_series_from_fact  # if you still want fallback
#from forecast.features.fact_loader import get_connection  # or wherever it is

from forecast.selection.bulk_fact_loader import load_series_many_from_fact


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


def _to_dlog(s: pd.Series) -> pd.Series:
    """
    Monthly log-diff transform.
    Uses log1p to tolerate zeros. Clips negatives to NaN.
    """
    s = _canon_monthly(s)
    s = s.where(s >= 0)  # negative values -> NaN
    return np.log1p(s).diff(1)


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


def _yoy_from_level_np(level: np.ndarray, season: int = 12) -> np.ndarray:
    out = np.full_like(level, np.nan, dtype=float)
    if level.size <= season:
        return out
    prev = level[:-season]
    cur = level[season:]
    mask = np.isfinite(cur) & np.isfinite(prev) & (prev != 0.0)
    out[season:][mask] = (cur[mask] / prev[mask]) - 1.0
    return out

def _dlog_from_level_np(level: np.ndarray) -> np.ndarray:
    out = np.full_like(level, np.nan, dtype=float)
    if level.size <= 1:
        return out
    mask = np.isfinite(level) & (level > 0)
    logx = np.full_like(level, np.nan, dtype=float)
    logx[mask] = np.log(level[mask])
    out[1:] = logx[1:] - logx[:-1]
    return out

def _best_xcorr_np(
    y: np.ndarray,
    x: np.ndarray,
    *,
    lead_months: Tuple[int, ...],
    min_eff: int,
) -> Tuple[float, int, int]:
    """
    Numpy version: y and x must be same length arrays aligned on the SAME monthly index.
    lead>0 means x is shifted forward (x leads y): compare y[t] with x[t-lead].
    Returns (best_abs_corr, best_lead, best_n_eff)
    """
    best_score = 0.0
    best_lead = 0
    best_n = 0

    n = y.shape[0]
    if n == 0:
        return 0.0, 0, 0

    for lead in lead_months:
        if lead < 0:
            raise ValueError("lead_months must be non-negative")

        if lead == 0:
            yy = y
            xx = x
        else:
            # x leads y: align y[lead:] with x[:-lead]
            yy = y[lead:]
            xx = x[:-lead]

        if yy.size < min_eff:
            continue

        mask = np.isfinite(yy) & np.isfinite(xx)
        n_eff = int(mask.sum())
        if n_eff < min_eff:
            continue

        yv = yy[mask]
        xv = xx[mask]

        # fast degeneracy checks (avoid expensive nunique)
        if np.nanstd(yv) == 0.0 or np.nanstd(xv) == 0.0:
            continue

        # Pearson corr
        y0 = yv - yv.mean()
        x0 = xv - xv.mean()
        denom = float(np.sqrt((y0 * y0).sum()) * np.sqrt((x0 * x0).sum()))
        if denom == 0.0:
            continue

        corr_abs = float(abs((y0 * x0).sum() / denom))
        if corr_abs > best_score:
            best_score = corr_abs
            best_lead = int(lead)
            best_n = int(n_eff)

    return best_score, best_lead, best_n

def _best_xcorr(
    y_s: pd.Series,
    x_s: pd.Series,
    *,
    lead_months: Tuple[int, ...],
    min_eff: int,
) -> Tuple[float, int, int]:
    """
    Return (best_abs_corr, best_lead, best_n_eff) over lead_months.
    lead>0 means x is shifted forward (x leads y).
    """
    best_score = 0.0
    best_lead = 0
    best_n = 0

    for lead in lead_months:
        df = pd.concat({"y": y_s, "x": x_s.shift(lead)}, axis=1).dropna()
        if len(df) < min_eff:
            continue

        yv, xv = df["y"], df["x"]
        if yv.nunique() < 2 or xv.nunique() < 2:
            continue

        s_abs = float(_score_pair_corr(yv, xv))
        if s_abs > best_score:
            best_score = s_abs
            best_lead = int(lead)
            best_n = int(len(df))

    return best_score, best_lead, best_n

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
    score_mode: str = "yoy_xcorr",  # "yoy_xcorr" | "yoy_corr0" | "level_xcorr" | "dlog_xcorr" | "combo"
) -> List[ScoredCandidate]:
    """
    Deterministic scoring.

    Modes:
      - yoy_xcorr: YoY transform, then max abs corr across lead window
      - yoy_corr0: YoY transform, lead=0 only
      - level_xcorr: raw levels, then max abs corr across lead window
      - dlog_xcorr: log-diff, then max abs corr across lead window
      - combo: blend of yoy_xcorr and dlog_xcorr
    """

    def _pt_norm(pt_id: Optional[str]) -> str:
        return pt_id if pt_id is not None else "all"
    
    def _effective_asof_for(source_id: Optional[str]) -> Optional[date]:
        if source_id and target.asof_by_source:
            return target.asof_by_source.get(source_id, target.data_asof)
        return target.data_asof
    
    def _key(metric_id: str, geo_id: str, pt_id: Optional[str], eff_asof: Optional[date]):
        return (str(metric_id), str(geo_id), _pt_norm(pt_id), eff_asof)

    if score_mode == "yoy_corr0":
        lead_months = (0,)

    # -------------------------
    # Bulk load series (ASOF-aware)
    # -------------------------
    # NOTE: current fact loader semantics do NOT filter by source_id, so the only
    # correct bulk key is (metric, geo, pt, effective_asof).
    # Build request list: target + all candidates
    reqs: List[Tuple[str, str, Optional[str], Optional[date]]] = []
    reqs.append((target.metric_id, target.geo_id, target.property_type_id, _effective_asof_for(None)))
    
    for spec in candidates:
        eff = _effective_asof_for(getattr(spec, "source_id", None))
        reqs.append((spec.metric_id, spec.geo_id, spec.property_type_id, eff))

    reqs = list(dict.fromkeys((m, g, pt, a) for (m, g, pt, a) in reqs))
    t0 = time.time()
    bulk = load_series_many_from_fact(requests=reqs)
    t1 = time.time()

    try:
        y = bulk[_key(target.metric_id, target.geo_id, target.property_type_id, _effective_asof_for(None))]
    except KeyError:
        return []

    # transforms...
    y_level = _canon_monthly(y)

    if train_end is not None:
        y_level = y_level.loc[:train_end]
        
    idx = y_level.index
    y_level_np = y_level.to_numpy(dtype=float)    
    # compute transforms from aligned level array (fast)
    y_yoy_np  = _yoy_from_level_np(y_level_np)
    y_dlog_np = _dlog_from_level_np(y_level_np)
    t2 = time.time()

    scored: List[ScoredCandidate] = []
    
    # -------------------------
    # Score each candidate
    # -------------------------
    for spec in candidates:
        eff = _effective_asof_for(getattr(spec, "source_id", None))
        k = _key(spec.metric_id, spec.geo_id, spec.property_type_id, eff)
        x = bulk.get(k)
        if x is None:
            continue
            
        # then your existing transform logic
        #x_level = _canon_monthly(x)
        x_level = x
        if train_end is not None:
            x_level = x_level.loc[:train_end]
        
        x_level_np = x_level.reindex(idx).to_numpy(dtype=float)

        if score_mode == "level_xcorr":
            best_score, best_lead, best_n = _best_xcorr_np(y_level_np, x_level_np, lead_months=lead_months, min_eff=min_eff)

        elif score_mode in ("yoy_xcorr", "yoy_corr0"):
            x_yoy_np = _yoy_from_level_np(x_level_np)
            best_score, best_lead, best_n = _best_xcorr_np(y_yoy_np, x_yoy_np, lead_months=lead_months, min_eff=min_eff)

        elif score_mode == "dlog_xcorr":
            x_dlog_np = _dlog_from_level_np(x_level_np)
            best_score, best_lead, best_n = _best_xcorr_np(y_dlog_np, x_dlog_np, lead_months=lead_months, min_eff=min_eff)

        elif score_mode == "combo":
            x_yoy_np  = _yoy_from_level_np(x_level_np)
            x_dlog_np = _dlog_from_level_np(x_level_np)
            s_yoy, lead_yoy, n_yoy = _best_xcorr_np(y_yoy_np,  x_yoy_np,  lead_months=lead_months, min_eff=min_eff)
            s_dlog, lead_dlog, n_dlog = _best_xcorr_np(y_dlog_np, x_dlog_np, lead_months=lead_months, min_eff=min_eff)            

            if max(n_yoy, n_dlog) < min_eff:
                continue

            best_score = 0.5 * float(s_yoy) + 0.5 * float(s_dlog)

            # audit/debug: carry lead/n from the stronger component
            if s_dlog > s_yoy:
                best_lead, best_n = int(lead_dlog), int(n_dlog)
            else:
                best_lead, best_n = int(lead_yoy), int(n_yoy)        
        
        else:
            raise ValueError(f"Unknown score_mode: {score_mode}")

        if best_n >= min_eff and abs(best_score) > 0:
            scored.append(
                ScoredCandidate(
                    spec=spec,
                    score=float(best_score),
                    best_lead=int(best_lead),
                    n_eff=int(best_n),
                )
            )

    t3 = time.time()

    print(f"[timing][score] bulk_load_sec={t1-t0:.2f}")
    print(f"[timing][score] prep_target_sec={t2-t1:.2f}")
    print(f"[timing][score] loop_sec={t3-t2:.2f}  n_candidates={len(candidates)}")

    scored.sort(key=lambda r: (-r.score, r.spec.name))
    return scored
