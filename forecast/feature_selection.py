from __future__ import annotations
# forecast/feature_selection.py

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from collections import Counter

import numpy as np
import pandas as pd

from forecast.backtest_utils import month_end_index
from forecast.feature_loader import FeatureSpec, TargetSpec, load_series_from_fact
from forecast.metric_tiers import canon_geo_id, redfin_metric_tier, RedfinTierShareCaps


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


def _to_dlog(s: pd.Series) -> pd.Series:
    """
    Monthly log-diff transform.
    Uses log1p to tolerate zeros. Clips negatives to NaN (shouldn't happen in these series).
    """
    s = _canon_monthly(s)
    s = s.where(s >= 0)               # negative values -> NaN
    return np.log1p(s).diff(1)


def _best_xcorr(
    y_s: pd.Series,
    x_s: pd.Series,
    *,
    lead_months: Tuple[int, ...],
    min_eff: int,
) -> Tuple[float, int, int]:
    """
    Return (best_abs_corr, best_lead, best_n_eff) over lead_months.
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
    score_mode: str = "yoy_xcorr",  # "yoy_xcorr" | "yoy_corr0" | "level_xcorr" | "dlog_xcorr" | "combo"
) -> List[ScoredCandidate]:
    """
    Deterministic scoring.

    Modes:
      - yoy_xcorr: YoY transform, then max abs corr across lead window
      - yoy_corr0: YoY transform, lead=0 only
      - level_xcorr: raw levels, then max abs corr across lead window (riskier)
      - dlog_xcorr: log-diff levels, then max abs corr across lead window
      - combo: blend of yoy_xcorr and dlog_xcorr (fairer to level metrics)
    """
    if score_mode == "yoy_corr0":
        lead_months = (0,)

    # -------------------------
    # Local cache (single source of truth)
    # -------------------------
    _cache: Dict[Tuple[str, str, Optional[str]], pd.Series] = {}

    def _load(metric_id: str, geo_id: str, pt_id: Optional[str]) -> pd.Series:
        key = (metric_id, geo_id, pt_id)
        if key not in _cache:
            _cache[key] = load_series_from_fact(metric_id, geo_id, pt_id)
        return _cache[key]

    # -------------------------
    # Load + precompute target transforms once
    # -------------------------
    try:
        y = _load(target.metric_id, target.geo_id, target.property_type_id)
    except Exception:
        return []

    y_level = _canon_monthly(y)
    y_yoy = _to_yoy(y)
    y_dlog = _to_dlog(y)

    if train_end is not None:
        y_level = y_level.loc[:train_end]
        y_yoy = y_yoy.loc[:train_end]
        y_dlog = y_dlog.loc[:train_end]

    scored: List[ScoredCandidate] = []

    # -------------------------
    # Score each candidate
    # -------------------------
    for spec in candidates:
        try:
            x = _load(spec.metric_id, spec.geo_id, spec.property_type_id)
        except Exception:
            continue

        x_level = _canon_monthly(x)
        x_yoy = _to_yoy(x)
        x_dlog = _to_dlog(x)

        if train_end is not None:
            x_level = x_level.loc[:train_end]
            x_yoy = x_yoy.loc[:train_end]
            x_dlog = x_dlog.loc[:train_end]

        if score_mode in ("yoy_xcorr", "yoy_corr0"):
            best_score, best_lead, best_n = _best_xcorr(
                y_yoy, x_yoy, lead_months=lead_months, min_eff=min_eff
            )

        elif score_mode == "level_xcorr":
            best_score, best_lead, best_n = _best_xcorr(
                y_level, x_level, lead_months=lead_months, min_eff=min_eff
            )

        elif score_mode == "dlog_xcorr":
            best_score, best_lead, best_n = _best_xcorr(
                y_dlog, x_dlog, lead_months=lead_months, min_eff=min_eff
            )

        elif score_mode == "combo":
            s_yoy, lead_yoy, n_yoy = _best_xcorr(
                y_yoy, x_yoy, lead_months=lead_months, min_eff=min_eff
            )
            s_dlog, lead_dlog, n_dlog = _best_xcorr(
                y_dlog, x_dlog, lead_months=lead_months, min_eff=min_eff
            )

            if max(n_yoy, n_dlog) < min_eff:
                continue

            # Simple equal-weight blend (v1). You can tune later.
            best_score = 0.5 * float(s_yoy) + 0.5 * float(s_dlog)

            # For audit/debug, carry lead/n from the stronger component
            if s_dlog > s_yoy:
                best_lead, best_n = int(lead_dlog), int(n_dlog)
            else:
                best_lead, best_n = int(lead_yoy), int(n_yoy)

        else:
            raise ValueError(f"Unknown score_mode: {score_mode}")

        if best_n >= min_eff and best_score > 0:
            scored.append(
                ScoredCandidate(
                    spec=spec,
                    score=float(best_score),
                    best_lead=int(best_lead),
                    n_eff=int(best_n),
                )
            )

    scored.sort(key=lambda r: (-r.score, r.spec.name))
    return scored


def default_bucket(spec: FeatureSpec, target: TargetSpec) -> str:
    """
    Simple geo diversity buckets + DC equivalence.
    You can refine later.
    """
    spec_geo = canon_geo_id(spec.geo_id)
    target_geo = canon_geo_id(target.geo_id)

    
    g = spec_geo
    if g in DC_EQUIV:
        g = "dc_equiv"

    if g == target_geo or (target_geo in DC_EQUIV and g == "dc_equiv"):
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
    max_base_series: int,
    category_caps: Dict[str, int],
    category_minimums: Dict[str, int],
    bucket_caps: Dict[str, int],
    bucket_fn,
    redfin_tier_caps=None,
    *,
    metric_pt_cap: Optional[int] = None,  # NEW: max per (metric_id, property_type_id)
) -> List[ScoredCandidate]:
    """
    Greedy deterministic selection on scored base series, BEFORE lag expansion.
    - First satisfy category_minimums (if any)
    - Then fill remaining while respecting category_caps and bucket_caps
    """
    category_minimums = category_minimums or {}
    bucket_caps = bucket_caps or {}
    bucket_fn = bucket_fn or (lambda spec: "all")

    used_cat: Dict[str, int] = {}
    used_bucket: Dict[str, int] = {}
    picked: List[ScoredCandidate] = []
    metric_pt_used = Counter()  # key=(metric_id, pt_id_str)


    def cat_of(item: ScoredCandidate) -> str:
        return (item.spec.category or "uncategorized").lower()

    def bucket_of(item: ScoredCandidate) -> str:
        return bucket_fn(item.spec)

    def metric_pt_key(item: ScoredCandidate) -> Tuple[str, str]:
        mid = str(getattr(item.spec, "metric_id", ""))
        pt = str(getattr(item.spec, "property_type_id", ""))
        return (mid, pt)

    # ----------------------------
    # Redfin tier caps (optional)
    # ----------------------------
    used_redfin_tier: Dict[int, int] = {0: 0, 1: 0, 2: 0, 3: 0}
    redfin_tier_quota: Optional[Dict[int, int]] = None

    def is_redfin(item: ScoredCandidate) -> bool:
        sid = getattr(item.spec, "source_id", None)
        return (sid or "").strip().lower() == "redfin"

    def tier_of(item: ScoredCandidate) -> int:
        return int(redfin_metric_tier(getattr(item.spec, "metric_id", "")))

    if redfin_tier_caps is not None:
        # You imported RedfinTierShareCaps already; assume it has validate()/shares()/mins()
        redfin_tier_caps.validate()

        # This is the *budget* of Redfin base-series we allow in the pick set.
        # IMPORTANT: this is not max_base_series — it's explicitly "how much Redfin can occupy".
        redfin_items = [it for it in scored if is_redfin(it)]
        # Redfin budget is bounded by:
        # - user-requested redfin_cap_n
        # - total pick budget max_base_series
        # - how many redfin candidates even exist
        R = min(
            int(getattr(redfin_tier_caps, "redfin_cap_n")),
            int(max_base_series),
            int(len(redfin_items)),
        )

        if R <= 0:
            redfin_tier_quota = None

        shares = redfin_tier_caps.shares()
        mins = redfin_tier_caps.mins()

        # floor allocation
        quota = {t: int(mins.get(t, 0)) for t in (0, 1, 2, 3)}
        if sum(quota.values()) > R:
            # truncate floors deterministically
            quota = {0: 0, 1: 0, 2: 0, 3: 0}
            remaining = R
            for t in (0, 1, 2, 3):
                take_n = min(int(mins.get(t, 0)), remaining)
                quota[t] = take_n
                remaining -= take_n
                if remaining <= 0:
                    break
            redfin_tier_quota = quota
        else:
            remaining = R - sum(quota.values())

            # proportional fill
            raw = {t: int(np.floor(shares.get(t, 0.0) * R)) for t in (0, 1, 2, 3)}
            raw = {t: max(0, raw[t] - quota[t]) for t in raw}

            # stable priority for allocating raw + leftovers
            for t in (1, 0, 2, 3):
                if remaining <= 0:
                    break
                add = min(raw.get(t, 0), remaining)
                quota[t] += add
                remaining -= add

            for t in (1, 0, 2, 3):
                if remaining <= 0:
                    break
                quota[t] += 1
                remaining -= 1

            redfin_tier_quota = quota

        print(f"[selector] redfin_tier_quota={redfin_tier_quota} redfin_cap_n={R}")


    def used_redfin_total() -> int:
        return sum(used_redfin_tier.values())

    def can_take(item: ScoredCandidate) -> bool:
        if redfin_tier_quota is not None and is_redfin(item):
            # Hard cap on total redfin count
            if used_redfin_total() >= int(R):
                return False

        # --- NEW: metric_id concentration cap (PT-aware) ---
        if metric_pt_cap is not None:
            k = (item.spec.metric_id, str(item.spec.property_type_id))
            if metric_pt_used.get(k, 0) >= int(metric_pt_cap):
                return False

        # Redfin tier gate (only if enabled)
        if redfin_tier_quota is not None and is_redfin(item):
            t = tier_of(item)
            if used_redfin_tier.get(t, 0) >= int(redfin_tier_quota.get(t, 0)):
                return False

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

        if redfin_tier_quota is not None and is_redfin(item):
            t = tier_of(item)
            used_redfin_tier[t] = used_redfin_tier.get(t, 0) + 1

        # --- NEW: metric_id concentration cap (PT-aware) ---
        if metric_pt_cap is not None:
            k = (item.spec.metric_id, str(item.spec.property_type_id))
            metric_pt_used[k] += 1


    # 0) satisfy Redfin tier minimums first (if enabled)
    if redfin_tier_quota is not None and redfin_tier_caps is not None:
        tier_mins = redfin_tier_caps.mins()
        for t in (0, 1, 2, 3):
            need = int(tier_mins.get(t, 0))
            if need <= 0:
                continue
            for item in scored:
                if len(picked) >= max_base_series:
                    break
                if not is_redfin(item):
                    continue
                if tier_of(item) != t:
                    continue
                if item in picked:
                    continue
                if can_take(item):
                    take(item)
                    need -= 1
                    if need <= 0:
                        break

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

    
    # 1.5) satisfy Redfin tier minimums (if enabled)
    if redfin_tier_caps is not None and redfin_tier_quota is not None:
        mins = redfin_tier_caps.mins()
        for t in (0, 1, 2, 3):
            need = int(mins.get(t, 0))
            if need <= 0:
                continue

            # Don't try to exceed the computed quota for that tier
            need = min(need, int(redfin_tier_quota.get(t, 0)))

            while need > 0 and len(picked) < max_base_series:
                took_one = False
                for item in scored:
                    if item in picked:
                        continue
                    if not is_redfin(item):
                        continue
                    if tier_of(item) != t:
                        continue
                    if can_take(item):
                        take(item)
                        need -= 1
                        took_one = True
                        break
                if not took_one:
                    # can't satisfy this tier minimum under other caps/buckets
                    break


    # 2) fill remainder
    for item in scored:
        if len(picked) >= max_base_series:
            break
        if item in picked:
            continue
        if can_take(item):
            take(item)

    if redfin_tier_quota is not None:
        print(f"[selector] redfin_tier_quota={redfin_tier_quota} used={used_redfin_tier}")
    """
    if metric_pt_cap is not None:
        top = sorted(metric_pt_used.items(), key=lambda kv: -kv[1])[:10]
        print(f"[selector] metric_pt_cap={metric_pt_cap} top_metric_pt_used={top}")
    """
        # Debug: concentration diagnostics
    metric_counts = Counter([str(it.spec.metric_id) for it in picked])
    metric_pt_counts = Counter([(str(it.spec.metric_id), str(it.spec.property_type_id)) for it in picked])

    print(
        f"[selector] picked_n={len(picked)} "
        f"unique_metrics={len(metric_counts)} "
        f"unique_metric_pt={len(metric_pt_counts)} "
        f"top_metrics={metric_counts.most_common(10)} "
        f"top_metric_pt={metric_pt_counts.most_common(10)}"
    )

    if metric_pt_cap is not None:
        print(f"[selector] metric_pt_cap={int(metric_pt_cap)}")

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
