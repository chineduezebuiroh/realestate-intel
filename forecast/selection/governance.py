from __future__ import annotations

from collections import Counter
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from forecast.features.feature_loader import FeatureSpec, TargetSpec
from forecast.features.metric_tiers import canon_geo_id, redfin_metric_tier, RedfinTierShareCaps
from forecast.selection.scoring import ScoredCandidate


def default_bucket(spec: FeatureSpec, target: TargetSpec) -> str:
    """
    Simple geo diversity buckets + DC equivalence via canon_geo_id().
    """
    spec_geo = canon_geo_id(spec.geo_id)
    target_geo = canon_geo_id(target.geo_id)

    # Canonical equivalence: dc_city/dc_county/dc_state all become "dc_core"
    if spec_geo == target_geo:
        return "geo:target_equiv"

    g = spec_geo

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

    # Treat dc_core as a special “geo class” only if you want; optional:
    if g == "dc_core":
        return "geo:dc_core"

    return "geo:other"


def scored_to_feature_specs(scored: List[ScoredCandidate]) -> List[FeatureSpec]:
    return [s.spec for s in scored]


def select_scored_candidates(
    scored: List[ScoredCandidate],
    max_base_series: int,
    category_caps: Dict[str, int],
    category_minimums: Dict[str, int],
    bucket_caps: Dict[str, int],
    bucket_fn: Optional[Callable[[FeatureSpec], str]],
    redfin_tier_caps: Optional[RedfinTierShareCaps] = None,
    *,
    metric_pt_cap: Optional[int] = None,  # max per (metric_id, property_type_id)
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

    used_base_id = set()

    def base_id(item: ScoredCandidate) -> Tuple[str, str, str, str]:
        mid = str(getattr(item.spec, "metric_id", ""))
        geo = canon_geo_id(str(getattr(item.spec, "geo_id", "")))
        pt  = str(getattr(item.spec, "property_type_id", ""))
        src = str(getattr(item.spec, "source_id", ""))
        return (mid, geo, pt, src)


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
    R: int = 0

    def is_redfin(item: ScoredCandidate) -> bool:
        sid = getattr(item.spec, "source_id", None)
        return (sid or "").strip().lower() == "redfin"

    def tier_of(item: ScoredCandidate) -> int:
        return int(redfin_metric_tier(getattr(item.spec, "metric_id", "")))

    if redfin_tier_caps is not None:
        redfin_tier_caps.validate()

        redfin_items = [it for it in scored if is_redfin(it)]
        R = min(
            int(getattr(redfin_tier_caps, "redfin_cap_n")),
            int(max_base_series),
            int(len(redfin_items)),
        )

        if R <= 0:
            redfin_tier_quota = None
        else:
            shares = redfin_tier_caps.shares()
            mins = redfin_tier_caps.mins()

            quota = {t: int(mins.get(t, 0)) for t in (0, 1, 2, 3)}
            if sum(quota.values()) > R:
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

                raw = {t: int(np.floor(shares.get(t, 0.0) * R)) for t in (0, 1, 2, 3)}
                raw = {t: max(0, raw[t] - quota[t]) for t in raw}

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
        if base_id(item) in used_base_id:
            return False

        if redfin_tier_quota is not None and is_redfin(item):
            if used_redfin_total() >= int(R):
                return False

        # metric_id concentration cap (PT-aware)
        if metric_pt_cap is not None:
            k = metric_pt_key(item)
            if metric_pt_used.get(k, 0) >= int(metric_pt_cap):
                return False

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
        used_base_id.add(base_id(item))

        if redfin_tier_quota is not None and is_redfin(item):
            t = tier_of(item)
            used_redfin_tier[t] = used_redfin_tier.get(t, 0) + 1

        if metric_pt_cap is not None:
            metric_pt_used[metric_pt_key(item)] += 1

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

    # 1) satisfy category minimums
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

    if redfin_tier_quota is not None:
        print(f"[selector] redfin_tier_quota={redfin_tier_quota} used={used_redfin_tier}")

    # Debug: concentration diagnostics
    if metric_pt_cap is not None:
        metric_counts = Counter([str(it.spec.metric_id) for it in picked])
        metric_pt_counts = Counter([(str(it.spec.metric_id), str(it.spec.property_type_id)) for it in picked])

        print(
            f"[selector] metric_pt_cap={int(metric_pt_cap)} "
            f"picked_n={len(picked)} "
            f"unique_metrics={len(metric_counts)} "
            f"unique_metric_pt={len(metric_pt_counts)} "
            f"top_metrics={metric_counts.most_common(10)} "
            f"top_metric_pt={metric_pt_counts.most_common(10)}"
        )

    return picked
