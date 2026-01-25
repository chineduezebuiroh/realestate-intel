from __future__ import annotations
# forecast/metric_tiers.py
from dataclasses import dataclass
from typing import Dict, Optional

# ----------------------------
# Redfin metric tiering (base metrics, NOT transforms)
# ----------------------------

REDFIN_METRIC_TIER: Dict[str, int] = {
    # Tier 0 — Core market state / direct targets
    "inventory": 0,
    "homes_sold": 0,
    "median_sale_price": 0,
    "median_ppsf": 0,
    "median_dom": 0,

    # Tier 1 — Market flow / pressure
    "pending_sales": 1,
    "new_listings": 1,
    "price_drops": 1,
    "sold_above_list": 1,

    # Tier 2 — Ratios / friction
    "avg_sale_to_list": 2,
    "months_of_supply": 2,
    "off_market_in_two_weeks": 2,
    "median_list_price": 2,
    "median_list_ppsf": 2,
}

# Tier 3 = transforms/deltas (fragile): we’ll treat as tier 3 by suffix rules.
REDFIN_TRANSFORM_SUFFIXES = (
    "_mom",
    "_yoy",
)

DEFAULT_REDFIN_TIER = 3  # if unknown but redfin -> treat as fragile by default


def redfin_metric_tier(metric_id: str) -> int:
    """
    Tiering rules:
      - If metric in explicit map -> use it
      - Else if it looks like a transform (_mom/_yoy) -> tier 3
      - Else -> tier 3 (conservative)
    """
    if metric_id in REDFIN_METRIC_TIER:
        return int(REDFIN_METRIC_TIER[metric_id])
    if any(metric_id.endswith(suf) for suf in REDFIN_TRANSFORM_SUFFIXES):
        return 3
    return DEFAULT_REDFIN_TIER


# ----------------------------
# Geo equivalence (avoid redundant “same DC” buckets)
# ----------------------------

GEO_EQUIVALENCE = {
    "dc_city": "dc_core",
    "dc_county": "dc_core",
    "dc_state": "dc_core",
    # IMPORTANT: dc_msa stays distinct (do NOT map it)
}


def canon_geo_id(geo_id: str) -> str:
    return GEO_EQUIVALENCE.get(geo_id, geo_id)


# ----------------------------
# Tier-share caps for Redfin-only selection
# ----------------------------

@dataclass(frozen=True)
class RedfinTierShareCaps:
    """
    Shares apply ONLY within the Redfin portion of the final selected base-series set.
    Must sum to 1.0.
    """
    tier0: float = 0.30
    tier1: float = 0.35
    tier2: float = 0.25
    tier3: float = 0.10

    # floor constraints to avoid rounding-to-zero weirdness
    min_tier0: int = 1
    min_tier1: int = 1
    min_tier2: int = 1
    min_tier3: int = 0  # fine for small sets

    def shares(self) -> Dict[int, float]:
        return {0: self.tier0, 1: self.tier1, 2: self.tier2, 3: self.tier3}

    def mins(self) -> Dict[int, int]:
        return {0: self.min_tier0, 1: self.min_tier1, 2: self.min_tier2, 3: self.min_tier3}

    def validate(self) -> None:
        s = self.tier0 + self.tier1 + self.tier2 + self.tier3
        if abs(s - 1.0) > 1e-6:
            raise ValueError(f"RedfinTierShareCaps shares must sum to 1.0, got {s}")
