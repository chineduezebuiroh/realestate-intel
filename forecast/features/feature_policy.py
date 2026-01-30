from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Set


@dataclass(frozen=True)
class FeaturePolicy:
    # Category governance (keys MUST match dim_metric.category lowercased)
    include_categories: Optional[Set[str]] = None   # None => allow all
    exclude_categories: Set[str] = field(default_factory=set)
    family_caps: Dict[str, int] = field(default_factory=dict)

    # Coverage thresholds by native frequency (dim_metric.frequency lowercased)
    min_coverage_ratio: Dict[str, float] = field(default_factory=dict)

    # Source/property-type exclusions (e.g. redfin: {"-1"})
    exclude_property_type_ids_by_source: Dict[str, Set[str]] = field(default_factory=dict)

    # Selection outputs
    seed: int = 1337
    xgb_top_k: int = 100
    sarimax_max_exog: int = 30
    
    # inside your Policy / FeaturePolicy dataclass
    xgb_selector_horizon_months: int = 1          # selector uses 1–3 months
    xgb_selector_latest_anchor_offset_months: Optional[int] = None
    xgb_selector_anchor_step_months: int = 1
    xgb_selector_max_anchors: int = 12
    
    # --- Data quality thresholds ---
    min_feature_coverage_ratio: float = 0.95
    max_consecutive_missing_months: int = 2
    tail_gap_months: int = 3


def default_policy() -> FeaturePolicy:
    return FeaturePolicy(
        include_categories=None,
        exclude_categories=set(),
        family_caps={
            "prices": 120,
            "sales": 80,
            "supply": 80,
            "speed": 60,
            "labor": 40,
            "rates": 25,
            "yields": 25,
            "spreads": 15,
            "inflation": 10,
            "gdp": 10,
            "census": 20,
            "uncategorized": 0,
        },
        min_coverage_ratio={
            "monthly": 0.80,
            "quarterly": 0.60,
            "annual": 0.40,
        },
        exclude_property_type_ids_by_source={
            # Decide intentionally:
            # "-1" = All Residential, "-2" = Single Units Only
            # If you *really* want to exclude, do it here.
            "redfin": {"-1", "-2"},
        },
        seed=1337,
        xgb_top_k=100,
        sarimax_max_exog=30,
        xgb_selector_horizon_months = 1,          # selector uses 1–3 months
        xgb_selector_latest_anchor_offset_months = None,
        xgb_selector_anchor_step_months = 1,
        xgb_selector_max_anchors = 12,
    )
