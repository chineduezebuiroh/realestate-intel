from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Set


@dataclass(frozen=True)
class FeaturePolicy:
    # Category governance (keys MUST match dim_metric.category lowercased)
    include_categories: Optional[Set[str]] = None   # None => allow all
    exclude_categories: Set[str] = None             # empty => exclude none
    family_caps: Dict[str, int] = None              # e.g. {"prices": 60, "labor": 30}

    # Coverage thresholds by native frequency (dim_metric.frequency lowercased)
    min_coverage_ratio: Dict[str, float] = None     # {"monthly":0.80,"quarterly":0.60,"annual":0.40}

    # Source/property-type exclusions (e.g. redfin: {"-1"})
    exclude_property_type_ids_by_source: Dict[str, Set[str]] = None

    # Selection outputs
    seed: int = 1337
    xgb_top_k: int = 100
    sarimax_max_exog: int = 30


def default_policy() -> FeaturePolicy:
    return FeaturePolicy(
        include_categories=None,
        exclude_categories=set(),
        family_caps={
            # start permissive; tighten once you inspect distribution
            "prices": 80,
            "sales": 60,
            "supply": 60,
            "speed": 40,
            "labor": 30,
            "rates": 20,
            "yields": 20,
            "spreads": 15,
            "inflation": 10,
            "gdp": 10,
            "census": 20,
        },
        min_coverage_ratio={
            "monthly": 0.80,
            "quarterly": 0.60,
            "annual": 0.40,
        },
        exclude_property_type_ids_by_source={
            "redfin": {"-1"},  # exclude “All Residential”
        },
        seed=1337,
        xgb_top_k=100,
        sarimax_max_exog=30,
    )
