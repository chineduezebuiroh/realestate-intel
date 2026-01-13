from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Set

@dataclass(frozen=True)
class FeaturePolicy:
    # how many features XGBGB is allowed to emit (shortlist artifact)
    xgb_top_k: int = 100

    # how many SARIMAX-Exog will consume from shortlist
    sarimax_max_exog: int = 30

    # family caps for candidate specs BEFORE design-matrix build
    # keys should match dim_metric.category lowercased
    family_caps: Dict[str, int] = None

    # exclude feature specs by property_type_id for certain sources (e.g. Redfin)
    exclude_property_type_ids: Set[str] = None

def default_policy() -> FeaturePolicy:
    return FeaturePolicy(
        xgb_top_k=100,
        sarimax_max_exog=30,
        family_caps={
            # tune later; start sane
            "housing": 60,
            "macro": 30,
            "labor": 30,
            "rates": 20,
            "demographics": 15,
            "other": 25,
            "uncategorized": 10,
        },
        exclude_property_type_ids=set(),
    )
