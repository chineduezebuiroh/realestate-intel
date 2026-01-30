from __future__ import annotations
# forecast/features/specs.py

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Optional, Tuple


@dataclass
class TargetSpec:
    metric_id: str
    geo_id: str
    # For Redfin, this is '-1', '6', '13', etc. For non-Redfin, use None -> 'all'.
    property_type_id: Optional[str] = None
    # As-of handling
    data_asof: Optional[date] = None
    asof_by_source: Optional[Dict[str, date]] = None


@dataclass(frozen=True)
class FeatureSpec:
    """
    Canonical FeatureSpec:
      - name is a base key like: metric__geo__pt__source
      - lags are stored as a tuple of ints
    """
    name: str
    metric_id: str
    geo_id: str
    property_type_id: Optional[str]
    source_id: Optional[str] = None
    category: Optional[str] = None
    frequency: Optional[str] = None
    lags: Tuple[int, ...] = field(default_factory=tuple)
