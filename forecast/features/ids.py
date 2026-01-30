from __future__ import annotations
# forecast/features/ids.py

import re
from typing import Optional, Tuple

from .specs import FeatureSpec


_LAG_RE = re.compile(r"^(?P<base>.+)_lag(?P<lag>\d+)$")


def parse_feature_id(fid: str) -> Tuple[str, int]:
    """
    fid example:
      avg_sale_to_list__20016_dc__13__redfin_lag12
    returns:
      (base_name="avg_sale_to_list__20016_dc__13__redfin", lag=12)
    """
    m = _LAG_RE.match(str(fid))
    if not m:
        raise ValueError(f"Invalid feature_id (expected *_lagN): {fid}")
    return m.group("base"), int(m.group("lag"))


def parse_base_name(base: str) -> Tuple[str, str, Optional[str], Optional[str]]:
    """
    base example:
      metric_id__geo_id__property_type_id__source_id
    """
    parts = str(base).split("__")
    if len(parts) != 4:
        raise ValueError(f"Invalid base feature name (expected 4 parts): {base}")
    metric_id, geo_id, pt_id, source_id = parts
    pt_id = pt_id if pt_id not in ("", "None", "null") else None
    source_id = source_id if source_id not in ("", "None", "null") else None
    return metric_id, geo_id, pt_id, source_id


def parse_feature_id_to_spec(feature_id: str) -> FeatureSpec:
    """
    Supports BOTH:
      - v1 (legacy): {metric}__{geo}__{pt}_lag{lag}
      - v2 (canonical): {metric}__{geo}__{pt}__{source}_lag{lag}
    """
    base, lag_part = str(feature_id).rsplit("_lag", 1)
    lag = int(lag_part)

    parts = base.split("__")

    if len(parts) == 3:
        metric_id, geo_id, pt_id = parts
        source_id = None
        name = f"{metric_id}__{geo_id}__{pt_id}"
    elif len(parts) == 4:
        metric_id, geo_id, pt_id, source_id = parts
        name = f"{metric_id}__{geo_id}__{pt_id}__{source_id}"
    else:
        raise ValueError(f"Invalid feature base name (expected 3 or 4 parts): {base}")

    return FeatureSpec(
        name=name,
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=pt_id,
        source_id=source_id,
        lags=(lag,),
    )


def specs_from_selected_feature_ids(feature_ids: list[str]) -> list[FeatureSpec]:
    """
    Enforces your invariant: selected features MUST be v2 (4-part base: metric__geo__pt__source).
    Aggregates lags onto one FeatureSpec per base.
    """
    by_base: dict[tuple[str, str, str, str], set[int]] = {}

    for fid in feature_ids:
        spec = parse_feature_id_to_spec(fid)

        if not spec.source_id:
            raise ValueError(f"feature_id missing source_id (would break 4-part base): {fid}")

        m = str(spec.metric_id)
        g = str(spec.geo_id)
        pt = str(spec.property_type_id)
        src = str(spec.source_id)

        key = (m, g, pt, src)
        by_base.setdefault(key, set()).update(spec.lags)

    out: list[FeatureSpec] = []
    for (m, g, pt, src), lags in sorted(by_base.items()):
        name = f"{m}__{g}__{pt}__{src}"
        out.append(
            FeatureSpec(
                name=name,
                metric_id=m,
                geo_id=g,
                property_type_id=pt,
                source_id=src,
                lags=tuple(sorted(lags)),
            )
        )
    return out
