from __future__ import annotations
# regime/geometry_engine.py

import pandas as pd

from regime.coordinate_engine import build_coordinates


def _major_regime(angle: float) -> str:
    if 0.0 <= angle < 90.0:
        return "expansion"
    if 90.0 <= angle < 180.0:
        return "tight_expansion"
    if 180.0 <= angle < 270.0:
        return "contraction"
    return "oversupply"


def _minor_regime(angle: float) -> str:
    # 8-sector wheel, 45 degrees each.
    if 0.0 <= angle < 45.0:
        return "balanced_growth"
    if 45.0 <= angle < 90.0:
        return "demand_led_growth"
    if 90.0 <= angle < 135.0:
        return "supply_constrained_growth"
    if 135.0 <= angle < 180.0:
        return "overheated_tight_market"
    if 180.0 <= angle < 225.0:
        return "demand_cooling_tight_supply"
    if 225.0 <= angle < 270.0:
        return "broad_contraction"
    if 270.0 <= angle < 315.0:
        return "oversupplied_weak_demand"
    return "supply_led_recovery"


def _quadrant(angle: float) -> int:
    if 0.0 <= angle < 90.0:
        return 1
    if 90.0 <= angle < 180.0:
        return 2
    if 180.0 <= angle < 270.0:
        return 3
    return 4


def _distance_to_boundary(angle: float) -> float:
    boundaries = [0.0, 45.0, 90.0, 135.0, 180.0, 225.0, 270.0, 315.0, 360.0]
    return min(abs(angle - b) for b in boundaries)


def assign_geometry(coordinates: pd.DataFrame | None = None) -> pd.DataFrame:
    if coordinates is None:
        coordinates = build_coordinates()

    out = coordinates.copy()
    out["date"] = pd.to_datetime(out["date"])

    out["quadrant"] = out["angle_degrees"].map(_quadrant)
    out["major_regime"] = out["angle_degrees"].map(_major_regime)
    out["minor_regime"] = out["angle_degrees"].map(_minor_regime)
    out["distance_to_boundary_degrees"] = out["angle_degrees"].map(_distance_to_boundary)

    return out[
        [
            "geo_id",
            "date",
            "x_supply",
            "y_demand",
            "radius",
            "angle_degrees",
            "quadrant",
            "major_regime",
            "minor_regime",
            "distance_to_boundary_degrees",
            "axis_count",
            "min_axis_score",
            "max_axis_score",
            "max_axis_age_days",
        ]
    ].sort_values(["geo_id", "date"]).reset_index(drop=True)
