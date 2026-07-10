from __future__ import annotations
# regime/geometry_engine.py

import pandas as pd

from regime.coordinate_engine import build_coordinates


def _major_regime(angle: float) -> str:
    if angle <= 45.0 or angle > 315.0:
        return "hypersupply"
    if angle <= 135.0:
        return "expansion"
    if angle <= 225.0:
        return "recovery"
    return "recession"


def _minor_regime(angle: float) -> str:
    if angle <= 15.0 or angle > 345.0:
        return "mid_hypersupply"
    if angle <= 45.0:
        return "early_hypersupply"
    if angle <= 75.0:
        return "late_expansion"
    if angle <= 105.0:
        return "mid_expansion"
    if angle <= 135.0:
        return "early_expansion"
    if angle <= 165.0:
        return "late_recovery"
    if angle <= 195.0:
        return "mid_recovery"
    if angle <= 225.0:
        return "early_recovery"
    if angle <= 255.0:
        return "late_recession"
    if angle <= 285.0:
        return "mid_recession"
    if angle <= 315.0:
        return "early_recession"
    return "late_hypersupply"


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
