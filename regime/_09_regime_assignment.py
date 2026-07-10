from __future__ import annotations
# regime/_09_regime_assignment.py

import pandas as pd

from regime._08_geometry_engine import assign_geometry


REGIME_ENGINE_VERSION = "C4.2e_v1"
REGIME_TYPE = "macro"


def assign_regimes(geometry: pd.DataFrame | None = None) -> pd.DataFrame:
    if geometry is None:
        geometry = assign_geometry()

    df = geometry.copy()
    df["date"] = pd.to_datetime(df["date"])

    df["regime_type"] = REGIME_TYPE
    df["regime_engine_version"] = REGIME_ENGINE_VERSION
    df["regime_strength"] = df["radius"]

    df = df.rename(
        columns={
            "x_supply": "supply_pressure_score",
            "y_demand": "demand_strength_score",
        }
    )

    return df[
        [
            "geo_id",
            "date",
            "regime_type",
            "major_regime",
            "minor_regime",
            "quadrant",
            "supply_pressure_score",
            "demand_strength_score",
            "regime_strength",
            "angle_degrees",
            "distance_to_boundary_degrees",
            "axis_count",
            "min_axis_score",
            "max_axis_score",
            "max_axis_age_days",
            "regime_engine_version",
        ]
    ].sort_values(["geo_id", "date"]).reset_index(drop=True)
