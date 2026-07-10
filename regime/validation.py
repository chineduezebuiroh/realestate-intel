from __future__ import annotations
# regime/validation.py

import pandas as pd

from regime.regime_assignment import assign_regimes


DEFAULT_VALIDATION_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]


def _angle_delta_degrees(current: pd.Series, previous: pd.Series) -> pd.Series:
    """
    Smallest signed angular change in degrees.

    Returns values in [-180, 180].
    """
    return ((current - previous + 180.0) % 360.0) - 180.0


def build_historical_trajectory(
    regimes: pd.DataFrame | None = None,
    geo_ids: list[str] | None = None,
) -> pd.DataFrame:
    if regimes is None:
        regimes = assign_regimes()

    if geo_ids is None:
        geo_ids = DEFAULT_VALIDATION_GEOS

    df = regimes.copy()
    df["date"] = pd.to_datetime(df["date"])

    df = df[df["geo_id"].isin(geo_ids)].copy()

    if df.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "major_regime",
                "minor_regime",
                "previous_major_regime",
                "previous_minor_regime",
                "major_changed",
                "minor_changed",
                "supply_pressure_score",
                "demand_strength_score",
                "regime_strength",
                "angle_degrees",
                "distance_to_boundary_degrees",
                "max_axis_age_days",
                "delta_supply_pressure_score",
                "delta_demand_strength_score",
                "delta_regime_strength",
                "delta_angle_degrees",
            ]
        )

    df = df.sort_values(["geo_id", "date"]).copy()

    by_geo = df.groupby("geo_id", group_keys=False)

    df["previous_major_regime"] = by_geo["major_regime"].shift(1)
    df["previous_minor_regime"] = by_geo["minor_regime"].shift(1)

    df["major_changed"] = (
        df["previous_major_regime"].notna()
        & (df["major_regime"] != df["previous_major_regime"])
    )

    df["minor_changed"] = (
        df["previous_minor_regime"].notna()
        & (df["minor_regime"] != df["previous_minor_regime"])
    )

    df["delta_supply_pressure_score"] = by_geo["supply_pressure_score"].diff()
    df["delta_demand_strength_score"] = by_geo["demand_strength_score"].diff()
    df["delta_regime_strength"] = by_geo["regime_strength"].diff()

    previous_angle = by_geo["angle_degrees"].shift(1)
    df["delta_angle_degrees"] = _angle_delta_degrees(
        df["angle_degrees"],
        previous_angle,
    )

    return df[
        [
            "geo_id",
            "date",
            "major_regime",
            "minor_regime",
            "previous_major_regime",
            "previous_minor_regime",
            "major_changed",
            "minor_changed",
            "supply_pressure_score",
            "demand_strength_score",
            "regime_strength",
            "angle_degrees",
            "distance_to_boundary_degrees",
            "max_axis_age_days",
            "delta_supply_pressure_score",
            "delta_demand_strength_score",
            "delta_regime_strength",
            "delta_angle_degrees",
        ]
    ].reset_index(drop=True)
