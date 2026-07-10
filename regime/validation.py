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


def build_transition_audit(
    trajectory: pd.DataFrame | None = None,
    geo_ids: list[str] | None = None,
) -> pd.DataFrame:
    if trajectory is None:
        trajectory = build_historical_trajectory(geo_ids=geo_ids)

    df = trajectory.copy()
    df["date"] = pd.to_datetime(df["date"])

    transitions = df[df["major_changed"] | df["minor_changed"]].copy()

    if transitions.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "transition_type",
                "transition_count",
                "avg_regime_strength",
                "median_regime_strength",
                "min_regime_strength",
                "p25_regime_strength",
                "p75_regime_strength",
                "avg_boundary_distance",
                "median_boundary_distance",
                "min_boundary_distance",
                "p25_boundary_distance",
                "p75_boundary_distance",
                "avg_abs_angle_delta",
                "median_abs_angle_delta",
                "max_abs_angle_delta",
                "avg_abs_supply_delta",
                "avg_abs_demand_delta",
                "avg_abs_radius_delta",
                "max_axis_age_days",
            ]
        )

    transitions["transition_type"] = transitions.apply(
        lambda r: (
            "major_and_minor"
            if bool(r["major_changed"]) and bool(r["minor_changed"])
            else "major_only"
            if bool(r["major_changed"])
            else "minor_only"
        ),
        axis=1,
    )

    transitions["abs_angle_delta"] = transitions["delta_angle_degrees"].abs()
    transitions["abs_supply_delta"] = transitions["delta_supply_pressure_score"].abs()
    transitions["abs_demand_delta"] = transitions["delta_demand_strength_score"].abs()
    transitions["abs_radius_delta"] = transitions["delta_regime_strength"].abs()

    def q25(s: pd.Series) -> float:
        return float(s.quantile(0.25))

    def q75(s: pd.Series) -> float:
        return float(s.quantile(0.75))

    out = (
        transitions.groupby(["geo_id", "transition_type"])
        .agg(
            transition_count=("date", "size"),
            avg_regime_strength=("regime_strength", "mean"),
            median_regime_strength=("regime_strength", "median"),
            min_regime_strength=("regime_strength", "min"),
            p25_regime_strength=("regime_strength", q25),
            p75_regime_strength=("regime_strength", q75),
            avg_boundary_distance=("distance_to_boundary_degrees", "mean"),
            median_boundary_distance=("distance_to_boundary_degrees", "median"),
            min_boundary_distance=("distance_to_boundary_degrees", "min"),
            p25_boundary_distance=("distance_to_boundary_degrees", q25),
            p75_boundary_distance=("distance_to_boundary_degrees", q75),
            avg_abs_angle_delta=("abs_angle_delta", "mean"),
            median_abs_angle_delta=("abs_angle_delta", "median"),
            max_abs_angle_delta=("abs_angle_delta", "max"),
            avg_abs_supply_delta=("abs_supply_delta", "mean"),
            avg_abs_demand_delta=("abs_demand_delta", "mean"),
            avg_abs_radius_delta=("abs_radius_delta", "mean"),
            max_axis_age_days=("max_axis_age_days", "max"),
        )
        .reset_index()
        .sort_values(["geo_id", "transition_type"])
    )

    return out


def build_transition_events(
    trajectory: pd.DataFrame | None = None,
    geo_ids: list[str] | None = None,
) -> pd.DataFrame:
    if trajectory is None:
        trajectory = build_historical_trajectory(geo_ids=geo_ids)

    df = trajectory.copy()
    df["date"] = pd.to_datetime(df["date"])

    transitions = df[df["major_changed"] | df["minor_changed"]].copy()

    if transitions.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "transition_type",
                "previous_major_regime",
                "major_regime",
                "previous_minor_regime",
                "minor_regime",
                "regime_strength",
                "distance_to_boundary_degrees",
                "angle_degrees",
                "delta_angle_degrees",
                "supply_pressure_score",
                "demand_strength_score",
                "delta_supply_pressure_score",
                "delta_demand_strength_score",
                "delta_regime_strength",
                "max_axis_age_days",
            ]
        )

    transitions["transition_type"] = transitions.apply(
        lambda r: (
            "major_and_minor"
            if bool(r["major_changed"]) and bool(r["minor_changed"])
            else "major_only"
            if bool(r["major_changed"])
            else "minor_only"
        ),
        axis=1,
    )

    return transitions[
        [
            "geo_id",
            "date",
            "transition_type",
            "previous_major_regime",
            "major_regime",
            "previous_minor_regime",
            "minor_regime",
            "regime_strength",
            "distance_to_boundary_degrees",
            "angle_degrees",
            "delta_angle_degrees",
            "supply_pressure_score",
            "demand_strength_score",
            "delta_supply_pressure_score",
            "delta_demand_strength_score",
            "delta_regime_strength",
            "max_axis_age_days",
        ]
    ].sort_values(["geo_id", "date"]).reset_index(drop=True)
