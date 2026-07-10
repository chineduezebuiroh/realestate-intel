from __future__ import annotations
# regime/coordinate_engine.py

import math

import pandas as pd

from regime.axis_engine import score_axes


REQUIRED_AXES = {"demand", "supply"}


def _angle_degrees(x: float, y: float) -> float:
    angle = math.degrees(math.atan2(y, x))
    if angle < 0:
        angle += 360.0
    return angle


def build_coordinates(axes: pd.DataFrame | None = None) -> pd.DataFrame:
    if axes is None:
        axes = score_axes()

    axes = axes.copy()
    axes["date"] = pd.to_datetime(axes["date"])

    pivot = (
        axes.pivot_table(
            index=["geo_id", "date"],
            columns="axis",
            values="axis_score",
            aggfunc="first",
        )
        .reset_index()
    )

    available_axes = set(pivot.columns) - {"geo_id", "date"}
    missing_axes = REQUIRED_AXES - available_axes
    if missing_axes:
        raise ValueError(f"Missing required axes for coordinates: {sorted(missing_axes)}")

    diag = (
        axes.groupby(["geo_id", "date"])
        .agg(
            axis_count=("axis", "nunique"),
            min_axis_score=("axis_score", "min"),
            max_axis_score=("axis_score", "max"),
            max_axis_age_days=("max_dimension_age_days", "max"),
        )
        .reset_index()
    )

    out = pivot.merge(diag, on=["geo_id", "date"], how="left")

    out = out.dropna(subset=["supply", "demand"]).copy()

    out = out.rename(
        columns={
            "supply": "x_supply",
            "demand": "y_demand",
        }
    )

    out["radius"] = (
        (out["x_supply"].astype(float) ** 2)
        + (out["y_demand"].astype(float) ** 2)
    ) ** 0.5

    out["angle_degrees"] = [
        _angle_degrees(x, y)
        for x, y in zip(out["x_supply"], out["y_demand"])
    ]

    out["x_supply"] = out["x_supply"].clip(-1.0, 1.0)
    out["y_demand"] = out["y_demand"].clip(-1.0, 1.0)
    out["radius"] = out["radius"].clip(0.0, 2 ** 0.5)

    return out[
        [
            "geo_id",
            "date",
            "x_supply",
            "y_demand",
            "radius",
            "angle_degrees",
            "axis_count",
            "min_axis_score",
            "max_axis_score",
            "max_axis_age_days",
        ]
    ].sort_values(["geo_id", "date"]).reset_index(drop=True)
