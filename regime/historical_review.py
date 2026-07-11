from __future__ import annotations
# regime/historical_review.py

from pathlib import Path

import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore


REVIEW_PERIODS = [
    ("post_gfc_2009_2012", "2009-01-01", "2012-12-31"),
    ("expansion_2013_2019", "2013-01-01", "2019-12-31"),
    ("pandemic_2020_2021", "2020-01-01", "2021-12-31"),
    ("rate_shock_2022", "2022-01-01", "2022-12-31"),
    ("post_shock_2023_2026", "2023-01-01", "2026-12-31"),
]


def build_historical_review(
    run_id: str = "macro_regime_v1",
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    if geo_ids is None:
        geo_ids = [
            "district_of_columbia_dc__county",
            "alameda_county_ca__county",
        ]

    store = RegimeArtifactStore(artifact_root)

    trajectory = store.read_dataframe(
        run_id,
        "historical_trajectory",
        validation=True,
    )
    transition_audit = store.read_dataframe(
        run_id,
        "transition_audit",
        validation=True,
    )

    trajectory["date"] = pd.to_datetime(trajectory["date"])
    trajectory = trajectory[
        trajectory["geo_id"].isin(geo_ids)
    ].copy()

    period_rows: list[dict[str, object]] = []

    for geo_id in geo_ids:
        geo = trajectory[trajectory["geo_id"].eq(geo_id)]

        for period_name, start, end in REVIEW_PERIODS:
            period = geo[
                geo["date"].between(
                    pd.Timestamp(start),
                    pd.Timestamp(end),
                    inclusive="both",
                )
            ].copy()

            if period.empty:
                continue

            major_mode = period["major_regime"].mode()
            minor_mode = period["minor_regime"].mode()

            period_rows.append(
                {
                    "geo_id": geo_id,
                    "period": period_name,
                    "start_date": period["date"].min(),
                    "end_date": period["date"].max(),
                    "months": len(period),
                    "dominant_major_regime": (
                        major_mode.iloc[0] if not major_mode.empty else None
                    ),
                    "dominant_minor_regime": (
                        minor_mode.iloc[0] if not minor_mode.empty else None
                    ),
                    "major_transitions": int(period["major_changed"].sum()),
                    "minor_transitions": int(period["minor_changed"].sum()),
                    "avg_supply_score": period[
                        "supply_pressure_score"
                    ].mean(),
                    "avg_demand_score": period[
                        "demand_strength_score"
                    ].mean(),
                    "avg_regime_strength": period[
                        "regime_strength"
                    ].mean(),
                    "median_regime_strength": period[
                        "regime_strength"
                    ].median(),
                    "avg_boundary_distance": period[
                        "distance_to_boundary_degrees"
                    ].mean(),
                    "max_axis_age_days": period[
                        "max_axis_age_days"
                    ].max(),
                }
            )

    period_summary = pd.DataFrame(period_rows)

    annual_summary = (
        trajectory.assign(year=trajectory["date"].dt.year)
        .groupby(["geo_id", "year"])
        .agg(
            dominant_major_regime=(
                "major_regime",
                lambda s: s.mode().iloc[0] if not s.mode().empty else None,
            ),
            dominant_minor_regime=(
                "minor_regime",
                lambda s: s.mode().iloc[0] if not s.mode().empty else None,
            ),
            major_transitions=("major_changed", "sum"),
            minor_transitions=("minor_changed", "sum"),
            avg_supply_score=("supply_pressure_score", "mean"),
            avg_demand_score=("demand_strength_score", "mean"),
            avg_regime_strength=("regime_strength", "mean"),
            avg_boundary_distance=(
                "distance_to_boundary_degrees",
                "mean",
            ),
        )
        .reset_index()
    )

    major_distribution = (
        trajectory.assign(year=trajectory["date"].dt.year)
        .groupby(["geo_id", "year", "major_regime"])
        .size()
        .reset_index(name="months")
        .sort_values(
            ["geo_id", "year", "months"],
            ascending=[True, True, False],
        )
    )

    return {
        "period_summary": period_summary,
        "annual_summary": annual_summary,
        "major_distribution": major_distribution,
        "transition_audit": transition_audit,
    }
