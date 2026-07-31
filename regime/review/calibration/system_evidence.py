"""Adapters for immutable downstream calibration evidence.

This module deliberately contains no regime-engine calculation.  It normalizes
already materialized engine outputs into a small, campaign-neutral review
contract which the bundle renderer can validate, copy, and plot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import pandas as pd


SYSTEM_SECTIONS = (
    "dimension_chronology",
    "axis_chronology",
    "coordinate_trajectories",
    "regime_chronology",
    "transition_windows",
    "cancellation_diagnostics",
)

# This is an engine-produced metric-level artifact, but it is presented with
# Phase A technical evidence so reviewers can distinguish feature values from
# their already-normalized downstream metric score.
NORMALIZED_METRIC_SECTION = "normalized_metric_score_chronology"
SYSTEM_EVIDENCE_CONTRACT_VERSION = "inventory_system_evidence_v2"


def _adapt_chronology(
    frame: pd.DataFrame, *, artifact: str, required: set[str],
    chronology_columns: tuple[str, ...] = ("date",),
) -> pd.DataFrame:
    """Map an artifact's documented chronology to canonical review ``date``.

    ``aligned_metric_scores`` permits evaluation_date and legacy date inputs;
    when both exist they must describe the same evaluation chronology.
    """
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{artifact} is missing required columns: {missing}")
    present = [column for column in chronology_columns if column in frame.columns]
    if not present:
        raise ValueError(
            f"{artifact} is missing chronology column; expected one of {list(chronology_columns)}"
        )
    parsed: dict[str, pd.Series] = {}
    for column in present:
        values = pd.to_datetime(frame[column], format="mixed", errors="coerce")
        if values.isna().any():
            raise ValueError(f"{artifact} contains invalid {column} values")
        parsed[column] = values
    canonical_source = "evaluation_date" if "evaluation_date" in parsed else "date"
    for column, values in parsed.items():
        if column != canonical_source and not values.equals(parsed[canonical_source]):
            raise ValueError(f"{artifact} has conflicting date and evaluation_date chronology")
    work = frame.copy(deep=True)
    work["date"] = parsed[canonical_source]
    if "evaluation_date" in work:
        work["evaluation_date"] = parsed["evaluation_date"]
    if "metric_date" in work:
        metric_dates = pd.to_datetime(work["metric_date"], format="mixed", errors="coerce")
        if work["metric_date"].notna().any() and metric_dates[work["metric_date"].notna()].isna().any():
            raise ValueError(f"{artifact} contains invalid metric_date values")
        work["metric_date"] = metric_dates
    leading = ["geo_id", "date"]
    lineage = [c for c in ("evaluation_date", "metric_date", "metric_age_days") if c in work and c not in leading]
    remaining = [c for c in work.columns if c not in {*leading, *lineage}]
    return work[[*leading, *lineage, *remaining]].sort_values(
        ["geo_id", "date"], kind="mergesort"
    ).reset_index(drop=True)


def adapt_aligned_metric_scores(frame: pd.DataFrame) -> pd.DataFrame:
    return _adapt_chronology(
        frame, artifact="aligned_metric_scores",
        required={"geo_id", "canonical_metric_key", "metric_score"},
        chronology_columns=("evaluation_date", "date"),
    )


def adapt_dimension_scores(frame: pd.DataFrame) -> pd.DataFrame:
    return _adapt_chronology(frame, artifact="dimension_scores", required={"geo_id", "dimension", "dimension_score"})


def adapt_axis_scores(frame: pd.DataFrame) -> pd.DataFrame:
    return _adapt_chronology(frame, artifact="axis_scores", required={"geo_id", "axis", "axis_score"})


def adapt_coordinates(frame: pd.DataFrame) -> pd.DataFrame:
    return _adapt_chronology(frame, artifact="coordinates", required={"geo_id", "x_supply", "y_demand"})


def adapt_regime_assignments(frame: pd.DataFrame) -> pd.DataFrame:
    return _adapt_chronology(
        frame, artifact="regime_assignments",
        required={"geo_id", "major_regime", "minor_regime", "quadrant"},
    )


ARTIFACT_ADAPTERS = {
    "aligned_metric_scores": adapt_aligned_metric_scores,
    "dimension_scores": adapt_dimension_scores,
    "axis_scores": adapt_axis_scores,
    "coordinates": adapt_coordinates,
    "regime_assignments": adapt_regime_assignments,
}


@dataclass(frozen=True, slots=True)
class CalibrationSystemEvidence:
    """Persisted engine outputs and deterministic review selections."""

    campaign_id: str
    campaign_version: str
    candidate_policy_ids: tuple[str, ...]
    incumbent_policy_id: str
    baseline_policy_id: str
    target_metric: str
    target_dimension: str
    target_axis: str
    tables: Mapping[str, pd.DataFrame]
    representative_geography_rule: str
    transition_window_rule: str

    def copied_tables(self) -> dict[str, pd.DataFrame]:
        return {name: frame.copy(deep=True) for name, frame in self.tables.items()}


def validate_system_evidence(evidence: CalibrationSystemEvidence) -> None:
    """Fail closed when any required immutable system view is inconsistent."""
    missing = sorted(set(SYSTEM_SECTIONS).difference(evidence.tables))
    if missing:
        raise ValueError(f"Missing required system evidence: {missing}")
    identities = {"baseline", *evidence.candidate_policy_ids}
    selected: set[str] | None = None
    for name in SYSTEM_SECTIONS:
        frame = evidence.tables[name]
        if frame.empty:
            raise ValueError(f"Required system evidence is empty: {name}")
        required = {"campaign_id", "campaign_version", "series_id", "geo_id"}
        absent = sorted(required.difference(frame.columns))
        if absent:
            raise ValueError(f"{name} is missing identity columns: {absent}")
        semantic_columns = {
            "dimension_chronology": {"date", "dimension_score"},
            "axis_chronology": {"date", "axis_score"},
            "coordinate_trajectories": {"date", "x_supply", "y_demand"},
            "regime_chronology": {"date", "major_regime", "minor_regime", "quadrant"},
            "transition_windows": {"date", "axis_score", "window_center_date", "window_id"},
            "cancellation_diagnostics": {"date", "dimension_cancellation_ratio"},
        }[name]
        absent_semantics = sorted(semantic_columns.difference(frame.columns))
        if absent_semantics:
            raise ValueError(f"{name} is missing semantic columns: {absent_semantics}")
        if set(frame["campaign_id"].astype(str)) != {evidence.campaign_id} or set(
            frame["campaign_version"].astype(str)
        ) != {evidence.campaign_version}:
            raise ValueError(f"{name} campaign identity mismatch")
        if set(frame["series_id"].astype(str)) != identities:
            raise ValueError(f"{name} candidate identity mismatch")
        geos = set(frame["geo_id"].astype(str))
        selected = geos if selected is None else selected
        if geos != selected:
            raise ValueError(f"{name} representative geography mismatch")
        keys = [c for c in ("series_id", "geo_id", "date", "window_id") if c in frame]
        if keys and frame.duplicated(keys).any():
            raise ValueError(f"{name} contains duplicate evidence keys")
    if not evidence.representative_geography_rule.strip():
        raise ValueError("Representative geography selection rule is required")
    if not evidence.transition_window_rule.strip():
        raise ValueError("System transition-window selection rule is required")
    if NORMALIZED_METRIC_SECTION in evidence.tables:
        frame = evidence.tables[NORMALIZED_METRIC_SECTION]
        required = {"campaign_id", "campaign_version", "series_id", "geo_id", "date", "metric_score"}
        absent = sorted(required.difference(frame.columns))
        if absent:
            raise ValueError(f"{NORMALIZED_METRIC_SECTION} is missing columns: {absent}")


def assemble_inventory_system_evidence(
    *, campaign, baseline_artifacts: Mapping[str, pd.DataFrame],
    candidate_artifacts: Mapping[str, Mapping[str, pd.DataFrame]],
    max_geographies: int = 6,
) -> CalibrationSystemEvidence:
    """Adapt already-produced baseline/candidate artifacts; never run engine stages."""
    from regime.diagnostics.axis_contribution import build_axis_cancellation_from_frames

    required_order = ("aligned_metric_scores", "dimension_scores", "axis_scores", "coordinates", "regime_assignments")
    required = set(required_order)
    if required.difference(baseline_artifacts):
        raise ValueError(f"Baseline system artifacts missing: {sorted(required.difference(baseline_artifacts))}")
    if tuple(candidate_artifacts) != campaign.candidate_policy_ids:
        raise ValueError("Candidate system artifact order does not match campaign")
    for candidate_id, artifacts in candidate_artifacts.items():
        missing = required.difference(artifacts)
        if missing:
            raise ValueError(f"{candidate_id} system artifacts missing: {sorted(missing)}")

    normalized_baseline = {name: ARTIFACT_ADAPTERS[name](baseline_artifacts[name]) for name in required_order}
    normalized_candidates = {
        candidate_id: {name: ARTIFACT_ADAPTERS[name](artifacts[name]) for name in required_order}
        for candidate_id, artifacts in candidate_artifacts.items()
    }
    baseline_coordinates = normalized_baseline["coordinates"]
    divergence = []
    for candidate_id, artifacts in normalized_candidates.items():
        merged = baseline_coordinates[["geo_id", "date", "x_supply"]].merge(
            artifacts["coordinates"][["geo_id", "date", "x_supply"]],
            on=["geo_id", "date"], suffixes=("_baseline", "_candidate"), validate="one_to_one",
        )
        merged["absolute_divergence"] = (merged["x_supply_candidate"] - merged["x_supply_baseline"]).abs()
        divergence.append(merged.groupby("geo_id", as_index=False)["absolute_divergence"].max())
    geo_scores = pd.concat(divergence).groupby("geo_id")["absolute_divergence"].max().sort_values(
        ascending=False, kind="mergesort"
    )
    dc = "district_of_columbia_dc__county"
    selected: list[str] = [dc] if dc in geo_scores.index else []
    for geo_id in list(geo_scores.index) + list(geo_scores.sort_values(kind="mergesort").index):
        if geo_id not in selected:
            selected.append(str(geo_id))
        if len(selected) >= min(max_geographies, len(geo_scores)):
            break
    if not selected:
        raise ValueError("No representative geography has coordinate overlap")

    identities = [("baseline", normalized_baseline)] + list(normalized_candidates.items())
    common = {"campaign_id": campaign.campaign_id, "campaign_version": campaign.campaign_version}

    def combine(name: str, predicate=None) -> pd.DataFrame:
        rows = []
        for series_id, artifacts in identities:
            frame = artifacts[name].copy()
            frame = frame[frame["geo_id"].isin(selected)]
            if predicate is not None:
                frame = predicate(frame)
            frame.insert(0, "series_id", series_id)
            frame.insert(0, "campaign_version", common["campaign_version"])
            frame.insert(0, "campaign_id", common["campaign_id"])
            rows.append(frame)
        return pd.concat(rows, ignore_index=True).sort_values(
            ["geo_id", "series_id", "date"], kind="mergesort"
        ).reset_index(drop=True)

    dimension = combine("dimension_scores", lambda f: f[f["dimension"].eq(campaign.target_dimension)])
    axis = combine("axis_scores", lambda f: f[f["axis"].eq(campaign.target_axis)])
    coordinates = combine("coordinates")
    regimes = combine("regime_assignments")
    metric_scores = combine(
        "aligned_metric_scores",
        lambda f: f[f["canonical_metric_key"].eq(campaign.target_metric)],
    )

    cancellation_rows = []
    for series_id, artifacts in identities:
        diagnostic = build_axis_cancellation_from_frames(
            dimension_scores=artifacts["dimension_scores"], axis_scores=artifacts["axis_scores"],
            geo_ids=selected, axis=campaign.target_axis,
        )
        diagnostic.insert(0, "series_id", series_id)
        diagnostic.insert(0, "campaign_version", common["campaign_version"])
        diagnostic.insert(0, "campaign_id", common["campaign_id"])
        cancellation_rows.append(diagnostic)
    cancellation = pd.concat(cancellation_rows, ignore_index=True).sort_values(
        ["geo_id", "series_id", "date"], kind="mergesort"
    ).reset_index(drop=True)

    windows = []
    baseline_axis = axis[axis["series_id"].eq("baseline")]
    for geo_id, frame in baseline_axis.groupby("geo_id", sort=True):
        frame = frame.sort_values("date", kind="mergesort")
        changes = pd.to_numeric(frame["axis_score"], errors="coerce").diff().abs()
        if changes.dropna().empty:
            raise ValueError(f"No system transition observation for {geo_id}")
        center = frame.loc[changes.idxmax(), "date"]
        dates = frame["date"].tolist(); position = dates.index(center)
        start, end = dates[max(0, position - 3)], dates[min(len(dates) - 1, position + 3)]
        part = axis[axis["geo_id"].eq(geo_id) & axis["date"].between(start, end)].copy()
        part["window_id"] = "largest_absolute_incumbent_supply_axis_change"
        part["window_center_date"] = center
        windows.append(part)
    transitions = pd.concat(windows, ignore_index=True)

    return CalibrationSystemEvidence(
        campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
        candidate_policy_ids=campaign.candidate_policy_ids,
        incumbent_policy_id=campaign.incumbent_policy_id, baseline_policy_id=campaign.baseline_policy_id,
        target_metric=campaign.target_metric, target_dimension=campaign.target_dimension,
        target_axis=campaign.target_axis,
        tables={"dimension_chronology": dimension, "axis_chronology": axis,
                "coordinate_trajectories": coordinates, "regime_chronology": regimes,
                "transition_windows": transitions, "cancellation_diagnostics": cancellation,
                NORMALIZED_METRIC_SECTION: metric_scores},
        representative_geography_rule=(
            "District of Columbia when present, then maximum and minimum candidate/incumbent "
            "Supply-coordinate divergence with canonical geo_id tie-breaking; maximum six"
        ),
        transition_window_rule=(
            "largest absolute month-over-month incumbent Supply-axis change per selected geography; "
            "three observations of context on each side"
        ),
    )
