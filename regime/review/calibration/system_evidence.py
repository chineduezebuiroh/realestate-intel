"""Adapters for immutable downstream calibration evidence.

This module deliberately contains no regime-engine calculation.  It normalizes
already materialized engine outputs into a small, campaign-neutral review
contract which the bundle renderer can validate, copy, and plot.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
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
SYSTEM_EVIDENCE_CONTRACT_VERSION = "inventory_system_evidence_v3"
REPRESENTATIVE_GEOGRAPHY_DIAGNOSTIC = "representative_geography_diagnostic"
REPRESENTATIVE_GEOGRAPHY_SELECTION = "representative_geography_selection"
PREFERRED_REVIEW_GEOGRAPHIES = (
    ("district_of_columbia_dc__county", "DC", "dense urban anchor; strongest known historical BPS context"),
    ("essex_county_nj__county", "NJ", "dense urban New Jersey comparison; required user-selected anchor"),
    ("montgomery_county_md__county", "MD", "affluent suburban Maryland context"),
    ("prince_george_s_county_md__county", "MD", "contrasting suburban/growth Maryland context"),
    ("fairfax_county_va__county", "VA", "major Virginia employment-center context"),
    ("san_francisco_county_ca__county", "CA", "dense urban California context"),
    ("los_angeles_county_ca__county", "CA", "large, diverse California market context"),
)
REPRESENTATIVE_TIE_BREAK_RULE = (
    "descending all-three Supply-date share, fully populated Supply observations, "
    "permit_activity observations, permit_intensity observations, active_inventory observations; "
    "ascending canonical geo_id"
)


def _manifest_counties(manifest_path: str | Path) -> pd.DataFrame:
    manifest = pd.read_csv(manifest_path, dtype=str)
    required = {"geo_slug", "geo_name", "level"}
    if not required.issubset(manifest):
        raise ValueError(f"Authoritative geography manifest is missing columns: {sorted(required.difference(manifest))}")
    counties = manifest[manifest["level"].eq("county")].copy()
    counties["state"] = counties["geo_name"].str.extract(r",\s*(CA|MD|VA|NJ|DC)(?:\s|$)", expand=False)
    counties = counties[counties["state"].notna()]
    if counties["geo_slug"].duplicated().any():
        raise ValueError("Authoritative geography manifest contains duplicate county geo_slug values")
    return counties[["geo_slug", "geo_name", "state"]].sort_values("geo_slug", kind="mergesort")


def build_representative_geography_diagnostic(
    *, baseline_artifacts: Mapping[str, pd.DataFrame], manifest_path: str | Path,
) -> pd.DataFrame:
    """Describe review coverage for the governed manifest universe without changing policy."""
    manifest = _manifest_counties(manifest_path)
    metrics = adapt_aligned_metric_scores(baseline_artifacts["aligned_metric_scores"])
    dimensions = adapt_dimension_scores(baseline_artifacts["dimension_scores"])
    metric_keys = ("active_inventory", "permit_activity", "permit_intensity")
    metrics = metrics[metrics["canonical_metric_key"].isin(metric_keys)].copy()
    metrics["metric_score"] = pd.to_numeric(metrics["metric_score"], errors="coerce")
    supply = dimensions[dimensions["dimension"].astype(str).str.lower().eq("supply")].copy()
    supply["dimension_score"] = pd.to_numeric(supply["dimension_score"], errors="coerce")
    artifact_sets = {
        name: set(ARTIFACT_ADAPTERS[name](baseline_artifacts[name])["geo_id"].astype(str))
        for name in ARTIFACT_ADAPTERS
    }
    rows = []
    for item in manifest.itertuples(index=False):
        geo_id = str(item.geo_slug)
        geo_metrics = metrics[metrics["geo_id"].astype(str).eq(geo_id)]
        geo_supply = supply[supply["geo_id"].astype(str).eq(geo_id) & supply["dimension_score"].notna()]
        supply_dates = set(geo_supply["date"])
        facts: dict[str, object] = {
            "geo_id": geo_id, "geo_name": item.geo_name, "state": item.state,
            "present_in_manifest": True,
        }
        available_dates = {}
        for key in metric_keys:
            part = geo_metrics[geo_metrics["canonical_metric_key"].eq(key) & geo_metrics["metric_score"].notna()]
            dates = set(part["date"])
            available_dates[key] = dates
            facts[f"first_{key}_date"] = part["date"].min()
            facts[f"{key}_observation_count"] = len(part)
        all_dates = set.intersection(*(available_dates[key] for key in metric_keys))
        fully = sorted(supply_dates & all_dates)
        facts["first_fully_populated_supply_date"] = fully[0] if fully else pd.NaT
        facts["fully_populated_supply_observation_count"] = len(fully)
        denominator = len(supply_dates)
        for key in metric_keys:
            facts[f"share_supply_dates_with_{key}"] = (
                len(supply_dates & available_dates[key]) / denominator if denominator else np.nan
            )
        facts["share_supply_dates_with_all_three"] = len(supply_dates & all_dates) / denominator if denominator else np.nan
        permit = geo_metrics.pivot(index="date", columns="canonical_metric_key", values="metric_score")
        facts["permit_activity_score_volatility"] = permit.get("permit_activity", pd.Series(dtype=float)).std()
        facts["permit_intensity_score_volatility"] = permit.get("permit_intensity", pd.Series(dtype=float)).std()
        facts["permit_activity_permit_intensity_score_correlation"] = (
            permit["permit_activity"].corr(permit["permit_intensity"])
            if {"permit_activity", "permit_intensity"}.issubset(permit) else np.nan
        )
        missing_artifacts = sorted(name for name, geos in artifact_sets.items() if geo_id not in geos)
        facts["required_artifacts_present"] = not missing_artifacts
        facts["missing_required_artifacts"] = ";".join(missing_artifacts)
        facts["valid_review_evidence"] = bool(
            not missing_artifacts and denominator
            and all(facts[f"{key}_observation_count"] > 0 for key in metric_keys)
        )
        facts["invalid_evidence_reason"] = "" if facts["valid_review_evidence"] else (
            f"missing required artifacts: {','.join(missing_artifacts)}" if missing_artifacts
            else "missing active_inventory, permit_activity, permit_intensity, or Supply observations"
        )
        rows.append(facts)
    return pd.DataFrame(rows).sort_values("geo_id", kind="mergesort").reset_index(drop=True)


def select_representative_geographies(diagnostic: pd.DataFrame) -> pd.DataFrame:
    """Resolve preferred anchors and explicit deterministic fallbacks."""
    ranked = diagnostic.sort_values(
        ["share_supply_dates_with_all_three", "fully_populated_supply_observation_count",
         "permit_activity_observation_count", "permit_intensity_observation_count",
         "active_inventory_observation_count", "geo_id"],
        ascending=[False, False, False, False, False, True], na_position="last", kind="mergesort",
    )
    by_geo = diagnostic.set_index("geo_id", drop=False)
    selected: set[str] = set()
    reserved_preferred = {item[0] for item in PREFERRED_REVIEW_GEOGRAPHIES}
    rows = []
    for preferred, state, reason in PREFERRED_REVIEW_GEOGRAPHIES:
        present = preferred in by_geo.index
        valid = bool(present and by_geo.loc[preferred, "valid_review_evidence"])
        final = preferred if valid else None
        fallback_reason = None
        role = "preferred_anchor"
        if not valid:
            failure = "absent from authoritative generated manifest" if not present else str(by_geo.loc[preferred, "invalid_evidence_reason"])
            same_state = ranked[
                ranked["state"].eq(state) & ranked["valid_review_evidence"].eq(True)
                & ~ranked["geo_id"].isin(selected)
                & ~ranked["geo_id"].isin(reserved_preferred)
            ]
            pool = same_state
            fallback_scope = "same-state"
            if pool.empty:
                pool = ranked[ranked["valid_review_evidence"].eq(True)
                              & ~ranked["geo_id"].isin(selected)
                              & ~ranked["geo_id"].isin(reserved_preferred)]
                fallback_scope = "best remaining manifest county; no same-state replacement available"
            if not pool.empty:
                final = str(pool.iloc[0]["geo_id"])
                role = "fallback"
                fallback_reason = f"{failure}; {fallback_scope} fallback selected"
            else:
                role = "unresolved"
                fallback_reason = f"{failure}; no valid manifest fallback available"
        if final is not None:
            selected.add(final)
            coverage = by_geo.loc[final].to_dict()
        else:
            coverage = {}
        rows.append({
            "preferred_geo_id": preferred, "final_selected_geo_id": final,
            "preferred_present": present, "preferred_valid_evidence": valid,
            "selection_role": role, "selection_reason": reason,
            "fallback_reason": fallback_reason,
            "coverage_facts": json.dumps(coverage, sort_keys=True, default=str),
            "deterministic_tie_break_rule": REPRESENTATIVE_TIE_BREAK_RULE,
        })
    return pd.DataFrame(rows)


def _governed_axis_scope(campaign) -> tuple[str, ...]:
    return tuple(dict.fromkeys((campaign.target_axis, *campaign.supporting_coordinate_axes)))


def _validate_transition_metric_uniqueness(metric_scores: pd.DataFrame) -> None:
    keys = ["geo_id", "date", "series_id"]
    if metric_scores.duplicated(keys).any():
        raise ValueError("Transition metric scores contain duplicate geo_id/date/series_id rows")


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
            "axis_chronology": {"date", "axis", "axis_score"},
            "coordinate_trajectories": {"date", "x_supply", "y_demand"},
            "regime_chronology": {"date", "major_regime", "minor_regime", "quadrant"},
            "transition_windows": {
                "date", "axis", "axis_score",
                "window_center_date", "window_id",
            },
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
        keys = [
            column
            for column in (
                "series_id",
                "geo_id",
                "date",
                "dimension",
                "axis",
                "window_id",
            )
            if column in frame.columns
        ]
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
    diagnostic = evidence.tables.get(REPRESENTATIVE_GEOGRAPHY_DIAGNOSTIC)
    selection = evidence.tables.get(REPRESENTATIVE_GEOGRAPHY_SELECTION)
    if (diagnostic is None) != (selection is None):
        raise ValueError("Representative geography diagnostic and selection must be supplied together")
    if diagnostic is not None and selection is not None:
        required_diagnostic = {"geo_id", "present_in_manifest", "valid_review_evidence"}
        required_selection = {"preferred_geo_id", "final_selected_geo_id", "preferred_present",
                              "preferred_valid_evidence", "selection_role", "selection_reason",
                              "fallback_reason", "coverage_facts", "deterministic_tie_break_rule"}
        if not required_diagnostic.issubset(diagnostic):
            raise ValueError("Representative geography diagnostic schema is incomplete")
        if not required_selection.issubset(selection):
            raise ValueError("Representative geography selection schema is incomplete")
        if diagnostic["geo_id"].duplicated().any() or selection["preferred_geo_id"].duplicated().any():
            raise ValueError("Representative geography artifacts contain duplicate identities")
        final = set(selection["final_selected_geo_id"].dropna().astype(str))
        manifest_geos = set(diagnostic["geo_id"].astype(str))
        if final != selected or not final.issubset(manifest_geos):
            raise ValueError("Representative geography selection does not match governed system evidence")
        anchors = {"district_of_columbia_dc__county", "essex_county_nj__county"}
        if not anchors.issubset(set(selection["preferred_geo_id"].astype(str))):
            raise ValueError("DC and Essex selection outcomes must be explicit")


def assemble_inventory_system_evidence(
    *, campaign, baseline_artifacts: Mapping[str, pd.DataFrame],
    candidate_artifacts: Mapping[str, Mapping[str, pd.DataFrame]],
    max_geographies: int | None = None,
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
    del max_geographies  # retained as a compatibility keyword; the governed shortlist controls size.
    manifest_path = campaign.metadata.get("geography_scope", {}).get(
        "authoritative_geography_manifest_path", "config/geo_manifest.generated.csv"
    )
    diagnostic = build_representative_geography_diagnostic(
        baseline_artifacts=baseline_artifacts, manifest_path=manifest_path,
    )
    selection = select_representative_geographies(diagnostic)
    selected = selection["final_selected_geo_id"].dropna().astype(str).tolist()
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
    governed_axes = _governed_axis_scope(campaign)
    # Supporting axes are retained as compact incumbent context, not
    # decomposition evidence. Challenger parity is validated separately.
    axis = combine("axis_scores", lambda f: f[f["axis"].isin(governed_axes)])
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
    baseline_axis = axis[axis["series_id"].eq("baseline") & axis["axis"].eq(campaign.target_axis)]
    _validate_transition_metric_uniqueness(metric_scores)
    target_availability = metric_scores[metric_scores["series_id"].eq("baseline")][
        ["geo_id", "date", "metric_score"]
    ]
    candidate_metric = metric_scores[metric_scores["series_id"].ne("baseline")]
    for geo_id, frame in baseline_axis.groupby("geo_id", sort=True):
        frame = frame.sort_values("date", kind="mergesort")
        changes = pd.to_numeric(frame["axis_score"], errors="coerce").diff().abs()
        available_dates = set(target_availability[
            target_availability["geo_id"].eq(geo_id) & target_availability["metric_score"].notna()
        ]["date"])
        pivot = candidate_metric[candidate_metric["geo_id"].eq(geo_id)].pivot(
            index="date", columns="series_id", values="metric_score"
        )
        baseline_metric = target_availability[
            target_availability["geo_id"].eq(geo_id)
        ].set_index("date")["metric_score"]

        # Compare only on the candidate chronology. Pandas otherwise
        # aligns to the union of baseline and candidate dates, producing
        # a Boolean mask longer than pivot.index.
        baseline_on_candidate_dates = baseline_metric.reindex(pivot.index)

        candidate_differs = pivot.ne(
            baseline_on_candidate_dates,
            axis=0,
        )

        # Missing challenger warmup rows are not divergence.
        candidate_differs[pivot.isna()] = False
        candidate_differs.loc[
            baseline_on_candidate_dates.isna(),
            :,
        ] = False

        differing_dates = set(
            candidate_differs.index[
                candidate_differs.any(axis=1)
            ]
        )
        eligible_dates = available_dates & differing_dates
        changes = changes.where(frame["date"].isin(eligible_dates))
        if changes.dropna().empty:
            raise ValueError(f"No system transition observation for {geo_id}")
        center = frame.loc[changes.idxmax(), "date"]
        dates = frame["date"].tolist(); position = dates.index(center)
        start, end = dates[max(0, position - 3)], dates[min(len(dates) - 1, position + 3)]
        part = axis[axis["geo_id"].eq(geo_id) & axis["axis"].eq(campaign.target_axis) & axis["date"].between(start, end)].copy()
        part["window_id"] = "largest_target_available_incumbent_supply_axis_change"
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
                NORMALIZED_METRIC_SECTION: metric_scores,
                REPRESENTATIVE_GEOGRAPHY_DIAGNOSTIC: diagnostic,
                REPRESENTATIVE_GEOGRAPHY_SELECTION: selection},
        representative_geography_rule=(
            "validated seven-county preferred shortlist; at most one same-state deterministic "
            "diagnostic fallback per invalid preferred geography, then best remaining manifest county"
        ),
        transition_window_rule=(
            "largest absolute month-over-month incumbent Supply-axis change on a date where "
            "active_inventory is available and at least one candidate can differ from baseline; "
            "three observations of context on each side"
        ),
    )
