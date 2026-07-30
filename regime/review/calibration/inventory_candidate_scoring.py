"""Deterministic scoring of already-materialized inventory Phase A evidence."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cmp_to_key
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from .campaign import CalibrationCampaign
from .inventory_campaign import FEATURE_COMPONENTS, PHASE_A_CANDIDATES, PhaseAEvidence


SCORING_CONTRACT_VERSION = "inventory_candidate_scoring_v1"
TIE_TOLERANCE = 1e-12
WEIGHT_TOLERANCE = 1e-12
AGGREGATIONS = frozenset({
    "component_ratio_then_equal_component_mean",
    "equal_month_mean_then_equal_component_mean",
    "equal_component_mean",
})
DIRECTIONS = frozenset({"higher_is_better", "lower_is_better"})
METRIC_SCORE_COLUMNS = [
    "candidate_policy_id", "metric_key", "feature_component", "raw_value",
    "raw_numerator", "raw_denominator", "warmup_rows",
    "source_table", "source_column", "aggregation", "direction", "weight", "eligible",
]
EXPECTED_METRICS = {
    "warmup_coverage_retention": ("inventory_candidate_feature_coverage", "valid_rows / rows", "component_ratio_then_equal_component_mean"),
    "seasonality_suppression": ("inventory_candidate_calendar_month_behavior", "mean_absolute_monthly_change", "equal_month_mean_then_equal_component_mean"),
    "volatility_reduction": ("inventory_candidate_feature_statistics", "standard_deviation", "equal_component_mean"),
    "sign_flip_reduction": ("inventory_candidate_feature_statistics", "sign_flip_rate", "equal_component_mean"),
    "trend_shape_preservation": ("inventory_candidate_baseline_feature_comparison", "correlation", "equal_component_mean"),
}
POLICY_COLUMNS = {
    "metric_key", "display_name", "source_table", "source_column", "aggregation",
    "direction", "weight", "enabled", "notes",
}
CAMPAIGN_IDENTITY_FIELDS = (
    "campaign_id", "campaign_version", "campaign_phase", "baseline_run_id",
    "incumbent_run_id", "baseline_policy_id", "incumbent_policy_id",
    "candidate_policy_ids", "target_metric", "target_dimension", "target_axis",
    "allowed_geo_levels", "manual_geo_ids", "metadata",
)


@dataclass(frozen=True, slots=True)
class InventoryScoringPolicy:
    """Validated, ordered inventory scoring metric configuration."""

    metrics: pd.DataFrame
    contract_version: str = SCORING_CONTRACT_VERSION


@dataclass(frozen=True, slots=True)
class InventoryCandidateScoringResult:
    """Review tables produced from a Phase A evidence bundle without recomputation."""

    inventory_candidate_eligibility: pd.DataFrame
    inventory_candidate_metric_scores: pd.DataFrame
    inventory_candidate_weighted_scores: pd.DataFrame
    inventory_candidate_ranking: pd.DataFrame
    inventory_campaign_recommendation: pd.DataFrame

    @property
    def tables(self) -> Mapping[str, pd.DataFrame]:
        """Return scoring artifacts in stable contract order."""
        return {
            "inventory_candidate_eligibility": self.inventory_candidate_eligibility,
            "inventory_candidate_metric_scores": self.inventory_candidate_metric_scores,
            "inventory_candidate_weighted_scores": self.inventory_candidate_weighted_scores,
            "inventory_candidate_ranking": self.inventory_candidate_ranking,
            "inventory_campaign_recommendation": self.inventory_campaign_recommendation,
        }


def _strict_enabled(value: object) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    raise ValueError(f"Inventory scoring enabled values must be boolean or true/false; received {value!r}")


def _validate_scoring_policy_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Return one canonical validated copy for CSV-loaded and direct policies."""
    missing = sorted(POLICY_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(f"Inventory scoring configuration is missing columns: {missing}")
    if frame["metric_key"].duplicated().any():
        raise ValueError("Inventory scoring metric_key values must be unique")
    enabled = frame["enabled"].map(_strict_enabled)
    frame = frame.loc[enabled].copy().reset_index(drop=True)
    if frame.empty:
        raise ValueError("Inventory scoring configuration has no enabled metrics")
    if set(frame["metric_key"]) != set(EXPECTED_METRICS):
        raise ValueError("Inventory scoring configuration must contain exactly the supported v1 metrics")
    for row in frame.itertuples(index=False):
        if (row.source_table, row.source_column, row.aggregation) != EXPECTED_METRICS[row.metric_key]:
            raise ValueError(f"Unsupported source contract for inventory metric: {row.metric_key}")
    invalid_directions = sorted(set(frame["direction"]).difference(DIRECTIONS))
    if invalid_directions:
        raise ValueError(f"Invalid inventory scoring directions: {invalid_directions}")
    invalid_aggregations = sorted(set(frame["aggregation"]).difference(AGGREGATIONS))
    if invalid_aggregations:
        raise ValueError(f"Invalid inventory scoring aggregations: {invalid_aggregations}")
    weights = pd.to_numeric(frame["weight"], errors="coerce")
    if not np.isfinite(weights).all() or weights.lt(0).any():
        raise ValueError("Enabled inventory scoring weights must be finite and nonnegative")
    if not np.isclose(weights.sum(), 1.0, atol=WEIGHT_TOLERANCE, rtol=0):
        raise ValueError(f"Enabled inventory scoring weights must sum to 1.0; received {weights.sum():.17g}")
    frame["weight"] = weights.astype(float)
    order = {metric: position for position, metric in enumerate(EXPECTED_METRICS)}
    frame["_metric_order"] = frame["metric_key"].map(order)
    return frame.sort_values("_metric_order", kind="mergesort").drop(
        columns="_metric_order"
    ).reset_index(drop=True)


def load_inventory_scoring_policy(
    path: str | Path = Path("config/inventory_candidate_scoring.csv"),
) -> InventoryScoringPolicy:
    """Load and strictly validate the explicit inventory scoring registry."""
    return InventoryScoringPolicy(_validate_scoring_policy_frame(pd.read_csv(path)))


def _tables(evidence: PhaseAEvidence) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
    for result in evidence.evidence_results.values():
        for name, frame in result.tables.items():
            if name in output:
                raise ValueError(f"Duplicate Phase A evidence table: {name}")
            output[name] = frame.copy(deep=True)
    return output


def _require(frame: pd.DataFrame, name: str, columns: set[str]) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required scoring columns: {missing}")


def _calendar_component_reconciles(frame: pd.DataFrame, value_column: str) -> bool:
    if len(frame) != 12 or "calendar_month" not in frame or value_column not in frame:
        return False
    months = pd.to_numeric(frame["calendar_month"], errors="coerce")
    values = pd.to_numeric(frame[value_column], errors="coerce")
    return bool(
        np.isfinite(months).all()
        and np.isfinite(values).all()
        and not months.duplicated().any()
        and set(months.astype(int)) == set(range(1, 13))
        and months.eq(months.astype(int)).all()
    )


def _component_observations(
    candidates: tuple[str, ...], tables: Mapping[str, pd.DataFrame], policy: InventoryScoringPolicy,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for metric in policy.metrics.itertuples(index=False):
        if metric.source_table not in tables:
            raise ValueError(f"Missing required Phase A scoring table: {metric.source_table}")
        source = tables[metric.source_table]
        _require(source, metric.source_table, {"candidate_policy_id", "feature_component"})
        if metric.metric_key == "warmup_coverage_retention":
            _require(source, metric.source_table, {"rows", "valid_rows", "warmup_rows"})
        elif metric.source_column not in source.columns:
            raise ValueError(f"{metric.source_table} is missing configured source column: {metric.source_column}")
        unknown = sorted(set(source["candidate_policy_id"]).difference(candidates))
        if unknown:
            raise ValueError(f"{metric.source_table} contains unknown candidates: {unknown}")
        for candidate in candidates:
            selected = source[source["candidate_policy_id"].eq(candidate)]
            for component in FEATURE_COMPONENTS:
                part = selected[selected["feature_component"].eq(component)]
                if metric.aggregation == "equal_month_mean_then_equal_component_mean":
                    values = pd.to_numeric(part[metric.source_column], errors="coerce")
                    raw = (float(values.mean()) if
                           _calendar_component_reconciles(part, metric.source_column)
                           else np.nan)
                    numerator = denominator = warmup = np.nan
                elif metric.metric_key == "warmup_coverage_retention":
                    if len(part) != 1:
                        raw = np.nan
                    else:
                        denominator = float(part.iloc[0]["rows"])
                        raw = float(part.iloc[0]["valid_rows"]) / denominator if denominator > 0 else np.nan
                    numerator = float(part.iloc[0]["valid_rows"]) if len(part) == 1 else np.nan
                    denominator = float(part.iloc[0]["rows"]) if len(part) == 1 else np.nan
                    warmup = float(part.iloc[0]["warmup_rows"]) if len(part) == 1 else np.nan
                else:
                    values = pd.to_numeric(part[metric.source_column], errors="coerce")
                    raw = float(values.iloc[0]) if len(values) == 1 else np.nan
                    numerator = denominator = warmup = np.nan
                rows.append({
                    "candidate_policy_id": candidate, "metric_key": metric.metric_key,
                    "feature_component": component, "raw_value": raw,
                    "raw_numerator": numerator, "raw_denominator": denominator,
                    "warmup_rows": warmup,
                    "source_table": metric.source_table, "source_column": metric.source_column,
                    "aggregation": metric.aggregation, "direction": metric.direction,
                    "weight": float(metric.weight),
                })
    return pd.DataFrame(rows)


def _eligibility(
    candidates: tuple[str, ...], tables: Mapping[str, pd.DataFrame], detail: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "inventory_candidate_feature_coverage": {"candidate_policy_id", "feature_component", "rows", "valid_rows", "non_finite_rows", "duplicate_key_rows"},
        "inventory_candidate_target_replacement": {"candidate_policy_id", "feature_component", "baseline_rows", "challenger_rows", "overlap_rows", "baseline_only_rows", "challenger_only_rows"},
        "inventory_candidate_non_target_parity": {"candidate_policy_id", "parity_pass"},
    }
    for name, columns in required.items():
        if name not in tables:
            raise ValueError(f"Missing required Phase A eligibility table: {name}")
        _require(tables[name], name, columns)
        unknown = sorted(set(tables[name]["candidate_policy_id"]).difference(candidates))
        if unknown:
            raise ValueError(f"{name} contains unknown candidates: {unknown}")
    coverage = tables["inventory_candidate_feature_coverage"]
    replacement = tables["inventory_candidate_target_replacement"]
    parity = tables["inventory_candidate_non_target_parity"]
    calendar = tables.get("inventory_candidate_calendar_month_behavior")
    if calendar is None:
        raise ValueError("Missing required Phase A eligibility table: inventory_candidate_calendar_month_behavior")
    _require(calendar, "inventory_candidate_calendar_month_behavior", {
        "candidate_policy_id", "feature_component", "calendar_month",
        "mean_absolute_monthly_change",
    })
    rows: list[dict[str, object]] = []
    def add(candidate: str, key: str, value: object, threshold: object, passed: bool, reason: str) -> None:
        rows.append({"candidate_policy_id": candidate, "gate_key": key, "gate_value": value,
                     "gate_threshold": threshold, "gate_pass": bool(passed),
                     "failure_reason": "" if passed else reason})
    for candidate in candidates:
        cov = coverage[coverage["candidate_policy_id"].eq(candidate)]
        rep = replacement[replacement["candidate_policy_id"].eq(candidate)]
        par = parity[parity["candidate_policy_id"].eq(candidate)]
        cal = calendar[calendar["candidate_policy_id"].eq(candidate)]
        components = tuple(cov["feature_component"]) if len(cov) else ()
        add(candidate, "candidate_exists", True, True, True, "candidate missing from campaign")
        add(candidate, "required_components_present", len(set(components)), len(FEATURE_COMPONENTS),
            len(cov) == 3 and set(components) == set(FEATURE_COMPONENTS) and not cov["feature_component"].duplicated().any(),
            "required target feature-component grain is incomplete or duplicated")
        integrity = (len(cov) == 3 and pd.to_numeric(cov["valid_rows"], errors="coerce").gt(0).all()
                     and pd.to_numeric(cov["non_finite_rows"], errors="coerce").eq(0).all()
                     and pd.to_numeric(cov["duplicate_key_rows"], errors="coerce").eq(0).all())
        add(candidate, "target_coverage_integrity", bool(integrity), True, bool(integrity),
            "target coverage has no valid rows, non-finite rows, or duplicate keys")
        calendar_ok = all(_calendar_component_reconciles(
            cal[cal["feature_component"].eq(component)], "mean_absolute_monthly_change"
        ) for component in FEATURE_COMPONENTS)
        add(candidate, "calendar_month_evidence_reconciles", bool(calendar_ok), True,
            bool(calendar_ok), "calendar-month evidence must contain exactly finite months 1..12 per component")
        overlap = pd.to_numeric(rep.get("overlap_rows", pd.Series(dtype=float)), errors="coerce")
        add(candidate, "nonzero_baseline_overlap", float(overlap.min()) if len(overlap) else np.nan, "> 0",
            len(rep) == 3 and np.isfinite(overlap).all() and overlap.gt(0).all(), "one or more target components has no baseline overlap")
        numeric_rep = rep[["baseline_rows", "challenger_rows", "overlap_rows", "baseline_only_rows", "challenger_only_rows"]].apply(pd.to_numeric, errors="coerce") if len(rep) else pd.DataFrame()
        reconciles = len(rep) == 3 and np.isfinite(numeric_rep.to_numpy()).all() and (
            numeric_rep["baseline_rows"].eq(numeric_rep["overlap_rows"] + numeric_rep["baseline_only_rows"]) &
            numeric_rep["challenger_rows"].eq(numeric_rep["overlap_rows"] + numeric_rep["challenger_only_rows"])
        ).all()
        add(candidate, "target_replacement_reconciles", bool(reconciles), True, bool(reconciles), "target replacement row identities do not reconcile")
        challenger_only_ok = len(rep) == 3 and pd.to_numeric(rep["challenger_only_rows"], errors="coerce").eq(0).all()
        add(candidate, "challenger_only_target_rows", int(pd.to_numeric(rep["challenger_only_rows"], errors="coerce").sum()) if len(rep) else np.nan, 0,
            challenger_only_ok, "challenger-only target rows violate Phase A contract")
        parity_value = par.iloc[0]["parity_pass"] if len(par) == 1 else None
        parity_valid = isinstance(parity_value, (bool, np.bool_))
        parity_ok = parity_valid and bool(parity_value)
        parity_reason = "non-target parity failed" if parity_valid else "non-target parity value must be boolean"
        add(candidate, "non_target_parity", parity_value, True, parity_ok, parity_reason)
        grain_ok = len(cov) == 3 and len(rep) == 3 and not cov.duplicated(["feature_component"]).any() and not rep.duplicated(["feature_component"]).any()
        add(candidate, "evidence_grain_reconciles", bool(grain_ok), True, grain_ok, "evidence does not reconcile to candidate/component grain")
        values = pd.to_numeric(detail.loc[detail["candidate_policy_id"].eq(candidate), "raw_value"], errors="coerce")
        finite = len(values) == len(FEATURE_COMPONENTS) * detail["metric_key"].nunique() and np.isfinite(values).all()
        add(candidate, "required_scoring_inputs_finite", bool(finite), True, finite, "required scoring input is missing or non-finite")
    return pd.DataFrame(rows)


def _ranking_comparison(
    left: str, right: str, *, totals: Mapping[str, float],
    normalized_scores: Mapping[tuple[str, str], float], candidates: tuple[str, ...],
) -> tuple[int, str]:
    """Compare two candidates and report the actual decisive criterion."""
    if left == right:
        return 0, "total_score"
    criteria = (
        (float(totals[left]), float(totals[right]), "total_score"),
        (float(normalized_scores[(left, "trend_shape_preservation")]),
         float(normalized_scores[(right, "trend_shape_preservation")]),
         "trend_shape_preservation_tiebreak"),
        (float(normalized_scores[(left, "warmup_coverage_retention")]),
         float(normalized_scores[(right, "warmup_coverage_retention")]),
         "warmup_coverage_tiebreak"),
    )
    for left_value, right_value, reason in criteria:
        difference = left_value - right_value
        if abs(difference) > TIE_TOLERANCE:
            return (-1 if difference > 0 else 1), reason
    return (-1 if candidates.index(left) < candidates.index(right) else 1), "canonical_shorter_window_tiebreak"


def _validate_scoring_artifacts(result: InventoryCandidateScoringResult,
                                candidates: tuple[str, ...], metric_count: int) -> None:
    """Validate final table grain, numeric bounds, ranking, and recommendation state."""
    detail = result.inventory_candidate_metric_scores
    gates = result.inventory_candidate_eligibility
    weighted = result.inventory_candidate_weighted_scores
    ranking = result.inventory_candidate_ranking
    recommendation = result.inventory_campaign_recommendation
    expected_detail = len(candidates) * metric_count * len(FEATURE_COMPONENTS)
    expected_weighted = len(candidates) * metric_count
    gate_count = gates["gate_key"].nunique()
    if len(gates) != len(candidates) * gate_count or gates.duplicated(
        ["candidate_policy_id", "gate_key"]
    ).any() or not gates.groupby("candidate_policy_id").size().eq(gate_count).all():
        raise ValueError("Eligibility must contain one row per candidate and gate")
    if len(detail) != expected_detail or detail.duplicated(
        ["candidate_policy_id", "metric_key", "feature_component"]
    ).any():
        raise ValueError(f"Metric detail must contain exactly {expected_detail} unique candidate/metric/component rows")
    component_counts = detail.groupby(["candidate_policy_id", "metric_key"]).size()
    if not component_counts.eq(len(FEATURE_COMPONENTS)).all():
        raise ValueError("Every candidate metric must contain exactly three component rows")
    if len(weighted) != expected_weighted or weighted.duplicated(
        ["candidate_policy_id", "metric_key"]
    ).any():
        raise ValueError(f"Weighted scores must contain exactly {expected_weighted} unique candidate/metric rows")
    if len(ranking) != len(candidates) or ranking["candidate_policy_id"].duplicated().any():
        raise ValueError("Ranking must contain exactly one row per candidate")
    if len(recommendation) != 1:
        raise ValueError("Campaign recommendation must contain exactly one row")
    eligible_rows = weighted["eligible"]
    eligible_numbers = weighted.loc[eligible_rows, ["normalized_score", "weighted_score"]].to_numpy()
    if not np.isfinite(eligible_numbers).all():
        raise ValueError("Eligible candidates must have finite normalized and weighted scores")
    if not weighted.loc[eligible_rows, "normalized_score"].between(0, 1).all():
        raise ValueError("Normalized scores must be within [0, 1]")
    if weighted.loc[~eligible_rows, ["normalized_score", "weighted_score"]].notna().any().any():
        raise ValueError("Ineligible candidates must have NaN normalized and weighted scores")
    eligible_ranking = ranking[ranking["eligible"]]
    expected_ranks = list(range(1, len(eligible_ranking) + 1))
    if sorted(eligible_ranking["rank"].tolist()) != expected_ranks:
        raise ValueError("Eligible ranks must be unique and contiguous")
    for row in eligible_ranking.itertuples(index=False):
        total = weighted.loc[weighted["candidate_policy_id"].eq(row.candidate_policy_id), "weighted_score"].sum()
        if not np.isclose(total, row.total_score, atol=TIE_TOLERANCE, rtol=0):
            raise ValueError(f"Weighted score does not reconcile for {row.candidate_policy_id}")
    recommended = ranking["recommendation_state"].eq("recommended").sum()
    if recommended != (1 if len(eligible_ranking) else 0):
        raise ValueError("Recommendation count does not reconcile with eligible candidates")


def score_inventory_candidates(
    *, campaign: CalibrationCampaign, phase_a_evidence: PhaseAEvidence,
    scoring_policy: InventoryScoringPolicy | None = None,
) -> InventoryCandidateScoringResult:
    """Score existing Phase A evidence; never materialize or normalize challengers."""
    supplied_policy = scoring_policy or load_inventory_scoring_policy()
    policy = InventoryScoringPolicy(
        _validate_scoring_policy_frame(supplied_policy.metrics),
        supplied_policy.contract_version,
    )
    candidates = campaign.candidate_policy_ids
    canonical = tuple(PHASE_A_CANDIDATES.values())
    if not candidates:
        raise ValueError("Inventory scoring requires a non-empty candidate set")
    if len(candidates) != len(set(candidates)):
        raise ValueError("Inventory scoring candidate IDs must be unique")
    if candidates != canonical:
        raise ValueError(f"Inventory scoring requires canonical candidate ordering: {canonical}")
    mismatches = [field for field in CAMPAIGN_IDENTITY_FIELDS if
                  getattr(campaign, field) != getattr(phase_a_evidence.campaign, field)]
    if mismatches:
        raise ValueError(f"Campaign/evidence identity mismatch for fields: {mismatches}")
    tables = _tables(phase_a_evidence)
    detail = _component_observations(candidates, tables, policy)
    gates = _eligibility(candidates, tables, detail)
    eligible_map = gates.groupby("candidate_policy_id", sort=False)["gate_pass"].all().to_dict()
    detail["eligible"] = detail["candidate_policy_id"].map(eligible_map).astype(bool)
    detail = detail[METRIC_SCORE_COLUMNS]

    aggregate = (detail.groupby(["candidate_policy_id", "metric_key"], sort=False, as_index=False)
                 .agg(raw_value=("raw_value", "mean"), direction=("direction", "first"),
                      weight=("weight", "first"), eligible=("eligible", "first")))
    weighted_rows: list[dict[str, object]] = []
    for metric in policy.metrics.itertuples(index=False):
        part = aggregate[aggregate["metric_key"].eq(metric.metric_key)].copy()
        eligible_values = part.loc[part["eligible"], "raw_value"]
        low, high = (eligible_values.min(), eligible_values.max()) if len(eligible_values) else (np.nan, np.nan)
        for row in part.itertuples(index=False):
            normalized = np.nan
            if row.eligible:
                if np.isclose(high, low, atol=TIE_TOLERANCE, rtol=0):
                    normalized = 0.5
                elif row.direction == "higher_is_better":
                    normalized = (row.raw_value - low) / (high - low)
                else:
                    normalized = (high - row.raw_value) / (high - low)
                normalized = float(np.clip(normalized, 0.0, 1.0))
            weighted_rows.append({
                "candidate_policy_id": row.candidate_policy_id, "metric_key": row.metric_key,
                "raw_value": row.raw_value, "normalized_score": normalized, "weight": row.weight,
                "weighted_score": normalized * row.weight if row.eligible else np.nan,
                "eligible": bool(row.eligible),
            })
    weighted = pd.DataFrame(weighted_rows)
    totals = weighted.groupby("candidate_policy_id", sort=False)["weighted_score"].sum(min_count=len(policy.metrics)).to_dict()
    normalized_lookup = weighted.set_index(["candidate_policy_id", "metric_key"])["normalized_score"].to_dict()
    eligible = [candidate for candidate in candidates if eligible_map[candidate]]
    def compare(left: str, right: str) -> int:
        return _ranking_comparison(
            left, right, totals=totals, normalized_scores=normalized_lookup,
            candidates=candidates,
        )[0]
    ordered = sorted(eligible, key=cmp_to_key(compare))
    ranks = {candidate: number for number, candidate in enumerate(ordered, 1)}
    winner = ordered[0] if ordered else None
    ranking_rows = []
    reasons: dict[str, str] = {}
    for position, candidate in enumerate(ordered):
        if len(ordered) == 1:
            reasons[candidate] = "total_score"
        elif position < len(ordered) - 1:
            reasons[candidate] = _ranking_comparison(
                candidate, ordered[position + 1], totals=totals,
                normalized_scores=normalized_lookup, candidates=candidates,
            )[1]
        else:
            reasons[candidate] = _ranking_comparison(
                ordered[position - 1], candidate, totals=totals,
                normalized_scores=normalized_lookup, candidates=candidates,
            )[1]
    for candidate in candidates:
        is_eligible = eligible_map[candidate]
        reason = reasons.get(candidate, "ineligible_hard_gate_failure")
        ranking_rows.append({
            "candidate_policy_id": candidate, "eligible": is_eligible,
            "total_score": totals[candidate] if is_eligible else np.nan,
            "rank": ranks.get(candidate, pd.NA),
            "recommendation_state": ("recommended" if candidate == winner else
                                     "eligible_not_recommended" if is_eligible else "ineligible"),
            "tie_break_reason": reason,
        })
    ranking = pd.DataFrame(ranking_rows)
    rationale = "no eligible candidates; inspect eligibility failures"
    status = "no_recommendation"
    if winner:
        scores = weighted[weighted["candidate_policy_id"].eq(winner)].sort_values(
            "weighted_score", ascending=False, kind="mergesort")
        dimensions = ",".join(scores.head(2)["metric_key"])
        decisive = reasons[winner]
        rationale = f"selected_by={decisive}; leading_dimensions={dimensions}; warmup_is_scored_tradeoff"
        status = "recommended_for_human_review"
    recommendation = pd.DataFrame([{
        "campaign_id": campaign.campaign_id, "campaign_version": campaign.campaign_version,
        "recommended_candidate_policy_id": winner if winner is not None else pd.NA,
        "recommendation_status": status, "eligible_candidate_count": len(eligible),
        "recommendation_rationale": rationale, "scoring_contract_version": policy.contract_version,
    }])
    result = InventoryCandidateScoringResult(gates, detail, weighted, ranking, recommendation)
    _validate_scoring_artifacts(result, candidates, len(policy.metrics))
    return result
