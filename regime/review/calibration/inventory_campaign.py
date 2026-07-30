from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime.review.models import ReviewBundle
from regime.review.results import ReviewResult
from regime.experiments.in_memory_challenger import (
    InMemoryChallengerArtifacts,
    build_in_memory_smoothing_challenger,
)
from regime.smoothing_policy import SmoothingExperiment, load_smoothing_experiments

from .campaign import CalibrationCampaign


PHASE_A_CANDIDATES = {
    3: "inventory_ma3_structural",
    6: "inventory_ma6_structural",
    9: "inventory_ma9_structural",
    12: "inventory_ma12_structural",
}
PHASE_A_POLICY_FIELDS = {
    window: (window, window, 3, window, 12)
    for window in PHASE_A_CANDIDATES
}
SECTION_IDENTIFIER = re.compile(r"[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?")
FEATURE_COMPONENTS = ("level", "short", "long")
FEATURE_KEY_BY_COMPONENT = {
    "level": "redfin_inventory_level",
    "short": "redfin_inventory_short",
    "long": "redfin_inventory_long",
}
COMPONENT_BY_FEATURE_KEY = {value: key for key, value in FEATURE_KEY_BY_COMPONENT.items()}
INVENTORY_FEATURE_KEYS = frozenset(COMPONENT_BY_FEATURE_KEY)
FEATURE_KEYS = [FEATURE_KEY_BY_COMPONENT[item] for item in FEATURE_COMPONENTS]
FEATURE_KEYS_COLUMNS = ["geo_id", "date", "canonical_metric_key", "feature_key"]
AUTHORITATIVE_RUN_ID = "macro_regime_v1_bps120_sources"


@dataclass(frozen=True, slots=True)
class _BaselineInputs:
    run_id: str
    features: pd.DataFrame
    source_metrics: pd.DataFrame
    manifest: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PhaseAEvidence:
    """Materialized Phase A evidence; intentionally contains no decision."""

    campaign: CalibrationCampaign
    challengers: Mapping[str, InMemoryChallengerArtifacts]
    evidence_results: Mapping[str, ReviewResult]
    review_bundle: ReviewBundle


def _validated_frame(
    frame: pd.DataFrame, *, name: str, required: set[str], keys: list[str]
) -> pd.DataFrame:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")
    work = frame.copy(deep=True)
    # Explicit mixed parsing is deterministic for the persisted ISO-like values
    # and avoids pandas' per-element format-inference warning on validation fixtures.
    parsed = pd.to_datetime(work["date"], format="mixed", errors="coerce")
    if parsed.isna().any():
        raise ValueError(f"{name} contains invalid dates")
    work["date"] = parsed
    if work.duplicated(keys, keep=False).any():
        raise ValueError(f"{name} contains duplicate keys: {keys}")
    return work.sort_values(keys, kind="mergesort").reset_index(drop=True)


def _load_authoritative_baseline(
    artifact_root: str | Path, run_id: str = AUTHORITATIVE_RUN_ID
) -> _BaselineInputs:
    """Load exactly the three persisted inputs required by Phase A."""
    if run_id != AUTHORITATIVE_RUN_ID:
        raise ValueError(f"Phase A baseline must be {AUTHORITATIVE_RUN_ID!r}")
    run_dir = Path(artifact_root) / run_id
    paths = {
        "features": run_dir / "features.parquet",
        "source_metrics": run_dir / "source_metrics.parquet",
        "manifest": run_dir / "manifest.json",
    }
    missing = sorted(name for name, path in paths.items() if not path.is_file())
    if missing:
        raise FileNotFoundError(f"Authoritative baseline artifacts are missing: {missing}")
    with paths["manifest"].open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("run_id") != run_id:
        raise ValueError("Authoritative manifest run_id does not match the requested run")
    features = _validated_frame(
        pd.read_parquet(paths["features"]),
        name="features",
        required={"geo_id", "date", "canonical_metric_key", "feature_key", "raw_feature_value"},
        keys=["geo_id", "date", "canonical_metric_key", "feature_key"],
    )
    sources = _validated_frame(
        pd.read_parquet(paths["source_metrics"]),
        name="source_metrics",
        required={"geo_id", "date", "canonical_metric_key", "value", "metric_origin"},
        keys=["geo_id", "date", "canonical_metric_key"],
    )
    return _BaselineInputs(run_id, features, sources, manifest)


def _inventory_feature_weights() -> dict[str, float]:
    features = load_regime_config(validate=True).features
    selected = features[features["feature_key"].isin(INVENTORY_FEATURE_KEYS)]
    if set(selected["feature_key"]) != INVENTORY_FEATURE_KEYS:
        missing = sorted(INVENTORY_FEATURE_KEYS.difference(selected["feature_key"]))
        raise ValueError(f"Inventory feature-weight contract is incomplete: {missing}")
    weights = {
        row.feature_key: float(row.feature_weight)
        for row in selected.itertuples(index=False)
    }
    if any(weight <= 0 for weight in weights.values()):
        raise ValueError("Inventory feature weights must be positive")
    return dict(sorted(weights.items()))


def _validate_phase_a_candidates(
    candidate_ids: tuple[str, ...],
    experiments: Mapping[str, SmoothingExperiment],
) -> dict[int, str]:
    canonical = tuple(PHASE_A_CANDIDATES.values())
    if candidate_ids != canonical:
        raise ValueError(
            "Phase A candidates must use canonical MA3/MA6/MA9/MA12 order; "
            f"expected={canonical}, received={candidate_ids}"
        )
    resolved: dict[int, str] = {}
    for candidate_id in candidate_ids:
        if candidate_id not in experiments:
            raise ValueError(f"Missing required smoothing candidate: {candidate_id}")
        experiment = experiments[candidate_id]
        policy = experiment.policy_for("active_inventory")
        if policy is None or set(experiment.metric_keys) != {"active_inventory"}:
            raise ValueError(f"{candidate_id} must target only active_inventory")
        if policy.transform_strategy != "ma_structural":
            raise ValueError(f"{candidate_id} must use ma_structural")
        expected_id = PHASE_A_CANDIDATES.get(policy.level_window)
        actual_fields = (
            policy.level_window,
            policy.short_window,
            policy.short_lag_periods,
            policy.long_window,
            policy.long_lag_periods,
        )
        expected_fields = PHASE_A_POLICY_FIELDS.get(policy.level_window)
        if expected_id != candidate_id or actual_fields != expected_fields:
            raise ValueError(f"{candidate_id} is not the canonical structural policy for MA{policy.level_window}")
        if policy.level_window in resolved:
            raise ValueError(f"Duplicate effective MA{policy.level_window} candidate")
        resolved[policy.level_window] = candidate_id
    if resolved != PHASE_A_CANDIDATES:
        missing = sorted(set(PHASE_A_CANDIDATES).difference(resolved))
        extra = sorted(set(resolved).difference(PHASE_A_CANDIDATES))
        raise ValueError(f"Phase A requires exactly MA3/MA6/MA9/MA12; missing={missing}, extra={extra}")
    return dict(sorted(resolved.items()))


def build_inventory_calibration_campaign(
    *,
    campaign_id: str,
    campaign_version: str,
    baseline_run_id: str,
    incumbent_run_id: str,
    baseline_policy_id: str = "baseline_current",
    incumbent_policy_id: str = "baseline_current",
    candidate_policy_ids: tuple[str, ...] = tuple(PHASE_A_CANDIDATES.values()),
    manual_geo_ids: tuple[str, ...] = (),
    metadata: Mapping[str, Any] | None = None,
    registry_path: str | Path | None = None,
) -> CalibrationCampaign:
    experiments = load_smoothing_experiments(
        path=registry_path if registry_path is not None else Path("config/metric_smoothing_experiments.csv"),
        validate=True,
    )
    resolved = _validate_phase_a_candidates(candidate_policy_ids, experiments)
    campaign = CalibrationCampaign(
        campaign_id=campaign_id,
        campaign_version=campaign_version,
        campaign_phase="phase_a",
        baseline_run_id=baseline_run_id,
        incumbent_run_id=incumbent_run_id,
        baseline_policy_id=baseline_policy_id,
        incumbent_policy_id=incumbent_policy_id,
        candidate_policy_ids=tuple(resolved.values()),
        target_metric="active_inventory",
        target_dimension="supply",
        target_axis="supply",
        manual_geo_ids=manual_geo_ids,
        metadata=dict(metadata or {}),
    )
    enriched = dict(campaign.metadata)
    enriched.update({
        "campaign_type": "inventory_calibration",
        "resolved_candidate_ids": {f"ma{window}": policy_id for window, policy_id in resolved.items()},
        "candidate_aliases": {policy_id: policy_id for policy_id in resolved.values()},
        "feature_weights": _inventory_feature_weights(),
        "feature_weight_source": "config/feature_registry.csv",
        "candidate_feature_weight_overrides": False,
        "feature_weights_held_constant": True,
    })
    return replace(campaign, metadata=enriched)


def materialize_phase_a_challengers(
    campaign: CalibrationCampaign,
    baseline_features: pd.DataFrame,
    source_metrics: pd.DataFrame,
) -> dict[str, InMemoryChallengerArtifacts]:
    """Build every canonical candidate once, preserving campaign order."""
    
    from time import perf_counter

    _validate_phase_a_candidates(
        campaign.candidate_policy_ids, load_smoothing_experiments(validate=True)
    )
    output: dict[str, InMemoryChallengerArtifacts] = {}
    baseline_snapshot = baseline_features.copy(deep=True)
    source_snapshot = source_metrics.copy(deep=True)
    for candidate_id in campaign.candidate_policy_ids:
        if candidate_id in output:
            raise ValueError(f"Duplicate candidate: {candidate_id}")
        
        started = perf_counter()
        print(
            f"[inventory-phase-a] materializing {candidate_id}...",
            flush=True,
        )

        challenger = build_in_memory_smoothing_challenger(
            baseline_features=baseline_features,
            source_metrics=source_metrics,
            experiment_id=candidate_id,
        )

        elapsed = perf_counter() - started
        print(
            f"[inventory-phase-a] completed {candidate_id} "
            f"in {elapsed:,.1f}s",
            flush=True,
        )

        if challenger.smoothing_lineage.empty:
            raise ValueError(f"Missing lineage for candidate: {candidate_id}")
        if set(challenger.smoothing_lineage["experiment_id"].dropna()) != {candidate_id}:
            raise ValueError(f"Incorrect lineage identity for candidate: {candidate_id}")
        present = set(challenger.features["feature_key"])
        if not INVENTORY_FEATURE_KEYS.issubset(present):
            raise ValueError(f"Missing target features for candidate: {candidate_id}")
        for feature_key in FEATURE_KEYS:
            target_values = pd.to_numeric(
                challenger.features.loc[
                    challenger.features["feature_key"].eq(feature_key),
                    "raw_feature_value",
                ],
                errors="coerce",
            )
            if not np.isfinite(target_values.dropna()).any():
                raise ValueError(
                    f"Candidate has no finite target values: {candidate_id}/{feature_key}"
                )
        keys = ["geo_id", "date", "canonical_metric_key", "feature_key"]
        if challenger.features.duplicated(keys, keep=False).any():
            raise ValueError(f"Duplicate feature keys for candidate: {candidate_id}")
        before = baseline_features[~baseline_features["feature_key"].isin(INVENTORY_FEATURE_KEYS)]
        after = challenger.features[~challenger.features["feature_key"].isin(INVENTORY_FEATURE_KEYS)]
        before = before.sort_values(keys, kind="mergesort").reset_index(drop=True)
        after = after.sort_values(keys, kind="mergesort").reset_index(drop=True)
        try:
            pd.testing.assert_frame_equal(before, after)
        except AssertionError as exc:
            raise ValueError(f"Non-target parity failure for candidate: {candidate_id}") from exc
        output[candidate_id] = challenger
    pd.testing.assert_frame_equal(baseline_features, baseline_snapshot)
    pd.testing.assert_frame_equal(source_metrics, source_snapshot)
    if tuple(output) != campaign.candidate_policy_ids:
        raise ValueError("Missing or unexpected Phase A candidate")
    return output


def _candidate_ids(challengers: Mapping[str, InMemoryChallengerArtifacts]) -> tuple[str, ...]:
    """Return and validate the insertion-ordered canonical candidate identity."""
    candidate_ids = tuple(challengers)
    canonical = tuple(PHASE_A_CANDIDATES.values())
    if candidate_ids != canonical:
        raise ValueError(
            "Challenger mapping must use canonical MA3/MA6/MA9/MA12 order; "
            f"expected={canonical}, received={candidate_ids}"
        )
    return candidate_ids


def _feature_rows(frame: pd.DataFrame, feature_key: str) -> pd.DataFrame:
    return frame[frame["feature_key"].eq(feature_key)].copy()


def _outer_value_reconciliation(
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
) -> pd.DataFrame:
    """Outer-reconcile feature keys and values with null-safe exact equality."""
    left = baseline[FEATURE_KEYS_COLUMNS + ["raw_feature_value"]]
    right = challenger[FEATURE_KEYS_COLUMNS + ["raw_feature_value"]]
    return left.merge(
        right,
        on=FEATURE_KEYS_COLUMNS,
        how="outer",
        suffixes=("_baseline", "_challenger"),
        indicator=True,
        validate="one_to_one",
        sort=True,
    )


def _matching_values(frame: pd.DataFrame) -> pd.Series:
    left = frame["raw_feature_value_baseline"]
    right = frame["raw_feature_value_challenger"]
    return left.eq(right) | (left.isna() & right.isna())


def _finite_numeric(series: pd.Series) -> pd.Series:
    """Coerce values to numeric and retain only finite observations."""
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.where(np.isfinite(numeric))


def _non_target_parity_row(
    candidate_id: str,
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
) -> dict[str, object]:
    base = baseline[~baseline["feature_key"].isin(INVENTORY_FEATURE_KEYS)]
    other = challenger[~challenger["feature_key"].isin(INVENTORY_FEATURE_KEYS)]
    merged = _outer_value_reconciliation(base, other)
    both = merged["_merge"].eq("both")
    mismatches = both & ~_matching_values(merged)
    baseline_only = merged["_merge"].eq("left_only")
    challenger_only = merged["_merge"].eq("right_only")
    parity = not (mismatches.any() or baseline_only.any() or challenger_only.any())
    return {
        "candidate_policy_id": candidate_id,
        "baseline_non_target_rows": len(base),
        "challenger_non_target_rows": len(other),
        "matching_key_rows": int(both.sum()),
        "value_mismatch_rows": int(mismatches.sum()),
        "baseline_only_rows": int(baseline_only.sum()),
        "challenger_only_rows": int(challenger_only.sum()),
        "parity_pass": bool(parity),
    }


def _monthly_changes(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy(deep=True)
    work = work.sort_values(["feature_key", "geo_id", "date"], kind="mergesort")
    work["raw_feature_value"] = _finite_numeric(work["raw_feature_value"])
    work["monthly_change"] = work.groupby(
        ["feature_key", "geo_id"], sort=False, dropna=False
    )["raw_feature_value"].diff()
    work["monthly_change"] = _finite_numeric(work["monthly_change"])
    return work


def _sign_flip_rate(frame: pd.DataFrame) -> float:
    """Compute nonzero change-sign transitions strictly within geography."""
    work = _monthly_changes(frame)
    flips = 0
    transitions = 0
    for (_, _), group in work.groupby(["feature_key", "geo_id"], sort=False):
        previous_sign: float | None = None
        for change in group["monthly_change"]:
            if pd.isna(change) or not np.isfinite(change):
                previous_sign = None
                continue
            if change == 0:
                continue
            sign = float(np.sign(change))
            if previous_sign is not None:
                transitions += 1
                flips += int(sign != previous_sign)
            previous_sign = sign
    return float(flips / transitions) if transitions else 0.0


def _campaign_definition_evidence(campaign: CalibrationCampaign) -> ReviewResult:
    experiments = load_smoothing_experiments(validate=True)
    campaign_columns = [
        "campaign_id", "campaign_version", "campaign_phase", "baseline_run_id",
        "incumbent_run_id", "baseline_policy_id", "incumbent_policy_id",
        "target_metric", "target_dimension", "target_axis", "candidate_count",
        "feature_weights_held_constant",
    ]
    campaign_row = {
        name: getattr(campaign, name)
        for name in campaign_columns
        if hasattr(campaign, name)
    }
    campaign_row["candidate_count"] = len(campaign.candidate_policy_ids)
    campaign_row["feature_weights_held_constant"] = bool(
        campaign.metadata["feature_weights_held_constant"]
    )
    candidates = []
    for candidate_id in campaign.candidate_policy_ids:
        experiment = experiments[candidate_id]
        policy = experiment.policy_for(campaign.target_metric)
        if policy is None:
            raise ValueError(f"Missing active_inventory policy for {candidate_id}")
        candidates.append({
            "candidate_policy_id": candidate_id,
            "experiment_name": experiment.experiment_name,
            "transform_strategy": policy.transform_strategy,
            "level_window": policy.level_window,
            "short_window": policy.short_window,
            "short_lag_periods": policy.short_lag_periods,
            "long_window": policy.long_window,
            "long_lag_periods": policy.long_lag_periods,
            "target_metric": policy.metric_key,
            "parent_run": experiment.parent_run,
            "policy_role": policy.policy_role,
            "is_baseline": policy.is_baseline,
            "recompute_dependents": policy.recompute_dependents,
        })
    weights = [{
        "feature_key": FEATURE_KEY_BY_COMPONENT[component],
        "feature_component": component,
        "feature_weight": campaign.metadata["feature_weights"][FEATURE_KEY_BY_COMPONENT[component]],
        "weight_source": campaign.metadata["feature_weight_source"],
        "held_constant": True,
    } for component in FEATURE_COMPONENTS]
    return ReviewResult(
        tables={
            "inventory_phase_a_campaign": pd.DataFrame([campaign_row], columns=campaign_columns),
            "inventory_phase_a_candidates": pd.DataFrame(candidates),
            "inventory_phase_a_feature_weights": pd.DataFrame(weights),
        },
        metadata={
            "generator_id": "inventory_phase_a_campaign_definition",
            "generator_version": "1.0",
            "campaign_phase": campaign.campaign_phase,
            "candidate_policy_ids": list(campaign.candidate_policy_ids),
            "source_run_id": campaign.baseline_run_id,
        },
    )


def _coverage_lineage_evidence(
    challengers: Mapping[str, InMemoryChallengerArtifacts],
    baseline_features: pd.DataFrame,
) -> ReviewResult:
    coverage = []
    lineage_summary = []
    replacements = []
    parity_rows = []
    for candidate_id in _candidate_ids(challengers):
        challenger = challengers[candidate_id]
        lineage = challenger.smoothing_lineage
        if lineage.empty or set(lineage["experiment_id"].dropna()) != {candidate_id}:
            raise ValueError(f"Missing or incorrect lineage for candidate: {candidate_id}")
        parity_rows.append(_non_target_parity_row(candidate_id, baseline_features, challenger.features))
        for component in FEATURE_COMPONENTS:
            feature_key = FEATURE_KEY_BY_COMPONENT[component]
            target = _feature_rows(challenger.features, feature_key)
            values = pd.to_numeric(target["raw_feature_value"], errors="coerce")
            finite = target["raw_feature_value"].notna() & np.isfinite(values)
            if not finite.any():
                raise ValueError(f"Empty required feature overlap for {candidate_id}/{feature_key}")
            duplicates = target.duplicated(FEATURE_KEYS_COLUMNS, keep=False)
            coverage.append({
                "candidate_policy_id": candidate_id, "feature_component": component,
                "feature_key": feature_key, "rows": len(target),
                "valid_rows": int(finite.sum()), "geography_count": target["geo_id"].nunique(),
                "first_date": target["date"].min(),
                "first_valid_date": target.loc[finite, "date"].min(),
                "last_valid_date": target.loc[finite, "date"].max(),
                "warmup_rows": int(target["raw_feature_value"].isna().sum()),
                "non_finite_rows": int((target["raw_feature_value"].notna() & ~np.isfinite(values)).sum()),
                "duplicate_key_rows": int(duplicates.sum()),
            })
            component_lineage = lineage[lineage["feature_key"].eq(feature_key)]
            if component_lineage.empty:
                raise ValueError(f"Missing lineage feature key for {candidate_id}/{feature_key}")
            lineage_summary.append({
                "candidate_policy_id": candidate_id, "feature_component": component,
                "feature_key": feature_key, "lineage_rows": len(component_lineage),
                "source_geography_count": component_lineage["geo_id"].nunique(),
                "first_source_date": component_lineage["date"].min(),
                "last_source_date": component_lineage["date"].max(),
                "first_challenger_date": component_lineage.loc[component_lineage["challenger_feature_value"].notna(), "date"].min(),
                "last_challenger_date": component_lineage.loc[component_lineage["challenger_feature_value"].notna(), "date"].max(),
                "source_metric_origin_count": component_lineage["source_metric_origin"].nunique(dropna=True),
            })
            base_target = _feature_rows(baseline_features, feature_key)
            merged = _outer_value_reconciliation(base_target, target)
            both = merged["_merge"].eq("both")
            equal = _matching_values(merged)
            replacements.append({
                "candidate_policy_id": candidate_id, "feature_component": component,
                "feature_key": feature_key, "baseline_rows": len(base_target),
                "challenger_rows": len(target), "overlap_rows": int(both.sum()),
                "changed_rows": int((both & ~equal).sum()),
                "unchanged_rows": int((both & equal).sum()),
                "baseline_only_rows": int(merged["_merge"].eq("left_only").sum()),
                "challenger_only_rows": int(merged["_merge"].eq("right_only").sum()),
            })
    series_rows = []
    for series_id, frame in (("baseline", baseline_features), *(
        (candidate_id, challengers[candidate_id].features)
        for candidate_id in _candidate_ids(challengers)
    )):
        target = frame[frame["feature_key"].isin(INVENTORY_FEATURE_KEYS)]
        for row in target.sort_values(
            ["geo_id", "feature_key", "date"], kind="mergesort"
        ).itertuples(index=False):
            series_rows.append({
                "series_id": series_id,
                "candidate_policy_id": (
                    series_id if series_id != "baseline" else pd.NA
                ),
                "is_baseline": series_id == "baseline",
                "geo_id": row.geo_id,
                "date": row.date,
                "feature_component": COMPONENT_BY_FEATURE_KEY[row.feature_key],
                "feature_key": row.feature_key,
                "raw_feature_value": row.raw_feature_value,
            })
    series = pd.DataFrame(series_rows)

    # Review windows are selected once as immutable Phase A evidence.  The
    # renderer only filters by these identifiers and never derives turning
    # points from the supplied series.
    transition_rows = []
    baseline_series = series[series["is_baseline"]].copy()
    for (geo_id, component), part in baseline_series.groupby(
        ["geo_id", "feature_component"], sort=True
    ):
        part = part.sort_values("date", kind="mergesort").reset_index(drop=True)
        changes = _finite_numeric(part["raw_feature_value"]).diff().abs()
        eligible = changes.dropna()
        if eligible.empty:
            raise ValueError(f"No transition-window observation for {geo_id}/{component}")
        center = int(eligible.idxmax())
        start = max(0, center - 3)
        end = min(len(part) - 1, center + 3)
        transition_rows.append({
            "geo_id": geo_id,
            "feature_component": component,
            "window_id": "largest_absolute_baseline_change",
            "selection_rule": "largest absolute month-over-month baseline change; three observations of context on each side",
            "center_date": part.loc[center, "date"],
            "window_start": part.loc[start, "date"],
            "window_end": part.loc[end, "date"],
        })

    return ReviewResult(tables={
        "inventory_candidate_feature_coverage": pd.DataFrame(coverage),
        "inventory_candidate_lineage_summary": pd.DataFrame(lineage_summary),
        "inventory_candidate_target_replacement": pd.DataFrame(replacements),
        "inventory_candidate_non_target_parity": pd.DataFrame(parity_rows),
        "inventory_candidate_feature_series": series,
        "inventory_transition_review_windows": pd.DataFrame(transition_rows),
    }, metadata={
        "generator_id": "inventory_phase_a_coverage_and_lineage",
        "generator_version": "1.0",
        "candidate_policy_ids": list(challengers),
        "value_comparison": "exact equality with null-safe matching",
        "series_evidence": "already-materialized baseline and challenger target features",
    })


def _structural_behavior_evidence(
    challengers: Mapping[str, InMemoryChallengerArtifacts],
) -> ReviewResult:
    statistics = []
    correlations = []
    calendar = []
    pairs = (("level", "short"), ("level", "long"), ("short", "long"))
    for candidate_id in _candidate_ids(challengers):
        target = challengers[candidate_id].features
        target = target[target["feature_key"].isin(INVENTORY_FEATURE_KEYS)].copy()
        changed = _monthly_changes(target)
        for component in FEATURE_COMPONENTS:
            feature_key = FEATURE_KEY_BY_COMPONENT[component]
            values = _feature_rows(changed, feature_key)
            valid = _finite_numeric(values["raw_feature_value"]).dropna()
            changes = _finite_numeric(values["monthly_change"]).dropna().abs()
            if valid.empty:
                raise ValueError(f"Empty required feature overlap for {candidate_id}/{feature_key}")
            statistics.append({
                "candidate_policy_id": candidate_id, "feature_component": component,
                "feature_key": feature_key, "rows": len(values), "valid_rows": len(valid),
                "mean": valid.mean(), "standard_deviation": valid.std(),
                "mean_absolute_monthly_change": changes.mean(),
                "median_absolute_monthly_change": changes.median(),
                "p90_absolute_monthly_change": changes.quantile(0.9),
                "maximum_absolute_monthly_change": changes.max(),
                "sign_flip_rate": _sign_flip_rate(values),
            })
            values["calendar_month"] = values["date"].dt.month
            for month in sorted(values["calendar_month"].unique()):
                monthly = values[values["calendar_month"].eq(month)]
                valid_monthly = _finite_numeric(monthly["raw_feature_value"]).dropna()
                finite_monthly_changes = _finite_numeric(monthly["monthly_change"]).dropna()
                calendar.append({
                    "candidate_policy_id": candidate_id, "feature_component": component,
                    "feature_key": feature_key, "calendar_month": int(month),
                    "rows": len(monthly), "valid_rows": len(valid_monthly),
                    "mean": valid_monthly.mean(), "standard_deviation": valid_monthly.std(),
                    "mean_absolute_value": valid_monthly.abs().mean(),
                    "mean_absolute_monthly_change": finite_monthly_changes.abs().mean(),
                })
        wide = target.pivot(index=["geo_id", "date"], columns="feature_key", values="raw_feature_value")
        for left_component, right_component in pairs:
            left_key = FEATURE_KEY_BY_COMPONENT[left_component]
            right_key = FEATURE_KEY_BY_COMPONENT[right_component]
            overlap = wide[[left_key, right_key]].apply(_finite_numeric).dropna()
            if overlap.empty:
                raise ValueError(f"Empty correlation overlap for {candidate_id}/{left_component}/{right_component}")
            correlations.append({
                "candidate_policy_id": candidate_id,
                "left_feature_component": left_component,
                "right_feature_component": right_component,
                "left_feature_key": left_key, "right_feature_key": right_key,
                "overlap_rows": len(overlap),
                "correlation": overlap[left_key].corr(overlap[right_key]),
            })
    return ReviewResult(tables={
        "inventory_candidate_feature_statistics": pd.DataFrame(statistics),
        "inventory_candidate_feature_correlations": pd.DataFrame(correlations),
        "inventory_candidate_calendar_month_behavior": pd.DataFrame(calendar),
    }, metadata={
        "generator_id": "inventory_phase_a_structural_window_behavior",
        "generator_version": "1.0",
        "candidate_policy_ids": list(challengers),
        "sign_flip_definition": (
            "Monthly differences are computed within feature and geography; missing or non-finite "
            "changes break eligible sequences and zero changes are ignored; flips are divided by "
            "eligible consecutive nonzero-sign transitions within geography; zero is returned "
            "when no transition is eligible."
        ),
        "constant_correlation_behavior": "NaN when overlap exists but either series is constant",
    })


def _baseline_comparison_evidence(
    baseline: pd.DataFrame,
    challengers: Mapping[str, InMemoryChallengerArtifacts],
) -> ReviewResult:
    rows = []
    for candidate_id in _candidate_ids(challengers):
        candidate = challengers[candidate_id].features
        for component in FEATURE_COMPONENTS:
            feature_key = FEATURE_KEY_BY_COMPONENT[component]
            base = _feature_rows(baseline, feature_key)
            other = _feature_rows(candidate, feature_key)
            merged = _outer_value_reconciliation(base, other)
            both = merged["_merge"].eq("both")
            if not both.any():
                raise ValueError(f"Empty required baseline key overlap for {candidate_id}/{feature_key}")
            valid = merged.loc[both].copy()
            valid["raw_feature_value_baseline"] = _finite_numeric(
                valid["raw_feature_value_baseline"]
            )
            valid["raw_feature_value_challenger"] = _finite_numeric(
                valid["raw_feature_value_challenger"]
            )
            valid = valid.dropna(
                subset=["raw_feature_value_baseline", "raw_feature_value_challenger"]
            )
            if valid.empty:
                raise ValueError(f"Empty required baseline value overlap for {candidate_id}/{feature_key}")
            differences = (valid["raw_feature_value_challenger"] - valid["raw_feature_value_baseline"]).abs()
            baseline_sign = _sign_flip_rate(base)
            challenger_sign = _sign_flip_rate(other)
            baseline_std = valid["raw_feature_value_baseline"].std()
            challenger_std = valid["raw_feature_value_challenger"].std()
            rows.append({
                "candidate_policy_id": candidate_id, "feature_component": component,
                "feature_key": feature_key, "baseline_rows": len(base),
                "challenger_rows": len(other), "overlap_rows": int(both.sum()),
                "baseline_only_rows": int(merged["_merge"].eq("left_only").sum()),
                "challenger_only_rows": int(merged["_merge"].eq("right_only").sum()),
                "valid_comparison_rows": len(valid),
                "correlation": valid["raw_feature_value_baseline"].corr(valid["raw_feature_value_challenger"]),
                "mean_absolute_difference": differences.mean(),
                "median_absolute_difference": differences.median(),
                "p90_absolute_difference": differences.quantile(0.9),
                "maximum_absolute_difference": differences.max(),
                "baseline_standard_deviation": baseline_std,
                "challenger_standard_deviation": challenger_std,
                "standard_deviation_difference": challenger_std - baseline_std,
                "baseline_sign_flip_rate": baseline_sign,
                "challenger_sign_flip_rate": challenger_sign,
                "sign_flip_rate_difference": challenger_sign - baseline_sign,
            })
    return ReviewResult(
        tables={"inventory_candidate_baseline_feature_comparison": pd.DataFrame(rows)},
        metadata={
            "generator_id": "inventory_phase_a_baseline_comparison",
            "generator_version": "1.0",
            "candidate_policy_ids": list(challengers),
            "key_reconciliation": "outer merge on geo_id/date/canonical_metric_key/feature_key",
        },
    )


def run_phase_a_foundation_evidence(
    *, campaign_id: str, campaign_version: str, artifact_root: str | Path,
    baseline_run_id: str = AUTHORITATIVE_RUN_ID,
) -> PhaseAEvidence:
    """Load, materialize and assemble Phase A descriptive evidence only."""
    baseline = _load_authoritative_baseline(artifact_root, baseline_run_id)
    campaign = build_inventory_calibration_campaign(
        campaign_id=campaign_id, campaign_version=campaign_version,
        baseline_run_id=baseline_run_id, incumbent_run_id=baseline_run_id,
    )
    challengers = materialize_phase_a_challengers(campaign, baseline.features, baseline.source_metrics)
    evidence = {
        "campaign_definition": _campaign_definition_evidence(campaign),
        "coverage_and_lineage": _coverage_lineage_evidence(challengers, baseline.features),
        "structural_window_behavior": _structural_behavior_evidence(challengers),
        "baseline_comparison": _baseline_comparison_evidence(baseline.features, challengers),
    }
    bundle = assemble_review_results(campaign.campaign_id, evidence)
    return PhaseAEvidence(campaign, challengers, evidence, bundle)


def assemble_review_results(
    campaign_id: str,
    sections: Mapping[str, ReviewResult],
) -> ReviewBundle:
    if not sections:
        raise ValueError("At least one review section is required")
    bundle = ReviewBundle(campaign_id=campaign_id)
    table_names: set[str] = set()
    plot_names: set[str] = set()
    normalized_sections: dict[str, ReviewResult] = {}
    for section_name, result in sections.items():
        if not isinstance(section_name, str):
            raise ValueError("Review section names must be strings")
        section = section_name.strip().lower()
        if not SECTION_IDENTIFIER.fullmatch(section):
            raise ValueError(
                "Review section names must be path-safe lowercase identifiers"
            )
        if section in normalized_sections:
            raise ValueError(f"Duplicate normalized review section: {section}")
        normalized_sections[section] = result
    for section, result in sorted(normalized_sections.items()):
        for name, frame in sorted(result.tables.items()):
            if name in table_names:
                raise ValueError(f"Duplicate review table name across sections: {name}")
            table_names.add(name)
            bundle.add_table(name, frame.copy(deep=True), subdirectory=f"tables/{section}")
        for plot in sorted(result.plots, key=lambda item: (item.name, item.path.as_posix())):
            if plot.name in plot_names:
                raise ValueError(f"Duplicate review plot name across sections: {plot.name}")
            plot_names.add(plot.name)
            bundle.add_plot(plot.name, plot.path, section=section)
    return bundle
