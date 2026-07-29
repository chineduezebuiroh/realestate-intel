from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from regime._00_config_loader import load_regime_config
from regime.review.models import ReviewBundle
from regime.review.results import ReviewResult
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
INVENTORY_FEATURE_KEYS = frozenset({
    "redfin_inventory_level",
    "redfin_inventory_short",
    "redfin_inventory_long",
})


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
