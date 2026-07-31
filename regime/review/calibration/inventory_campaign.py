from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
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
EVIDENCE_CONTRACT_VERSION = "inventory_phase_a_evidence_v2"
AUTHORITATIVE_PRODUCER_ID = "inventory_phase_a_authoritative_smoke"
AUTHORITATIVE_PRODUCER_CODE_IDENTITY = "inventory_phase_a_authoritative_producer_v2"
COMPLETION_MARKER = "producer_completion.json"
GEO_METADATA_SOURCE = "config/geo_manifest.generated.csv (geo_slug/level)"
GEO_IDENTITY_CROSSWALK = "config/inventory_phase8c_geo_identity_crosswalk.csv"


@dataclass(frozen=True, slots=True)
class _BaselineInputs:
    run_id: str
    features: pd.DataFrame
    source_metrics: pd.DataFrame
    system_artifacts: Mapping[str, pd.DataFrame]
    manifest: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PhaseAEvidence:
    """Materialized Phase A evidence; intentionally contains no decision."""

    campaign: CalibrationCampaign
    challengers: Mapping[str, InMemoryChallengerArtifacts]
    evidence_results: Mapping[str, ReviewResult]
    review_bundle: ReviewBundle
    system_evidence: Any | None = None


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
    artifact_root: str | Path, run_id: str = AUTHORITATIVE_RUN_ID, *, include_system: bool = False,
) -> _BaselineInputs:
    """Load the immutable baseline inputs and downstream system artifacts."""
    if run_id != AUTHORITATIVE_RUN_ID:
        raise ValueError(f"Phase A baseline must be {AUTHORITATIVE_RUN_ID!r}")
    run_dir = Path(artifact_root) / run_id
    paths = {
        "features": run_dir / "features.parquet",
        "source_metrics": run_dir / "source_metrics.parquet",
        "manifest": run_dir / "manifest.json",
        **({name: run_dir / f"{name}.parquet" for name in (
            "aligned_metric_scores", "dimension_scores", "axis_scores",
            "coordinates", "regime_assignments",
        )} if include_system else {}),
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
    system_artifacts = {
        name: pd.read_parquet(path)
        for name, path in paths.items()
        if name not in {"features", "source_metrics", "manifest"}
    }
    return _BaselineInputs(run_id, features, sources, system_artifacts, manifest)


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
    allowed_geo_levels: tuple[str, ...] = ("county",),
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
        allowed_geo_levels=allowed_geo_levels,
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


def _file_sha256(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _geography_identities(
    geo_ids: set[str], manifest_path: str | Path = "config/geo_manifest.generated.csv",
    crosswalk_path: str | Path = GEO_IDENTITY_CROSSWALK,
) -> dict[str, dict[str, str]]:
    """Resolve canonical and governed retired identities, without name heuristics."""
    manifest = pd.read_csv(manifest_path, dtype=str)
    if not {"geo_slug", "level"}.issubset(manifest.columns):
        raise ValueError("Geography manifest must contain geo_slug and level")
    if manifest["geo_slug"].duplicated().any():
        raise ValueError("Geography manifest geo_slug values must be unique")
    manifest = manifest.fillna("")
    canonical = manifest.set_index("geo_slug")["level"].str.strip().str.lower().to_dict()
    aliases = pd.read_csv(crosswalk_path, dtype=str).fillna("")
    required = {"legacy_geo_id", "canonical_geo_slug", "mapping_method", "mapping_source",
                "mapping_version", "legacy_level", "canonical_level"}
    if not required.issubset(aliases.columns):
        raise ValueError(f"Geography identity crosswalk is missing columns: {sorted(required - set(aliases.columns))}")
    if aliases.duplicated().any() or aliases["legacy_geo_id"].duplicated().any():
        raise ValueError("Duplicate or ambiguous legacy identity in geography crosswalk")
    resolved: dict[str, dict[str, str]] = {}
    alias_map = aliases.set_index("legacy_geo_id").to_dict("index")
    for source_id in sorted(geo_ids):
        if source_id in canonical:
            resolved[source_id] = {"canonical_geo_slug": source_id, "resolved_level": canonical[source_id],
                "identity_resolution_method": "canonical_geo_slug_direct", "identity_resolution_source": str(manifest_path)}
            continue
        if source_id not in alias_map:
            raise ValueError(f"Unresolved legacy geography identity: {source_id}")
        alias = alias_map[source_id]
        target = alias["canonical_geo_slug"].strip()
        level = alias["canonical_level"].strip().lower()
        if target:
            if target not in canonical:
                raise ValueError(f"Invalid crosswalk target absent from authoritative geography manifest: {target}")
            if level != canonical[target] or alias["legacy_level"].strip().lower() != level:
                raise ValueError(f"Contradictory geography level mapping for legacy identity: {source_id}")
        elif alias["mapping_method"] != "governed_retired_identity_exclusion" or level == "county":
            raise ValueError(f"Invalid crosswalk target for legacy identity: {source_id}")
        resolved[source_id] = {"canonical_geo_slug": target, "resolved_level": level,
            "identity_resolution_method": alias["mapping_method"],
            "identity_resolution_source": alias["mapping_source"]}
    return resolved


def resolve_phase_a_geography_scope(
    campaign: CalibrationCampaign,
    baseline_features: pd.DataFrame,
    source_metrics: pd.DataFrame,
    *,
    manifest_path: str | Path = "config/geo_manifest.generated.csv",
    crosswalk_path: str | Path = GEO_IDENTITY_CROSSWALK,
) -> tuple[CalibrationCampaign, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fail closed and filter both baseline inputs at the campaign boundary."""
    all_ids = set(baseline_features["geo_id"].astype(str)) | set(source_metrics["geo_id"].astype(str))
    identities = _geography_identities(all_ids, manifest_path, crosswalk_path)
    levels = {key: value["resolved_level"] for key, value in identities.items()}
    allowed = set(campaign.allowed_geo_levels)
    if allowed != {"county"}:
        raise ValueError("Macro Phase 8c v1 allowed_geo_levels must be exactly ('county',)")
    feature_target_ids = set(baseline_features.loc[
        baseline_features["feature_key"].isin(INVENTORY_FEATURE_KEYS), "geo_id"
    ].astype(str))
    source_target_ids = set(source_metrics.loc[
        source_metrics["canonical_metric_key"].eq(campaign.target_metric), "geo_id"
    ].astype(str))
    available = feature_target_ids & source_target_ids
    def preferred_sources(source_ids: set[str]) -> list[str]:
        by_canonical: dict[str, list[str]] = {}
        for source_id in sorted(source_ids):
            identity = identities[source_id]
            if identity["resolved_level"] in allowed:
                by_canonical.setdefault(identity["canonical_geo_slug"], []).append(source_id)
        chosen = []
        for canonical_slug, sources_for_canonical in sorted(by_canonical.items()):
            if canonical_slug in sources_for_canonical:
                chosen.append(canonical_slug)
            elif len(sources_for_canonical) == 1:
                chosen.append(sources_for_canonical[0])
            else:
                raise ValueError(f"Ambiguous legacy geography identity for canonical county {canonical_slug}: {sources_for_canonical}")
        return chosen
    if campaign.manual_geo_ids:
        manual = _geography_identities(set(campaign.manual_geo_ids), manifest_path, crosswalk_path)
        forbidden = sorted(geo for geo, identity in manual.items() if identity["resolved_level"] not in allowed)
        if forbidden:
            status = " ZIP is reserved for future local-regime work." if any(manual[g]["resolved_level"] in {"zip", "zip_code"} for g in forbidden) else ""
            raise ValueError(f"Non-county manual request outside macro Phase 8c county scope: {forbidden}.{status}")
        requested_to_canonical = {key: value["canonical_geo_slug"] for key, value in manual.items()}
        if len(set(requested_to_canonical.values())) != len(requested_to_canonical):
            raise ValueError("Mixed manual geography IDs resolve to duplicate canonical counties")
        available_canonical = {identities[g]["canonical_geo_slug"] for g in available}
        missing = sorted(set(requested_to_canonical.values()).difference(available_canonical))
        if missing:
            raise ValueError(f"Manual county geography IDs lack required authoritative target coverage: {missing}")
        included_canonical = set(requested_to_canonical.values())
        included = [g for g in preferred_sources(available) if identities[g]["canonical_geo_slug"] in included_canonical]
    else:
        included = preferred_sources(available)
    if not included:
        raise ValueError("Macro Phase 8c resolved an empty county geography universe")
    included_set = set(included)
    rows = []
    for geo_id in sorted(all_ids):
        is_included = geo_id in included_set
        rows.append({
            "campaign_id": campaign.campaign_id, "campaign_version": campaign.campaign_version,
            "geo_id": geo_id, "source_geo_id": geo_id,
            "canonical_geo_slug": identities[geo_id]["canonical_geo_slug"],
            "geo_level": levels[geo_id], "resolved_level": levels[geo_id], "included": is_included,
            "identity_resolution_method": identities[geo_id]["identity_resolution_method"],
            "identity_resolution_source": identities[geo_id]["identity_resolution_source"],
            "inclusion_reason": "manual_county_subset" if is_included and campaign.manual_geo_ids else ("all_authoritative_counties" if is_included else None),
            "exclusion_reason": None if is_included else (
                "superseded_legacy_identity" if levels[geo_id] in allowed and identities[geo_id]["canonical_geo_slug"] in {identities[g]["canonical_geo_slug"] for g in included}
                else ("manual_subset_not_selected" if levels[geo_id] in allowed else "excluded_level_not_allowed_for_macro_phase_8c")),
            "metadata_source": GEO_METADATA_SOURCE,
        })
    lineage = pd.DataFrame(rows)
    excluded = lineage.loc[~lineage["included"], "geo_level"].value_counts().sort_index().to_dict()
    metadata = dict(campaign.metadata)
    metadata["geography_scope"] = {
        "regime_scope": "macro", "requested_allowed_geo_levels": list(campaign.allowed_geo_levels),
        "included_geo_levels": sorted({levels[item] for item in included}),
        "included_geo_ids": sorted(identities[g]["canonical_geo_slug"] for g in included), "included_geo_count": len(included),
        "excluded_geo_counts_by_level": {str(k): int(v) for k, v in excluded.items()},
        "metadata_source": GEO_METADATA_SOURCE, "manual_subset_applied": bool(campaign.manual_geo_ids),
        "authoritative_geography_manifest_path": str(manifest_path),
        "authoritative_geography_manifest_hash": _file_sha256(manifest_path),
        "authoritative_identity_column": "geo_slug", "identity_crosswalk_path": str(crosswalk_path),
        "identity_crosswalk_hash": _file_sha256(crosswalk_path), "identity_resolution_mode": "canonical_then_governed_crosswalk",
        "source_geo_count": len(all_ids), "resolved_geo_count": len(identities), "unresolved_geo_count": 0,
        "zip_future_status": "reserved_for_future_local_regime",
        "city_status": "out_of_scope_no_current_regime_role",
    }
    resolved = replace(campaign, metadata=metadata)
    features = baseline_features[baseline_features["geo_id"].isin(included)].copy()
    sources = source_metrics[source_metrics["geo_id"].isin(included)].copy()
    features["geo_id"] = features["geo_id"].map(lambda item: identities[str(item)]["canonical_geo_slug"])
    sources["geo_id"] = sources["geo_id"].map(lambda item: identities[str(item)]["canonical_geo_slug"])
    features = features.sort_values(FEATURE_KEYS_COLUMNS, kind="mergesort").reset_index(drop=True)
    sources = sources.sort_values(["geo_id", "date", "canonical_metric_key"], kind="mergesort").reset_index(drop=True)
    if features.empty or sources.empty:
        raise ValueError("Resolved county inputs lack required campaign coverage")
    return resolved, features, sources, lineage


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
    approved_geos = set(baseline_features.loc[
        baseline_features["feature_key"].isin(INVENTORY_FEATURE_KEYS), "geo_id"
    ].astype(str))
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
        candidate_geos = set(challenger.features.loc[
            challenger.features["feature_key"].isin(INVENTORY_FEATURE_KEYS), "geo_id"
        ].astype(str))
        if candidate_geos != approved_geos:
            raise ValueError(f"Candidate geography universe mismatch: {candidate_id}")
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
    manual_geo_ids: tuple[str, ...] = (),
    persist_system_evidence: bool = False,
) -> PhaseAEvidence:
    """Load, materialize and assemble Phase A descriptive evidence only."""
    baseline = _load_authoritative_baseline(
        artifact_root, baseline_run_id, include_system=persist_system_evidence
    )
    campaign = build_inventory_calibration_campaign(
        campaign_id=campaign_id, campaign_version=campaign_version,
        baseline_run_id=baseline_run_id, incumbent_run_id=baseline_run_id,
        manual_geo_ids=manual_geo_ids,
    )
    campaign, scoped_features, scoped_sources, geography_lineage = resolve_phase_a_geography_scope(
        campaign, baseline.features, baseline.source_metrics
    )
    challengers = materialize_phase_a_challengers(campaign, scoped_features, scoped_sources)
    evidence = {
        "campaign_definition": _campaign_definition_evidence(campaign),
        "geography_scope": ReviewResult(
            tables={"inventory_campaign_geography_scope": geography_lineage},
            metadata={"generator_id": "inventory_campaign_geography_scope", **campaign.metadata["geography_scope"]},
        ),
        "coverage_and_lineage": _coverage_lineage_evidence(challengers, scoped_features),
        "structural_window_behavior": _structural_behavior_evidence(challengers),
        "baseline_comparison": _baseline_comparison_evidence(scoped_features, challengers),
    }
    bundle = assemble_review_results(campaign.campaign_id, evidence)
    system_evidence = None
    if persist_system_evidence:
        from .system_evidence import assemble_inventory_system_evidence
        system_evidence = assemble_inventory_system_evidence(
            campaign=campaign, baseline_artifacts=baseline.system_artifacts,
            candidate_artifacts={key: value.as_mapping() for key, value in challengers.items()},
        )
    result = PhaseAEvidence(campaign, challengers, evidence, bundle, system_evidence)
    if persist_system_evidence:
        persist_phase_a_foundation_evidence(result, Path(artifact_root) / "calibration_campaigns")
    return result


def evidence_directory(root: str | Path, campaign_id: str, campaign_version: str) -> Path:
    return Path(root) / "calibration_campaigns" / campaign_id / campaign_version


def invalidate_authoritative_evidence_readiness(
    *, artifact_root: str | Path, campaign_id: str, campaign_version: str,
) -> Path:
    """Invalidate only current-producer readiness; historical evidence stays intact."""
    marker = evidence_directory(artifact_root, campaign_id, campaign_version) / COMPLETION_MARKER
    marker.unlink(missing_ok=True)
    return marker


def persist_phase_a_foundation_evidence(
    evidence: PhaseAEvidence, root: str | Path, *, _failure_point: str | None = None,
) -> Path:
    """Validate, stage, hash, atomically publish, then certify immutable evidence."""
    from .system_evidence import (
        CalibrationSystemEvidence, SYSTEM_EVIDENCE_CONTRACT_VERSION, validate_system_evidence,
    )

    if evidence.system_evidence is None:
        raise ValueError("Complete System Evidence is required for persistence")
    validate_system_evidence(evidence.system_evidence)
    phase_tables = _all_evidence_tables(evidence)
    directory = Path(root) / evidence.campaign.campaign_id / evidence.campaign.campaign_version
    directory.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{directory.name}.staging-", dir=directory.parent))
    backup = directory.parent / f".{directory.name}.previous"
    try:
        (staging / "phase_a").mkdir(); (staging / "system").mkdir()
        records = []
        for kind, supplied in (("phase_a", phase_tables), ("system", evidence.system_evidence.tables)):
            for name, frame in sorted(supplied.items()):
                path = staging / kind / f"{name}.parquet"; frame.to_parquet(path, index=False)
                records.append({"kind": kind, "name": name, "sha256": _file_sha256(path)})
                if _failure_point == "write":
                    raise RuntimeError("injected staging write failure")
        source_manifest = Path(root).parent / evidence.campaign.baseline_run_id / "manifest.json"
        metadata = {
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
            "system_evidence_contract_version": SYSTEM_EVIDENCE_CONTRACT_VERSION,
            "campaign": evidence.campaign.to_dict(), "files": records,
            "representative_geography_rule": evidence.system_evidence.representative_geography_rule,
            "transition_window_rule": evidence.system_evidence.transition_window_rule,
            "source_run_id": evidence.campaign.baseline_run_id,
            "source_artifact_identity": {
                "manifest_sha256": _file_sha256(source_manifest) if source_manifest.is_file() else None,
            },
        }
        if _failure_point == "manifest":
            raise RuntimeError("injected pre-manifest failure")
        manifest_path = staging / "evidence_manifest.json"
        manifest_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        for record in records:
            path = staging / record["kind"] / f"{record['name']}.parquet"
            if not path.is_file() or _file_sha256(path) != record["sha256"]:
                raise ValueError(f"Staging artifact hash mismatch: {path}")
        staged_system = {
            record["name"]: pd.read_parquet(staging / "system" / f"{record['name']}.parquet")
            for record in records if record["kind"] == "system"
        }
        validate_system_evidence(CalibrationSystemEvidence(
            campaign_id=evidence.campaign.campaign_id,
            campaign_version=evidence.campaign.campaign_version,
            candidate_policy_ids=evidence.campaign.candidate_policy_ids,
            incumbent_policy_id=evidence.campaign.incumbent_policy_id,
            baseline_policy_id=evidence.campaign.baseline_policy_id,
            target_metric=evidence.campaign.target_metric,
            target_dimension=evidence.campaign.target_dimension,
            target_axis=evidence.campaign.target_axis,
            tables=staged_system,
            representative_geography_rule=evidence.system_evidence.representative_geography_rule,
            transition_window_rule=evidence.system_evidence.transition_window_rule,
        ))
        if backup.exists(): shutil.rmtree(backup)
        if directory.exists(): os.replace(directory, backup)
        try:
            os.replace(staging, directory)
        except Exception:
            if backup.exists(): os.replace(backup, directory)
            raise
        if backup.exists(): shutil.rmtree(backup)
        completion = {
            "campaign_id": evidence.campaign.campaign_id,
            "campaign_version": evidence.campaign.campaign_version,
            "evidence_manifest_sha256": _file_sha256(directory / "evidence_manifest.json"),
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
            "system_evidence_contract_version": SYSTEM_EVIDENCE_CONTRACT_VERSION,
            "producer_id": AUTHORITATIVE_PRODUCER_ID,
            "producer_code_identity": AUTHORITATIVE_PRODUCER_CODE_IDENTITY,
            "source_run_id": evidence.campaign.baseline_run_id,
            "source_artifact_identity": metadata["source_artifact_identity"],
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        temporary_marker = directory / f".{COMPLETION_MARKER}.tmp"
        temporary_marker.write_text(json.dumps(completion, indent=2, sort_keys=True) + "\n")
        os.replace(temporary_marker, directory / COMPLETION_MARKER)
        return directory
    finally:
        if staging.exists(): shutil.rmtree(staging)


def _all_evidence_tables(evidence: PhaseAEvidence) -> dict[str, pd.DataFrame]:
    tables = {}
    for result in evidence.evidence_results.values():
        for name, frame in result.tables.items():
            if name in tables:
                raise ValueError(f"Duplicate Phase A evidence table: {name}")
            tables[name] = frame
    return tables


def load_phase_a_foundation_evidence(
    *, campaign_id: str, campaign_version: str, artifact_root: str | Path,
) -> PhaseAEvidence:
    """Load and hash-verify renderer-ready immutable authoritative evidence."""
    from .system_evidence import CalibrationSystemEvidence, validate_system_evidence
    directory = Path(artifact_root) / "calibration_campaigns" / campaign_id / campaign_version
    manifest_path = directory / "evidence_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Persisted Phase A evidence is missing: {manifest_path}")
    metadata = json.loads(manifest_path.read_text())
    campaign = CalibrationCampaign(**metadata["campaign"])
    phase_tables, system_tables = {}, {}
    for record in metadata["files"]:
        path = directory / record["kind"] / f"{record['name']}.parquet"
        if _file_sha256(path) != record["sha256"]:
            raise ValueError(f"Persisted evidence hash mismatch: {path}")
        (phase_tables if record["kind"] == "phase_a" else system_tables)[record["name"]] = pd.read_parquet(path)
    result = ReviewResult(tables=phase_tables)
    system = CalibrationSystemEvidence(
        campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
        candidate_policy_ids=campaign.candidate_policy_ids, incumbent_policy_id=campaign.incumbent_policy_id,
        baseline_policy_id=campaign.baseline_policy_id, target_metric=campaign.target_metric,
        target_dimension=campaign.target_dimension, target_axis=campaign.target_axis, tables=system_tables,
        representative_geography_rule=metadata["representative_geography_rule"],
        transition_window_rule=metadata["transition_window_rule"],
    )
    validate_system_evidence(system)
    return PhaseAEvidence(campaign, {}, {"persisted_authoritative": result},
                          assemble_review_results(campaign.campaign_id, {"persisted_authoritative": result}), system)


def validate_current_authoritative_evidence(
    *, campaign_id: str, campaign_version: str, artifact_root: str | Path,
    source_run_id: str = AUTHORITATIVE_RUN_ID,
) -> PhaseAEvidence:
    """Fail closed unless historical evidence has current-producer certification."""
    from .system_evidence import SYSTEM_EVIDENCE_CONTRACT_VERSION
    directory = evidence_directory(artifact_root, campaign_id, campaign_version)
    if not directory.is_dir(): raise FileNotFoundError(f"persisted evidence absent: {directory}")
    manifest = directory / "evidence_manifest.json"
    if not manifest.is_file(): raise ValueError(f"persisted evidence incomplete: {manifest}")
    marker_path = directory / COMPLETION_MARKER
    if not marker_path.is_file(): raise ValueError(f"producer completion marker absent: {marker_path}")
    marker = json.loads(marker_path.read_text())
    if marker.get("campaign_id") != campaign_id or marker.get("campaign_version") != campaign_version:
        raise ValueError("producer identity stale: campaign identity mismatch")
    if marker.get("producer_id") != AUTHORITATIVE_PRODUCER_ID or marker.get("producer_code_identity") != AUTHORITATIVE_PRODUCER_CODE_IDENTITY:
        raise ValueError("producer identity stale")
    if marker.get("evidence_contract_version") != EVIDENCE_CONTRACT_VERSION or marker.get("system_evidence_contract_version") != SYSTEM_EVIDENCE_CONTRACT_VERSION:
        raise ValueError("contract-version mismatch")
    if marker.get("source_run_id") != source_run_id:
        raise ValueError("source-run mismatch")
    if marker.get("evidence_manifest_sha256") != _file_sha256(manifest):
        raise ValueError("manifest hash mismatch")
    metadata = json.loads(manifest.read_text())
    for record in metadata.get("files", []):
        path = directory / record["kind"] / f"{record['name']}.parquet"
        if not path.is_file():
            raise ValueError(f"persisted evidence incomplete: {path}")
        if _file_sha256(path) != record.get("sha256"):
            raise ValueError(f"artifact hash mismatch: {path}")
    if marker.get("source_artifact_identity") != metadata.get("source_artifact_identity"):
        raise ValueError("source-run mismatch: source artifact identity differs")
    source_manifest = Path(artifact_root) / source_run_id / "manifest.json"
    expected_source_hash = metadata.get("source_artifact_identity", {}).get("manifest_sha256")
    if expected_source_hash is not None and (
        not source_manifest.is_file() or _file_sha256(source_manifest) != expected_source_hash
    ):
        raise ValueError("source-run mismatch: source manifest identity differs")
    evidence = load_phase_a_foundation_evidence(
        campaign_id=campaign_id, campaign_version=campaign_version, artifact_root=artifact_root,
    )
    if metadata.get("evidence_contract_version") != EVIDENCE_CONTRACT_VERSION or metadata.get("system_evidence_contract_version") != SYSTEM_EVIDENCE_CONTRACT_VERSION:
        raise ValueError("contract-version mismatch")
    if metadata.get("source_run_id") != source_run_id:
        raise ValueError("source-run mismatch")
    return evidence


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
