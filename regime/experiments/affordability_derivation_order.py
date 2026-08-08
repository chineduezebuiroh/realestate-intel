"""Phase 4A diagnostic for Affordability derivation/smoothing order.

This module is deliberately experimental.  It reuses the canonical derived
metric builder and does not alter any production registry.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

from regime.derived_metrics import DERIVED_METRIC_COMPONENTS, build_derived_metrics_with_lineage
from regime.linked_price_family import build_structural_level


POLICY_A: Final = "AFF-DERIVATION-A"
POLICY_B: Final = "AFF-DERIVATION-B"
TARGET_METRICS: Final = ("price_to_income", "payment_burden")
FEATURE_WEIGHTS: Final = {"level": 0.50, "short": 0.20, "long": 0.30}
LEVEL_WINDOW: Final = 12
SHORT_LAG: Final = 3
LONG_LAG: Final = 12
FORMULA_TOLERANCE: Final = 1e-12


@dataclass(frozen=True)
class AffordabilityDerivationEvidence:
    tables: dict[str, pd.DataFrame]


def policy_registry() -> pd.DataFrame:
    rows = [
        (POLICY_A, "MA12(price) -> canonical derive -> lag3/lag12", True),
        (POLICY_B, "canonical derive(raw inputs) -> MA12 -> lag3/lag12", False),
    ]
    return pd.DataFrame([
        {
            "policy": policy, "derivation_order": order,
            "price_smoothed_before_derivation": price_smoothed,
            "income_smoothed_before_derivation": False,
            "mortgage_smoothed_before_derivation": False,
            "level_window": LEVEL_WINDOW, "short_lag": SHORT_LAG,
            "long_lag": LONG_LAG, "level_weight": FEATURE_WEIGHTS["level"],
            "short_weight": FEATURE_WEIGHTS["short"],
            "long_weight": FEATURE_WEIGHTS["long"],
            "recommendation_state": "none", "promotion_state": "none",
            "human_decision": "pending",
        }
        for policy, order, price_smoothed in rows
    ])


def _validate_source(source: pd.DataFrame) -> pd.DataFrame:
    required = {"geo_id", "date", "canonical_metric_key", "value"}
    missing = required - set(source.columns)
    if missing:
        raise ValueError(f"Source observations are missing columns: {sorted(missing)}")
    work = source.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    relevant = work[work.canonical_metric_key.isin({
        "median_sale_price", "median_household_income", "mortgage_30y"
    })]
    if relevant["date"].isna().any() or not np.isfinite(relevant["value"]).all():
        raise ValueError("Required source inputs contain invalid dates or non-finite values")
    dup = work.duplicated(["geo_id", "date", "canonical_metric_key"], keep=False)
    if dup.any():
        raise ValueError("Source observations contain duplicate geo/date/metric keys")
    # Monthly price histories define the local diagnostic chronology.
    for geo_id, group in relevant[relevant.canonical_metric_key.eq("median_sale_price")].groupby("geo_id"):
        dates = group.date.sort_values()
        if not dates.is_monotonic_increasing:
            raise ValueError(f"Non-monotonic price chronology for {geo_id}")
        expected = pd.date_range(dates.iloc[0], dates.iloc[-1], freq="ME")
        if not dates.reset_index(drop=True).equals(pd.Series(expected)):
            raise ValueError(f"Unexpected interior monthly price gap for {geo_id}")
    return work.sort_values(["geo_id", "canonical_metric_key", "date"]).reset_index(drop=True)


def _policy_source(source: pd.DataFrame, policy: str) -> pd.DataFrame:
    if policy == POLICY_B:
        return source.copy()
    price = source[source.canonical_metric_key.eq("median_sale_price")]
    level = build_structural_level(price, level_window=LEVEL_WINDOW)
    replacement = level[["geo_id", "date", "structural_level_value"]]
    out = source.merge(replacement, on=["geo_id", "date"], how="left", validate="many_to_one")
    mask = out.canonical_metric_key.eq("median_sale_price")
    out.loc[mask, "value"] = out.loc[mask, "structural_level_value"]
    return out.drop(columns="structural_level_value")


def _derive(source: pd.DataFrame, policy: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    derived, lineage = build_derived_metrics_with_lineage(_policy_source(source, policy))
    derived = derived[derived.canonical_metric_key.isin(TARGET_METRICS)].copy()
    lineage = lineage[lineage.derived_metric_key.isin(TARGET_METRICS)].copy()
    derived["policy"] = policy
    if policy == POLICY_A:
        derived["pre_feature_derived_value"] = derived["value"]
        derived["structural_level"] = derived["value"]
    else:
        derived["pre_feature_derived_value"] = derived["value"]
        smoothed = build_structural_level(derived, level_window=LEVEL_WINDOW)
        derived["structural_level"] = smoothed["structural_level_value"]
    return derived, lineage


def _input_audit(lineage: pd.DataFrame, policy: str) -> pd.DataFrame:
    out = lineage.rename(columns={
        "derived_metric_key": "metric", "component_metric_key": "input_name",
        "component_value": "input_value", "component_source_date": "input_source_date",
        "component_source_geo_id": "input_source_geo", "was_carried_forward": "forward_filled_flag",
    }).copy()
    out["input_frequency"] = np.where(out.input_name.eq("median_household_income"), "annual_forward_filled", "monthly")
    out["smoothing_applied_before_derivation"] = out.input_name.eq("median_sale_price") & (policy == POLICY_A)
    out["policy"] = policy
    columns = ["metric", "date", "geo_id", "input_name", "input_value", "input_source_date",
               "input_source_geo", "input_frequency", "forward_filled_flag",
               "smoothing_applied_before_derivation", "policy"]
    return out[columns]


def _features(raw: pd.DataFrame) -> pd.DataFrame:
    work = raw.sort_values(["policy", "canonical_metric_key", "geo_id", "date"]).copy()
    group = work.groupby(["policy", "canonical_metric_key", "geo_id"], sort=False)
    work["short_feature"] = work.structural_level / group.structural_level.shift(SHORT_LAG) - 1
    work["long_feature"] = work.structural_level / group.structural_level.shift(LONG_LAG) - 1
    return work.rename(columns={"canonical_metric_key": "metric"})[
        ["policy", "metric", "geo_id", "date", "structural_level", "short_feature", "long_feature"]
    ]


def _formula_audit(raw: pd.DataFrame, audits: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for policy in (POLICY_A, POLICY_B):
        p = audits[audits.policy.eq(policy)]
        reconstructed_source = p.rename(columns={"metric": "derived_metric", "input_name": "canonical_metric_key", "input_value": "value"})
        reconstructed_source = reconstructed_source[["geo_id", "date", "canonical_metric_key", "value"]].drop_duplicates()
        rebuilt, _ = build_derived_metrics_with_lineage(reconstructed_source)
        expected = raw[raw.policy.eq(policy)][["geo_id", "date", "canonical_metric_key", "pre_feature_derived_value"]]
        check = expected.merge(rebuilt, on=["geo_id", "date", "canonical_metric_key"], how="left", validate="one_to_one")
        check["absolute_error"] = (check.pre_feature_derived_value - check.value).abs()
        check["within_tolerance"] = check.absolute_error.le(FORMULA_TOLERANCE)
        check["policy"] = policy
        rows.append(check.rename(columns={"canonical_metric_key": "metric", "value": "reconstructed_value"}))
    result = pd.concat(rows, ignore_index=True)
    if result.reconstructed_value.isna().any() or not result.within_tolerance.all():
        raise AssertionError("Canonical formula reconstruction disagreement")
    return result


def _stability(features: pd.DataFrame) -> pd.DataFrame:
    work = features.copy()
    group = work.groupby(["policy", "metric", "geo_id"], sort=False)
    work["mom"] = group.structural_level.pct_change(fill_method=None)
    rows = []
    for (policy, metric), frame in work.groupby(["policy", "metric"]):
        movement = frame.mom.dropna().abs()
        signed = frame.mom.dropna()
        rows.append({"policy": policy, "metric": metric,
                     "median_abs_mom": movement.median(), "p90_abs_mom": movement.quantile(.90),
                     "p99_abs_mom": movement.quantile(.99), "max_abs_jump": movement.max(),
                     "sign_flips": int((np.sign(signed) != np.sign(signed.shift())).sum() - (len(signed) > 0))})
    return pd.DataFrame(rows)


def build_affordability_derivation_evidence(source: pd.DataFrame) -> AffordabilityDerivationEvidence:
    source = _validate_source(source)
    raw_parts, audit_parts = [], []
    for policy in (POLICY_A, POLICY_B):
        derived, lineage = _derive(source, policy)
        raw_parts.append(derived)
        audit_parts.append(_input_audit(lineage, policy))
    raw = pd.concat(raw_parts, ignore_index=True)
    audit = pd.concat(audit_parts, ignore_index=True)
    expected_components = {key: set(DERIVED_METRIC_COMPONENTS[key]) for key in TARGET_METRICS}
    membership = audit.groupby(["policy", "metric", "geo_id", "date"]).input_name.agg(set)
    if any(inputs != expected_components[key[1]] for key, inputs in membership.items()):
        raise AssertionError("Input lineage audit is incomplete")
    features = _features(raw)
    formula = _formula_audit(raw, audit)
    stability = _stability(features)
    wide = features.pivot(index=["metric", "geo_id", "date"], columns="policy", values="structural_level").dropna().reset_index()
    wide["level_difference_b_minus_a"] = wide[POLICY_B] - wide[POLICY_A]
    divergence = wide.groupby("metric").level_difference_b_minus_a.agg(
        median_absolute_difference=lambda x: x.abs().median(), p90_absolute_difference=lambda x: x.abs().quantile(.90),
        p99_absolute_difference=lambda x: x.abs().quantile(.99), maximum_absolute_difference=lambda x: x.abs().max()).reset_index()
    decision = policy_registry().rename(columns={"policy": "Policy", "derivation_order": "Derivation order"})[["Policy", "Derivation order"]]
    required_decision_columns = [
        "Price-to-income median abs MoM", "Price-to-income P90", "Price-to-income P99",
        "Price-to-income max jump", "Price-to-income sign flips", "Price-to-income turning points",
        "Payment-burden median abs MoM", "Payment-burden P90", "Payment-burden P99",
        "Payment-burden max jump", "Payment-burden sign flips", "Payment-burden turning points",
        "Affordability dimension median abs MoM", "Affordability dimension P90",
        "Affordability dimension P99", "Affordability dimension turning points",
        "Latest-36m Affordability turns", "Largest level divergence between policies",
        "Median absolute level divergence", "Demand-axis changed months", "Changed county-month regimes",
    ]
    for column in required_decision_columns:
        decision[column] = np.nan
    for index, row in decision.iterrows():
        for metric, prefix in (("price_to_income", "Price-to-income"), ("payment_burden", "Payment-burden")):
            stats = stability[(stability.policy.eq(row.Policy)) & (stability.metric.eq(metric))].iloc[0]
            decision.loc[index, f"{prefix} median abs MoM"] = stats.median_abs_mom
            decision.loc[index, f"{prefix} P90"] = stats.p90_abs_mom
            decision.loc[index, f"{prefix} P99"] = stats.p99_abs_mom
            decision.loc[index, f"{prefix} max jump"] = stats.max_abs_jump
            decision.loc[index, f"{prefix} sign flips"] = stats.sign_flips
        decision.loc[index, "Largest level divergence between policies"] = divergence.maximum_absolute_difference.max()
        decision.loc[index, "Median absolute level divergence"] = divergence.median_absolute_difference.median()
    decision["Decision"] = "pending"
    empty = pd.DataFrame()
    tables = {
        "affordability_derivation_policy_registry": policy_registry(),
        "affordability_derivation_input_audit": audit,
        "affordability_derivation_formula_audit": formula,
        "affordability_derivation_raw_chronology": raw,
        "affordability_derivation_feature_chronology": features,
        "affordability_derivation_normalized_feature_scores": empty,
        "affordability_derivation_metric_scores": empty,
        "affordability_derivation_metric_stability": stability,
        "affordability_derivation_metric_turning_points": empty,
        "affordability_derivation_metric_turning_point_summary": empty,
        "affordability_derivation_dimension_chronology": empty,
        "affordability_derivation_dimension_stability": empty,
        "affordability_derivation_dimension_turning_point_summary": empty,
        "affordability_derivation_extreme_jumps": empty,
        "affordability_derivation_recent_chronology": features.groupby(["policy", "metric", "geo_id"]).tail(36),
        "affordability_derivation_demand_axis_context": empty,
        "affordability_derivation_regime_change_summary": empty,
        "affordability_derivation_parity_audit": divergence,
        "affordability_derivation_decision_matrix": decision,
        "affordability_derivation_human_decision_status": pd.DataFrame([{
            "recommendation_state": "none", "promotion_state": "none", "human_decision": "pending"
        }]),
        "affordability_derivation_runtime_summary": pd.DataFrame([{
            "policies": 2, "metrics": 2, "formula_reconstruction_passed": True,
            "authoritative_evidence_required": True
        }]),
    }
    return AffordabilityDerivationEvidence(tables)
