"""Settled production contract for derived Affordability features."""
from __future__ import annotations

from typing import Final

import pandas as pd

from regime.derived_metrics import build_derived_metrics_with_lineage
from regime.linked_price_family import build_structural_level


PROMOTED_POLICY: Final = "AFF-DERIVATION-B"
PROMOTION_IDENTITY: Final = "affordability_derivation_b_derive_first_ma12_2026_08_08"
TARGET_METRICS: Final = ("price_to_income", "payment_burden")
FEATURE_WEIGHTS: Final = {"level": 0.50, "short": 0.20, "long": 0.30}
LEVEL_WINDOW: Final = 12
SHORT_LAG: Final = 3
LONG_LAG: Final = 12
PARITY_TOLERANCE: Final = 1e-12


def build_promoted_affordability_chronology(
    canonical_source: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Derive from canonical raw inputs, then build full-window MA12 features."""
    derived, lineage = build_derived_metrics_with_lineage(canonical_source)
    raw = derived[derived.canonical_metric_key.isin(TARGET_METRICS)].copy()
    level = build_structural_level(raw, level_window=LEVEL_WINDOW).rename(
        columns={"structural_level_value": "structural_level"}
    )
    group = level.groupby(["canonical_metric_key", "geo_id"], sort=False)
    level["short_feature"] = level.structural_level / group.structural_level.shift(SHORT_LAG) - 1
    level["long_feature"] = level.structural_level / group.structural_level.shift(LONG_LAG) - 1
    return raw, level, lineage[lineage.derived_metric_key.isin(TARGET_METRICS)].copy()


def build_affordability_promotion_evidence(
    canonical_source: pd.DataFrame,
    diagnostic_raw: pd.DataFrame,
    diagnostic_features: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Build compact, fail-closed settlement evidence against diagnostic B."""
    raw, production, lineage = build_promoted_affordability_chronology(canonical_source)
    diagnostic = diagnostic_features[diagnostic_features.policy.eq(PROMOTED_POLICY)].copy()
    raw_compare = raw.rename(columns={"canonical_metric_key": "metric", "value": "production_value"})
    diagnostic_raw = diagnostic_raw[diagnostic_raw.policy.eq(PROMOTED_POLICY)].rename(
        columns={"canonical_metric_key": "metric", "pre_feature_derived_value": "diagnostic_value"}
    )
    comparisons = {
        "raw_derived": raw_compare.merge(
            diagnostic_raw[["metric", "geo_id", "date", "diagnostic_value"]],
            on=["metric", "geo_id", "date"], how="outer", validate="one_to_one", indicator=True,
        ),
    }
    production = production.rename(columns={"canonical_metric_key": "metric"})
    for comparison_level, column in (
        ("structural_level", "structural_level"),
        ("short_feature", "short_feature"),
        ("long_feature", "long_feature"),
    ):
        comparisons[comparison_level] = production[["metric", "geo_id", "date", column]].rename(
            columns={column: "production_value"}
        ).merge(
            diagnostic[["metric", "geo_id", "date", column]].rename(columns={column: "diagnostic_value"}),
            on=["metric", "geo_id", "date"], how="outer", validate="one_to_one", indicator=True,
        )

    parity_rows = []
    for comparison_level, frame in comparisons.items():
        if not frame["_merge"].eq("both").all():
            raise AssertionError(f"Promotion parity key mismatch: {comparison_level}")
        for metric, metric_frame in frame.groupby("metric", sort=True):
            aligned = metric_frame.dropna(subset=["production_value", "diagnostic_value"])
            if len(aligned) != int(metric_frame.production_value.notna().sum()) or len(aligned) != int(metric_frame.diagnostic_value.notna().sum()):
                raise AssertionError(f"Promotion parity null/key mismatch: {metric}/{comparison_level}")
            error = (aligned.production_value - aligned.diagnostic_value).abs()
            maximum = float(error.max()) if len(error) else 0.0
            passed = bool(maximum <= PARITY_TOLERANCE)
            parity_rows.append({"metric": metric, "comparison_level": comparison_level,
                "observation_count": len(aligned), "maximum_absolute_error": maximum,
                "within_tolerance": passed, "status": "pass" if passed else "fail"})
    parity = pd.DataFrame(parity_rows)
    if len(parity) != 8 or not parity.within_tolerance.all():
        raise AssertionError("Promoted production chronology does not match AFF-DERIVATION-B")

    registry = pd.DataFrame([{
        "promotion_identity": PROMOTION_IDENTITY, "selected_policy": PROMOTED_POLICY,
        "derivation_order": "canonical derive(raw inputs) -> MA12 -> lag3/lag12",
        "income_smoothed_before_derivation": False, "mortgage_smoothed_before_derivation": False,
        "level_window": LEVEL_WINDOW, "short_lag": SHORT_LAG, "long_lag": LONG_LAG,
        "level_weight": FEATURE_WEIGHTS["level"], "short_weight": FEATURE_WEIGHTS["short"],
        "long_weight": FEATURE_WEIGHTS["long"], "recommendation_state": "selected",
        "promotion_state": "promoted", "human_decision": "approved",
        "calibration_stage": "affordability_complete", "phase4b_state": "closed",
    }])
    diff_rows = [{"config_area": "derive_smoothing_order", "metric": metric,
        "old_value": "MA12(price) -> derive", "new_value": "derive(raw inputs) -> MA12",
        "change_reason": "Promote human-selected AFF-DERIVATION-B"} for metric in TARGET_METRICS]
    unchanged = ("economic_formula", "feature_weights", "metric_weights", "affordability_dimension_weight",
                 "demand_axis_weights", "source_precedence", "capital_markets_policy", "supply_policy", "demand_policy")
    diff_rows.extend({"config_area": area, "metric": "all", "old_value": "unchanged",
        "new_value": "unchanged", "change_reason": "Regression control; no semantic change"} for area in unchanged)
    status = pd.DataFrame([{"selected_policy": PROMOTED_POLICY, "recommendation_state": "selected",
        "promotion_state": "promoted", "human_decision": "approved", "calibration_stage": "affordability_complete",
        "derivation_order": "settled", "feature_weights": "settled 50/20/30"}])
    runtime = pd.DataFrame([{"stage": "promotion_parity", "policy_count": 1, "metric_count": 2,
        "parity_tolerance": PARITY_TOLERANCE, "parity_status": "pass",
        "source_lineage_rows": len(lineage), "reused_phase4a_diagnostic": True}])
    return {
        "affordability_derivation_promotion_policy_registry": registry,
        "affordability_derivation_promotion_config_diff": pd.DataFrame(diff_rows),
        "affordability_derivation_promotion_parity_audit": parity,
        "affordability_derivation_promotion_human_decision_status": status,
        "affordability_derivation_promotion_runtime_summary": runtime,
    }
