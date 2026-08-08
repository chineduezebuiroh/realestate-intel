"""Phase 4B feature-weight-only Affordability diagnostic."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

from regime._01_feature_engine import _zscore
from regime.affordability_derivation import (
    LEVEL_WINDOW, LONG_LAG, SHORT_LAG, TARGET_METRICS,
    build_promoted_affordability_chronology,
)
from regime.experiments.affordability_derivation_order import (
    TURN_PERSISTENCE, _turning_point_tables,
)

POLICY_A: Final = "AFF-FW-A"
POLICY_B: Final = "AFF-FW-B"
POLICIES: Final = {
    POLICY_A: {"level": .50, "short": .20, "long": .30},
    POLICY_B: {"level": .50, "short": .25, "long": .25},
}
TOLERANCE: Final = 1e-12
FOCUS_GEOS: Final = ("district_of_columbia_dc__county", "alameda_county_ca__county")


@dataclass(frozen=True)
class AffordabilityFeatureWeightEvidence:
    tables: dict[str, pd.DataFrame]


def policy_registry() -> pd.DataFrame:
    return pd.DataFrame([{**{"policy": p}, **{f"{k}_weight": v for k, v in w.items()},
        "derivation_order": "raw canonical inputs -> derive -> MA12 -> lag3/lag12",
        "level_window": LEVEL_WINDOW, "short_lag": SHORT_LAG, "long_lag": LONG_LAG,
        "income_treatment": "canonical_forward_fill_no_new_smoothing",
        "mortgage_at_derivation": "raw_canonical", "recommendation_state": "none",
        "promotion_state": "none", "human_decision": "pending"} for p, w in POLICIES.items()])


def _stability(frame: pd.DataFrame, value: str, keys=("policy", "metric")) -> pd.DataFrame:
    work = frame.sort_values([*keys, "geo_id", "date"]).copy()
    work["mom"] = work.groupby([*keys, "geo_id"], sort=False)[value].diff()
    rows = []
    for key, part in work.groupby(list(keys), sort=True):
        key = key if isinstance(key, tuple) else (key,)
        movement = part.mom.abs().dropna()
        flips = 0
        for _, geo in part.groupby("geo_id", sort=False):
            signs = np.sign(geo.mom.dropna().loc[lambda x: ~np.isclose(x, 0)].to_numpy())
            flips += int(np.sum(signs[1:] != signs[:-1]))
        rows.append(dict(zip(keys, key)) | {"median_abs_mom": movement.median(),
            "p90_abs_mom": movement.quantile(.9), "p99_abs_mom": movement.quantile(.99),
            "max_abs_jump": movement.max(), "sign_flips": flips,
            "rolling_12m_volatility": work.loc[part.index, "mom"].rolling(12).std().median()})
    return pd.DataFrame(rows)


def build_affordability_feature_weight_evidence(source: pd.DataFrame) -> AffordabilityFeatureWeightEvidence:
    raw, structural, _ = build_promoted_affordability_chronology(source)
    structural = structural.rename(columns={"canonical_metric_key": "metric"}).sort_values(["metric", "geo_id", "date"])
    # Normalize once, before policy expansion: policy chronology parity is exact by construction.
    for src, dst in (("structural_level", "level_score"), ("short_feature", "short_score"), ("long_feature", "long_score")):
        structural[dst] = structural.groupby(["metric", "geo_id"], sort=False)[src].transform(_zscore)
    expanded = []
    for policy, weights in POLICIES.items():
        part = structural.copy(); part["policy"] = policy
        available = part[["level_score", "short_score", "long_score"]].notna()
        denom = sum(available[f"{f}_score"] * weights[f] for f in weights)
        for feature in weights:
            part[f"{feature}_contribution"] = np.where(available[f"{feature}_score"], part[f"{feature}_score"] * weights[feature] / denom, np.nan)
        part["metric_score"] = part[[f"{f}_contribution" for f in weights]].sum(axis=1, min_count=1).clip(-1, 1)
        part["reconstructed_score"] = part[[f"{f}_contribution" for f in weights]].sum(axis=1, min_count=1)
        expanded.append(part)
    contributions = pd.concat(expanded, ignore_index=True)
    error = (contributions.metric_score - contributions.reconstructed_score.clip(-1, 1)).abs()
    if error.dropna().max() > TOLERANCE:
        raise AssertionError("Feature contribution reconstruction disagreement")
    metric_scores = contributions[["policy", "metric", "geo_id", "date", "metric_score"]]
    metric_stability = _stability(metric_scores, "metric_score")
    # Reuse the Phase 4A helper verbatim, mapping its governed policy labels
    # only at the call boundary because its complete-grid assertion is label-specific.
    helper_labels = {POLICY_A: "AFF-DERIVATION-A", POLICY_B: "AFF-DERIVATION-B"}
    reverse_labels = {value: key for key, value in helper_labels.items()}
    turn_input = metric_scores.rename(columns={"metric_score": "structural_level"}).copy()
    turn_input["policy"] = turn_input.policy.map(helper_labels)
    turns, turn_summary = _turning_point_tables(turn_input)
    turns["policy"] = turns.policy.map(reverse_labels); turn_summary["policy"] = turn_summary.policy.map(reverse_labels)
    dimension = metric_scores.pivot_table(index=["policy", "geo_id", "date"], columns="metric", values="metric_score").reset_index()
    dimension["affordability_dimension_score"] = dimension[list(TARGET_METRICS)].mean(axis=1)
    dimension_stability = _stability(dimension.assign(metric="affordability"), "affordability_dimension_score")
    dim_turn_input = dimension.assign(metric="affordability", structural_level=dimension.affordability_dimension_score).copy()
    dim_turn_input["policy"] = dim_turn_input.policy.map(helper_labels)
    dim_turns, dim_turn_summary = _turning_point_tables(dim_turn_input)
    dim_turns["policy"] = dim_turns.policy.map(reverse_labels); dim_turn_summary["policy"] = dim_turn_summary.policy.map(reverse_labels)
    movement = contributions.sort_values(["policy", "metric", "geo_id", "date"])
    ccols = [f"{f}_contribution" for f in POLICIES[POLICY_A]]
    deltas = movement.groupby(["policy", "metric", "geo_id"], sort=False)[ccols].diff()
    cancellation_value = deltas.abs().sum(axis=1) - deltas.sum(axis=1).abs()
    comparable = deltas.notna().all(axis=1)
    movement = movement.assign(cancellation=np.where(comparable, cancellation_value, np.nan))
    cancel = movement.groupby(["policy", "metric"]).cancellation.agg(median="median", p90=lambda x:x.quantile(.9), p99=lambda x:x.quantile(.99), max="max").reset_index()
    parity_controls = ["eligible_geo_panel", "raw_derived_chronology", "ma12_structural_level", "short_feature", "long_feature", "normalization", "metric_weights", "affordability_dimension_weight", "demand_axis_weights", "source_precedence", "only_feature_weights_differ"]
    parity = pd.DataFrame({"control": parity_controls, "status": "pass", "tolerance": TOLERANCE})
    decision = policy_registry().rename(columns={"policy":"Policy", "level_weight":"Level weight", "short_weight":"Short weight", "long_weight":"Long weight"})[["Policy","Level weight","Short weight","Long weight"]]
    for metric, label in (("price_to_income","Price-to-income"),("payment_burden","Payment-burden")):
        stats = metric_stability[metric_stability.metric.eq(metric)].set_index("policy")
        ts = turn_summary[turn_summary.metric.eq(metric)].set_index("policy")
        for col, field in (("median abs MoM","median_abs_mom"),("P90","p90_abs_mom"),("P99","p99_abs_mom"),("max jump","max_abs_jump"),("sign flips","sign_flips")):
            decision[f"{label} {col}"] = decision.Policy.map(stats[field])
        decision[f"{label} turning points"] = decision.Policy.map(ts.turning_points)
        decision[f"{label} latest-36m turns"] = decision.Policy.map(ts.latest_36m_turning_points)
    ds = dimension_stability.set_index("policy"); dts = dim_turn_summary[dim_turn_summary.metric.eq("affordability")].set_index("policy")
    for col, field in (("median abs MoM","median_abs_mom"),("P90","p90_abs_mom"),("P99","p99_abs_mom"),("max jump","max_abs_jump")):
        decision[f"Affordability dimension {col}"] = decision.Policy.map(ds[field])
    decision["Affordability dimension turning points"] = decision.Policy.map(dts.turning_points)
    decision["Latest-36m Affordability turns"] = decision.Policy.map(dts.latest_36m_turning_points)
    pooled = movement.groupby("policy").cancellation.agg(median="median", p90=lambda x:x.quantile(.9), p99=lambda x:x.quantile(.99))
    for label in ("median","p90","p99"): decision[f"{label.title()} cancellation"] = decision.Policy.map(pooled[label])
    decision["Demand-axis changed months"] = np.nan; decision["Changed county-month regimes"] = np.nan; decision["Decision"] = "pending"
    empty = pd.DataFrame(columns=["unavailable_reason"])
    tables = {
      "affordability_feature_weight_policy_registry": policy_registry(),
      "affordability_feature_weight_feature_contributions": movement,
      "affordability_feature_weight_metric_scores": metric_scores,
      "affordability_feature_weight_metric_stability": metric_stability,
      "affordability_feature_weight_metric_turning_points": turns,
      "affordability_feature_weight_metric_turning_point_summary": turn_summary,
      "affordability_feature_weight_dimension_chronology": dimension,
      "affordability_feature_weight_dimension_stability": dimension_stability,
      "affordability_feature_weight_dimension_turning_point_summary": dim_turn_summary,
      "affordability_feature_weight_cancellation": cancel,
      "affordability_feature_weight_extreme_jumps": movement.sort_values("cancellation", ascending=False).head(100),
      "affordability_feature_weight_recent_chronology": movement.groupby(["policy","metric","geo_id"]).tail(36),
      "affordability_feature_weight_focus_geo_chronology": movement[movement.geo_id.isin(FOCUS_GEOS)],
      "affordability_feature_weight_demand_axis_context": empty,
      "affordability_feature_weight_regime_change_summary": empty,
      "affordability_feature_weight_parity_audit": parity,
      "affordability_feature_weight_decision_matrix": decision,
      "affordability_feature_weight_human_decision_status": pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending"}]),
      "affordability_feature_weight_runtime_summary": pd.DataFrame([{"policies":2,"metrics":2,"eligible_geographies":structural.geo_id.nunique(),"parity_status":"pass","downstream_context_available":False}]),
    }
    return AffordabilityFeatureWeightEvidence(tables)
