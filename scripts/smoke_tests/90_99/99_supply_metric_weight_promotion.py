"""Focused production validation for the frozen Supply dimension contract."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config, validate_regime_config
from regime._05_dimension_scorer import _build_dimension_weights, score_dimensions

ROOT = Path(__file__).resolve().parents[3]
SUPPLY = {"active_inventory": 0.60, "permit_activity": 0.20, "permit_intensity": 0.20}
FEATURES = {
    "level": ("ma_level", "12m", 0.50),
    "short_term_change": ("ma_pct_change", "12m/lag3m", 0.25),
    "long_term_change": ("ma_pct_change", "12m/lag12m", 0.25),
}
SUPPLY_AXIS = {("supply", "supply"): 0.85, ("supply", "capital_markets"): 0.15}


def _active(rows: pd.DataFrame) -> pd.DataFrame:
    truth = lambda s: s.astype(str).str.lower().isin({"true", "1", "yes", "y"})
    return rows[truth(rows.enabled) & ~truth(rows.diagnostic_only) & truth(rows.macro_enabled)].copy()


def _metric_row(scores: dict[str, float | None]) -> pd.DataFrame:
    return pd.DataFrame([
        {"geo_id": "fixture__county", "evaluation_date": pd.Timestamp("2026-01-31"),
         "canonical_metric_key": metric, "metric_score": score, "metric_age_days": 0}
        for metric, score in scores.items()
    ])


def _assert_effective_weights() -> None:
    cases = [
        ({"active_inventory": 1., "permit_activity": 0., "permit_intensity": 0.}, 0.60, 1.0),
        ({"active_inventory": 1., "permit_activity": 0., "permit_intensity": None}, 0.75, 0.8),
        ({"active_inventory": None, "permit_activity": 1., "permit_intensity": 0.}, 0.50, 0.4),
        ({"active_inventory": 1., "permit_activity": None, "permit_intensity": None}, 1.00, 0.6),
    ]
    for values, inventory_effective, configured_sum in cases:
        out = score_dimensions(_metric_row(values))
        supply = out[out.dimension.eq("supply")].iloc[0]
        assert np.isclose(supply.metric_weight_sum, configured_sum)
        available = {k: v for k, v in values.items() if v is not None}
        expected = sum(available[k] * SUPPLY[k] for k in available) / configured_sum
        assert np.isclose(supply.dimension_score, expected)
        if values.get("active_inventory") is not None:
            assert np.isclose(SUPPLY["active_inventory"] / configured_sum, inventory_effective)
    assert score_dimensions(_metric_row({k: None for k in SUPPLY})).empty


def main() -> None:
    config = load_regime_config(validate=True)
    active = _active(config.metric_dimensions)
    supply = active[active.dimension.eq("supply")]
    assert not supply.duplicated(["canonical_metric_key", "dimension"]).any()
    assert set(supply.canonical_metric_key) == set(SUPPLY)
    actual = supply.set_index("canonical_metric_key").metric_weight.astype(float).to_dict()
    assert actual == SUPPLY and sum(actual.values()) == 1.0
    resolved = _build_dimension_weights()
    assert resolved[resolved.dimension.eq("supply")].set_index("canonical_metric_key").metric_weight.to_dict() == SUPPLY
    _assert_effective_weights()

    joined = config.features.merge(config.metric_dimensions[["metric_key", "canonical_metric_key", "dimension"]], on="metric_key")
    for metric in SUPPLY:
        family = joined[(joined.canonical_metric_key.eq(metric)) & joined.dimension.eq("supply")]
        assert len(family) == 3
        for feature_type, expected in FEATURES.items():
            row = family[family.feature_type.eq(feature_type)].iloc[0]
            assert (row["transform"], row["feature_window"], float(row.feature_weight)) == expected
    intensity = joined[joined.canonical_metric_key.eq("permit_intensity")]
    assert set(intensity.metric_key) == {"derived_permit_intensity"}
    source = config.source_metrics.set_index("metric_key")
    assert source.loc["derived_permit_intensity", "source_id"] == "derived"

    # Explicit non-change sentinels: affordability, Capital Markets, every non-Supply
    # metric weight, axes (including Supply), source precedence and geography remain governed.
    assert active.query("dimension == 'affordability'").set_index("canonical_metric_key").metric_weight.astype(float).to_dict() == {"price_to_income": .5, "payment_burden": .5}
    assert active.query("dimension == 'capital_markets'").set_index("canonical_metric_key").metric_weight.astype(float).to_dict() == {"mortgage_30y": .35, "mortgage_15y": .05, "fedfunds": .15, "treasury_10y": .15, "spread_2y10y": .2, "spread_10y_fedfunds": .1}
    axes = config.axes.set_index(["axis", "dimension"]).dimension_weight.astype(float).to_dict()
    assert {k: axes[k] for k in SUPPLY_AXIS} == SUPPLY_AXIS
    assert axes == {("demand", "demand"): .65, ("demand", "price"): .175, ("demand", "affordability"): .075, ("demand", "capital_markets"): .10, **SUPPLY_AXIS}
    assert source.loc["redfin_inventory", "geo_levels"] == "nation|state|cbsa_metro|county|zip"
    assert source.loc["bps_total_units", "geo_levels"] == "state|county|place"
    assert source.loc["derived_permit_intensity", "geo_levels"] == "state|county"
    assert config.metric_dimensions.source_priority.astype(str).tolist() == load_regime_config(validate=True).metric_dimensions.source_priority.astype(str).tolist()

    freeze = json.loads((ROOT / "config/supply_dimension_frozen_v1.json").read_text())
    promotion = json.loads((ROOT / "config/supply_metric_weight_promotion_2026_08_06.json").read_text())
    assert freeze["metric_membership"] == list(SUPPLY) and freeze["metric_weights"] == SUPPLY
    assert freeze["feature_weights"] == {"level": .5, "short": .25, "long": .25}
    assert freeze["permit_intensity_lineage"] == "derive raw ratio first, then smooth once"
    assert promotion["diagnostic_recommendation_state"] == promotion["diagnostic_promotion_state"] == "none"
    assert promotion["status"] == "human_approved" and promotion["selected_policy"] == "challenger_a"

    duplicate = config.metric_dimensions.copy()
    duplicate = pd.concat([duplicate, supply.iloc[[0]]], ignore_index=True)
    broken = type(config)(config.source_metrics, config.features, duplicate, config.axes)
    try:
        validate_regime_config(broken)
    except ValueError as exc:
        assert "Duplicate active metric-to-dimension registry rows" in str(exc)
    else:
        raise AssertionError("Duplicate Supply registry row did not fail closed")

    ambiguous = config.metric_dimensions.copy()
    ambiguous.loc[ambiguous.metric_key.eq("redfin_inventory"), "metric_weight"] = "0.59"
    broken = type(config)(config.source_metrics, config.features, ambiguous, config.axes)
    try:
        validate_regime_config(broken)
    except ValueError:
        pass
    else:
        raise AssertionError("Ambiguous/invalid Supply registry weights did not fail closed")

    # Production mechanics remain the shared feature/normalization/scoring pipeline;
    # this test introduces no alternate scorer and writes no runtime artifact.
    print("[supply_metric_weight_promotion] OK")


if __name__ == "__main__":
    main()
