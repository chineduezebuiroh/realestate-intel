#!/usr/bin/env python3
"""Deterministic contract and parity smoke for promoted production Demand."""
import inspect
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import _compute_feature
from regime._05_dimension_scorer import score_dimensions
import regime._05_dimension_scorer as dimension_scorer
from regime.calendar_ma import minimum_valid_observations
from regime.pipeline_runner import DEFAULT_CONFIG_PATHS

LAUS = {"laus_labor_force", "laus_employment", "laus_unemployment_rate"}
DEMAND_SCORES = {
    "population": 0.9,
    "median_household_income": -0.3,
    "gdp_annual": 0.6,
    "labor_force": -0.8,
    "employment": 0.2,
    "laus_unemployment_rate": 0.7,
}


def _metric_rows(scores: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame([
        {"geo_id": "g", "evaluation_date": pd.Timestamp("2026-01-31"),
         "canonical_metric_key": metric, "metric_score": score,
         "metric_age_days": 0}
        for metric, score in scores.items()
    ])


def _promoted_reference(scores: dict[str, float], registry: pd.DataFrame) -> float:
    """Reconstruct the promoted two-level availability-normalized math."""
    active = registry[
        registry.enabled.astype(str).str.lower().eq("true")
        & ~registry.diagnostic_only.astype(str).str.lower().eq("true")
        & registry.macro_enabled.astype(str).str.lower().eq("true")
        & registry.dimension.eq("demand")
    ].drop_duplicates("canonical_metric_key").set_index("canonical_metric_key")
    weighted_blocks = []
    available_weight = 0.0
    for block, block_weight in (("structural", 0.25), ("cyclical", 0.75)):
        members = active[(active.demand_block == block) & active.index.isin(scores)].copy()
        if members.empty:
            continue
        weights = pd.to_numeric(members.metric_weight)
        block_score = sum(scores[key] * weight for key, weight in weights.items()) / weights.sum()
        weighted_blocks.append(block_score * block_weight)
        available_weight += block_weight
    return sum(weighted_blocks) / available_weight


def main() -> None:
    config = load_regime_config(validate=True)
    features = config.features[config.features.metric_key.isin(LAUS)]
    expected = {
        "level": ("ma_level", "9m", 0.80),
        "short_term_change": ("ma_pct_change", "9m/lag3m", 0.10),
        "long_term_change": ("ma_pct_change", "9m/lag12m", 0.10),
    }
    assert len(features) == 9
    for _, rows in features.groupby("metric_key"):
        assert set(rows.feature_type) == set(expected)
        assert np.isclose(pd.to_numeric(rows.feature_weight).sum(), 1.0, atol=0, rtol=0)
        for row in rows.itertuples():
            assert (row.transform, row.feature_window, float(row.feature_weight)) == expected[row.feature_type]

    # Explicit missing month and missing observation prove calendar MA9, 2/3
    # coverage, no sparse-row rolling, and no forward/zero fill.
    dates = pd.date_range("2020-01-31", periods=24, freq="M")
    values = pd.Series(np.arange(1.0, 25.0))
    values.iloc[4] = np.nan
    group = pd.DataFrame({"date": dates, "value": values})
    level = _compute_feature(group, "ma_level", "9m", "level")
    short = _compute_feature(group, "ma_pct_change", "9m/lag3m", "short")
    long = _compute_feature(group, "ma_pct_change", "9m/lag12m", "long")
    assert minimum_valid_observations(9) == 6 and pd.isna(level.iloc[4])
    assert np.isclose(level.iloc[8], values.iloc[:9].mean())
    assert np.isclose(short.iloc[11], level.iloc[11] / level.iloc[8] - 1)
    assert np.isclose(long.iloc[20], level.iloc[20] / level.iloc[8] - 1)

    registry = config.metric_dimensions
    active = registry[
        registry.enabled.astype(str).str.lower().eq("true")
        & ~registry.diagnostic_only.astype(str).str.lower().eq("true")
        & registry.macro_enabled.astype(str).str.lower().eq("true")
    ]
    governed = active[active.dimension.eq("demand")].drop_duplicates("canonical_metric_key")
    assert set(governed.demand_block) == {"structural", "cyclical"}
    assert set(governed.loc[governed.demand_block.eq("structural"), "canonical_metric_key"]) == {
        "population", "median_household_income", "gdp_annual"}
    assert set(governed.loc[governed.demand_block.eq("cyclical"), "canonical_metric_key"]) == {
        "labor_force", "employment", "laus_unemployment_rate"}
    assert governed.groupby("demand_block").block_weight.first().astype(float).to_dict() == {
        "cyclical": 0.75, "structural": 0.25}
    assert active.loc[active.dimension.ne("demand"), "demand_block"].eq("").all()
    assert not Path("config/demand_block_registry.csv").exists()
    assert Path("config/metric_dimension_registry.csv") in DEFAULT_CONFIG_PATHS
    assert Path("config/demand_block_registry.csv") not in DEFAULT_CONFIG_PATHS
    scorer_source = inspect.getsource(dimension_scorer)
    assert "demand_block_registry.csv" not in scorer_source and "pd.read_csv" not in scorer_source
    assert scorer_source.index("_build_dimension_weights()") < scorer_source.index("for keys, g in df.groupby")

    cases = {
        "complete": DEMAND_SCORES,
        "missing_structural_metric": {k: v for k, v in DEMAND_SCORES.items() if k != "gdp_annual"},
        "missing_cyclical_metric": {k: v for k, v in DEMAND_SCORES.items() if k != "labor_force"},
        "missing_structural_block": {k: v for k, v in DEMAND_SCORES.items() if k in {"labor_force", "employment", "laus_unemployment_rate"}},
        "missing_cyclical_block": {k: v for k, v in DEMAND_SCORES.items() if k in {"population", "median_household_income", "gdp_annual"}},
    }
    for name, scores in cases.items():
        actual = score_dimensions(_metric_rows(scores)).iloc[0].dimension_score
        expected_score = _promoted_reference(scores, registry)
        assert np.isclose(actual, expected_score, atol=1e-12, rtol=0), (name, actual, expected_score)

    # The ordinary flat weighted-average path remains unchanged for non-Demand.
    supply_scores = {"active_inventory": 0.8, "permit_activity": -0.4, "permit_intensity": 0.1}
    supply = score_dimensions(_metric_rows(supply_scores)).iloc[0]
    assert supply.dimension == "supply"
    assert np.isclose(supply.dimension_score, 0.6 * 0.8 + 0.2 * -0.4 + 0.2 * 0.1, atol=1e-12, rtol=0)

    governance = json.loads(Path("config/demand_architecture_promotion_2026_08_12.json").read_text())
    assert governance["recommendation_state"] == "human_selected"
    assert governance["promotion_state"] == "promoted"
    assert governance["human_decision"] == "approved"
    assert governance["automated_winner"] is False
    assert governance["production_policy_changed"] is True
    assert governance["demand_calibration_state"] == "closed"
    print("[final_demand_architecture] OK: LF-IN / MA9 / 80-10-10 / S25-C75; parity cases=5")


if __name__ == "__main__":
    main()
