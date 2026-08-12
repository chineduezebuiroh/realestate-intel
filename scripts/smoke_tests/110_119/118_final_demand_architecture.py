#!/usr/bin/env python3
"""Deterministic contract smoke for the promoted production Demand policy."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import _compute_feature
from regime._05_dimension_scorer import score_dimensions
from regime.calendar_ma import minimum_valid_observations

LAUS = {"laus_labor_force", "laus_employment", "laus_unemployment_rate"}


def main() -> None:
    config = load_regime_config(validate=True)
    features = config.features[config.features.metric_key.isin(LAUS)]
    expected = {
        "level": ("ma_level", "9m", 0.80),
        "short_term_change": ("ma_pct_change", "9m/lag3m", 0.10),
        "long_term_change": ("ma_pct_change", "9m/lag12m", 0.10),
    }
    assert len(features) == 9
    for metric, rows in features.groupby("metric_key"):
        assert set(rows.feature_type) == set(expected)
        assert np.isclose(pd.to_numeric(rows.feature_weight).sum(), 1.0, atol=0, rtol=0)
        for row in rows.itertuples():
            assert (row.transform, row.feature_window, float(row.feature_weight)) == expected[row.feature_type]

    # Explicit missing month and missing observation prove calendar MA9, 2/3
    # coverage, no sparse-row rolling, and no forward/zero fill.
    dates = pd.date_range("2020-01-31", periods=24, freq="ME")
    values = pd.Series(np.arange(1.0, 25.0))
    values.iloc[4] = np.nan
    group = pd.DataFrame({"date": dates, "value": values})
    level = _compute_feature(group, "ma_level", "9m", "level")
    short = _compute_feature(group, "ma_pct_change", "9m/lag3m", "short")
    long = _compute_feature(group, "ma_pct_change", "9m/lag12m", "long")
    assert minimum_valid_observations(9) == 6
    assert pd.isna(level.iloc[4])
    assert np.isclose(level.iloc[8], values.iloc[:9].mean())
    assert np.isclose(short.iloc[11], level.iloc[11] / level.iloc[8] - 1)
    assert np.isclose(long.iloc[20], level.iloc[20] / level.iloc[8] - 1)

    membership = pd.read_csv("config/demand_block_registry.csv")
    enabled = membership[membership.enabled]
    assert set(enabled.demand_block) == {"structural", "cyclical"}
    assert set(enabled.loc[enabled.demand_block.eq("cyclical"), "canonical_metric_key"]) == {
        "labor_force", "employment", "laus_unemployment_rate"
    }
    weights = enabled.groupby("demand_block").block_weight.first().to_dict()
    assert weights == {"cyclical": 0.75, "structural": 0.25}

    metrics = pd.DataFrame([
        {"geo_id": "g", "evaluation_date": pd.Timestamp("2026-01-31"),
         "canonical_metric_key": metric, "metric_score": score, "metric_age_days": 0}
        for metric, score in [("population", 1.0), ("median_household_income", 1.0),
                              ("gdp_annual", 1.0), ("labor_force", -1.0),
                              ("employment", -1.0), ("laus_unemployment_rate", -1.0)]
    ])
    demand = score_dimensions(metrics).iloc[0]
    assert demand.dimension == "demand" and np.isclose(demand.dimension_score, -0.5)

    governance = json.loads(Path("config/demand_architecture_promotion_2026_08_12.json").read_text())
    assert governance["recommendation_state"] == "human_selected"
    assert governance["promotion_state"] == "promoted"
    assert governance["human_decision"] == "approved"
    assert governance["automated_winner"] is False
    assert governance["production_policy_changed"] is True
    print("[final_demand_architecture] OK: LF-IN / MA9 / 80-10-10 / S25-C75")


if __name__ == "__main__":
    main()
