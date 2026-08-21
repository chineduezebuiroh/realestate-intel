#!/usr/bin/env python3
"""Deterministic contract smoke test for the bounded 12-scenario diagnostic."""
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from regime.calendar_ma import minimum_valid_observations
from regime.experiments.laus_ma_feature_calibration import (
    FEATURE_WEIGHTS, FIXED_BALANCE, FIXED_LABOR_MEMBERSHIP, GOVERNANCE,
    MA_WINDOWS, REQUIRED_EXPORTS, RUN_ID, VISUAL_FAMILIES,
    align_challenger_laus_scores, construct_laus_features,
    marginal_effects, require_authoritative_run, scenario_registry,
)


def main() -> None:
    grid = scenario_registry()
    assert grid.scenario_id.tolist() == [f"MA{ma}__W{i}" for ma in (6, 9) for i in range(6)]
    assert set(grid.ma_months) == {6, 9} and tuple(MA_WINDOWS) == (6, 9)
    assert FEATURE_WEIGHTS == {"W0": (.25,.35,.40), "W1": (.40,.25,.35),
        "W2": (.50,.20,.30), "W3": (.60,.15,.25), "W4": (.70,.15,.15),
        "W5": (.80,.10,.10)}
    assert (grid[["level_weight","short_weight","long_weight"]].sum(axis=1) == 1).all()
    assert set(grid.labor_force_membership) == {FIXED_LABOR_MEMBERSHIP} == {"LF-IN"}
    assert set(grid.balance_policy) == {FIXED_BALANCE} == {"BAL-S25-C75"}
    assert all((grid[key] == value).all() for key, value in GOVERNANCE.items())
    assert minimum_valid_observations(6) == 4 and minimum_valid_observations(9) == 6
    assert len(REQUIRED_EXPORTS) == 13 and len(set(REQUIRED_EXPORTS)) == 13
    assert set(VISUAL_FAMILIES) == {"raw_ma_dc", "raw_ma_seven_county_standardized",
        "feature_scores_by_metric_ma", "representative_feature_contributions",
        "cyclical_ma_and_weight_fixed", "core_demand_ma_and_weight_fixed",
        "response_surfaces"}

    dates = pd.date_range("2018-01-31", periods=30, freq="ME")
    rows = []
    for metric in ("labor_force", "employment", "laus_unemployment_rate"):
        for i, date in enumerate(dates):
            if i != 1:  # explicit missing February: catches sparse-row rolling
                rows.append({"geo_id":"district_of_columbia_dc__county", "date":date,
                    "canonical_metric_key":metric, "raw_value":np.nan if i == 8 else 100+i})
    source = pd.DataFrame(rows)
    for ma in MA_WINDOWS:
        features = construct_laus_features(source, ma)
        level = features.loc[features.feature_key.eq("laus_labor_force_level")].sort_values("date")
        feb = level.loc[level.date.dt.to_period("M").eq(pd.Period("2018-02", "M"))]
        assert len(feb) == 1 and feb.raw_feature_value.isna().all()
        levels = level.set_index(level.date.dt.to_period("M")).raw_feature_value
        for lag, suffix in ((3, "short"), (12, "long")):
            feature = features.loc[features.feature_key.eq(f"laus_labor_force_{suffix}")]
            for row in feature.loc[feature.raw_feature_value.notna()].itertuples():
                month = pd.Timestamp(row.date).to_period("M")
                assert np.isclose(row.raw_feature_value, levels.loc[month]/levels.loc[month-lag]-1)

    geo = "district_of_columbia_dc__county"
    calendar = pd.date_range("2026-01-31", "2026-07-31", freq="ME")
    persisted = pd.DataFrame({"geo_id":geo, "evaluation_date":calendar,
        "canonical_metric_key":"population", "metric_score":.2, "feature_count":1,
        "feature_weight_sum":1., "min_feature_score":.2, "max_feature_score":.2})
    challenger = pd.DataFrame([{"geo_id":geo, "date":date, "metric":metric,
        "metric_score":score, "level_score":score, "short_score":score, "long_score":score}
        for metric, score in (("labor_force",.3),("employment",.4),("laus_unemployment_rate",-.2))
        for date in calendar[:5]])
    aligned = align_challenger_laus_scores(challenger, persisted)
    assert (aligned.loc[aligned.date.isin(calendar[-2:]), "metric_date"] == pd.Timestamp("2026-05-31")).all()

    stats = pd.DataFrame([{"scenario_id":f"MA{ma}__{p}", "ma_months":ma,
        "weight_policy":p, "standard_deviation":ma+i} for ma in (9,6)
        for i,p in reversed(list(enumerate(FEATURE_WEIGHTS)))])
    first = marginal_effects(stats); second = marginal_effects(stats.sample(frac=1, random_state=9))
    pd.testing.assert_frame_equal(first, second)
    assert (first.standard_deviation_difference_ma9_minus_ma6 == 3).all()

    with tempfile.TemporaryDirectory() as tmp:
        absent = Path(tmp) / RUN_ID
        try: require_authoritative_run(absent)
        except FileNotFoundError: pass
        else: raise AssertionError("missing authoritative run must fail closed")
    print("PASS: bounded 12-scenario LAUS MA x feature-weight calibration contract")


if __name__ == "__main__":
    main()
