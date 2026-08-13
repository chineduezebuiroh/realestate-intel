#!/usr/bin/env python3
"""Fast contract checks for the diagnostic Demand attenuation review."""
import numpy as np
import pandas as pd

from scripts.build_demand_chronology_attenuation import (
    MARGINS, SCENARIOS, _raw_ma_evidence, chronology_statistics,
    scenario_registry,
)


def main() -> None:
    registry = scenario_registry()
    assert registry.scenario_id.tolist() == ["A", "B", "C", "D"]
    assert registry.ma_months.tolist() == [6, 9, 6, 9]
    assert set(registry.balance_policy) == {"BAL-S25-C75"}
    assert tuple(MARGINS) == ("A-B", "A-C", "B-D", "C-D")
    assert SCENARIOS["A"] == (6, "LAUS-W-70-15-15")
    frame = pd.DataFrame({
        "date": pd.date_range("2025-01-31", periods=6, freq="M"),
        "value": [-1.0, -0.5, 0.5, 1.0, 0.25, -0.25],
        "raw_reference": [-1.0, -0.5, 0.5, 1.0, 0.25, -0.25],
    })
    stats = chronology_statistics(frame)
    assert stats["zero_crossings"] == 2
    assert stats["reversal_count"] == 1
    assert stats["correlation_to_raw_laus"] == 1.0

    # October is a real calendar row with a missing source observation.  The
    # governed MAs retain it when their exact trailing calendar windows meet
    # two-thirds coverage; they must never collapse to sparse-row windows.
    dates = pd.date_range("2024-01-31", periods=12, freq="ME")
    values = np.arange(1.0, 13.0)
    values[9] = np.nan
    source = pd.DataFrame({
        "geo_id": "district_of_columbia_dc__county",
        "canonical_metric_key": "labor_force",
        "date": dates,
        "raw_value": values,
    })
    by_county, *_ = _raw_ma_evidence(source)
    stages = by_county.pivot(index="date", columns="stage", values="value")
    october = pd.Timestamp("2024-10-31")
    assert pd.isna(stages.loc[october, "Raw"])
    assert np.isclose(stages.loc[october, "MA3"], np.mean([8.0, 9.0]))
    assert np.isclose(stages.loc[october, "MA6"], np.mean([5., 6., 7., 8., 9.]))
    assert np.isclose(stages.loc[october, "MA9"], np.mean(np.arange(2., 10.)))
    # November's MA3 is [September, missing October, November], rather than
    # the last three non-missing rows [August, September, November].
    assert np.isclose(stages.loc[pd.Timestamp("2024-11-30"), "MA3"], 10.0)
    print("PASS: controlled Demand chronology attenuation contract")


if __name__ == "__main__":
    main()
