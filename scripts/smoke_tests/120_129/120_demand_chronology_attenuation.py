#!/usr/bin/env python3
"""Fast contract checks for the diagnostic Demand attenuation review."""
import pandas as pd

from scripts.build_demand_chronology_attenuation import (
    MARGINS, SCENARIOS, chronology_statistics, scenario_registry,
)


def main() -> None:
    registry = scenario_registry()
    assert registry.scenario_id.tolist() == ["A", "B", "C", "D"]
    assert registry.ma_months.tolist() == [6, 9, 6, 9]
    assert set(registry.balance_policy) == {"BAL-S25-C75"}
    assert tuple(MARGINS) == ("A-B", "A-C", "B-D", "C-D")
    assert SCENARIOS["A"] == (6, "LAUS-W-70-15-15")
    frame = pd.DataFrame({
        "date": pd.date_range("2025-01-31", periods=6, freq="ME"),
        "value": [-1.0, -0.5, 0.5, 1.0, 0.25, -0.25],
        "raw_reference": [-1.0, -0.5, 0.5, 1.0, 0.25, -0.25],
    })
    stats = chronology_statistics(frame)
    assert stats["zero_crossings"] == 2
    assert stats["reversal_count"] == 1
    assert stats["correlation_to_raw_laus"] == 1.0
    print("PASS: controlled Demand chronology attenuation contract")


if __name__ == "__main__":
    main()
