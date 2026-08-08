"""Focused Phase 4A contract smoke test."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from regime.experiments.affordability_derivation_order import (
    FEATURE_WEIGHTS, POLICY_A, POLICY_B, TARGET_METRICS,
    build_affordability_derivation_evidence, policy_registry,
)


def fixture() -> pd.DataFrame:
    dates = pd.date_range("2018-01-31", periods=60, freq="ME")
    rows = []
    for geo_index, geo in enumerate(("county-a", "county-b")):
        for i, date in enumerate(dates):
            rows.append((geo, date, "median_sale_price", 250000 + geo_index * 40000 + i * 1500 + 5000 * np.sin(i / 3)))
        for i in range(5):
            rows.append((geo, dates[i * 12], "median_household_income", 70000 + geo_index * 5000 + i * 2200))
    for i, date in enumerate(dates):
        rows.append(("national", date, "mortgage_30y", 3.0 + (i >= 36) * 3.0 + .2 * np.sin(i)))
    return pd.DataFrame(rows, columns=["geo_id", "date", "canonical_metric_key", "value"])


def main() -> None:
    registry = policy_registry()
    assert registry.policy.tolist() == [POLICY_A, POLICY_B]
    assert set(TARGET_METRICS) == {"price_to_income", "payment_burden"}
    assert FEATURE_WEIGHTS == {"level": .50, "short": .20, "long": .30}
    assert registry.level_window.eq(12).all() and registry.short_lag.eq(3).all() and registry.long_lag.eq(12).all()
    assert not registry.income_smoothed_before_derivation.any()
    assert not registry.mortgage_smoothed_before_derivation.any()
    result = build_affordability_derivation_evidence(fixture()).tables
    audit = result["affordability_derivation_input_audit"]
    b = audit[audit.policy.eq(POLICY_B)]
    assert not b.smoothing_applied_before_derivation.any()
    a = audit[audit.policy.eq(POLICY_A)]
    assert a.groupby("input_name").smoothing_applied_before_derivation.any().to_dict() == {
        "median_household_income": False, "median_sale_price": True, "mortgage_30y": False,
    }
    components = audit.groupby("metric").input_name.agg(set).to_dict()
    assert components["price_to_income"] == {"median_sale_price", "median_household_income"}
    assert components["payment_burden"] == {"median_sale_price", "median_household_income", "mortgage_30y"}
    assert result["affordability_derivation_formula_audit"].within_tolerance.all()
    assert len(result["affordability_derivation_decision_matrix"]) == 2
    assert result["affordability_derivation_decision_matrix"].Decision.eq("pending").all()
    status = result["affordability_derivation_human_decision_status"].iloc[0]
    assert (status.recommendation_state, status.promotion_state, status.human_decision) == ("none", "none", "pending")
    assert not any("rank" in column.lower() or "composite" in column.lower()
                   for column in result["affordability_derivation_decision_matrix"].columns)
    # Fail-closed chronology and uniqueness checks.
    broken = fixture().drop(index=10)
    try:
        build_affordability_derivation_evidence(broken)
    except ValueError as exc:
        assert "gap" in str(exc)
    else:
        raise AssertionError("Interior chronology gap did not fail closed")
    duplicate = pd.concat([fixture(), fixture().iloc[[0]]], ignore_index=True)
    try:
        build_affordability_derivation_evidence(duplicate)
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:
        raise AssertionError("Duplicate input did not fail closed")
    print("PASS: Phase 4A Affordability derivation-order contract")


if __name__ == "__main__":
    main()
