#!/usr/bin/env python3
"""Fast fixture-only smoke for Demand promotion fidelity mechanics."""
import numpy as np
import pandas as pd

from regime.diagnostics.demand_promotion_fidelity import (
    cancellation_detected, comparison, contribution_rows,
    production_files_unchanged, reconstruct_axis, resolve_scenario,
)


def main() -> None:
    registry = pd.DataFrame([{ "scenario_id": "winner", "labor_force_membership": "LF-IN",
        "ma_window": "MA9", "laus_weight_policy": "LAUS-W-80-10-10",
        "balance_policy": "BAL-S25-C75"}])
    assert resolve_scenario(registry).scenario_id.item() == "winner"
    duplicate = pd.concat([registry, registry], ignore_index=True)
    try: resolve_scenario(duplicate)
    except ValueError: pass
    else: raise AssertionError("duplicate identity did not fail closed")

    dates = pd.date_range("2024-01-31", periods=3, freq="M")
    a = pd.DataFrame({"geo_id": "g", "date": dates, "a": [-.2, .1, .3]})
    b = a.rename(columns={"a": "b"})
    assert comparison(a, b, "a", "b")["exact_parity"]
    changed = b.copy(); changed.loc[1, "b"] += .01
    assert not comparison(a, changed, "a", "b")["numerical_parity"]

    dims = pd.DataFrame([{"geo_id":"g", "date":d, "dimension":dim, "dimension_score":score}
        for d in dates for dim,score in (("demand", .2), ("price", -.2))])
    axes = pd.DataFrame({"axis":["demand"]*2, "dimension":["demand","price"],
                         "dimension_weight":[.75,.25], "enabled":[True,True]})
    rebuilt = reconstruct_axis(dims, axes)
    assert np.allclose(rebuilt.reconstructed_axis_score, .1)  # B -> C arithmetic
    axis = rebuilt[["geo_id","date","reconstructed_axis_score"]].drop_duplicates()
    coords = axis.rename(columns={"reconstructed_axis_score":"y_demand"})
    regimes = coords.rename(columns={"y_demand":"demand_strength_score"})
    assert comparison(axis, coords, "reconstructed_axis_score", "y_demand")["exact_parity"]
    assert comparison(coords, regimes, "y_demand", "demand_strength_score")["exact_parity"]

    scores = pd.DataFrame([{"geo_id":"g", "date":dates[0],
        "canonical_metric_key":m, "metric_score":s} for m,s in (("labor_force",.5),("employment",-.5))])
    metrics = pd.DataFrame({"canonical_metric_key":["labor_force","employment"],
        "dimension":["demand"]*2, "enabled":[True]*2, "metric_weight":[.5,.5],
        "demand_block":["cyclical"]*2, "block_weight":[.75]*2})
    contributions = contribution_rows(scores, metrics)
    assert contributions.final_weighted_metric_contribution.sum() == 0
    assert cancellation_detected(contributions, "final_weighted_metric_contribution")
    assert production_files_unchanged({"run/a":"digest"}, {"run/a":"digest"})
    print("PASS: Demand promotion fidelity mechanics")


if __name__ == "__main__": main()
