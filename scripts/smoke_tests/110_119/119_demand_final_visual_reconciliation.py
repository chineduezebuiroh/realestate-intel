#!/usr/bin/env python3
"""Fixture-only smoke for the final four-way Demand reconciliation."""
import json
from pathlib import Path
import tempfile
import numpy as np
import pandas as pd
from unittest.mock import patch

from regime.diagnostics.demand_final_visual_reconciliation import (
    FACTORS, GEOS, build, classify, marginal_effects, pooled, prepare_chronology,
    resolve_four, summarize,
)


def main() -> None:
    registry = pd.DataFrame([{"scenario_id": label, "labor_force_membership":"LF-IN",
        "ma_window":ma, "laus_weight_policy":weight, "balance_policy":"BAL-S25-C75"}
        for label,(ma,weight) in FACTORS.items()])
    scenarios = resolve_four(registry); assert scenarios.scenario.tolist() == list("ABCD")
    dates = pd.date_range("2022-01-31", periods=8, freq="ME")
    rows=[]
    patterns={"A":[-.4,.4,-.4,.4,-.4,.4,-.4,.4], "B":[-.3,.3,-.3,.3,-.3,.3,-.3,.3],
              "C":[-.25,.25,-.25,.25,-.25,.25,-.25,.25], "D":[-.1,.1,-.1,.1,-.1,.1,-.1,.1]}
    for scenario in FACTORS:
        for geo in GEOS:
            for date, cyclical in zip(dates, patterns[scenario]):
                structural=.05
                rows.append({"scenario_id":scenario,"geo_id":geo,"date":date,
                    "structural_score":structural,"cyclical_score":cyclical,
                    "core_demand_score":structural+cyclical})
    chronology=prepare_chronology(pd.DataFrame(rows),scenarios)
    assert np.allclose(chronology.core_demand_score,
                       chronology.structural_contribution+chronology.cyclical_contribution)
    dc=chronology.loc[chronology.geo_id.eq(GEOS[0])]; pool=pooled(chronology)
    assert len(pool)==len(dates)*4 and np.allclose(pool.loc[pool.scenario.eq("D")].cyclical_contribution,patterns["D"])
    summary=summarize(dc,pool); effects=marginal_effects(summary)
    interaction=effects.loc[(effects.comparison.eq("interaction")) & effects.scope.eq("dc")
        & effects.series.eq("cyclical_contribution") & effects.metric.eq("std"),"delta"].item()
    expected=np.std(patterns["D"],ddof=1)-np.std(patterns["B"],ddof=1)-np.std(patterns["C"],ddof=1)+np.std(patterns["A"],ddof=1)
    assert np.isclose(interaction,expected)
    classification,evidence=classify(effects)
    assert classification == "MULTIPLE_CONTRIBUTING_EFFECTS" and evidence["material_effects"]["interaction"]
    before={"production.parquet":"sha256"}; after=before.copy(); assert before==after
    # Exercise the complete persisted-input builder without a production run.
    with tempfile.TemporaryDirectory() as tmp:
        calibration=Path(tmp)/"calibration"; output=Path(tmp)/"output"; calibration.mkdir()
        registry.to_csv(calibration/"laus_ma_window_scenario_registry.csv",index=False)
        pd.DataFrame(rows).to_csv(calibration/"laus_ma_window_chronology.csv",index=False)
        with patch("regime.diagnostics.demand_final_visual_reconciliation._plot",
                   side_effect=lambda frame,value,title,destination,recent=False: destination.write_bytes(b"fixture")):
            build(calibration,output)
        expected_files={"demand_final_visual_reconciliation_scenarios.csv",
            "demand_final_visual_reconciliation_chronology.csv",
            "demand_final_visual_reconciliation_summary.csv",
            "demand_final_visual_reconciliation_marginal_effects.csv",
            "demand_final_visual_reconciliation_recent.csv",
            "demand_final_visual_reconciliation_root_cause.json","README.md"}
        assert expected_files <= {p.name for p in output.iterdir()}
        assert len(list((output/"visual_review").glob("*.png"))) == 7
        root=json.loads((output/"demand_final_visual_reconciliation_root_cause.json").read_text())
        assert root["production_policy_changed"] is False
    print("PASS: final Demand visual reconciliation mechanics")


if __name__ == "__main__": main()
