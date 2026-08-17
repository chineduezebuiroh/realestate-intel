"""Smoke 138: Supply Phase-2 grids, independent isolation, propagation, and governance."""
from __future__ import annotations
import hashlib, importlib.util, tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from regime.diagnostics.supply_feature_weight_calibration import (
    EXPERIMENTS, EXPORTS, POLICY_TARGET, PRODUCTION_FEATURE_WEIGHTS, build, write_review,
)


def fixture():
    spec=importlib.util.spec_from_file_location("supply_phase1_smoke",Path(__file__).with_name("137_supply_feature_anatomy.py"))
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    frames=module.fixture()
    cutoff=pd.Timestamp("2024-07-31")
    for name,frame in frames.items():
        date_col="evaluation_date" if "evaluation_date" in frame else "date"
        frames[name]=frame[pd.to_datetime(frame[date_col]).le(cutoff)].copy()
    supply=frames["dimension_scores"].rename(columns={"dimension_score":"axis_score"})[["geo_id","date","axis_score"]]; supply["axis"]="supply"
    demand=supply.copy(); demand["axis"]="demand"; demand["axis_score"]=.25
    frames["axis_scores"]=pd.concat([supply,demand],ignore_index=True)
    return frames


def main():
    assert list(EXPERIMENTS["active_inventory"]) == [f"I{i}" for i in range(6)]
    assert list(EXPERIMENTS["permit_activity"]) == [f"A{i}" for i in range(5)]
    assert "A5" not in POLICY_TARGET
    assert list(EXPERIMENTS["permit_intensity"]) == [f"N{i}" for i in range(6)]
    protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/axis_registry.csv")]
    before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
    tables=build(fixture(),Path(".")); assert set(EXPORTS).issubset(tables)
    registry=tables["scenario_registry"]
    assert len(registry)==17 and registry.groupby("experiment_metric").size().to_dict()=={"active_inventory":6,"permit_activity":5,"permit_intensity":6}
    contributions=tables["feature_contributions"]
    for policy,target in POLICY_TARGET.items():
        controls=contributions.query("policy==@policy and metric!=@target")
        expected=controls.apply(lambda row: PRODUCTION_FEATURE_WEIGHTS[row.metric][("level","short","long").index(row.feature_type)],axis=1)
        assert np.allclose(controls.configured_feature_weight,expected)
    demand=tables["demand_axis_statistics"]
    assert set(demand.maximum_absolute_delta_from_production)=={0.0} and set(demand.chronology_correlation_to_production)=={1.0}
    gov=tables["governance_status"].iloc[0]
    assert gov.recommendation_state=="none" and not gov.production_policy_changed and not gov.metric_weights_changed and not gov.ma_calibration and not gov.capital_markets_changed
    with tempfile.TemporaryDirectory() as tmp:
        out=Path(tmp); write_review(tables,out)
        assert all((out/f"supply_phase2_{name}.csv").is_file() for name in EXPORTS)
        assert (out/"supply_phase2_review_index.html").is_file() and list(out.glob("*.svg"))
    assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
    print("Smoke 138 passed: exact independent grids, production controls, review package, Demand isolation, and no promotion")

if __name__=="__main__": main()
