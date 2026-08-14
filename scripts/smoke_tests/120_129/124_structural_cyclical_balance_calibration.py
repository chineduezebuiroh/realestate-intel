"""Contracts for the final bounded Structural/Cyclical balance calibration."""
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from regime.experiments.structural_cyclical_balance_calibration import *

registry=scenario_registry()
assert list(registry.scenario_id)==list(BALANCES)
assert list(BALANCES)==["BAL-S15-C85","BAL-S20-C80","BAL-S25-C75","BAL-S30-C70","BAL-S35-C65","BAL-S40-C60"]
assert list(BALANCES.values())==[(.15,.85),(.20,.80),(.25,.75),(.30,.70),(.35,.65),(.40,.60)]
assert len(registry)==6 and registry.scenario_id.is_unique
assert (registry.labor_force_membership=="LF-IN").all() and (registry.ma_window=="MA9").all()
assert (registry.feature_policy=="B3").all()
assert (registry[["level_weight","short_weight","long_weight"]].iloc[0].to_numpy()==[.40,.15,.45]).all()
assert np.allclose(registry.structural_weight+registry.cyclical_weight,1)
assert not registry.automated_winner.any() and not registry.production_policy_changed.any()

dates=pd.to_datetime(["2024-01-31","2024-02-29"])
structural=pd.DataFrame({"geo_id":[GEOS[0]]*2,"date":dates,"metric":[STRUCTURAL[0]]*2,"score":[1.,1.]})
cyclical=pd.DataFrame({"geo_id":[GEOS[0]],"date":[dates[0]],"metric":[LABOR[0]],"score":[-1.]})
base=pd.Series({m:1. for m in (*STRUCTURAL,*LABOR)})
chron,detail=reconstruct_chronology(structural,cyclical,base)
assert set(chron.scenario_id)==set(BALANCES)
assert chron.loc[chron.date.eq(dates[1]),"effective_structural_weight"].eq(1).all()
assert chron.loc[chron.date.eq(dates[1]),"effective_cyclical_weight"].eq(0).all()
assert chron.loc[chron.date.eq(dates[1]),"core_demand_score"].eq(1).all()
for (_,date),g in chron.groupby(["scenario_id","date"]):
    assert np.isclose(g.core_demand_score.iloc[0],g.structural_weighted_contribution.iloc[0]+g.cyclical_weighted_contribution.iloc[0])
assert chron.groupby(["geo_id","date"]).structural_block_score.nunique().le(1).all()
assert chron.groupby(["geo_id","date"]).cyclical_block_score.nunique().le(1).all()
shuffled,_=reconstruct_chronology(structural.sample(frac=1,random_state=4),cyclical.sample(frac=1,random_state=5),base)
pd.testing.assert_frame_equal(chron.sort_values(list(chron.columns[:3])).reset_index(drop=True),shuffled.sort_values(list(shuffled.columns[:3])).reset_index(drop=True))
source=Path("regime/experiments/structural_cyclical_balance_calibration.py").read_text()
assert "build_shared_laus_evidence" in source and '"MA9__B3"' in source
assert 'laus=metric.loc[(metric.scenario_id=="MA9__B3")' in source
assert 'structural=persisted.loc[persisted.metric.isin(STRUCTURAL)' in source
assert 'persisted.metric.isin(LABOR)' not in source
assert "production_policy_changed\": False" in source and "config/" not in source
assert 'for name in EXPORTS' in source
assert 'structural_cyclical_balance_{name}.csv' in source
builder=Path("scripts/build_structural_cyclical_balance_calibration.py").read_text()
assert "structural_cyclical_balance_calibration import RUN_ID, build_review" in builder
assert 'Path("artifacts/regime/runs") / RUN_ID' in builder
assert 'artifacts/regime/comparisons/structural_cyclical_balance_calibration' in builder
with tempfile.TemporaryDirectory() as tmp:
    out=Path(tmp)/"out"
    try: build_review(Path(tmp)/RUN_ID,out,Path.cwd())
    except FileNotFoundError: pass
    else: raise AssertionError("missing authoritative run must fail closed")
    assert not out.exists()
print("Structural/Cyclical balance calibration smoke test passed")
