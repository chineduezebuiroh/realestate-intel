"""Contracts for the fail-closed structural/cyclical diagnostic."""
from pathlib import Path
import tempfile
import numpy as np
import pandas as pd
from regime.experiments.structural_cyclical_demand_architecture import *
from regime.pandas_compat import MONTH_END

assert RUN_ID == "macro_regime_v1_0_1_candidate_20260810"
assert len(GEOS)==len(set(GEOS))==7 and not any("cbsa" in x or "__zip" in x for x in GEOS)
assert STRUCTURAL == ("population","median_household_income","gdp_annual")
assert CYCLICAL == ("labor_force","employment","laus_unemployment_rate")
assert LF_MEMBERSHIPS == ("LF-IN","LF-OUT")
assert list(LAUS_WEIGHT_POLICIES)==["LAUS-W-25-35-40","LAUS-W-40-30-30","LAUS-W-50-25-25","LAUS-W-60-20-20","LAUS-W-70-15-15","LAUS-W-80-10-10"]
assert LAUS_WEIGHT_POLICIES["LAUS-W-70-15-15"] == (.70,.15,.15)
assert all(abs(sum(x)-1)<=TOL for x in LAUS_WEIGHT_POLICIES.values())
assert len(EXPLICIT_BALANCES)==5 and "BAL-INCUMBENT-EXACT" in BALANCE_POLICIES
base=pd.Series({m:w for m,w in zip(CORE_DEMAND,[1,2,3,4,5,6])},dtype=float)
w=realized_metric_weights(base,"LF-OUT","BAL-S35-C65")
assert "labor_force" not in w and np.isclose(w.loc[list(STRUCTURAL)].sum(),.35) and np.isclose(w.loc[["employment","laus_unemployment_rate"]].sum(),.65)
grid=scenario_grid(); assert grid.scenario_id.is_unique and set(grid.labor_force_membership)==set(LF_MEMBERSHIPS)
assert len(grid.loc[grid.balance_policy.eq("BAL-INCUMBENT-EXACT")]) == len(LAUS_WEIGHT_POLICIES)
assert conflict_month(pd.Series([1.,1.,0.,np.nan]),pd.Series([-1.,1.,-1.,1.])).tolist()[:2]==[True,False]
assert len(recent_36(pd.DataFrame({"date":pd.date_range("2020-01-31",periods=60,freq=MONTH_END)})))==36
source=Path("regime/experiments/structural_cyclical_demand_architecture.py").read_text()
assert "detect_turning_points, match_turning_points" in source
assert "incumbent_similarity" not in " ".join(grid.columns).lower()
assert '"automated_winner":False' in source and '"production_policy_changed":False' in source
assert ".to_csv(output/" in source and "config/" not in source.split("to_csv")[1]
with tempfile.TemporaryDirectory() as tmp:
    out=Path(tmp)/"out"
    try: build_review(Path(tmp)/RUN_ID,out,Path.cwd())
    except FileNotFoundError: pass
    else: raise AssertionError("must fail closed")
    assert not out.exists()
print("Structural/cyclical Demand architecture smoke test passed")
