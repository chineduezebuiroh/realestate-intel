"""Contracts for the fail-closed structural/cyclical diagnostic."""
from pathlib import Path
import tempfile
import numpy as np
import pandas as pd
from regime.experiments.structural_cyclical_demand_architecture import *
from regime.experiments.structural_cyclical_demand_architecture import _summary_row
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
required_exports = [
 "structural_cyclical_block_summary", "structural_cyclical_block_pairwise",
 "structural_cyclical_block_by_county", "structural_cyclical_labor_force_turns",
 "structural_cyclical_laus_weight_interactions", "structural_cyclical_balance_by_county",
 "structural_cyclical_demand_axis_scenarios", "structural_cyclical_demand_axis_summary",
 "structural_cyclical_demand_supply_context", "structural_cyclical_county_consistency",
 "structural_cyclical_interactions"]
assert all(f'"{name}"' in source for name in required_exports)
assert "empty=pd.DataFrame" not in source and ":empty" not in source
for field in ["cyclical_turn_expression_share", "structural_turn_expression_share",
              "demand_axis_std", "recent_demand_axis_std", "demand_axis_median_abs",
              "seven_county_consistency"]:
    assert f'"{field}":np.nan' not in source
# Deterministic low-level fixture verifies chronology statistics and qualified
# turn matching without weakening the authoritative-run fail-closed contract.
fixture=pd.DataFrame({"geo_id":[GEOS[0]]*12,"date":pd.date_range("2020-01-31",periods=12,freq=MONTH_END),
                      "score":[-3,-2,-1,0,1,2,3,2,1,0,-1,-2]})
assert _summary_row(fixture,"score")["observations"] == 12
assert _summary_row(fixture,"score") == _summary_row(fixture.copy(),"score")
assert "incumbent_similarity" not in " ".join(grid.columns).lower()
assert '"automated_winner":False' in source and '"production_policy_changed":False' in source
assert ".to_csv(output/" in source and "config/" not in source.split("to_csv")[1]
with tempfile.TemporaryDirectory() as tmp:
    out=Path(tmp)/"out"
    try: build_review(Path(tmp)/RUN_ID,out,Path.cwd())
    except FileNotFoundError: pass
    else: raise AssertionError("must fail closed")
    assert not out.exists()

# The hosted checkout intentionally may omit immutable production artifacts.
# When the one authoritative run is mounted, exercise the complete evidence
# bundle and its determinism; no alternate run is ever accepted.
authoritative=Path("artifacts/regime/runs")/RUN_ID
if authoritative.is_dir():
  with tempfile.TemporaryDirectory() as tmp:
    first=build_review(authoritative,Path(tmp)/"first",Path.cwd())
    second=build_review(authoritative,Path(tmp)/"second",Path.cwd())
    for name in required_exports:
      a=pd.read_csv(first/f"{name}.csv"); b=pd.read_csv(second/f"{name}.csv")
      assert len(a)>0, f"required analytical export is empty: {name}"
      assert list(a.columns)!=["scope","period"], f"placeholder schema: {name}"
      pd.testing.assert_frame_equal(a,b,check_dtype=False)
    evaluation=pd.read_csv(first/"structural_cyclical_evaluation_matrix.csv")
    evidence=["cyclical_turn_expression_share","structural_turn_expression_share",
              "demand_axis_std","recent_demand_axis_std","demand_axis_median_abs",
              "seven_county_consistency"]
    assert evaluation[evidence].notna().all().all()
    axes=pd.read_csv(first/"structural_cyclical_demand_axis_scenarios.csv")
    assert axes.scenario_id.nunique()==len(grid)
    assert axes[["price_dimension","affordability_dimension","capital_markets_dimension"]].notna().all().all()
print("Structural/cyclical Demand architecture smoke test passed")
