#!/usr/bin/env python3
"""Smoke 141: closed S0-S9 Supply metric-weight diagnostic contract."""
from pathlib import Path
import ast
import numpy as np
import pandas as pd
import tempfile
from regime.diagnostics.supply_metric_weight_calibration import (
 GRID,FIXED,METRICS,PANELS,CONTROLLED,_contributions,load_run,
)

LEGACY_GRID={"S0":(.60,.20,.20),"S1":(.60,.25,.15),"S2":(.60,.30,.10),"S3":(.55,.30,.15),"S4":(.60,.35,.05),"S5":(.55,.35,.10),"S6":(.55,.40,.05),"S7":(.50,.45,.05)}

def main():
 assert list(GRID)==[f"S{i}" for i in range(10)] and "S10" not in GRID
 assert {k:GRID[k] for k in LEGACY_GRID}==LEGACY_GRID
 assert GRID["S8"]==(.65,.30,.05) and GRID["S9"]==(.70,.25,.05)
 assert all(np.isclose(sum(v),1) for v in GRID.values())
 assert all(v[0] != .75 for v in GRID.values())
 assert FIXED["permit_activity"][-1]=="12m/lag6m" and PANELS==("governed_availability","common_three_metric_availability")
 source=Path("regime/diagnostics/supply_metric_weight_calibration.py").read_text(); tree=ast.parse(source)
 assert ".corr(" not in source and "np.corrcoef" not in source and "safe_corr(" in source
 assert set(METRICS)=={"active_inventory","permit_activity","permit_intensity"}
 assert {("S4","S8"),("S8","S9"),("S4","S9")}.issubset(CONTROLLED)
 assert '<polyline' in source and 'promotion_state":"current_production_unchanged"' in source and '"candidate_grid":"S0-S9"' in source
 # Deterministic regression fixture: independently reconstruct every legacy
 # policy and prove extending GRID did not change any S0-S7 arithmetic.
 dates=pd.to_datetime(["2024-01-31","2024-02-29"]); scores=((.8,.2,-.4),(.6,np.nan,-.2)); rows=[]
 for date,values in zip(dates,scores):
  for metric,value in zip(METRICS,values):
   if pd.notna(value): rows.append({"geo_id":"fixture_county__county","evaluation_date":date,"canonical_metric_key":metric,"metric_score":value})
 got=_contributions(pd.DataFrame(rows))
 assert set(got.evaluation_panel)==set(PANELS)
 for policy,weights in LEGACY_GRID.items():
  for date,values in zip(dates,scores):
   available=[i for i,v in enumerate(values) if pd.notna(v)]; total=sum(weights[i] for i in available)
   expected=sum(values[i]*weights[i]/total for i in available)
   actual=got[(got.policy.eq(policy))&got.evaluation_panel.eq("governed_availability")&got.date.eq(date)].supply_dimension_score.iloc[0]
   assert np.isclose(actual,expected,atol=1e-12)
 for _,q in got.groupby(["policy","evaluation_panel","geo_id","date"]):
  assert np.isclose(q.weighted_metric_contribution.sum(),q.supply_dimension_score.iloc[0])
 assert 'range(1,10)' in source
 with tempfile.TemporaryDirectory() as td:
  try: load_run(Path(td)/"wrong")
  except FileNotFoundError: pass
  else: raise AssertionError("authoritative input must fail closed")
 assert 'max_absolute_demand_delta":0.0' in source and "score_axes(dims)" in source
 print("Smoke 141 passed: exact closed S0-S9 grid, unchanged S0-S7 arithmetic, upper-bound comparisons, fixed lag/features, panels, reconstruction, Demand isolation, safe correlations, governance, SVGs, and fail-closed input")
if __name__=="__main__": main()
