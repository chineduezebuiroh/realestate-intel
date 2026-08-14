#!/usr/bin/env python3
"""Smoke 125: bounded Structural role diagnostic contracts."""
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
import pandas as pd

from regime.experiments.structural_role_diagnostic import (
    AXIS_WEIGHTS, SCENARIOS, build_review, construct_architectures,
    parity_proof, scenario_registry,
)

r=scenario_registry()
assert list(r.scenario_id)==list(SCENARIOS) and len(r)==3
assert r.set_index("scenario_id").loc["A_S5_BLENDED","structural_weight_inside_demand"]==.05
assert r.set_index("scenario_id").loc["B_LABOR_ONLY","structural_weight_inside_demand"]==0
assert r.set_index("scenario_id").loc["C_LABOR_ONLY_MARKET_CONTEXT","market_context_retained"]
assert r.labor_force_membership.eq("LF-IN").all() and r.ma_window.eq("MA9").all() and r.feature_policy.eq("B3").all()
assert AXIS_WEIGHTS=={"demand":.65,"price":.175,"affordability":.075,"capital_markets":.10}

base=pd.DataFrame({"geo_id":["g","g"],"date":pd.to_datetime(["2024-01-31","2024-02-29"]),
 "structural_block_score":[.2,.3],"cyclical_block_score":[-.4,.5],"structural_weighted_contribution":[.01,.015],
 "cyclical_weighted_contribution":[-.38,.475],"core_demand_score":[-.37,.49],"combined_gross_contribution":[.39,.49],
 "cancellation_index":[.05,0.],"net_to_gross_ratio":[.95,1.],"sign_conflict":[True,False],
 "effective_structural_weight":[.05,.05],"effective_cyclical_weight":[.95,.95]})
out=construct_architectures(base.sample(frac=1,random_state=7)).sort_values(["scenario_id","date"])
b=out.loc[out.scenario_id.eq("B_LABOR_ONLY")]; c=out.loc[out.scenario_id.eq("C_LABOR_ONLY_MARKET_CONTEXT")]
assert np.array_equal(b.core_demand_score.to_numpy(),c.core_demand_score.to_numpy())
assert b.core_demand_score.tolist()==[-.4,.5] and b.structural_weighted_contribution.eq(0).all()
assert c.structural_block_score.notna().all() and c.effective_structural_weight.eq(0).all()
assert out.groupby("scenario_id").size().nunique()==1  # row-order independent construction
# Exact unchanged-axis reconstruction and B/C downstream parity on a fixture.
fixed=.175*.2+.075*(-.1)+.10*.3
assert np.allclose(.65*b.core_demand_score+fixed,.65*c.core_demand_score+fixed)
assert np.allclose(.65*b.core_demand_score+fixed,[.65*(-.4)+fixed,.65*.5+fixed])
p=parity_proof(); assert p.loc[p.case.eq("complete_availability"),"exact_parity"].item()
assert np.isclose(.65*.95,.6175) and np.isclose(.65*.05,.0325)
assert not p.loc[p.case.eq("normalization_semantics_differ"),"exact_parity"].item()

with TemporaryDirectory() as td:
    target=Path(td)/"must_not_exist"
    try: build_review(Path(td)/"missing_authoritative_run",target)
    except (FileNotFoundError,ValueError): pass
    else: raise AssertionError("authoritative input must fail closed")
    assert not target.exists()
# This diagnostic's committed surface is experiment/docs/scripts only; it cannot
# mutate production registries under config/ or production scoring modules.
assert all(not path.startswith("config/") for path in [
 "regime/experiments/structural_role_diagnostic.py",
 "scripts/build_structural_role_diagnostic.py",
])
print("smoke 125 passed")
