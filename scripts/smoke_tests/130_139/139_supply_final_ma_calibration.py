#!/usr/bin/env python3
"""Smoke 139: exact Supply final-MA grid, isolation, propagation, and SVGs."""
from pathlib import Path
import hashlib,importlib.util,re,tempfile,warnings
import numpy as np,pandas as pd
from regime._01_feature_engine import _compute_feature
from regime.diagnostics.supply_final_ma_calibration import SCENARIOS,EXPORTS,FIXED_WEIGHTS,METRIC_WEIGHTS,build,load_run,write_review

def fixture():
    spec=importlib.util.spec_from_file_location("p1",Path(__file__).with_name("137_supply_feature_anatomy.py")); mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); f=mod.fixture()
    # Final MA reconstructs features from source; persisted Phase-1 normalized features are deliberately irrelevant.
    supply=f["dimension_scores"].rename(columns={"dimension_score":"axis_score"})[["geo_id","date","axis_score"]]; supply["axis"]="supply"
    demand=supply.copy(); demand["axis"]="demand"; demand["axis_score"]=.25
    f["axis_scores"]=pd.concat([supply,demand],ignore_index=True); return f

warnings.simplefilter("error",RuntimeWarning)
assert [(x[0],x[1],x[2],tuple(x[3:])) for x in SCENARIOS]==[("MA12__I4","active_inventory",12,(.4,.15,.45)),("MA9__I4","active_inventory",9,(.4,.15,.45)),("MA12__N4","permit_intensity",12,(.4,.15,.45)),("MA9__N4","permit_intensity",9,(.4,.15,.45))]
assert not any(x[1]=="permit_activity" or x[2] in (6,10,11) or "I5" in x[0] or "N5" in x[0] for x in SCENARIOS)
assert FIXED_WEIGHTS["permit_activity"]==(.75,.10,.15) and METRIC_WEIGHTS=={"active_inventory":.65,"permit_activity":.30,"permit_intensity":.05}
protected=[Path("config/feature_registry.csv"),Path("config/normalization_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/axis_registry.csv")]; before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
t=build(fixture(),Path(".")); assert set(EXPORTS).issubset(t) and len(t["scenario_registry"])==4
assert set(t["controlled_ma_comparisons"].comparison_type)=={"I4_fixed","N4_fixed"}
assert set(t["metric_chronology"].groupby("scenario_id").metric.first())=={"active_inventory","permit_intensity"}
assert set(t["demand_isolation"].max_absolute_demand_delta)=={0.0} and t["demand_isolation"].unchanged_demand_chronology.all()
g=t["governance_status"].iloc[0]; assert g.recommendation_state=="none" and g.promotion_state=="current_production_unchanged" and not g.automated_winner and not g.production_policy_changed and not g.metric_weight_policy_changed and not g.capital_markets_changed and g.candidate_grid_closed
assert set(t["raw_cycle_comparison"].correlation_status).issubset({"ok","insufficient_overlap","left_nonfinite","right_nonfinite","both_nonfinite","left_constant","right_constant","both_constant"})
# Shared governed constructor: exact MA window and lag3/lag12; missing input poisons rather than sparse-rolls.
q=pd.DataFrame({"date":pd.date_range("2020-01-31",periods=24,freq="ME"),"value":np.arange(24.),"metric_origin":"x"}); q.loc[5,"value"]=np.nan
level=_compute_feature(q,"ma_level","9m","x"); short=_compute_feature(q,"ma_pct_change","9m/lag3m","x"); long=_compute_feature(q,"ma_pct_change","9m/lag12m","x")
assert short.equals(level.div(level.shift(3)).replace([np.inf,-np.inf],np.nan)-1) and long.equals(level.div(level.shift(12)).replace([np.inf,-np.inf],np.nan)-1) and pd.isna(level.iloc[5])
with tempfile.TemporaryDirectory() as d:
    out=Path(d); write_review(t,out); assert all((out/f"supply_final_ma_{x}.csv").is_file() for x in EXPORTS); assert (out/"supply_final_ma_review_index.html").is_file()
    svgs=list(out.glob("*.svg")); assert len(svgs)==10
    for p in svgs:
        x=p.read_text().lower(); assert "<path" in x and not re.search(r"(?<![a-z])(nan|[+-]?inf)(?![a-z])",x)
    try: load_run(out/"missing")
    except FileNotFoundError as e: assert "no substitute permitted" in str(e)
    else: raise AssertionError("authoritative input did not fail closed")
assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
print("Smoke 139 passed: exact closed grid, MA/lags, safe correlation, isolation, propagation, governance, fail-closed input, and plotted SVGs")
