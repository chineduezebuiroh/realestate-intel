#!/usr/bin/env python3
"""Smoke 131: closed-grid final Price MA diagnostic contracts."""
from pathlib import Path
import hashlib
import tempfile
import numpy as np
import pandas as pd

from regime._01_feature_engine import _compute_feature
from regime.calendar_ma import minimum_valid_observations
from regime.diagnostics.price_feature_anatomy import REVIEW_GEOS, TARGET_METRICS
from regime.diagnostics.price_final_ma_calibration import (
    DEMAND_WEIGHTS, EXPORTS, MATCH_MONTHS, PERSISTENCE, PRICE_WEIGHTS,
    SCENARIOS, build, load_run, write_review,
)


def fixture():
    dates=pd.date_range("2015-01-31",periods=132,freq="ME"); source=[]; dims=[]; axes=[]
    keys={"median_sale_price":"redfin_median_sale_price","median_ppsf":"redfin_median_ppsf"}
    for j,geo in enumerate(REVIEW_GEOS):
        for i,date in enumerate(dates):
            prices=[]
            for mi,m in enumerate(TARGET_METRICS):
                value=(350000 if mi==0 else 280)*(1+.002*i+.035*np.sin(i/5+mi/4+j/20)); prices.append(np.tanh((i-60)/30)+mi*.02)
                if not (j==0 and mi==0 and i==35): source.append({"geo_id":geo,"date":date,"metric_key":keys[m],"value":value,"metric_origin":keys[m]})
            production_price=sum(prices)/2
            dims.append({"geo_id":geo,"date":date,"dimension":"price","dimension_score":production_price})
            axes.append({"geo_id":geo,"date":date,"axis":"demand","axis_score":.65*np.sin(i/15)+.175*production_price})
    return {"source_metrics":pd.DataFrame(source),"dimension_scores":pd.DataFrame(dims),"axis_scores":pd.DataFrame(axes)}


protected=[Path("config/feature_registry.csv"),Path("config/normalization_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/axis_registry.csv")]
before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
assert tuple(x[0] for x in SCENARIOS)==("MA12__P4","MA12__P6","MA9__P4","MA9__P6")
assert {(x[1],x[2],x[3],x[4],x[5]) for x in SCENARIOS}=={(12,"P4",.35,.15,.5),(12,"P6",.35,.2,.45),(9,"P4",.35,.15,.5),(9,"P6",.35,.2,.45)}
assert not any(x[1] in (10,11,6) for x in SCENARIOS); assert PERSISTENCE==2 and MATCH_MONTHS==3
assert PRICE_WEIGHTS=={"median_sale_price":.5,"median_ppsf":.5}; assert DEMAND_WEIGHTS=={"labor_demand":.650,"price":.175,"affordability":.075,"capital_markets":.100}
tables=build(fixture(),Path(".")); assert set(EXPORTS).issubset(tables)
reg=tables["scenario_registry"]; assert len(reg)==4 and set(reg.detector_persistence)=={2}
assert set(tables.raw_cycle_comparison.reference_type)=={"raw_cycle_reference"} if hasattr(tables,"raw_cycle_comparison") else set(tables["raw_cycle_comparison"].reference_type)=={"raw_cycle_reference"}
assert set(tables["long_reference_comparison"].reference_type)=={"long_feature_reference"}
assert set(tables["controlled_ma_comparisons"].comparison_type)=={"P4_fixed","P6_fixed"}
assert set(tables["policy_comparisons"].comparison_type)=={"MA12_fixed","MA9_fixed"}
assert {"median_signed_delay","mean_signed_delay","median_absolute_delay","mean_absolute_delay","p90_absolute_delay","peak_median_delay","trough_median_delay"}.issubset(tables["effective_delay"])
gov=tables["governance_status"].iloc[0]; assert not gov.automated_winner and not gov.production_policy_changed and gov.candidate_grid_closed and gov.detector_persistence==2
# Shared calendar constructor: full horizon, two-thirds coverage, and calendar lag.
g=pd.DataFrame({"date":pd.date_range("2020-01-31",periods=20,freq="ME"),"value":np.arange(20,dtype=float),"metric_origin":"x"}); g.loc[5,"value"]=np.nan
ma=_compute_feature(g,"ma_level","9m","fixture"); assert ma.iloc[:8].isna().all() and minimum_valid_observations(9)==6 and pd.notna(ma.iloc[8])
short=_compute_feature(g,"ma_pct_change","9m/lag3m","fixture"); long=_compute_feature(g,"ma_pct_change","9m/lag12m","fixture"); assert short.iloc[:11].isna().all() and long.isna().all()
with tempfile.TemporaryDirectory() as d:
    out=Path(d); write_review(tables,out)
    assert all((out/f"price_final_ma_{x}.csv").is_file() for x in EXPORTS)
    svgs=list(out.glob("*.svg")); assert svgs and all("<path" in p.read_text() for p in svgs)
    assert all("<circle" in p.read_text() for p in out.glob("*turning_points.svg"))
    try: load_run(out/"missing")
    except FileNotFoundError as e: assert "no substitute permitted" in str(e)
    else: raise AssertionError("missing authoritative input did not fail closed")
assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
print("Smoke 131 passed: exact closed grid, shared MA/normalization, calendar lags, persistence 2, controlled comparisons, propagation, SVGs, governance, fail closed")
