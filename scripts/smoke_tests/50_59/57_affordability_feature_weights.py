"""Focused Phase 4B feature-weight isolation smoke test."""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from regime.experiments.affordability_feature_weights import *

dates = pd.date_range("2018-01-31", periods=72, freq=MONTH_END)
rows=[]
for j, geo in enumerate(("district_of_columbia_dc__county","alameda_county_ca__county")):
    rows += [(geo,d,"median_sale_price",250000+j*50000+i*1200+5000*np.sin(i/4)) for i,d in enumerate(dates)]
    rows += [(geo,dates[i*12],"median_household_income",70000+j*6000+i*2000) for i in range(6)]
rows += [("national",d,"mortgage_30y",3+.03*i) for i,d in enumerate(dates)]
source=pd.DataFrame(rows,columns=["geo_id","date","canonical_metric_key","value"])
registry=policy_registry(); assert registry.policy.tolist()==[POLICY_A,POLICY_B]
assert POLICIES=={POLICY_A:{"level":.5,"short":.2,"long":.3},POLICY_B:{"level":.5,"short":.25,"long":.25}}
assert registry.level_window.eq(12).all() and registry.short_lag.eq(3).all() and registry.long_lag.eq(12).all()
assert registry.mortgage_at_derivation.eq("raw_canonical").all() and registry.income_treatment.str.contains("forward_fill").all()
t=build_affordability_feature_weight_evidence(source).tables
c=t["affordability_feature_weight_feature_contributions"]
for col in ["structural_level","short_feature","long_feature","level_score","short_score","long_score"]:
    wide=c.pivot(index=["metric","geo_id","date"],columns="policy",values=col).dropna(); assert np.allclose(wide[POLICY_A],wide[POLICY_B],atol=0,rtol=0)
assert (c.metric_score-c.reconstructed_score.clip(-1,1)).abs().dropna().max() <= TOLERANCE
assert set(c.metric)==set(TARGET_METRICS) and t["affordability_feature_weight_parity_audit"].status.eq("pass").all()
m=t["affordability_feature_weight_decision_matrix"]; assert len(m)==2
assert dict(zip(m.Policy, m.Decision)) == {POLICY_A:"selected", POLICY_B:"not_selected"}
turns=t["affordability_feature_weight_dimension_turning_point_summary"]
assert len(turns)==2 and set(turns.metric)=={"affordability"} and set(turns.policy)==set(POLICIES)
assert turns.turning_points.notna().all() and turns.latest_36m_turning_points.notna().all()
assert m["Affordability dimension turning points"].notna().all()
assert m["Latest-36m Affordability turns"].notna().all()
assert not any("rank" in x.lower() or "composite" in x.lower() for x in m.columns)
s=t["affordability_feature_weight_human_decision_status"].iloc[0]
assert (s.selected_policy,s.recommendation_state,s.promotion_state,s.human_decision)==(POLICY_A,"selected","retained","approved")
assert s.phase4b_state=="closed" and s.affordability_calibration_state=="complete"
settlement=t["affordability_feature_weight_settlement_policy_registry"]
assert len(settlement)==2 and settlement.loc[settlement.policy.eq(POLICY_A),"production_policy"].item()
assert not settlement.loc[settlement.policy.eq(POLICY_B),"selected_policy"].item()
audit=t["affordability_feature_weight_settlement_config_audit"]
assert audit.status.eq("pass").all() and {"capital_markets","supply"}.issubset(set(audit.control))
print("PASS: Phase 4B Affordability feature-weight contract")
