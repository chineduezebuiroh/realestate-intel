#!/usr/bin/env python3
"""Smoke 136: governed Affordability MA12/P4 production closure."""
import json
from pathlib import Path
import numpy as np
from regime._00_config_loader import load_regime_config,validate_regime_config
ROOT=Path(__file__).resolve().parents[3]
TARGETS={"derived_price_to_income","derived_payment_burden"}
EXPECTED={"level":("ma_level","12m",.35),"short_term_change":("ma_pct_change","12m/lag3m",.20),"long_term_change":("ma_pct_change","12m/lag12m",.45)}
c=load_regime_config(); validate_regime_config(c); q=c.features[c.features.metric_key.isin(TARGETS)]
assert set(q.metric_key)==TARGETS and len(q)==6
for metric in TARGETS:
 family=q[q.metric_key==metric]; assert np.isclose(family.feature_weight.astype(float).sum(),1)
 for ft,e in EXPECTED.items():
  r=family[family.feature_type==ft].iloc[0]; assert (r["transform"],r.feature_window,float(r.feature_weight))==e
record=json.loads((ROOT/'config/affordability_policy_promotion_2026_08_16.json').read_text())
assert record['selected_policy']=='MA12/P4' and record['derive_first'] and record['calibration_state']=='closed'
assert set(record['metrics'])=={'price_to_income','payment_burden'} and record['feature_weights']=={'level':.35,'short':.2,'long':.45}
assert record['explicit_non_changes']==['metric weights','Affordability dimension weight','Demand-axis weights','normalization','Price','Labor','Supply','Capital Markets']
print('Smoke 136 passed: exact MA12/P4 registry scope, derive-first promotion, closure, and non-change contract')
