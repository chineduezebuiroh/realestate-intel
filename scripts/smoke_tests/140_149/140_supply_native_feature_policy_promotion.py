#!/usr/bin/env python3
"""Smoke 140: governed native Supply feature-policy production closure."""
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
from regime._00_config_loader import load_regime_config, validate_regime_config

ROOT=Path(__file__).resolve().parents[3]
POLICIES={
 "redfin_inventory":{"level":("ma_level","12m",.40),"short_term_change":("ma_pct_change","12m/lag3m",.15),"long_term_change":("ma_pct_change","12m/lag12m",.45)},
 "bps_total_units":{"level":("ma_level","12m",.75),"short_term_change":("ma_pct_change","12m/lag6m",.10),"long_term_change":("ma_pct_change","12m/lag12m",.15)},
 "derived_permit_intensity":{"level":("ma_level","12m",.40),"short_term_change":("ma_pct_change","12m/lag3m",.15),"long_term_change":("ma_pct_change","12m/lag12m",.45)},
}
WEIGHTS={"redfin_inventory":.65,"bps_total_units":.30,"derived_permit_intensity":.05}

def main():
 c=load_regime_config(); validate_regime_config(c)
 q=c.features[c.features.metric_key.isin(POLICIES)]
 assert set(q.metric_key)==set(POLICIES) and len(q)==9
 for metric, expected in POLICIES.items():
  family=q[q.metric_key.eq(metric)]; assert len(family)==3 and np.isclose(family.feature_weight.astype(float).sum(),1)
  for kind, wanted in expected.items():
   row=family[family.feature_type.eq(kind)].iloc[0]
   assert (row["transform"],row.feature_window,float(row.feature_weight))==wanted
 metrics=c.metric_dimensions[c.metric_dimensions.metric_key.isin(WEIGHTS)]
 assert len(metrics)==3
 for key,weight in WEIGHTS.items(): assert np.isclose(float(metrics[metrics.metric_key.eq(key)].iloc[0].metric_weight),weight)
 record=json.loads((ROOT/'config/supply_native_feature_policy_2026_08_17.json').read_text())
 assert record['metric_scope']==['active_inventory','permit_activity','permit_intensity']
 assert record['native_supply_feature_calibration']=='closed' and record['metric_weight_calibration']=='pending'
 assert record['production_feature_policy_promoted'] is True and record['automated_winner'] is False
 assert record['human_decision']=='supply_native_feature_policy_approved'
 assert record['pending_metric_weight_candidates']==[f'S{i}' for i in range(8)]
 assert record['explicit_non_changes']==['Supply metric weights','axis weights','normalization','Demand','Price','Affordability','Labor','Capital Markets']
 assert record['policies']['permit_activity']['short']['window']=='12m/lag6m'
 assert c.axes.equals(__import__('pandas').read_csv(ROOT/'config/axis_registry.csv',dtype=str).fillna(''))
 tracked_artifacts=__import__('subprocess').run(['git','ls-files','artifacts/regime/runs'],cwd=ROOT,text=True,capture_output=True,check=True).stdout
 assert not tracked_artifacts.strip()
 assert record['supply_metric_weights']=={'active_inventory':.60,'permit_activity':.20,'permit_intensity':.20}, 'native-feature promotion must preserve its historical S0 context'
 print('Smoke 140 passed: exact native feature policies and lag semantics, historical S0 context preserved, current S8 weights, axes and unrelated-domain isolation, no generated runs tracked')
if __name__=='__main__': main()
