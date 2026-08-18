"""Smoke 145: governed Capital Markets native feature-policy promotion."""
from __future__ import annotations
import json, subprocess, sys, tempfile
from pathlib import Path
import pandas as pd
from regime._00_config_loader import load_regime_config

ROOT=Path(__file__).resolve().parents[3]
CONTRACT=ROOT/"config/capital_markets_native_feature_policy_2026_08_18.json"
REPAIR=ROOT/"config/capital_markets_spread_polarity_repair_2026_08_18.json"
EXPECTED={"mortgage_30y":("P4",.55,.10,.35),"mortgage_15y":("P2",.60,.10,.30),"treasury_10y":("P1",.60,.15,.25),"fedfunds":("P5",.50,.10,.40),"spread_10y_2y":("P7",.35,.10,.55),"spread_10y_fedfunds":("P9",.40,.10,.50)}
METRIC_WEIGHTS={"mortgage_30y":.15,"mortgage_15y":.15,"treasury_10y":.15,"fedfunds":.10,"spread_10y_2y":.225,"spread_10y_fedfunds":.225}
def main():
 c=json.loads(CONTRACT.read_text()); assert c["promotion_contract"]=="capital_markets_native_feature_policy_2026_08_18"
 assert c["metric_scope"]==list(EXPECTED) and c["feature_calibration"]=="closed" and c["family_metric_weight_calibration"]=="pending"
 assert c["automated_winner"] is False and c["production_feature_policy_promoted"] is True and c["no_unrelated_production_mutation"] is True
 assert c["capital_markets_metric_weights"]==METRIC_WEIGHTS and c["axis_weights_unchanged"]=={"demand_capital_markets":.10,"supply_capital_markets":.15}
 cfg=load_regime_config(validate=True); policies=c["policies"]
 keys={p["registry_metric_key"] for p in policies.values()}; assert len(policies)==6 and len(keys)==6
 promoted=cfg.features[cfg.features.metric_key.isin(keys)]; assert len(promoted)==18
 types=(("level","level"),("short","short_term_change"),("long","long_term_change"))
 for metric,(policy,*weights) in EXPECTED.items():
  p=policies[metric]; assert p["policy"]==policy
  assert "policy_status" not in p
  rows=cfg.features[cfg.features.metric_key.eq(p["registry_metric_key"])].set_index("feature_type")
  if metric=="spread_10y_2y":
   # This contract is immutable defect-era history; P6 now owns production.
   assert weights==[.35,.10,.55]
   continue
  for (name,ft),weight in zip(types,weights):
   assert (p[name]["transform"],p[name]["window"],p[name]["weight"])==(rows.loc[ft,"transform"],rows.loc[ft,"feature_window"],float(rows.loc[ft,"feature_weight"]))
  direction=pd.read_csv(ROOT/"config/indicator_regime_registry.csv").set_index("metric_key").loc[p["registry_metric_key"],"direction"]; assert p["score_direction"]==direction
 governed=cfg.metric_dimensions[cfg.metric_dimensions.canonical_metric_key.isin(METRIC_WEIGHTS)].set_index("canonical_metric_key")
 assert governed.metric_weight.astype(float).to_dict()==METRIC_WEIGHTS
 assert cfg.axes.query("dimension=='capital_markets'").set_index("axis").dimension_weight.astype(float).to_dict()=={"demand":.10,"supply":.15}
 assert set(c["explicit_non_changes"])>={"normalization","Supply","Price","Affordability","Labor"}
 repair=json.loads(REPAIR.read_text()); assert repair["defect_confirmed"] is True
 assert repair["spread_10y_2y_feature_policy"]=="P7" and repair["spread_10y_2y_feature_policy_status"]=="revalidation_required"
 assert repair["family_weight_evidence_status"]=="invalidated_pending_rerun" and repair["family_weight_production_decision"]=="none"
 tracked=subprocess.run(["git","ls-files","artifacts/regime/runs/capital_markets_feature_policy_production_20260818"],cwd=ROOT,text=True,capture_output=True,check=True); assert not tracked.stdout.strip()
 with tempfile.TemporaryDirectory() as tmp:
  result=subprocess.run([sys.executable,str(ROOT/"scripts/validate_capital_markets_feature_policy_run.py"),"--artifact-root",tmp],cwd=ROOT,text=True,capture_output=True)
  assert result.returncode!=0 and "required immutable run is absent" in result.stderr
 print("Smoke 145 passed: exact six-policy governance, unchanged weights/policies, no artifacts, validator fails closed")
if __name__=="__main__": main()
