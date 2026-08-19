"""Smoke 150: final Capital Markets F4 promotion and freeze."""
from __future__ import annotations
import hashlib, json, subprocess, sys, tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from regime._00_config_loader import load_regime_config

ROOT=Path(__file__).resolve().parents[3]
POLICIES={"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_2y":"P6","spread_10y_fedfunds":"P9"}
WEIGHTS={"mortgage_30y":.11666666666666667,"mortgage_15y":.11666666666666667,"treasury_10y":.11666666666666667,"fedfunds":.10,"spread_10y_2y":.275,"spread_10y_fedfunds":.275}
HISTORY=["capital_markets_native_feature_policy_2026_08_18.json","capital_markets_spread_polarity_repair_2026_08_18.json","capital_markets_spread_10y_2y_p6_2026_08_18.json"]

def main():
 before={name:hashlib.sha256((ROOT/"config"/name).read_bytes()).hexdigest() for name in HISTORY}
 promotion=json.loads((ROOT/"config/capital_markets_family_weight_f4_2026_08_18.json").read_text())
 freeze=json.loads((ROOT/"config/capital_markets_frozen_f4_2026_08_18.json").read_text())
 assert promotion["policy"]=="F4" and promotion["family_weights"]=={"long_term_rates":.35,"fedfunds":.10,"spreads":.55}
 assert promotion["metric_weights"]==WEIGHTS and np.isclose(sum(WEIGHTS.values()),1,rtol=0,atol=1e-12)
 assert promotion["feature_policies"]==POLICIES and promotion["equal_intra_family_weighting"] is True
 assert np.isclose(WEIGHTS["mortgage_30y"]*3,.35) and np.isclose(WEIGHTS["spread_10y_2y"]*2,.55)
 assert promotion["intra_family_metric_weight_calibration"]=="not_required"
 assert promotion["native_feature_calibration"]==promotion["spread_polarity_repair"]==promotion["family_metric_weight_calibration"]==promotion["capital_markets_calibration"]=="closed"
 assert promotion["capital_markets_fully_frozen"] is True and promotion["automated_winner"] is False
 assert promotion["axis_weights"]=={"demand":.10,"supply":.15} and not promotion["normalization_changed"] and not promotion["feature_policy_changed"] and not promotion["non_capital_markets_changed"]
 assert freeze["capital_markets_fully_frozen"] and not freeze["preference_driven_retuning_sufficient"] and freeze["next_active_workstream"]=="Macro Regime Visualization MVP"
 cfg=load_regime_config(validate=True)
 got=cfg.metric_dimensions[cfg.metric_dimensions.canonical_metric_key.isin(WEIGHTS)].set_index("canonical_metric_key").metric_weight.astype(float).to_dict(); assert got==WEIGHTS
 rows=cfg.features[cfg.features.canonical_metric_key.isin(WEIGHTS)] if "canonical_metric_key" in cfg.features else cfg.features[cfg.features.metric_key.isin(cfg.metric_dimensions[cfg.metric_dimensions.canonical_metric_key.isin(WEIGHTS)].metric_key)]
 spread=cfg.features.query("metric_key=='fred_2y10y_spread'").set_index("feature_type")
 assert spread.feature_weight.astype(float).to_dict()=={"level":.60,"short_term_change":.05,"long_term_change":.35}
 norm=pd.read_csv(ROOT/"config/normalization_registry.csv"); assert set(norm[norm.policy_key.isin(spread.feature_key)].score_direction)=={"positive"}
 assert cfg.axes.query("dimension=='capital_markets'").set_index("axis").dimension_weight.astype(float).to_dict()=={"demand":.10,"supply":.15}
 assert before=={name:hashlib.sha256((ROOT/"config"/name).read_bytes()).hexdigest() for name in HISTORY}
 memory=(ROOT/"docs/project_memory.md").read_text(); roadmap=(ROOT/"docs/regime_engine_roadmap.md").read_text()
 assert "Capital Markets is fully calibrated and frozen" in memory and "Macro Regime Visualization MVP" in memory
 assert "intra-family calibration = `not_required`" in roadmap and "Capital Markets = **fully frozen**" in roadmap
 tracked=subprocess.run(["git","ls-files","artifacts/regime/runs"],cwd=ROOT,text=True,capture_output=True,check=True); assert not tracked.stdout.strip()
 with tempfile.TemporaryDirectory() as tmp:
  result=subprocess.run([sys.executable,str(ROOT/"scripts/validate_capital_markets_f4_production_run.py"),"--artifact-root",tmp],cwd=ROOT,text=True,capture_output=True)
  assert result.returncode!=0 and "required immutable run is absent" in result.stderr
 print("Smoke 150 passed: exact F4 promotion, preserved policies/polarity/history, complete freeze, fail closed")
if __name__=="__main__": main()
