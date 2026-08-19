"""Smoke 149: corrected-polarity spread P6 production promotion."""
from __future__ import annotations
import hashlib, json, subprocess, sys, tempfile
from pathlib import Path
import pandas as pd
from regime._00_config_loader import load_regime_config

ROOT=Path(__file__).resolve().parents[3]
CM={"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_2y":"P6","spread_10y_fedfunds":"P9"}
WEIGHTS={"mortgage_30y":.15,"mortgage_15y":.15,"treasury_10y":.15,"fedfunds":.10,"spread_10y_2y":.225,"spread_10y_fedfunds":.225}
CURRENT_WEIGHTS={"mortgage_30y":.11666666666666667,"mortgage_15y":.11666666666666667,"treasury_10y":.11666666666666667,"fedfunds":.10,"spread_10y_2y":.275,"spread_10y_fedfunds":.275}

def main():
 historical_path=ROOT/"config/capital_markets_native_feature_policy_2026_08_18.json"
 repair_path=ROOT/"config/capital_markets_spread_polarity_repair_2026_08_18.json"
 before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in (historical_path,repair_path)}
 historical=json.loads(historical_path.read_text()); repair=json.loads(repair_path.read_text())
 promotion=json.loads((ROOT/"config/capital_markets_spread_10y_2y_p6_2026_08_18.json").read_text())
 assert historical["policies"]["spread_10y_2y"]["policy"]=="P7"
 assert repair["spread_10y_2y_feature_policy"]=="P7" and repair["spread_10y_2y_feature_policy_status"]=="revalidation_required"
 assert promotion["previous_policy"]=="P7" and promotion["previous_policy_status"]=="revalidation_failed"
 assert promotion["selected_policy"]=="P6" and promotion["selected_feature_weights"]=={"level":.60,"short":.05,"long":.35}
 assert promotion["corrected_source_contract"]=="treasury_10y - treasury_2y"
 assert promotion["canonicalization"]=="spread_10y_2y = -fred_spread_2y_10y"
 assert promotion["polarity_repair_validated"] is True and promotion["feature_revalidation"]=="closed"
 assert promotion["human_decision"]=="spread_10y_2y_p6_approved" and promotion["automated_winner"] is False
 assert promotion["family_metric_weight_calibration"]=="pending_rerun"
 assert promotion["other_five_feature_policies"]=={k:v for k,v in CM.items() if k!="spread_10y_2y"}
 assert promotion["capital_markets_metric_weights"]==WEIGHTS and promotion["axis_weights"]=={"demand":.10,"supply":.15}
 assert not promotion["metric_weights_changed"] and not promotion["axis_weights_changed"] and not promotion["normalization_changed"]
 cfg=load_regime_config(validate=True); rows=cfg.features.query("metric_key=='fred_2y10y_spread'").set_index("feature_type")
 assert rows.feature_weight.astype(float).to_dict()=={"level":.60,"short_term_change":.05,"long_term_change":.35}
 assert set(rows["transform"])=={"ma_level","ma_difference"} and set(rows.feature_window)=={"9m","9m/lag3m","9m/lag12m"}
 norm=pd.read_csv(ROOT/"config/normalization_registry.csv"); assert set(norm[norm.policy_key.isin(rows.feature_key)].score_direction)=={"positive"}
 got=cfg.metric_dimensions[cfg.metric_dimensions.canonical_metric_key.isin(WEIGHTS)].set_index("canonical_metric_key").metric_weight.astype(float).to_dict(); assert got==CURRENT_WEIGHTS
 assert cfg.axes.query("dimension=='capital_markets'").set_index("axis").dimension_weight.astype(float).to_dict()=={"demand":.10,"supply":.15}
 assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in (historical_path,repair_path)}
 tracked=subprocess.run(["git","ls-files","artifacts/regime/runs"],cwd=ROOT,text=True,capture_output=True,check=True); assert not tracked.stdout.strip()
 with tempfile.TemporaryDirectory() as tmp:
  result=subprocess.run([sys.executable,str(ROOT/"scripts/validate_capital_markets_corrected_feature_policy_run.py"),"--artifact-root",tmp],cwd=ROOT,text=True,capture_output=True)
  assert result.returncode!=0 and "required immutable run is absent" in result.stderr
 print("Smoke 149 passed: exact P6-only change, preserved construction/polarity/history/weights, pending family rerun, fail closed")
if __name__=="__main__": main()
