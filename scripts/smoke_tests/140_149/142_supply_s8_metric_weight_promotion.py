#!/usr/bin/env python3
"""Smoke 142: human-approved Supply S8 production closure."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess

import numpy as np

from regime._00_config_loader import load_regime_config, validate_regime_config

ROOT=Path(__file__).resolve().parents[3]
S8={"active_inventory":.65,"permit_activity":.30,"permit_intensity":.05}
FEATURES={
 "redfin_inventory":{"level":("ma_level","12m",.40),"short_term_change":("ma_pct_change","12m/lag3m",.15),"long_term_change":("ma_pct_change","12m/lag12m",.45)},
 "bps_total_units":{"level":("ma_level","12m",.75),"short_term_change":("ma_pct_change","12m/lag6m",.10),"long_term_change":("ma_pct_change","12m/lag12m",.15)},
 "derived_permit_intensity":{"level":("ma_level","12m",.40),"short_term_change":("ma_pct_change","12m/lag3m",.15),"long_term_change":("ma_pct_change","12m/lag12m",.45)},
}

def main():
 config=load_regime_config(); validate_regime_config(config)
 supply=config.metric_dimensions[config.metric_dimensions.canonical_metric_key.isin(S8)]
 assert len(supply)==3 and set(supply.canonical_metric_key)==set(S8)
 assert supply.set_index("canonical_metric_key").metric_weight.astype(float).to_dict()==S8 and np.isclose(sum(S8.values()),1)
 for metric,expected in FEATURES.items():
  family=config.features[config.features.metric_key.eq(metric)]; assert len(family)==3
  for kind,wanted in expected.items():
   row=family[family.feature_type.eq(kind)].iloc[0]
   assert (row["transform"],row.feature_window,float(row.feature_weight))==wanted
 record=json.loads((ROOT/"config/supply_metric_weight_s8_2026_08_17.json").read_text())
 assert record["production_metric_weight_policy"]=="S8" and record["promoted_weights"]==S8
 assert record["human_decision"]=="supply_s8_metric_weight_approved" and record["automated_winner"] is False
 assert record["native_supply_feature_calibration"]==record["supply_metric_weight_calibration"]==record["supply_calibration"]=="closed"
 assert record["bounded_calibration"]=="S0-S9" and record["upper_inventory_boundary_percent"]==65
 assert record["upper_inventory_tested_through_percent"]==70 and record["inventory_75_tested"] is record["inventory_75_warranted"] is False
 assert record["capital_markets"]=="unchanged" and record["native_feature_policies"]["permit_activity"]["short_lag_months"]==6
 historical=json.loads((ROOT/"config/supply_metric_weight_promotion_2026_08_06.json").read_text())
 assert historical["promoted_weights"]=={"active_inventory":.60,"permit_activity":.20,"permit_intensity":.20}
 frozen=json.loads((ROOT/"config/supply_dimension_frozen_s8_2026_08_17.json").read_text())
 assert frozen["metric_weights"]==S8 and frozen["historical_freeze_preserved"]=="supply_dimension_frozen_v1"
 axes=config.axes.copy(); assert axes.equals(__import__("pandas").read_csv(ROOT/"config/axis_registry.csv",dtype=str).fillna(""))
 tracked=subprocess.run(["git","ls-files","artifacts/regime/runs"],cwd=ROOT,text=True,capture_output=True,check=True).stdout
 assert not tracked.strip()
 print("Smoke 142 passed: exact S8 scope/weights, fixed native features and lag-6 Short, historical S0 preserved, S0-S9 and Supply closed, 65% boundary, no 75%, Capital Markets unchanged, no tracked runs")

if __name__=="__main__": main()
