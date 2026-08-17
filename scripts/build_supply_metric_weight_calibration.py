#!/usr/bin/env python3
"""Build the governed S0-S7 Supply metric-weight diagnostic."""
from pathlib import Path
import argparse
from regime.diagnostics.supply_metric_weight_calibration import build,load_run,write_review

def main():
 p=argparse.ArgumentParser(); p.add_argument("--input",type=Path,default=Path("artifacts/regime/runs/supply_feature_policy_production_20260817")); p.add_argument("--output",type=Path,default=Path("artifacts/regime/comparisons/supply_metric_weight_calibration")); a=p.parse_args()
 write_review(build(load_run(a.input)),a.output); print(f"Supply metric-weight diagnostic written to {a.output}")
if __name__=="__main__": main()
