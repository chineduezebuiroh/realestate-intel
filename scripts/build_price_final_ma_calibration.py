#!/usr/bin/env python3
from pathlib import Path
import argparse
from regime.diagnostics.price_final_ma_calibration import build, load_run, write_review

def main():
    p=argparse.ArgumentParser(); p.add_argument("--input",type=Path,default=Path("artifacts/regime/runs/county_labor_demand_market_context_candidate_20260814")); p.add_argument("--output",type=Path,default=Path("artifacts/regime/comparisons/price_final_ma_calibration")); a=p.parse_args()
    write_review(build(load_run(a.input),Path(".")),a.output); print(f"Final Price MA diagnostic written to {a.output}")
if __name__ == "__main__": main()
