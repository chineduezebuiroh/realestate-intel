#!/usr/bin/env python3
"""Build the bounded Supply final-MA diagnostic package."""
from pathlib import Path
import argparse
from regime.diagnostics.supply_final_ma_calibration import build,load_run,write_review

def main():
    p=argparse.ArgumentParser(); p.add_argument("--input",type=Path,default=Path("artifacts/regime/runs/affordability_ma12_p4_production_20260816")); p.add_argument("--output",type=Path,default=Path("artifacts/regime/comparisons/supply_final_ma_calibration")); a=p.parse_args()
    write_review(build(load_run(a.input),Path(".")),a.output); print(f"Supply final-MA diagnostic written to {a.output}")
if __name__=="__main__": main()
