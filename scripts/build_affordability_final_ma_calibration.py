#!/usr/bin/env python3
from pathlib import Path
import argparse
from regime.diagnostics.affordability_final_ma_calibration import build,load_run,write_review
def main():
 p=argparse.ArgumentParser(); p.add_argument("--input",type=Path,default=Path("artifacts/regime/runs/price_ma12_p6_production_20260815")); p.add_argument("--output",type=Path,default=Path("artifacts/regime/comparisons/affordability_final_ma_calibration")); a=p.parse_args(); write_review(build(load_run(a.input),Path(".")),a.output)
if __name__=="__main__": main()
