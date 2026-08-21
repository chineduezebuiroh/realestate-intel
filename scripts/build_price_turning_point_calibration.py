#!/usr/bin/env python3
from pathlib import Path
import argparse
from regime.diagnostics.price_turning_point_calibration import build,load_authoritative,write_review

def main():
    p=argparse.ArgumentParser(); p.add_argument("--phase2",type=Path,default=Path("artifacts/regime/comparisons/price_feature_weight_calibration")); p.add_argument("--output",type=Path,default=Path("artifacts/regime/comparisons/price_turning_point_calibration")); a=p.parse_args()
    raw,candidates=load_authoritative(a.phase2); write_review(build(raw,candidates),a.output)
    print(f"Price turning-point diagnostic written to {a.output}")
if __name__=="__main__": main()
