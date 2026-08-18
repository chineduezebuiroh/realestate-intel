#!/usr/bin/env python3
"""Build the artifact-first Capital Markets Phase-2 review package."""
from pathlib import Path
import argparse
from time import perf_counter
from regime.diagnostics.capital_markets_feature_weight_calibration import build, load_run, write_review

DEFAULT_RUN = Path("artifacts/regime/runs/supply_s8_production_20260817")
DEFAULT_OUTPUT = Path("artifacts/regime/comparisons/capital_markets_feature_weight_calibration")

def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--input",type=Path,default=DEFAULT_RUN); parser.add_argument("--output",type=Path,default=DEFAULT_OUTPUT); args=parser.parse_args()
    started=perf_counter(); loaded=perf_counter(); artifacts=load_run(args.input); loaded=perf_counter()-loaded
    tables=build(artifacts,Path(".")); built=perf_counter()-started-loaded
    visual=perf_counter(); write_review(tables,args.output); visual=perf_counter()-visual
    total=perf_counter()-started
    audit=tables["performance_audit"].copy()
    audit.loc[audit.stage.eq("load"),["elapsed_seconds","call_count"]]=[loaded,1]
    audit.loc[audit.stage.eq("visualization"),["elapsed_seconds","call_count"]]=[visual,1]
    audit.loc[audit.stage.eq("total"),"elapsed_seconds"]=total
    audit.to_csv(args.output/"capital_markets_phase2_performance_audit.csv",index=False)
    print(f"Capital Markets Phase-2.5 diagnostic written to {args.output}")
    print(f"Corrected diagnostic status: P0 incumbent chronology reference; legacy raw-movement evidence retained")
    print(f"Runtime: total={total:.3f}s load={loaded:.3f}s build={built:.3f}s visualization={visual:.3f}s")

if __name__ == "__main__": main()
