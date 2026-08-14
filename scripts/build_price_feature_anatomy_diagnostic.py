"""Build the artifact-first Price Phase-1 feature-anatomy review."""
from __future__ import annotations
import argparse
from pathlib import Path
from regime.diagnostics.price_feature_anatomy import build,load_run,write_review

DEFAULT_RUN=Path("artifacts/regime/runs/county_labor_demand_market_context_candidate_20260814")
DEFAULT_OUTPUT=Path("artifacts/regime/comparisons/price_feature_anatomy")
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("input_run",type=Path,nargs="?",default=DEFAULT_RUN); p.add_argument("output_dir",type=Path,nargs="?",default=DEFAULT_OUTPUT); a=p.parse_args(argv)
 root=Path(__file__).resolve().parents[1]; tables=build(load_run(a.input_run),root); write_review(tables,a.output_dir)
 print(f"[price-phase1] input={a.input_run} output={a.output_dir} recommendation=none production=unchanged")
if __name__=="__main__": main()
