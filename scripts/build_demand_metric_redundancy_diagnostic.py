"""CLI for the immutable Demand metric redundancy diagnostic."""
from __future__ import annotations
import argparse
from pathlib import Path
from regime.experiments.demand_metric_redundancy import build, write_review

def main(argv=None):
    p=argparse.ArgumentParser(); p.add_argument("--run-dir",type=Path,required=True); p.add_argument("--output-dir",type=Path,required=True)
    a=p.parse_args(argv); root=Path(__file__).resolve().parents[1]
    expected="macro_regime_v1_0_release_20260810"
    if a.run_dir.name != expected or not a.run_dir.is_dir(): raise FileNotFoundError(f"immutable {expected} run is required; no substitution: {a.run_dir}")
    tables=build(a.run_dir,root,a.output_dir/"debug_movement"); write_review(tables,a.output_dir); print(f"[demand-metric-redundancy] files={len(tables)+5} output={a.output_dir}")
if __name__=="__main__": main()
