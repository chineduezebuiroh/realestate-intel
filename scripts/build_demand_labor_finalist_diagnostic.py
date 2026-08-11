"""CLI for the immutable Demand labor finalist diagnostic."""
from __future__ import annotations
import argparse
from pathlib import Path
from regime.experiments.demand_labor_finalist import build, write_review

def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument("--run-dir",type=Path,required=True); parser.add_argument("--output-dir",type=Path,required=True)
    args=parser.parse_args(argv); root=Path(__file__).resolve().parents[1]
    expected="macro_regime_v1_0_release_20260810"
    if args.run_dir.name != expected or not args.run_dir.is_dir():
        raise FileNotFoundError(f"immutable {expected} run is required; no substitution: {args.run_dir}")
    tables=build(args.run_dir,root); write_review(tables,args.output_dir)
    print(f"[demand-labor-finalist] files={len(tables)+1} output={args.output_dir}")

if __name__=="__main__": main()
