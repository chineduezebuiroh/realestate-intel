"""CLI for the two-stage LAUS feature architecture diagnostic."""
from __future__ import annotations
import argparse
from pathlib import Path
from regime.experiments.laus_feature_architecture import build, write_review

def main(argv=None):
    p=argparse.ArgumentParser(); p.add_argument("--stage",choices=("ma","weights"),required=True)
    p.add_argument("--selected-ma",type=int); p.add_argument("--run-dir",type=Path,required=True); p.add_argument("--output-dir",type=Path,required=True)
    a=p.parse_args(argv); root=Path(__file__).resolve().parents[1]
    if a.stage=="weights" and a.selected_ma is None: p.error("--selected-ma is required for stage weights")
    if a.run_dir.name!="macro_regime_v1_0_release_20260810" or not a.run_dir.is_dir():
        raise FileNotFoundError(f"immutable macro_regime_v1_0_release_20260810 run is required; no substitution: {a.run_dir}")
    tables=build(a.run_dir,root,a.stage,a.selected_ma); write_review(tables,a.output_dir,a.stage)
    print(f"[laus-feature-architecture] stage={a.stage} policies={len(tables['registry'])} output={a.output_dir}")
if __name__=="__main__": main()
