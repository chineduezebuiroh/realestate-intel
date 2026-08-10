"""Build the focused diagnostic from an immutable canonical source artifact."""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from regime.diagnostics.bps_permit_volatility import build_evidence, write_bundle

def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-metrics",type=Path,required=True)
    parser.add_argument("--source-run-id",required=True)
    parser.add_argument("--output-dir",type=Path,required=True)
    args=parser.parse_args()
    if not args.source_metrics.is_file():
        raise FileNotFoundError(f"authoritative source artifact is required; no substitution allowed: {args.source_metrics}")
    source=pd.read_parquet(args.source_metrics) if args.source_metrics.suffix.lower() in {".parquet",".pq"} else pd.read_csv(args.source_metrics)
    count=write_bundle(build_evidence(source,args.source_run_id),args.output_dir,args.source_run_id)
    print(f"[bps-permit-volatility] files={count} output={args.output_dir}")

if __name__ == "__main__": main()
