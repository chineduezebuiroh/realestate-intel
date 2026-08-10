"""Build the final diagnostic from the authoritative source artifact."""
import argparse
from pathlib import Path
import pandas as pd
from regime.experiments.bps_finalist_incremental_value import build_evidence,write_bundle
def main():
    p=argparse.ArgumentParser(); p.add_argument('--source-metrics',type=Path,required=True); p.add_argument('--source-run-id',required=True); p.add_argument('--output-dir',type=Path,required=True); a=p.parse_args()
    if not a.source_metrics.is_file(): raise FileNotFoundError(f"authoritative source artifact is required; no substitution allowed: {a.source_metrics}")
    source=pd.read_parquet(a.source_metrics) if a.source_metrics.suffix.lower() in {'.parquet','.pq'} else pd.read_csv(a.source_metrics); print(f"[bps-finalist] files={write_bundle(build_evidence(source,a.source_run_id),a.output_dir,a.source_run_id)} output={a.output_dir}")
if __name__=='__main__': main()
