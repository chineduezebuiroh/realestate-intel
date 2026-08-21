import argparse, json
from pathlib import Path
from sources.redfin.ingest import build_candidate
p=argparse.ArgumentParser(); p.add_argument("drop_id",help="YYYY-MM drop ID, or 2026-07 for governed baseline-only acceptance"); p.add_argument("--output", type=Path, required=True); a=p.parse_args(); print(json.dumps(build_candidate(a.drop_id,a.output),indent=2))
