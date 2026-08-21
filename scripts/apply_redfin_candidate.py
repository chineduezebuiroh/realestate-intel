import argparse
from pathlib import Path
from sources.redfin.transform import apply_candidate
p=argparse.ArgumentParser(); p.add_argument("drop_id"); p.add_argument("--candidate",type=Path,required=True); a=p.parse_args(); print(f"refreshed {apply_candidate(a.drop_id,a.candidate)} Redfin rows")
