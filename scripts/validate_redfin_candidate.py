import argparse, json
from pathlib import Path
from sources.redfin.validate import validate_candidate
p=argparse.ArgumentParser(); p.add_argument("drop_id"); p.add_argument("--candidate",type=Path,required=True); p.add_argument("--compare-db",type=Path); a=p.parse_args(); print(json.dumps(validate_candidate(a.candidate,a.drop_id,db_path=a.compare_db),indent=2))
