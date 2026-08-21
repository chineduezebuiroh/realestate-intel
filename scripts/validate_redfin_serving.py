import argparse, json
from pathlib import Path
from sources.redfin.validate import validate_serving
p=argparse.ArgumentParser(); p.add_argument("--db",type=Path,required=True); p.add_argument("--expected-latest",required=True); a=p.parse_args(); print(json.dumps(validate_serving(a.db,a.expected_latest),indent=2))
