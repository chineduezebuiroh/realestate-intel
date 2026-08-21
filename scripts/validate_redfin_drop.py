import argparse, json
from sources.redfin.validate import validate_drop
p=argparse.ArgumentParser(); p.add_argument("drop_id"); a=p.parse_args(); print(json.dumps(validate_drop(a.drop_id), indent=2))
