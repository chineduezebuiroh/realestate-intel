import argparse, json
from sources.redfin.ingest import register_drop
p=argparse.ArgumentParser(); p.add_argument("drop_id"); a=p.parse_args(); print(json.dumps(register_drop(a.drop_id), indent=2))
