import argparse, json
from pathlib import Path
from sources.redfin.ingest import monthly_gate
from sources.redfin.storage import current, read_json
p=argparse.ArgumentParser(); p.add_argument("drop_id", nargs="?"); p.add_argument("--current", action="store_true"); a=p.parse_args()
if a.current: result=current()
elif a.drop_id and monthly_gate(a.drop_id)=="registered": result=read_json(Path("data/redfin/raw/drops")/a.drop_id/"metadata.json")
else: result={"drop_id":a.drop_id,"status":"waiting_for_manual_redfin"}
print(json.dumps(result, indent=2))
