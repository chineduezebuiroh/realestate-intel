import argparse
from pathlib import Path
from sources.redfin.storage import atomic_json, current, promote, read_json

p=argparse.ArgumentParser(); p.add_argument("drop_id"); p.add_argument("--downstream-validated",action="store_true"); a=p.parse_args()
if not a.downstream_validated: raise SystemExit("--downstream-validated is required")
path=Path("data/redfin/raw/drops")/a.drop_id/"metadata.json"; metadata=read_json(path)
if metadata.get("status")=="promoted" and (current() or {}).get("promoted_drop")==a.drop_id:
    print(f"already published and promoted {a.drop_id}"); raise SystemExit(0)
if metadata.get("status")!="serving_refreshed": raise SystemExit("drop has not completed serving refresh")
metadata.update(status="downstream_validated",downstream_validation_status="validated"); atomic_json(path,metadata)
metadata.update(status="published",publication_status="published"); atomic_json(path,metadata)
promote(a.drop_id); print(f"published and promoted {a.drop_id}")
