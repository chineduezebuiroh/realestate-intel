import argparse, json
from sources.redfin.storage import retain
p=argparse.ArgumentParser(); p.add_argument("--apply",action="store_true"); p.add_argument("--keep",type=int,default=3); p.add_argument("--quarantine-days",type=int,default=90); a=p.parse_args(); print(json.dumps({"dry_run":not a.apply,"eligible":retain(keep=a.keep,quarantine_days=a.quarantine_days,dry_run=not a.apply)},indent=2))
