from __future__ import annotations
import argparse,json
from pathlib import Path
from sources.redfin.inbox import register_incoming
def main():
 p=argparse.ArgumentParser(); p.add_argument("--root",type=Path,default=Path("data/redfin/raw")); p.add_argument("--keep-incoming",action="store_true"); a=p.parse_args(); print(json.dumps(register_incoming(a.root,clear_incoming=not a.keep_incoming),indent=2))
if __name__=="__main__": main()
