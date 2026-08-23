from __future__ import annotations
import argparse,json
from pathlib import Path
from core.source_artifacts.source_set import validate_source_set
from core.source_artifacts.storage import LocalArtifactResolver
def main():
 p=argparse.ArgumentParser(); p.add_argument("source_set",type=Path); p.add_argument("--artifact",action="append",default=[],help="URI=directory"); a=p.parse_args()
 resolver=LocalArtifactResolver(dict((item.split("=",1)[0],Path(item.split("=",1)[1])) for item in a.artifact))
 result=validate_source_set(a.source_set,resolver); print(json.dumps({"status":result["status"],"source_set_id":result["source_set"]["source_set_id"]},indent=2))
if __name__=="__main__": main()
