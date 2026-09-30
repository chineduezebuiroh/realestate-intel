from __future__ import annotations
import argparse, json
from pathlib import Path
from core.source_artifacts.validation import validate_artifact
def main():
 p=argparse.ArgumentParser(); p.add_argument("artifact_dir",type=Path); p.add_argument("--max-single-asset-bytes",type=int); a=p.parse_args()
 print(json.dumps(validate_artifact(a.artifact_dir,max_single_asset_bytes=a.max_single_asset_bytes),default=str,indent=2))
if __name__=="__main__": main()
