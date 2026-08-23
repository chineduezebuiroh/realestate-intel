from __future__ import annotations
import argparse,json
from pathlib import Path
from core.source_artifacts.assembly import assemble
from core.source_artifacts.storage import LocalArtifactResolver
def main():
 p=argparse.ArgumentParser(); p.add_argument("--source-set",required=True,type=Path); p.add_argument("--output",required=True,type=Path); p.add_argument("--artifact",action="append",default=[]); p.add_argument("--metric-registry",type=Path); p.add_argument("--geo-manifest",type=Path); a=p.parse_args()
 resolver=LocalArtifactResolver(dict((x.split("=",1)[0],Path(x.split("=",1)[1])) for x in a.artifact))
 print(json.dumps(assemble(a.source_set,a.output,resolver,metric_registry=a.metric_registry,geo_manifest=a.geo_manifest),indent=2))
if __name__=="__main__": main()
