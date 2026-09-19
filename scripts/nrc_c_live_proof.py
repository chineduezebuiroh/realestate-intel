#!/usr/bin/env python3
"""Local network proof of NRC exact pin, candidate, and pinned-byte replay."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd
from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.nrc_monthly import candidate, discover_pin, recover_pinned_workbooks
from jobs.monthly_refresh.source_inputs import FilePinStore

def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('--cycle-id',required=True)
    parser.add_argument('--workspace',type=Path,required=True); args=parser.parse_args()
    if args.workspace.exists(): raise ValueError('proof workspace must be fresh')
    store=FilePinStore(args.workspace/'authority')
    pin,paths=discover_pin(cycle_id=args.cycle_id,workspace=args.workspace/'discovery')
    store.put(pin)
    durable=store.get(args.cycle_id,'census_nrc'); assert durable == pin
    normal=candidate(pin=durable,paths=paths,output=args.workspace/'candidate-normal',cycle_id=args.cycle_id)
    recovered=recover_pinned_workbooks(durable,args.workspace/'replay-input')
    replay=candidate(pin=durable,paths=recovered,output=args.workspace/'candidate-replay',cycle_id=args.cycle_id)
    frame=pd.read_parquet(args.workspace/'candidate-normal/data.parquet')
    summary={'schema_version':'nrc_c_local_live_proof_v1','cycle_id':args.cycle_id,
        'pin_id':pin['pin_id'],'provider_release_id':pin['provider_release_id'],
        'members':{k:{field:value for field,value in member.items() if field in {'url','sha256','size_bytes'}} for k,member in pin['members'].items()},
        'artifact_id':normal['manifest']['artifact_id'],'replay_artifact_id':replay['manifest']['artifact_id'],
        'replay_identity_match':normal['manifest']['artifact_id']==replay['manifest']['artifact_id'],
        'row_count':len(frame),'observation_min':str(frame.date.min()),'observation_max':str(frame.date.max()),
        'series':frame.groupby(['metric_id','geo_id']).agg(count=('value','size'),first=('date','min'),last=('date','max')).reset_index().astype(str).to_dict('records')}
    write_canonical_json(args.workspace/'summary.json',summary); print(json.dumps(summary,indent=2,sort_keys=True))
    return 0
if __name__ == '__main__': raise SystemExit(main())
