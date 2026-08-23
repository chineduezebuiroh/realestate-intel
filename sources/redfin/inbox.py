from __future__ import annotations
import shutil, tempfile
from pathlib import Path
import pandas as pd
from .governance import FAMILIES, GovernanceError, RAW_ROOT, bootstrap
from .ingest import infer_family
from .storage import atomic_json, raw_files, sha256

def _latest_month(path: Path) -> str:
    try:
        frame=pd.read_csv(path,sep=None,engine="python",usecols=lambda c:c.lower() in {"period_end","date","month"})
        column=next(c for c in frame if c.lower() in {"period_end","date","month"})
        values=pd.to_datetime(frame[column],errors="raise")
    except Exception as exc: raise GovernanceError(f"cannot inspect endpoint: {path.name}") from exc
    if values.empty: raise GovernanceError(f"empty incoming file: {path.name}")
    return values.max().strftime("%Y-%m")

def register_incoming(root: Path=RAW_ROOT, *, clear_incoming: bool=True) -> dict:
    bootstrap(root); incoming=root/"incoming"; files=raw_files(incoming)
    families={}
    for path in files:
        family=infer_family(path.name)
        if family in families: raise GovernanceError(f"duplicate family: {family}")
        families[family]=path
    if set(families)!=set(FAMILIES): raise GovernanceError(f"missing families: {sorted(set(FAMILIES)-set(families))}")
    months={family:_latest_month(path) for family,path in families.items()}
    if len(set(months.values()))!=1: raise GovernanceError(f"mixed latest months: {months}")
    drop_id=next(iter(months.values())); hashes={family:sha256(path) for family,path in families.items()}; destination=root/"drops"/drop_id
    if destination.exists():
        metadata_path=destination/"metadata.json"
        if not metadata_path.exists(): raise GovernanceError("conflicting_drop")
        old={x["family"]:x["sha256"] for x in __import__("json").loads(metadata_path.read_text()).get("files",[]) if "family" in x}
        if old==hashes: return {"status":"already_registered","drop_id":drop_id}
        raise GovernanceError("conflicting_drop")
    destination.parent.mkdir(parents=True,exist_ok=True)
    staging=Path(tempfile.mkdtemp(prefix=f".{drop_id}-",dir=destination.parent))
    try:
        records=[]
        for family,path in sorted(families.items()):
            copied=staging/path.name; shutil.copy2(path,copied)
            if sha256(copied)!=hashes[family]: raise GovernanceError("copy verification failed")
            records.append({"family":family,"filename":path.name,"sha256":hashes[family]})
        atomic_json(staging/"metadata.json",{"drop_id":drop_id,"latest_month":drop_id,"status":"registered","files":records})
        staging.replace(destination)
    except Exception:
        shutil.rmtree(staging,ignore_errors=True); raise
    if clear_incoming:
        for path in files: path.unlink()
    return {"status":"registered","drop_id":drop_id,"path":str(destination),"hashes":hashes}
