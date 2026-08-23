from __future__ import annotations
import shutil, tempfile
from pathlib import Path
from .governance import FAMILIES, GovernanceError, RAW_ROOT, bootstrap
from .ingest import infer_family, register_drop
from .storage import raw_files, read_json, sha256
from .validate import latest_observation_month

def _latest_month(path: Path) -> str:
    return latest_observation_month(path)

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
        old={x["geography_family"]:x["sha256"] for x in read_json(metadata_path).get("files",[]) if "geography_family" in x}
        if old==hashes:
            if clear_incoming:
                for path in files: path.unlink()
            return {"status":"already_registered","drop_id":drop_id,"path":str(destination),"hashes":hashes}
        raise GovernanceError("conflicting_drop")
    destination.parent.mkdir(parents=True,exist_ok=True)
    staging=Path(tempfile.mkdtemp(prefix=f".{drop_id}-",dir=destination.parent))
    installed = False
    try:
        for family,path in sorted(families.items()):
            copied=staging/path.name; shutil.copy2(path,copied)
            if sha256(copied)!=hashes[family]: raise GovernanceError("copy verification failed")
        staging.replace(destination)
        installed = True
        # The managed inbox is only a landing mechanism. Delegate durable
        # metadata creation to the same governed v2 registration primitive as
        # manually staged drops.
        metadata = register_drop(drop_id, root)
    except Exception:
        shutil.rmtree(staging,ignore_errors=True)
        if installed:
            shutil.rmtree(destination, ignore_errors=True)
        raise
    if clear_incoming:
        for path in files: path.unlink()
    return {"status":"registered","drop_id":drop_id,"path":str(destination),"hashes":hashes,"metadata":metadata}
