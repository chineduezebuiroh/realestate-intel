from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .governance import BASELINE_ID, FAMILY_FILENAME_TOKENS, FAMILY_LEVELS, FAMILIES, METRICS, RAW_ROOT, GovernanceError, bootstrap, load_baseline_manifest
from .storage import atomic_json, current, raw_files, read_json, sha256
from .validate import governed_geographies, read_raw, validate_baseline, validate_drop


def infer_family(name: str) -> str:
    lowered = name.lower()
    matches = [family for family, tokens in FAMILY_FILENAME_TOKENS.items() if any(token in lowered for token in tokens)]
    if len(matches) != 1: raise GovernanceError(f"expected exactly one geography-family filename token in {name}; matched {matches}")
    return matches[0]


def register_drop(drop_id: str, root: Path = RAW_ROOT) -> dict:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", drop_id): raise GovernanceError("drop ID must be YYYY-MM")
    bootstrap(root); folder = root / "drops" / drop_id; files = raw_files(folder) if folder.is_dir() else []
    if not files: raise GovernanceError(f"no untouched raw files in {folder}")
    records = [{"filename": p.name, "sha256": sha256(p), "size_bytes": p.stat().st_size, "geography_family": infer_family(p.name)} for p in files]
    if len(records) != 7 or {r["geography_family"] for r in records} != set(FAMILIES): raise GovernanceError("registration requires exactly one file for each of seven geography families")
    path = folder / "metadata.json"
    if path.exists():
        existing = read_json(path)
        if existing["files"] != records: raise GovernanceError(f"conflicting hashes for registered month {drop_id}")
        return existing
    payload = {"drop_id":drop_id,"registered_at":datetime.now(timezone.utc).isoformat(),"files":records,"status":"registered","validation_status":"pending","promotion_status":"not_promoted","publication_status":"not_published"}
    atomic_json(path,payload); return payload


def active_family_mappings(geo_manifest: Path) -> dict[str, pd.DataFrame]:
    governed = governed_geographies(geo_manifest); mappings = {}
    for family, levels in FAMILY_LEVELS.items():
        scoped = governed[governed.level.str.strip().str.lower().isin(levels)].copy()
        if not scoped.empty: mappings[family] = scoped[["geo_id", "redfin_code", "level"]]
    return mappings


def resolve_sources(drop_id: str | None, root: Path = RAW_ROOT, manifest_path: Path = Path("config/redfin_baseline_manifest.json")) -> list[tuple[Path,int,str]]:
    validate_baseline(root, manifest_path); manifest = load_baseline_manifest(manifest_path)
    paths=[(root/"baseline"/BASELINE_ID/item["filename"],1,item["geography_family"]) for item in manifest["files"]]
    if drop_id and drop_id != BASELINE_ID:
        meta=read_json(root/"drops"/drop_id/"metadata.json")
        if meta.get("status") not in {"validated","candidate_built","candidate_validated","serving_refreshed","published","promoted"}: raise GovernanceError("drop is not validated")
        paths += [(root/"drops"/drop_id/item["filename"],2,item["geography_family"]) for item in meta["files"]]
    return paths


def merge_precedence(frames: list[pd.DataFrame]) -> pd.DataFrame:
    keys=["geo_id","metric_id","date","property_type_id"]
    combined=pd.concat(frames,ignore_index=True)
    return combined.sort_values(keys+["_priority"]).drop_duplicates(keys,keep="last").sort_values(keys).reset_index(drop=True)


def _source_to_long(path: Path, priority: int, family: str, mapping: pd.DataFrame) -> pd.DataFrame:
    frame=read_raw(path)
    date_col="period_end" if "period_end" in frame else "period_begin" if "period_begin" in frame else None
    if not date_col: raise GovernanceError(f"missing period column: {path.name}")
    frame["date"]=pd.to_datetime(frame[date_col],errors="raise").dt.to_period("M").dt.to_timestamp("M")
    if "region_id" in frame and "table_id" in frame: frame["join_id"]=frame.region_id.where(frame.region_id.notna(),frame.table_id)
    elif "region_id" in frame: frame["join_id"]=frame.region_id
    elif "table_id" in frame: frame["join_id"]=frame.table_id
    else: raise GovernanceError(f"missing canonical geography identifier: {path.name}")
    frame["join_id"]=frame.join_id.astype(str).str.replace(r"\.0$","",regex=True); mapping=mapping.copy(); mapping["redfin_code"]=mapping.redfin_code.astype(str).str.replace(r"\.0$","",regex=True)
    frame=frame.merge(mapping,left_on="join_id",right_on="redfin_code",how="inner",validate="many_to_one")
    if "is_seasonally_adjusted" in frame:
        flag=frame.is_seasonally_adjusted.astype(str).str.lower().str.strip(); frame=frame[flag.isin({"false","0","no","n","nan","none",""})]
    if "property_type_id" in frame:
        prop=frame.property_type_id.astype(str).str.lower().str.strip(); frame=frame[prop.isin({"all","-1","-1.0","nan","none",""})]
    elif "property_type" in frame:
        prop=frame.property_type.astype(str).str.lower().str.strip(); frame=frame[prop.isin({"all","all residential","nan","none",""})]
    if "inventory" not in frame:
        if "active_listings" not in frame: raise GovernanceError(f"missing inventory fallback in {path.name}")
        frame["inventory"]=frame.active_listings
    missing=METRICS-set(frame.columns)
    if missing: raise GovernanceError(f"missing governed candidate metrics in {path.name}: {sorted(missing)}")
    long=frame.melt(id_vars=["geo_id","date"],value_vars=sorted(METRICS),var_name="metric_id",value_name="value")
    long["value"]=pd.to_numeric(long.value,errors="raise"); long=long.dropna(subset=["value"])
    long["property_type_id"]=long["property_type"]="all"; long["geography_family"]=family; long["_priority"]=priority
    return long


def build_candidate(drop_id: str, output: Path, root: Path = RAW_ROOT, geo_manifest: Path = Path("config/geo_manifest.generated.csv"), manifest_path: Path = Path("config/redfin_baseline_manifest.json")) -> dict:
    if drop_id == BASELINE_ID: validate_baseline(root,manifest_path)
    else: validate_drop(drop_id,root)
    mappings=active_family_mappings(geo_manifest)
    if not mappings: raise GovernanceError("no governed Redfin geographies")
    frames=[]; loaded=[]; skipped=[]
    for path,priority,family in resolve_sources(drop_id,root,manifest_path):
        if family not in mappings: skipped.append({"filename":path.name,"family":family}); continue
        frames.append(_source_to_long(path,priority,family,mappings[family])); loaded.append(path.name)
    if not frames: raise GovernanceError("no governed Redfin source family loaded")
    candidate=merge_precedence(frames); keys=["geo_id","metric_id","date","property_type_id"]
    if candidate.duplicated(keys).any(): raise GovernanceError("duplicate canonical keys")
    output.parent.mkdir(parents=True,exist_ok=True); candidate.drop(columns="_priority").to_parquet(output,index=False)
    meta_path=root/"drops"/drop_id/"metadata.json" if drop_id != BASELINE_ID else root/"baseline"/BASELINE_ID/"candidate_metadata.json"
    meta=read_json(meta_path) if meta_path.exists() else {"baseline_id":BASELINE_ID}
    meta.update(status="candidate_built",candidate_path=str(output),candidate_rows=len(candidate),governed_geographies=sorted(candidate.geo_id.unique()),loaded_files=loaded,skipped_ungoverned_files=skipped,latest_month=drop_id)
    atomic_json(meta_path,meta); return meta


def monthly_gate(drop_id: str, root: Path = RAW_ROOT) -> str:
    return "registered" if (root/"drops"/drop_id/"metadata.json").exists() else "waiting_for_manual_redfin"


def main() -> int:
    state=current() or {}; drop=state.get("promoted_drop")
    if not drop: print("waiting_for_manual_redfin"); return 0
    build_candidate(drop,Path("data/redfin/redfin_candidate.parquet")); return 0

if __name__ == "__main__": raise SystemExit(main())
