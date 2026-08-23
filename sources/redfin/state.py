from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import duckdb, pandas as pd
from core.source_artifacts.artifact import canonicalize, create_artifact
from core.source_artifacts.models import CANONICAL_KEY, LINEAGE_COLUMNS
from core.source_artifacts.hashing import sha256_json
from .governance import BASELINE_ID, RAW_ROOT
from .ingest import build_baseline_contribution, build_drop_contribution
from .storage import read_json

STATE_SCHEMA="""CREATE TABLE IF NOT EXISTS canonical_redfin(
 geo_id VARCHAR, metric_id VARCHAR, date DATE, property_type_id VARCHAR, value DOUBLE,
 property_type VARCHAR, latest_source_vintage VARCHAR, latest_source_drop_id VARCHAR,
 latest_source_hash_or_artifact_identity VARCHAR, source_request_identity VARCHAR,
 source_artifact_id VARCHAR, promoted_at TIMESTAMP,
 UNIQUE(geo_id,metric_id,date,property_type_id))"""

def bootstrap_state(db: Path, baseline: pd.DataFrame, *, baseline_hash: str, promoted_at: str="2026-07-31T00:00:00Z") -> int:
    data=canonicalize(baseline); db.parent.mkdir(parents=True,exist_ok=True); con=duckdb.connect(str(db)); con.execute(STATE_SCHEMA)
    if con.execute("select count(*) from canonical_redfin").fetchone()[0]: con.close(); return 0
    con.register("incoming",data); con.execute("INSERT INTO canonical_redfin SELECT geo_id,metric_id,date,property_type_id,value,property_type,'2026-07','2026-07',?,'baseline:2026-07','baseline:2026-07',CAST(? AS TIMESTAMP) FROM incoming",[baseline_hash,promoted_at]); n=len(data); con.close(); return n

def reconcile_state(db: Path, contribution: pd.DataFrame, *, drop_id: str, source_hash: str, request_identity: str, fail_after_upsert: bool=False) -> int:
    data=canonicalize(contribution); con=duckdb.connect(str(db)); con.execute(STATE_SCHEMA); con.register("incoming",data)
    try:
        con.execute("BEGIN")
        con.execute("DELETE FROM canonical_redfin USING incoming WHERE canonical_redfin.geo_id=incoming.geo_id AND canonical_redfin.metric_id=incoming.metric_id AND canonical_redfin.date=incoming.date AND canonical_redfin.property_type_id=incoming.property_type_id")
        con.execute("INSERT INTO canonical_redfin SELECT geo_id,metric_id,date,property_type_id,value,property_type,?,?,?,?,'pending',current_timestamp FROM incoming",[drop_id,drop_id,source_hash,request_identity])
        if fail_after_upsert: raise RuntimeError("forced reconciliation failure")
        if con.execute("SELECT count(*) FROM (SELECT geo_id,metric_id,date,property_type_id,count(*) n FROM canonical_redfin GROUP BY ALL HAVING n>1)").fetchone()[0]: raise RuntimeError("duplicate state key")
        con.execute("COMMIT")
    except Exception: con.execute("ROLLBACK"); con.close(); raise
    con.close(); return len(data)


def bootstrap_from_governed_baseline(
    db: Path,
    *,
    root: Path = RAW_ROOT,
    geo_manifest: Path = Path("config/geo_manifest.generated.csv"),
    manifest_path: Path = Path("config/redfin_baseline_manifest.json"),
) -> int:
    """Bootstrap state through ingest.py's authoritative baseline extraction."""
    contribution = build_baseline_contribution(root, geo_manifest, manifest_path)
    manifest = read_json(manifest_path)
    baseline_hash = sha256_json({"baseline_id": BASELINE_ID, "files": manifest["files"]})
    return bootstrap_state(db, contribution, baseline_hash=baseline_hash)


def reconcile_governed_drop(
    db: Path,
    drop_id: str,
    *,
    root: Path = RAW_ROOT,
    geo_manifest: Path = Path("config/geo_manifest.generated.csv"),
) -> int:
    """Reconcile only keys actually present in one validated provider drop."""
    contribution = build_drop_contribution(drop_id, root, geo_manifest)
    metadata = read_json(root / "drops" / drop_id / "metadata.json")
    source_hash = sha256_json(metadata["files"])
    request_identity = "redfin-drop:" + sha256_json({"drop_id": drop_id, "files": metadata["files"]})
    return reconcile_state(
        db,
        contribution,
        drop_id=drop_id,
        source_hash=source_hash,
        request_identity=request_identity,
    )

def emit_artifact(db: Path, output: Path, *, target_month: str, retrieved_at: str, git_sha: str="unknown", max_single_asset_bytes: int|None=None) -> dict:
    con=duckdb.connect(str(db),read_only=True); state=con.execute("select * from canonical_redfin order by geo_id,metric_id,date,property_type_id").df(); con.close()
    if state.empty or state.latest_source_vintage.max()!=target_month: raise ValueError("state latest governed vintage does not equal target month")
    data=state.assign(source_id="redfin")[["geo_id","metric_id","date","property_type_id","value","source_id","property_type"]]
    lineage=state.rename(columns={"latest_source_vintage":"provider_vintage","latest_source_drop_id":"provider_release_id","latest_source_hash_or_artifact_identity":"latest_source_hash_or_drop_id"})
    lineage=lineage[LINEAGE_COLUMNS]
    return create_artifact(output,data,source_id="redfin",source_family="Redfin monthly market data",source_type="rolling_full_snapshot_manual",provider="Redfin",distribution_channel="manual export",provider_release_id=target_month,provider_release_timestamp_or_date=target_month+"-01",retrieved_at=retrieved_at,target_month=target_month,source_request_identity=f"redfin-state:{target_month}",source_urls_or_endpoint_identity=["provider-export:redfin:seven-family"],lineage=lineage,git_sha=git_sha,max_single_asset_bytes=max_single_asset_bytes)
