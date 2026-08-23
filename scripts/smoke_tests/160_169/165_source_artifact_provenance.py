"""Smoke 165: precise source-artifact provenance and semantic identity."""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pandas as pd

from core.source_artifacts import create_artifact, validate_artifact
from sources.fred_macro.artifact import produce
from sources.redfin.state import GOVERNED_CONFIG_PATHS, bootstrap_state, emit_artifact, governed_config_hashes, reconcile_state


def fact(source: str) -> pd.DataFrame:
    return pd.DataFrame([{"geo_id":"us","metric_id":"metric","date":"2026-07-31","property_type_id":"all","value":1.0,"source_id":source,"property_type":"all"}])


with tempfile.TemporaryDirectory() as td:
    root=Path(td); repo=root/"repo"
    for relative in GOVERNED_CONFIG_PATHS:
        destination=repo/relative; destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(relative,destination)
    hashes=governed_config_hashes(repo)
    assert list(hashes)==sorted(GOVERNED_CONFIG_PATHS)
    assert all(not Path(key).is_absolute() and len(value)==64 for key,value in hashes.items())
    missing=repo/GOVERNED_CONFIG_PATHS[0]; missing.unlink()
    try: governed_config_hashes(repo)
    except FileNotFoundError: pass
    else: raise AssertionError("missing governed config did not fail closed")
    shutil.copy2(GOVERNED_CONFIG_PATHS[0],missing)

    db=root/"state.duckdb"; bootstrap_state(db,fact("redfin"),baseline_hash="a"*64)
    first=emit_artifact(db,root/"first",target_month="2026-07",repository_root=repo,artifact_created_at="2026-08-01T00:00:00Z")
    assert first["provider_release_timestamp_or_date"] is None
    assert first["retrieved_at"] is None and first["acquisition_time_status"]=="historical_not_recorded"
    assert first["artifact_created_at"]!="2026-07-31T00:00:00Z"
    assert first["raw_source_lineage"]["kind"]=="immutable_governed_baseline"
    assert validate_artifact(root/"first")["status"]=="passed"
    second=emit_artifact(db,root/"second",target_month="2026-07",repository_root=repo,artifact_created_at="2026-08-02T00:00:00Z")
    assert first["artifact_id"]==second["artifact_id"] and first["artifact_content_hash"]==second["artifact_content_hash"]
    assert first["data_sha256"]==second["data_sha256"]

    config=repo/GOVERNED_CONFIG_PATHS[0]; config.write_bytes(config.read_bytes()+b"\n")
    changed=emit_artifact(db,root/"changed",target_month="2026-07",repository_root=repo,artifact_created_at="2026-08-01T00:00:00Z")
    assert changed["data_sha256"]==first["data_sha256"] and changed["artifact_id"]!=first["artifact_id"]

    registered=emit_artifact(db,root/"registered",target_month="2026-07",repository_root=repo,registered_at="2026-08-03T00:00:00Z",artifact_created_at="2026-08-04T00:00:00Z")
    # A historical baseline stays historical even if an unrelated registration timestamp is supplied.
    assert registered["acquisition_time_status"]=="historical_not_recorded"

    reconcile_state(db,fact("redfin").assign(date="2026-08-31"),drop_id="2026-08",source_hash="b"*64,request_identity="fixture")
    drop=root/"raw/drops/2026-08"; drop.mkdir(parents=True)
    (drop/"metadata.json").write_text('{"registered_at":"2026-09-02T03:04:05Z","files":[{"filename":"fixture.csv","sha256":"'+'b'*64+'"}]}')
    future=emit_artifact(db,root/"future",target_month="2026-08",repository_root=repo,raw_root=root/"raw",artifact_created_at="2026-09-03T00:00:00Z")
    assert future["retrieved_at"] is None and future["registered_at"]=="2026-09-02T03:04:05Z"
    assert future["acquisition_time_status"]=="registration_time_only"
    assert future["raw_source_lineage"]["kind"]=="governed_registered_drop"
    assert validate_artifact(root/"future")["status"]=="passed"

    fred=produce(root/"fred",fact("fred_macro"),target_month="2026-07",provider_release_id="ordinary-current:fixture",retrieved_at="2026-08-03T00:00:00Z",artifact_created_at="2026-08-04T00:00:00Z")
    assert fred["provider_release_timestamp_or_date"] is None
    assert fred["retrieved_at"]!=fred["artifact_created_at"]
    assert validate_artifact(root/"fred")["status"]=="passed"

print("source artifact provenance smoke: ok")
