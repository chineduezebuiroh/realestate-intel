"""Smoke 173: offline one-command Redfin candidate boundary and state isolation."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd

from core.source_artifacts.package import build_publication_package
from jobs.monthly_refresh.production import validate_source_result
from jobs.monthly_refresh.redfin import run
from sources.redfin.governance import FAMILIES, GovernanceError, bootstrap
from sources.redfin.state import STATE_SCHEMA


def raw(path: Path, month: str) -> None:
    row={"period_end":month+"-28","region_id":"1","average_sale_to_list_ratio":100,
         "homes_sold":2,"inventory":3,"median_days_on_market_days":4,"median_sale_price_nsa":5,
         "median_sale_price_per_sqft":6,"months_of_supply":7,"new_listings":8,"pending_sales":9,
         "percent_off_market_in_two_weeks":10,"share_sold_above_original_list":11}
    pd.DataFrame([row]).to_csv(path,index=False)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


with TemporaryDirectory() as td:
    root=Path(td); raw_root=root/"raw"; bootstrap(raw_root)
    state=root/"accepted.duckdb"; con=duckdb.connect(str(state)); con.execute(STATE_SCHEMA)
    # A prior-only key proves preserve-prior behavior and lineage survival.
    con.execute("INSERT INTO canonical_redfin VALUES ('prior','inventory','2026-07-31','all',1,'all','2026-07','2026-07',?,'baseline','prior-artifact','2026-07-31')",["a"*64]); con.close()
    accepted_before=digest(state)
    prior_id="src__redfin__2026-07__r1__b10214595868c2ff"
    catalog={"accepted":{"source":{"redfin":prior_id}},"immutable_records":[{
      "object_type":"source","object_id":prior_id,"metadata":{"source_id":"redfin","data_sha256":"0"*64}}]}
    calls=[]
    def publisher(artifact, workspace):
        workspace.mkdir(parents=True,exist_ok=True); manifest=json.loads((artifact/"manifest.json").read_text())
        package=workspace/(manifest["artifact_id"]+".tar"); info=build_publication_package(artifact,package)
        calls.append(manifest["artifact_id"])
        return {"package_sha256":info["package_sha256"],"publication_state":"published_verified",
                "catalog_changed":len(calls)==1,"accepted_pointer_changed":False,
                "receipt":{"release_id":1,"asset_id":2}}
    empty=run(accepted_state=state,raw_root=raw_root,workspace_root=root/"candidates",
      ledger_path=root/"ledger.json",evidence_root=root/"evidence",catalog=catalog,publisher=publisher)
    assert empty["status"]=="not_ready"
    names={"nation":"country","state":"states","metro":"metros","county":"counties",
           "city":"cities","neighborhood":"neighborhoods","zip":"zips"}
    for family in FAMILIES: raw(raw_root/"incoming"/f"redfin_{names[family]}.csv","2026-08")
    first=run(accepted_state=state,raw_root=raw_root,workspace_root=root/"candidates",
      ledger_path=root/"ledger.json",evidence_root=root/"evidence",catalog=catalog,publisher=publisher,
      repository_root=Path("."),git_sha="fixture")
    validate_source_result(first,expected_cycle_id=first["cycle_id"])
    assert digest(state)==accepted_before and first["prior_artifact_id"]==prior_id
    candidate=next((root/"candidates").glob("*/candidate_redfin.duckdb"))
    check=duckdb.connect(str(candidate),read_only=True)
    assert check.execute("select count(*) from canonical_redfin where geo_id='prior'").fetchone()[0]==1
    assert check.execute("select count(*) from canonical_redfin where latest_source_vintage='2026-08'").fetchone()[0]>0
    check.close()
    second=run(accepted_state=state,raw_root=raw_root,workspace_root=root/"candidates",
      ledger_path=root/"ledger.json",evidence_root=root/"evidence",catalog=catalog,publisher=publisher,
      repository_root=Path("."),git_sha="fixture")
    assert second["cycle_id"]==first["cycle_id"] and second["candidate_artifact_id"]==first["candidate_artifact_id"]
    assert digest(state)==accepted_before
    ledger=json.loads((root/"ledger.json").read_text()); assert len(ledger["cycles"])==1
    assert next(iter(ledger["cycles"].values()))["state"]=="candidate_ready"
    assert catalog["accepted"]["source"]["redfin"]==prior_id
    # Routine bootstrap is forbidden and auth failures are actionable before any remote work.
    for family in FAMILIES: raw(raw_root/"incoming"/f"redfin_{names[family]}.csv","2026-09")
    try:
        run(accepted_state=root/"missing.duckdb",raw_root=raw_root,workspace_root=root/"other",
          ledger_path=root/"other-ledger.json",evidence_root=root/"other-evidence",catalog=catalog,publisher=publisher)
    except GovernanceError as exc: assert "never bootstrap" in str(exc)
    else: raise AssertionError("missing accepted state was bootstrapped")

print("Smoke 173 Redfin monthly source runner passed")
