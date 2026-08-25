"""Smoke 173: offline one-command Redfin candidate boundary and state isolation."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import duckdb
import pandas as pd

from core.source_artifacts.package import build_publication_package
from jobs.monthly_refresh.production import validate_source_result
from jobs.monthly_refresh.redfin import (JULY_ARTIFACT_ID, JULY_DATA_SHA256,
                                         bootstrap_accepted, run)
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


class BootstrapCatalogFixture:
    def __init__(self) -> None:
        self.activation_calls = 0

    def activate_source(self, source_id: str, artifact_id: str):
        assert source_id == "redfin" and artifact_id == JULY_ARTIFACT_ID
        self.activation_calls += 1
        return {}, False


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
    assert second["artifact_content_hash"]==first["artifact_content_hash"]
    assert second["package_sha256"]==first["package_sha256"]
    manifest=json.loads(next((root/"candidates").glob("*/artifact/manifest.json")).read_text())
    assert manifest["git_sha"]=="operational-evidence-excluded"
    assert len(calls)==2 and calls[0]==calls[1]
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

    # The explicit bootstrap compares parquet directly through the read-only
    # accepted-state connection. Publication and catalog boundaries stay fake.
    bootstrap_artifact=root/"bootstrap-artifact"; bootstrap_artifact.mkdir()
    bootstrap_row={"geo_id":"fixture","metric_id":"inventory","date":pd.Timestamp("2026-07-31"),
      "property_type_id":"all","value":42.0,"source_id":"redfin","property_type":"all"}
    pd.DataFrame([bootstrap_row]).to_parquet(bootstrap_artifact/"data.parquet",index=False)
    bootstrap_state=root/"bootstrap-accepted.duckdb"; con=duckdb.connect(str(bootstrap_state)); con.execute(STATE_SCHEMA)
    con.execute("INSERT INTO canonical_redfin VALUES ('fixture','inventory','2026-07-31','all',42,'all','2026-07','2026-07',?,'baseline','prior-artifact','2026-07-31')",["b"*64]); con.close()
    bootstrap_before=digest(bootstrap_state); bootstrap_manifest={"artifact_id":JULY_ARTIFACT_ID,
      "data_sha256":JULY_DATA_SHA256,"target_month":"2026-07"}
    catalog_fixture=BootstrapCatalogFixture(); publication_calls=[]
    def bootstrap_publisher(*args, **kwargs):
        publication_calls.append(args)
        return {"publication_state":"published_verified","accepted_pointer_changed":False}
    with patch("jobs.monthly_refresh.redfin.validate_artifact",
               return_value={"manifest":bootstrap_manifest}), \
         patch("jobs.monthly_refresh.redfin.publish_candidate",side_effect=bootstrap_publisher):
        bootstrapped=bootstrap_accepted(artifact=bootstrap_artifact,accepted_state=bootstrap_state,
          workspace=root/"bootstrap-publication",api=object(),cas=catalog_fixture,git_sha="fixture")
        assert bootstrapped["bootstrap_operation"] and len(publication_calls)==1
        assert catalog_fixture.activation_calls==1 and not bootstrapped["accepted_pointer_changed"]
        assert digest(bootstrap_state)==bootstrap_before

        changed=dict(bootstrap_row); changed["value"]=43.0
        pd.DataFrame([changed]).to_parquet(bootstrap_artifact/"data.parquet",index=False)
        try:
            bootstrap_accepted(artifact=bootstrap_artifact,accepted_state=bootstrap_state,
              workspace=root/"rejected-publication",api=object(),cas=catalog_fixture,git_sha="fixture")
        except GovernanceError as exc: assert "does not reproduce" in str(exc)
        else: raise AssertionError("mismatching bootstrap artifact passed parity validation")
    assert len(publication_calls)==1 and catalog_fixture.activation_calls==1
    assert digest(bootstrap_state)==bootstrap_before

print("Smoke 173 Redfin monthly source runner passed")
