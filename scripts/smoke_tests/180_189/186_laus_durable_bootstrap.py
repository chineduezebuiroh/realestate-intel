"""Smoke 186: offline common artifact lifecycle for governed LAUS bootstrap."""
from __future__ import annotations
import copy
import tempfile
from pathlib import Path
import pandas as pd

from core.source_artifacts.catalog import add_record, empty_catalog
from core.source_artifacts.fixture_remote import CatalogPackageResolver, OfflineArtifactPublisher
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.laus_bootstrap import (
    activate_catalog, catalog_record, create_bootstrap_artifact, publication_metadata,
    publication_preconditions,
)
from sources.bls_laus.artifact import build_request_plan


def expect(error,fn):
    try: fn()
    except error: return
    raise AssertionError(f"expected {error.__name__}")


def main():
    plan=build_request_plan(acquisition_mode="bootstrap",end_year=2026,config_hashes={"fixture":"0"*64})
    rows=[]
    for item in plan["series"]:
        rows.append({"geo_id":item["geo_id"],"metric_id":item["metric_id"],"date":pd.Timestamp("2026-05-31").date(),
                     "property_type_id":"all","value":100.,"source_id":"laus","property_type":"all"})
    frame=pd.DataFrame(rows).sort_values(["geo_id","metric_id","date","property_type_id"])
    diagnostics={"target_month":"2026-05","provider_release_id":"laus-bootstrap-current:"+"1"*64}
    with tempfile.TemporaryDirectory() as td:
        root=Path(td); stamp="2026-08-30T00:00:00Z"
        first=create_bootstrap_artifact(root/"artifact",frame,plan,diagnostics,retrieved_at=stamp,artifact_created_at=stamp)
        second=create_bootstrap_artifact(root/"artifact2",frame,plan,diagnostics,retrieved_at=stamp,artifact_created_at=stamp)
        assert first["artifact_id"]==second["artifact_id"] and first["data_sha256"]==second["data_sha256"]
        (root/"acceptance.json").write_text('{"status":"passed"}')
        expect(RuntimeError,lambda:publication_preconditions(root,remote_inventory_complete=False))
        assert all(publication_preconditions(root,remote_inventory_complete=True).values())
        package_path=root/"artifact.tar"; package=build_publication_package(root/"artifact",package_path)
        metadata=publication_metadata(root/"artifact",package); metadata.update(remote_backend="fixture",remote_repository="owner/repo",
          release_tag="source-artifact/laus/"+first["artifact_id"],release_id=30,asset_id=40,
          asset_filename=first["artifact_id"]+".tar",published_at=stamp,verified_at="2026-08-30T00:00:01Z")
        uri=first["artifact_uri"]
        interrupted=OfflineArtifactPublisher(fail_upload=True); interrupted.prepare(uri,package_path.read_bytes(),metadata)
        expect(PublicationError,lambda:interrupted.upload(uri))
        assert empty_catalog()["immutable_records"]==[]
        publisher=OfflineArtifactPublisher(); publisher.prepare(uri,package_path.read_bytes(),metadata); publisher.upload(uri); publisher.verify(uri)
        receipt=publisher.finalize(uri); record=catalog_record(first,receipt); catalog=add_record(empty_catalog(),record,receipt)
        assert add_record(catalog,record,receipt)==catalog and "laus" not in catalog["accepted"]["source"]
        active_clean=activate_catalog(catalog,first["artifact_id"])
        resolver=CatalogPackageResolver(active_clean,{40:package_path},{receipt["receipt_id"]:receipt},root/"fresh")
        resolved=resolver.resolve(uri); assert (resolved/"manifest.json").is_file() and resolver.resolve(uri)==resolved
        protected={"redfin":"r","fred_macro":"f","ces":"c"}; catalog["accepted"]["source"].update(protected)
        active=activate_catalog(catalog,first["artifact_id"]); assert active["accepted"]["source"]["laus"]==first["artifact_id"]
        assert {key:active["accepted"]["source"][key] for key in protected}==protected
        assert activate_catalog(active,first["artifact_id"])["accepted"]==active["accepted"]
        collision=OfflineArtifactPublisher(); collision.prepare(uri,package_path.read_bytes(),metadata)
        expect(IdentityCollisionError,lambda:collision.prepare(uri,package_path.read_bytes()+b"x",metadata))
        wrong=copy.deepcopy(catalog); wrong["accepted"]["source"]["laus"]="other"
        expect(RuntimeError,lambda:activate_catalog(wrong,first["artifact_id"]))
    print("Smoke 186 passed: LAUS uses the common durable lifecycle fail closed.")

if __name__=="__main__": main()
