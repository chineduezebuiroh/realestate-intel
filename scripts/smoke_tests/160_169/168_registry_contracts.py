"""Offline deterministic package, publication, catalog, and resolver smoke."""
from __future__ import annotations
import copy, io, json, os, tarfile, tempfile
from pathlib import Path
import pandas as pd

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.catalog import add_record, empty_catalog, validate_catalog
from core.source_artifacts.fixture_remote import CatalogPackageResolver, OfflineArtifactPublisher
from core.source_artifacts.hashing import sha256_file
from core.source_artifacts.package import build_publication_package, extract_publication_package
from core.source_artifacts.publication import (IdentityCollisionError, PublicationError, create_receipt,
    receipt_identity, transition, validate_receipt)
from core.source_artifacts.validation import ArtifactValidationError

SHA="a"*64

def expect(exc, fn):
    try: fn()
    except exc: return
    raise AssertionError(f"expected {exc.__name__}")

def artifact(root: Path) -> tuple[Path,dict]:
    frame=pd.DataFrame([{"geo_id":"US","metric_id":"m","date":"2026-08-31","property_type_id":"all","value":1.0,"source_id":"fixture","property_type":"all"}])
    path=root/"artifact"
    manifest=create_artifact(path,frame,source_id="fixture",source_family="Fixture",source_type="fixture",provider="Fixture",distribution_channel="offline",provider_release_id="p1",provider_release_timestamp_or_date="2026-08-31",retrieved_at="2026-09-01T00:00:00Z",target_month="2026-08",source_request_identity="fixture",source_urls_or_endpoint_identity=["fixture"])
    return path,manifest

def receipt_values(manifest, package_sha, *, state="published_immutable_verified", asset_id=22):
    return dict(logical_artifact_uri=manifest["artifact_uri"],object_id=manifest["artifact_id"],object_type="source",
        object_metadata={"source_id":"fixture"},
        artifact_content_hash=manifest["artifact_content_hash"],package_sha256=package_sha,
        member_hashes={"data.parquet":manifest["data_sha256"]},remote_backend="offline_fixture",
        remote_repository="fixture/repo",release_tag="source-artifact/fixture/"+manifest["artifact_id"],release_id=11,
        asset_id=asset_id,asset_filename=manifest["artifact_id"]+".tar",published_at="2026-09-01T01:00:00Z",
        verified_at="2026-09-01T01:01:00Z",publication_state=state,publisher_git_sha="fixture-sha",
        contract_versions=["source_artifact_contract_v1"])

def record(manifest, package_sha, receipt):
    return {"object_type":"source","object_id":manifest["artifact_id"],"logical_artifact_uri":manifest["artifact_uri"],
        "remote_repository":"fixture/repo","release_tag":receipt["release_tag"],"release_id":11,"asset_id":22,
        "asset_filename":receipt["asset_filename"],"package_sha256":package_sha,"artifact_content_hash":manifest["artifact_content_hash"],
        "publication_receipt_id":receipt["receipt_id"],"publication_state":"published_immutable_verified",
        "metadata":{"source_id":"fixture","data_sha256":manifest["data_sha256"],"provider_release_id":"p1","observation_max":"2026-08-31"}}

with tempfile.TemporaryDirectory() as td:
    root=Path(td); art,manifest=artifact(root); p1=root/"one.tar"; p2=root/"two.tar"
    one=build_publication_package(art,p1)
    for child in art.iterdir(): os.utime(child,(999999,999999))
    two=build_publication_package(art,p2)
    assert p1.read_bytes()==p2.read_bytes() and one["package_sha256"]==two["package_sha256"]
    extra=art/"scratch"; extra.write_text("no"); expect(ArtifactValidationError,lambda:build_publication_package(art,root/"bad.tar")); extra.unlink()
    link=art/"link"; link.symlink_to("manifest.json"); expect(ArtifactValidationError,lambda:build_publication_package(art,root/"link.tar")); link.unlink()
    extracted=extract_publication_package(p1,root/"extracted",expected_sha256=one["package_sha256"]); assert json.loads((extracted/"manifest.json").read_text())["artifact_id"]==manifest["artifact_id"]
    malicious=root/"traversal.tar"
    with tarfile.open(malicious,"w") as t:
        info=tarfile.TarInfo("../escape"); info.size=1; t.addfile(info,io.BytesIO(b"x"))
    expect(ArtifactValidationError,lambda:extract_publication_package(malicious,root/"unsafe",expected_sha256=sha256_file(malicious)))

    receipt=create_receipt(**receipt_values(manifest,one["package_sha256"])); validate_receipt(receipt,require_eligible=True)
    unverified=create_receipt(**receipt_values(manifest,one["package_sha256"],state="uploaded")); expect(PublicationError,lambda:validate_receipt(unverified,require_eligible=True))
    malformed=copy.deepcopy(receipt); malformed["package_sha256"]="bad"; expect(PublicationError,lambda:validate_receipt(malformed))
    expect(PublicationError,lambda:transition("prepared","published_immutable_verified"))

    rec=record(manifest,one["package_sha256"],receipt); catalog=add_record(empty_catalog(),rec,receipt)
    assert add_record(catalog,rec,receipt)==catalog
    fresh_values=receipt_values(manifest,one["package_sha256"]); fresh_values["publisher_git_sha"]="fresh-sha"
    fresh_receipt=create_receipt(**fresh_values); fresh_record=record(manifest,one["package_sha256"],fresh_receipt)
    assert fresh_receipt["receipt_id"] != receipt["receipt_id"]
    assert add_record(catalog,fresh_record,fresh_receipt)==catalog
    assert catalog["immutable_records"][0]["publication_receipt_id"]==receipt["receipt_id"]
    mismatched=copy.deepcopy(fresh_receipt); mismatched["asset_id"]=999; mismatched["receipt_id"]=""
    mismatched["receipt_id"]=receipt_identity(mismatched)
    expect(PublicationError,lambda:add_record(catalog,fresh_record,mismatched))
    mismatched=copy.deepcopy(fresh_receipt); mismatched["object_metadata"]["source_id"]="wrong"; mismatched["receipt_id"]=receipt_identity(mismatched)
    mismatched_record=copy.deepcopy(fresh_record); mismatched_record["publication_receipt_id"]=mismatched["receipt_id"]
    expect(PublicationError,lambda:add_record(catalog,mismatched_record,mismatched))
    for field,value in (("package_sha256","b"*64),("artifact_content_hash","c"*64),("release_tag","source-artifact/fixture/other"),("release_id",12),("asset_id",23)):
        conflict=copy.deepcopy(fresh_record); conflict[field]=value
        conflict_receipt=create_receipt(**{**fresh_values,field:value})
        conflict["publication_receipt_id"]=conflict_receipt["receipt_id"]
        expect(IdentityCollisionError,lambda conflict=conflict, conflict_receipt=conflict_receipt:
            add_record(catalog,conflict,conflict_receipt))
    conflict=copy.deepcopy(fresh_record); conflict["metadata"]["data_sha256"]="d"*64
    expect(IdentityCollisionError,lambda:add_record(catalog,conflict,fresh_receipt))
    conflict=copy.deepcopy(fresh_record); conflict["metadata"]["provider_release_id"]="other"
    expect(IdentityCollisionError,lambda:add_record(catalog,conflict,fresh_receipt))
    conflict=copy.deepcopy(fresh_record); conflict["metadata"]["observation_max"]="2026-07-31"
    expect(IdentityCollisionError,lambda:add_record(catalog,conflict,fresh_receipt))
    second=copy.deepcopy(rec); second["object_id"]="other"; second["logical_artifact_uri"]="artifact://source/fixture/other"
    second_receipt=copy.deepcopy(receipt); second_receipt["object_id"]="other"; second_receipt["logical_artifact_uri"]=second["logical_artifact_uri"]
    second_receipt["receipt_id"]=receipt_identity(second_receipt); second["publication_receipt_id"]=second_receipt["receipt_id"]
    expect(IdentityCollisionError,lambda:add_record(catalog,second,second_receipt))
    dangling=copy.deepcopy(catalog); dangling["accepted"]["source"]["fixture"]="missing"; expect(PublicationError,lambda:validate_catalog(dangling))
    changed_pointer=copy.deepcopy(catalog); changed_pointer["accepted"]["source"]["fixture"]=manifest["artifact_id"]; validate_catalog(changed_pointer); assert changed_pointer["immutable_records"]==catalog["immutable_records"]

    publisher=OfflineArtifactPublisher(); metadata=receipt_values(manifest,one["package_sha256"]); metadata.pop("package_sha256"); metadata.pop("publication_state")
    publisher.prepare(manifest["artifact_uri"],p1.read_bytes(),metadata); publisher.upload(manifest["artifact_uri"]); publisher.verify(manifest["artifact_uri"]); final=publisher.finalize(manifest["artifact_uri"])
    assert publisher.finalize(manifest["artifact_uri"])==final
    expect(IdentityCollisionError,lambda:publisher.prepare(manifest["artifact_uri"],b"different",metadata))
    interrupted=OfflineArtifactPublisher(fail_upload=True); interrupted.prepare("artifact://source/x/x",b"x",metadata); expect(PublicationError,lambda:interrupted.upload("artifact://source/x/x")); assert interrupted.inspect("artifact://source/x/x").state=="failed"
    failed=OfflineArtifactPublisher(fail_verify=True); failed.prepare("artifact://source/y/y",b"y",metadata); failed.upload("artifact://source/y/y"); expect(PublicationError,lambda:failed.verify("artifact://source/y/y")); assert failed.inspect("artifact://source/y/y").state=="failed"

    receipts={receipt["receipt_id"]:receipt}
    resolver=CatalogPackageResolver(catalog,{22:p1},receipts,root/"cache"); assert resolver.resolve(manifest["artifact_uri"]).is_dir()
    expect(FileNotFoundError,lambda:resolver.resolve("artifact://source/fixture/uncataloged"))
    tampered=root/"tampered.tar"; tampered.write_bytes(p1.read_bytes()+b"x"); expect(PublicationError,lambda:CatalogPackageResolver(catalog,{22:tampered},receipts,root/"cache2").resolve(manifest["artifact_uri"]))
    not_cataloged=OfflineArtifactPublisher(); not_cataloged.prepare(manifest["artifact_uri"],p1.read_bytes(),metadata); not_cataloged.upload(manifest["artifact_uri"])
    expect(FileNotFoundError,lambda:CatalogPackageResolver(empty_catalog(),{22:p1},receipts,root/"cache3").resolve(manifest["artifact_uri"]))
    expect(FileNotFoundError,lambda:CatalogPackageResolver(catalog,{22:p1},{},root/"cache4").resolve(manifest["artifact_uri"]))

print("Smoke 168 registry contracts passed")
