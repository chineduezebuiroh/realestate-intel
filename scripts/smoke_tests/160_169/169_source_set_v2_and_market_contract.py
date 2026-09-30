"""Production source-set v2 and canonical market artifact contract smoke."""
from __future__ import annotations
import copy, tempfile
from pathlib import Path
import duckdb
from core.source_artifacts.catalog import add_record, empty_catalog, validate_catalog
from core.source_artifacts.market_artifact import create_canonical_market_manifest, validate_canonical_market_artifact
from core.source_artifacts.publication import PublicationError, create_receipt
from core.source_artifacts.source_set_v2 import create_source_set_v2, governed_config_hashes

SHA="a"*64

def expect(exc,fn):
    try: fn()
    except exc: return
    raise AssertionError(f"expected {exc.__name__}")

def entry(source,status,target="2026-08"):
    artifact=f"src__{source}__{target}__r1__0123456789abcdef"
    return {"source_id":source,"artifact_id":artifact,"logical_artifact_uri":f"artifact://source/{source}/{artifact}","package_sha256":SHA,
        "artifact_content_hash":"b"*64,"provider_release_id":"p1","observation_max":target+"-31","validation_status":"passed",
        "monthly_status":status,"release_tag":f"source-artifact/{source}/{artifact}","asset_id":1 if source=="redfin" else 2,
        "publication_receipt_id":"publication_receipt__fixture","cycle_check_succeeded":True,
        "carried_forward":status in {"unchanged","provider_still_stale"},"carry_forward_policy_allowed":status=="provider_still_stale"}

with tempfile.TemporaryDirectory() as td:
    root=Path(td); configs=governed_config_hashes(Path(".")); entries=[entry("redfin","refreshed"),entry("fred_macro","unchanged")]
    ss=create_source_set_v2(root/"set.json",target_month="2026-08",created_at="one",builder_git_sha="git-a",entries=entries,config_hashes=configs)
    same=create_source_set_v2(root/"set2.json",target_month="2026-08",created_at="two",builder_git_sha="git-b",entries=entries,config_hashes=configs)
    assert ss["source_set_id"]==same["source_set_id"]
    changed=dict(configs); changed["config/monthly_refresh_policy.json"]="c"*64
    changed_set=create_source_set_v2(root/"set3.json",target_month="2026-08",created_at="one",builder_git_sha="git-a",entries=entries,config_hashes=changed)
    assert changed_set["source_set_id"]!=ss["source_set_id"]
    expect(PublicationError,lambda:create_source_set_v2(root/"empty.json",target_month="2026-08",created_at="x",builder_git_sha="x",entries=entries,config_hashes={}))
    stale=[entry("redfin","refreshed"),entry("fred_macro","provider_still_stale")]; create_source_set_v2(root/"stale.json",target_month="2026-08",created_at="x",builder_git_sha="x",entries=stale,config_hashes=configs)
    failed=[entry("redfin","refreshed"),entry("fred_macro","failed")]; expect(PublicationError,lambda:create_source_set_v2(root/"failed.json",target_month="2026-08",created_at="x",builder_git_sha="x",entries=failed,config_hashes=configs))
    fake_stale=[entry("redfin","refreshed"),entry("fred_macro","provider_still_stale")]; fake_stale[1]["cycle_check_succeeded"]=False; expect(PublicationError,lambda:create_source_set_v2(root/"fake.json",target_month="2026-08",created_at="x",builder_git_sha="x",entries=fake_stale,config_hashes=configs))
    prior_redfin=[entry("redfin","refreshed","2026-07")]; expect(PublicationError,lambda:create_source_set_v2(root/"redfin.json",target_month="2026-08",created_at="x",builder_git_sha="x",entries=prior_redfin,config_hashes=configs))
    latest=copy.deepcopy(entries); latest[1]["logical_artifact_uri"]="artifact://source/fred_macro/latest"; expect(PublicationError,lambda:create_source_set_v2(root/"latest.json",target_month="2026-08",created_at="x",builder_git_sha="x",entries=latest,config_hashes=configs))

    db=root/"market.duckdb"; con=duckdb.connect(str(db)); con.execute("create table fact_timeseries(geo_id varchar, metric_id varchar, date date, property_type_id varchar, value double, source_id varchar, property_type varchar)"); con.execute("insert into fact_timeseries values ('US','m','2026-08-31','all',1,'fixture','all')"); con.close()
    values=dict(source_set_id=ss["source_set_id"],source_set_semantic_sha256="d"*64,source_set_package_sha256="e"*64,
        canonical_assembly_contract_version="canonical_market_assembly_v1",canonical_schema_identity="schema-v1",config_hashes=configs,
        builder_contract_identity="core-source-artifacts-assembly-v1",dependency_lock_identity="requirements-sha256:"+"f"*64,
        assembly_revision=1,compressed_package_sha256="1"*64,table_inventory=["fact_timeseries"],row_count=1,source_count=1,
        geography_count=1,metric_count=1,first_date="2026-08-31",last_date="2026-08-31",duplicate_key_count=0,
        validation_status="passed",assembly_warnings=[],builder_git_sha="git-a",built_at="2026-09-01T00:00:00Z")
    market=create_canonical_market_manifest(root/"market.json",database_path=db,**values)
    market2=create_canonical_market_manifest(root/"market2.json",database_path=db,**{**values,"built_at":"later","builder_git_sha":"git-b"}); assert market["market_artifact_id"]==market2["market_artifact_id"]
    altered=root/"altered.duckdb"; altered.write_bytes(db.read_bytes()+b"x"); other=create_canonical_market_manifest(root/"other.json",database_path=altered,**values); assert other["market_artifact_id"]!=market["market_artifact_id"]
    collision=copy.deepcopy(other); collision["market_artifact_id"]=market["market_artifact_id"]; expect(PublicationError,lambda:validate_canonical_market_artifact(collision))
    missing=copy.deepcopy(market); missing["source_set_id"]=""; expect(PublicationError,lambda:validate_canonical_market_artifact(missing))

    receipt=create_receipt(logical_artifact_uri="artifact://serving_market/serving_fixture",object_id="serving_fixture",object_type="serving_market",object_metadata={"canonical_market_artifact_id":market["market_artifact_id"]},artifact_content_hash="2"*64,package_sha256="3"*64,member_hashes={"market_serving.duckdb":"4"*64},remote_backend="offline_fixture",remote_repository="fixture/repo",release_tag="serving-market/2026-08/serving_fixture",release_id=4,asset_id=5,asset_filename="serving_fixture.duckdb.tar",published_at="x",verified_at="x",publication_state="published_immutable_verified",publisher_git_sha="x",contract_versions=["serving-future"])
    record={"object_type":"serving_market","object_id":"serving_fixture","logical_artifact_uri":"artifact://serving_market/serving_fixture","remote_repository":"fixture/repo","release_tag":receipt["release_tag"],"release_id":4,"asset_id":5,"asset_filename":receipt["asset_filename"],"package_sha256":"3"*64,"artifact_content_hash":"2"*64,"publication_receipt_id":receipt["receipt_id"],"publication_state":"published_immutable_verified","metadata":{"canonical_market_artifact_id":market["market_artifact_id"],"database_sha256":"4"*64}}
    validate_catalog(add_record(empty_catalog(),record,receipt))

print("Smoke 169 source-set v2 and canonical market contracts passed")
