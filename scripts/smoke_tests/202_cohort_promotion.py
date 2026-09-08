"""Smoke 202: logical Source Set, canonical authority, and recovery semantics."""
import copy
import json
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.assembly_v2 import assemble_source_set_v2
from core.source_artifacts.catalog import validate_catalog, validate_catalog_namespace
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.fixture_remote import OfflineArtifactPublisher
from core.source_artifacts.object_package import build_object_package, publish_object
from core.source_artifacts.promotion import (add_promotion_record, create_promotion_record,
    recover_promotion)
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from core.source_artifacts.source_set_v2 import validate_source_set_v2
from core.source_artifacts.storage import LocalArtifactResolver
from jobs.monthly_refresh.cohort_promotion import build_logical_source_set

CYCLE = "monthly_cycle__2026-07__7cab1c5df177a1e4"
ROOT = Path("config/monthly_source_cycle_results") / CYCLE
catalog = json.loads(Path("config/artifact_catalog.json").read_text())
readiness = json.loads(Path("config/monthly_refresh_readiness.json").read_text())
resolution = json.loads(Path("config/bps_family_resolutions/bps_family_resolution__457b5a17a73da623cfcfea08.json").read_text())
results = [json.loads(path.read_text())["result"] for path in sorted(ROOT.glob("*.json"))]
republications = [json.loads(path.read_text()) for path in sorted(Path("config/monthly_source_republications",CYCLE).glob("*/*.json"))]
# Redfin is intentionally authoritative in readiness rather than the automated
# cycle-result registry.
redfin_record = next(r for r in catalog["immutable_records"] if r["object_id"] == readiness["records"][0]["candidate_artifact_id"])
results.append({"schema_version":"monthly_source_execution_result_v1", "source_id":"redfin", "cycle_id":CYCLE,
    "status":"succeeded", "candidate_artifact_id":redfin_record["object_id"],
    "artifact_content_hash":redfin_record["artifact_content_hash"], "package_sha256":redfin_record["package_sha256"],
    "publication_state":"published_verified", "validation_status":"passed",
    "provider_release_id":redfin_record["metadata"]["provider_release_id"],
    "observation_max":redfin_record["metadata"]["observation_max"],
    "prior_artifact_id":catalog["accepted"]["source"]["redfin"], "source_change_detected":True,
    "retryability":"not_applicable", "evidence_uri":redfin_record["logical_artifact_uri"],
    "accepted_pointer_changed":False})

with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    source_set = build_logical_source_set(output=root/"source-set.json", cycle_id=CYCLE,
        target_month="2026-07", physical_results=results, catalog=catalog, readiness=readiness,
        resolution=resolution, family_parent_republications=republications,
        created_at="first", builder_git_sha="git-a")
    repeat = build_logical_source_set(output=root/"source-set-repeat.json", cycle_id=CYCLE,
        target_month="2026-07", physical_results=results, catalog=catalog, readiness=readiness,
        resolution=resolution, family_parent_republications=republications,
        created_at="second", builder_git_sha="git-b")
    concept = Path("config/bps_cbsa_canonical_concepts_v1.csv")
    original = concept.read_bytes()
    concept.write_bytes(original + b"\n")
    try:
        try:
            build_logical_source_set(output=root/"drifted-source-set.json", cycle_id=CYCLE,
                target_month="2026-07", physical_results=results, catalog=catalog,
                readiness=readiness, resolution=resolution,
                family_parent_republications=republications, created_at="drift",
                builder_git_sha="git-drift")
        except ValueError as exc:
            assert str(exc) == "BPS family resolution governed config drift"
        else:
            raise AssertionError("material BPS family config drift was accepted")
    finally:
        concept.write_bytes(original)
    assert source_set["source_set_id"] == repeat["source_set_id"]
    assert source_set["included_source_inventory"] == ["bps", "ces", "fred_macro", "laus", "redfin"]
    assert sum(e["source_id"] == "bps" for e in source_set["sources"]) == 1
    assert not {"census_bps", "census_bps_provisional"} & set(source_set["included_source_inventory"])
    family = source_set["family_resolution"]["families"][0]
    assert family["resolution_id"] == resolution["resolution_id"]
    assert {m["artifact_id"] for m in family["physical_sources"]} == {p["artifact_id"] for p in resolution["parents"]}
    contradictory = copy.deepcopy(source_set); contradictory["family_resolution"]["families"][0]["output_package_sha256"] = "a"*64
    try: validate_source_set_v2(contradictory)
    except PublicationError: pass
    else: raise AssertionError("contradictory family mapping accepted")
    package = build_object_package({"source-set.json":root/"source-set.json"},root/"source-set.tar")
    assert package == build_object_package({"source-set.json":root/"source-set.json"},root/"source-set-repeat.tar")
    offline = OfflineArtifactPublisher()
    published_catalog, receipt, created = publish_object(publisher=offline,catalog=catalog,
        package=root/"source-set.tar",logical_uri="artifact://source_set/"+source_set["source_set_id"],
        object_id=source_set["source_set_id"],object_type="source_set",artifact_content_hash=sha256_json(source_set),
        object_metadata={"cycle_id":CYCLE,"source_set_semantic_sha256":sha256_json(source_set)},
        member_hashes=package["member_hashes"],remote_repository="fixture/repo",
        release_tag="source-set/"+source_set["source_set_id"],release_id=800001,asset_id=800002,
        asset_filename=source_set["source_set_id"]+".tar",publisher_git_sha="fixture",
        published_at="fixed",contract_versions=["source_set_manifest_v2"])
    assert created and receipt["publication_state"] == "published_immutable_verified"
    reused, _, changed = publish_object(publisher=offline,catalog=published_catalog,
        package=root/"source-set.tar",logical_uri="artifact://source_set/"+source_set["source_set_id"],
        object_id=source_set["source_set_id"],object_type="source_set",artifact_content_hash=sha256_json(source_set),
        object_metadata={"cycle_id":CYCLE,"source_set_semantic_sha256":sha256_json(source_set)},
        member_hashes=package["member_hashes"],remote_repository="fixture/repo",
        release_tag="source-set/"+source_set["source_set_id"],release_id=800001,asset_id=800002,
        asset_filename=source_set["source_set_id"]+".tar",publisher_git_sha="fixture",
        published_at="fixed",contract_versions=["source_set_manifest_v2"])
    assert not changed and reused == published_catalog
    conflicting_package=root/"source-set-conflict.tar"
    conflicting_package.write_bytes((root/"source-set.tar").read_bytes()+b"contradiction")
    try:
        publish_object(publisher=offline,catalog=published_catalog,
            package=conflicting_package,logical_uri="artifact://source_set/"+source_set["source_set_id"],
            object_id=source_set["source_set_id"],object_type="source_set",artifact_content_hash=sha256_json(source_set),
            object_metadata={"cycle_id":CYCLE,"source_set_semantic_sha256":sha256_json(source_set)},
            member_hashes=package["member_hashes"],remote_repository="fixture/repo",
            release_tag="source-set/"+source_set["source_set_id"],release_id=800001,asset_id=800002,
            asset_filename=source_set["source_set_id"]+".tar",publisher_git_sha="fixture",
            published_at="fixed",contract_versions=["source_set_manifest_v2"])
    except (IdentityCollisionError, PublicationError): pass
    else: raise AssertionError("contradictory same-identity publication accepted")

    # Add already-published fixture Source Set/canonical records. Publication is
    # a distinct prerequisite; promotion never manufactures catalog records.
    working = copy.deepcopy(catalog)
    ss_hash = sha256_json(source_set)
    def object_record(kind, object_id, content_hash, asset_id):
        prefix={"source_set":"source-set","canonical_market":"canonical-market"}[kind]
        return {"object_type":kind, "object_id":object_id,
            "logical_artifact_uri":f"artifact://{kind}/{object_id}", "remote_repository":"fixture/repo",
            "release_tag":f"{prefix}/{object_id}", "release_id":asset_id, "asset_id":asset_id,
            "asset_filename":object_id+".tar", "package_sha256":chr(97+asset_id%5)*64,
            "artifact_content_hash":content_hash, "publication_receipt_id":f"publication_receipt__{asset_id}",
            "publication_state":"published_immutable_verified", "metadata": {"source_set_id":source_set["source_set_id"]}}
    ss_record = object_record("source_set", source_set["source_set_id"], ss_hash, 700001)
    market_id = "market__2026-07__r1__" + "1"*16
    market_hash = "d"*64
    market_record = object_record("canonical_market", market_id, market_hash, 700002)
    working["immutable_records"].extend([ss_record, market_record])
    working["immutable_records"].sort(key=lambda r:(r["object_type"],r["object_id"]))
    validate_catalog_namespace(working,fixture=False)
    targets = {e["source_id"]:e["artifact_id"] for e in source_set["sources"]}
    expected = {source:working["accepted"]["source"].get(source) for source in targets}
    promotion = create_promotion_record(cycle_id=CYCLE, source_set_id=source_set["source_set_id"],
        source_set_semantic_sha256=ss_hash, canonical_artifact_id=market_id,
        canonical_artifact_hash=market_hash, expected_source_pointers=expected,
        target_source_pointers=targets, expected_source_set=None,
        expected_canonical=working["accepted"]["canonical_market"],
        readiness_id=readiness["records"][0]["readiness_id"], resolution_id=resolution["resolution_id"])
    assert add_promotion_record(None,promotion)[1]
    assert not add_promotion_record(promotion,promotion)[1]
    bad = copy.deepcopy(promotion); bad["canonical_artifact_hash"]="e"*64
    try: add_promotion_record(promotion, create_promotion_record(**{k:v for k,v in bad.items()
        if k not in {"schema_version","promotion_id","operation_order"}}))
    except IdentityCollisionError: pass
    else: raise AssertionError("contradictory promotion record accepted")

    # Interrupt after every atomic boundary and recover from persisted state.
    total_operations = 2 + len(targets) + 1
    for stop in range(total_operations + 1):
        partial_catalog, partial_readiness, progress = recover_promotion(
            promotion, working, readiness, max_operations=stop)
        final_catalog, final_readiness, final = recover_promotion(promotion, partial_catalog, partial_readiness)
        assert final["complete"] and final["redfin_consumed"]
        assert final_catalog["accepted"]["source"]["bps"] == resolution["output_artifact_id"]
        assert "census_bps" not in final_catalog["accepted"]["source"]
        assert "census_bps_provisional" not in final_catalog["accepted"]["source"]
        assert final_catalog["accepted"]["serving_market"] == catalog["accepted"]["serving_market"]
        repeated = recover_promotion(promotion, final_catalog, final_readiness)
        assert repeated[0] == final_catalog and repeated[1] == final_readiness and repeated[2]["complete"]
    drift = copy.deepcopy(working); drift["accepted"]["source"]["ces"] = targets["fred_macro"]
    try: recover_promotion(promotion, drift, readiness)
    except (IdentityCollisionError, PublicationError): pass
    else: raise AssertionError("expected-old pointer contradiction accepted")

    # Canonical assembly resolves only exact logical entries.  A compact
    # registry fixture proves that BPS appears once and its physical parents
    # cannot be loaded accidentally.
    artifacts, assembly_entries = {}, []
    metric_rows, geo_rows = [], []
    for index, source in enumerate(("bps", "ces"), 1):
        metric = "fixture_" + source
        geo = "fixture_geo__county"
        frame = pd.DataFrame([[geo,metric,"2026-07-01","all",float(index),source,"all"]],
            columns=["geo_id","metric_id","date","property_type_id","value","source_id","property_type"])
        directory = root / ("artifact-"+source)
        manifest = create_artifact(directory, frame, source_id=source, source_family=source,
            source_type="fixture", provider="fixture", distribution_channel="fixture",
            provider_release_id="fixture", provider_release_timestamp_or_date=None,
            retrieved_at=None, target_month="2026-07", source_request_identity="fixture:"+source,
            source_urls_or_endpoint_identity=["fixture://"+source], artifact_created_at="fixed")
        uri = manifest["artifact_uri"]; artifacts[uri] = directory
        assembly_entries.append({"source_id":source,"artifact_id":manifest["artifact_id"],
            "logical_artifact_uri":uri,"package_sha256":"a"*64,
            "artifact_content_hash":manifest["artifact_content_hash"],"provider_release_id":"fixture",
            "observation_max":"2026-07-01","validation_status":"passed","monthly_status":"refreshed",
            "release_tag":"fixture","asset_id":index,"publication_receipt_id":"fixture",
            "cycle_check_succeeded":True,"carried_forward":False,"carry_forward_policy_allowed":False})
        metric_rows.append({"metric_id":metric,"source_id":source})
        geo_rows.append({"geo_slug":geo})
    assembly_set = copy.deepcopy(source_set)
    assembly_set["sources"] = sorted(assembly_entries,key=lambda e:e["source_id"])
    assembly_set["required_source_inventory"] = assembly_set["included_source_inventory"] = ["bps","ces"]
    assembly_set["family_resolution"] = {}
    from core.source_artifacts.source_set_v2 import source_set_semantic_sha256
    assembly_set["source_set_id"] = "source_set__2026-07__v2__" + source_set_semantic_sha256(assembly_set)[:16]
    metric_file, geo_file = root/"metrics.csv", root/"geos.csv"
    pd.DataFrame(metric_rows).to_csv(metric_file,index=False); pd.DataFrame(geo_rows).drop_duplicates().to_csv(geo_file,index=False)
    db = root/"candidate.duckdb"
    assembled = assemble_source_set_v2(assembly_set,db,LocalArtifactResolver(artifacts),
        metric_registry=metric_file,geo_manifest=geo_file)
    assert assembled["sources"] == ["bps","ces"] and assembled["source_count"] == 2
    con=duckdb.connect(str(db),read_only=True)
    assert con.execute("select count(*) from source_artifact_metadata where source_id='bps'").fetchone()[0] == 1
    assert con.execute("select count(*) from fact_timeseries where source_id like 'census_bps%'").fetchone()[0] == 0
    con.close()

print("Smoke 202 cohort promotion passed")
