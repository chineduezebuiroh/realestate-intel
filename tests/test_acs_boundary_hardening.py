"""Offline counterexamples at authoritative ACS storage and recovery boundaries."""
from copy import deepcopy
from pathlib import Path
import json
import shutil

import pytest

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.hashing import sha256_json, write_canonical_json
from core.source_artifacts.object_package import build_object_package
from core.source_artifacts.promotion import create_promotion_record, SOURCE_TRANSITION_ORDER
from core.source_artifacts.publication import PublicationError
from core.source_artifacts.source_set_v2 import create_source_set_v2, governed_config_hashes, source_set_semantic_sha256
from core.source_artifacts.market_artifact import create_canonical_market_manifest
from core.source_artifacts.storage import LocalArtifactResolver
from jobs.monthly_refresh.acs_scope import ACSGovernance
from jobs.monthly_refresh.acs_attestation import issue, verify, require
from jobs.monthly_refresh.acs_history_family import resolution_record
from jobs.monthly_refresh import cohort_promotion_hosted as hosted
from jobs.monthly_refresh.cohort_plan_store import GitHubCohortPlanStore, validate_durable_plan
from test_acs_historical_materialization import make_family, record, main_governance, registry, accept, scope


class AuthorityAPI:
    """Offline Git tree/commit/ref model, including non-fast-forward rejection."""
    repository = "fixture/repo"
    def __init__(self, value=None, path="prepared.json", on_write=None):
        self.value = deepcopy(value); self.path = path; self.on_write = on_write
        self.oid = None if value is None else "2" * 40
        self.tip = "a" * 40; self.calls = []; self.pending = None; self.before_ref = None
    def request(self, method, url, *, payload=None, **kwargs):
        import base64
        self.calls.append((method, url))
        if url.startswith("/contents/"):
            return ({"content": base64.b64encode(json.dumps(self.value).encode()).decode(), "sha": self.oid}
                    if self.value is not None else None), {}
        if method == "GET" and url.startswith("/git/commits/"):
            return {"sha": url.rsplit("/", 1)[1], "tree": {"sha": "tree-base"}}, {}
        if url == "/git/trees":
            self.pending = json.loads(payload["tree"][0]["content"])
            return {"sha": "tree-new"}, {}
        if url == "/git/commits":
            self.parent = payload["parents"][0]
            return {"sha": "c" * 40}, {}
        if url == "/git/refs/heads/main":
            assert method == "PATCH" and payload["force"] is False
            if self.before_ref: self.before_ref(self)
            if self.tip != self.parent: raise PublicationError("non-fast-forward main")
            self.value = deepcopy(self.pending); self.oid = "3" * 40; self.tip = payload["sha"]
            if self.on_write: self.on_write(self.value, self.oid)
            return {}, {}
        raise AssertionError((method, url))


class CatalogCAS:
    def __init__(self, value): self.value = deepcopy(value); self.writes = 0; self.fail = False; self.oid = "1" * 40; self.api = AuthorityAPI()
    def read(self): return deepcopy(self.value), self.oid
    def _write(self, value, oid, message):
        from core.source_artifacts.publication import PublicationError
        if self.fail: raise PublicationError("stale expected-old catalog SHA")
        assert oid == self.oid
        self.value = deepcopy(value); self.value["compare_and_swap"]["expected_git_blob_sha"] = oid
        self.writes += 1; self.oid = (hex(self.writes + 1)[2:] * 40)[:40]


class JSONCAS:
    def __init__(self, value=None):
        self.value = deepcopy(value); self.writes = 0; self.oid = None if value is None else "2" * 40
        self.path = "prepared.json"; self.branch = "main"
        def committed(value, oid): self.value = deepcopy(value); self.oid = oid; self.writes += 1
        self.api = AuthorityAPI(value, on_write=committed)
    def read(self): return deepcopy(self.value), self.oid
    def write(self, value, oid, message):
        assert oid == self.oid
        self.value = deepcopy(value); self.writes += 1; self.oid = "3" * 40


def promotion_fixture(tmp):
    """Actual ACS acquisition/history/logical and derived-object package bytes."""
    inputs, manifest = make_family(tmp, namespace="production")
    logical = record(tmp / "logical", manifest, tmp)
    inputs["records"][logical["object_id"]] = logical; inputs["paths"][logical["object_id"]] = tmp / "logical"
    resolution = resolution_record(artifact=tmp / "logical", catalog_record=logical, **inputs)
    catalog = empty_catalog(); packages = {}; releases = {}; n = 0
    def transport(rec, package):
        nonlocal n
        n += 1; rec = deepcopy(rec)
        source = rec.get("metadata", {}).get("source_id")
        kind = rec["object_type"]
        tag = f"source-artifact/{source}/{rec['object_id']}" if source else f"{kind.replace('_', '-')}/{rec['object_id']}"
        rec.update(logical_artifact_uri=f"artifact://source/{source}/{rec['object_id']}" if source else f"artifact://{kind}/{rec['object_id']}",
                   remote_repository="fixture/repo", release_tag=tag, release_id=n, asset_id=n,
                   asset_filename=rec["object_id"] + ".tar", publication_receipt_id=f"receipt-{n}")
        for key, value in dict(provider_release_id="fixture", observation_max="2024-12-31", data_sha256="a" * 64).items():
            rec["metadata"].setdefault(key, value)
        packages[n] = package; releases[n] = {"tag_name": tag, "draft": False,
            "assets": [{"id": n, "name": rec["asset_filename"]}]}
        catalog["immutable_records"].append(rec)
        return rec
    for rec in inputs["records"].values():
        transport(rec, tmp / (rec["object_id"] + ".tar"))
    entries = []
    for source in SOURCE_TRANSITION_ORDER:
        aid = resolution["output_artifact_id"] if source == "acs" else f"src__{source}__2026-07__r1__" + "1" * 16
        rec = next((r for r in catalog["immutable_records"] if r["object_id"] == aid), None)
        if rec is None:
            rec = transport({"object_type": "source", "object_id": aid, "artifact_content_hash": "f" * 64,
                             "package_sha256": "a" * 64, "publication_state": "published_immutable_verified",
                             "metadata": {"source_id": source}}, None)
        entries.append({"source_id": source, "artifact_id": aid,
            **{k: rec[k] for k in ("logical_artifact_uri", "package_sha256", "artifact_content_hash", "release_tag", "asset_id", "publication_receipt_id")},
            "provider_release_id": "fixture", "observation_max": "2024-12-31", "validation_status": "passed",
            "monthly_status": "refreshed", "cycle_check_succeeded": True, "carried_forward": False,
            "carry_forward_policy_allowed": False})
    family = {"logical_source_id": "acs", "resolution_id": resolution["resolution_id"],
              **{k: resolution[k] for k in ("output_artifact_id", "output_content_hash", "output_package_sha256")},
              "physical_sources": [{k: p[k] for k in ("source_id", "artifact_id", "artifact_content_hash", "package_sha256")} for p in resolution["parents"]]}
    ss = create_source_set_v2(tmp / "source-set.json", target_month="2026-07", created_at="fixed", builder_git_sha="test",
        entries=entries, config_hashes=governed_config_hashes(), family_resolution={"schema_version": "source_family_resolution_map_v1",
        "cycle_id": "monthly_cycle__2026-07__fixture", "logical_source_inventory": sorted(SOURCE_TRANSITION_ORDER),
        "physical_source_inventory": sorted((set(SOURCE_TRANSITION_ORDER) - {"acs"}) | {"census_acs1", "census_acs5"}), "families": [family]})
    sp = build_object_package({"source-set.json": tmp / "source-set.json"}, tmp / "ss.tar")
    (tmp / "market.duckdb").write_bytes(b"offline transport fixture")
    market = create_canonical_market_manifest(tmp / "canonical-market.json", database_path=tmp / "market.duckdb",
        source_set_id=ss["source_set_id"], source_set_semantic_sha256=source_set_semantic_sha256(ss), source_set_package_sha256=sp["package_sha256"],
        canonical_assembly_contract_version="canonical_market_assembly_v1", canonical_schema_identity="canonical-timeseries-v1",
        config_hashes=ss["config_hashes"], builder_contract_identity="test", dependency_lock_identity="test", assembly_revision=1,
        compressed_package_sha256="a" * 64, table_inventory=["fact_timeseries"], row_count=3, source_count=10,
        geography_count=1, metric_count=1, first_date="2022-12-31", last_date="2024-12-31", duplicate_key_count=0,
        validation_status="passed", assembly_warnings=[], builder_git_sha="test", built_at="fixed")
    mp = build_object_package({"canonical-market.json": tmp / "canonical-market.json", "market.duckdb": tmp / "market.duckdb"}, tmp / "market.tar")
    for kind, obj, package, identity in (("source_set", ss, sp, ss["source_set_id"]), ("canonical_market", market, mp, market["market_artifact_id"])):
        transport({"object_type": kind, "object_id": identity, "artifact_content_hash": sha256_json(obj),
                   "package_sha256": package["package_sha256"], "publication_state": "published_immutable_verified", "metadata": {}},
                  tmp / ("ss.tar" if kind == "source_set" else "market.tar"))
    catalog["immutable_records"].sort(key=lambda r: (r["object_type"], r["object_id"]))
    targets = {s["source_id"]: s["artifact_id"] for s in entries}
    prepared = create_promotion_record(cycle_id="cycle-july", source_set_id=ss["source_set_id"], source_set_semantic_sha256=source_set_semantic_sha256(ss),
        canonical_artifact_id=market["market_artifact_id"], canonical_artifact_hash=sha256_json(market),
        expected_source_pointers={s: None for s in targets}, target_source_pointers=targets, expected_source_set=None,
        expected_canonical=None, readiness_id="ready-july", resolution_id="resolution-july")
    class API:
        repository = "fixture/repo"; token = "test-token"
        def request(self, method, url, **kwargs):
            assert method == "GET" and url.startswith("/releases/")
            return releases[int(url.split("/")[-1])], {}
        def download_asset(self, asset_id, target): shutil.copyfile(packages[asset_id], target)
    api = API()
    resolver = LocalArtifactResolver({f"artifact://source/{r['metadata']['source_id']}/{aid}": inputs["paths"][aid] for aid, r in inputs["records"].items()})
    validation = dict(catalog=catalog, resolver=resolver, governance=inputs["governance"])
    attested, proof = issue(prepared, resolution, promotion_api=api, promotion_workspace=tmp / "objects", **validation)
    readiness = {"schema_version": "monthly_refresh_readiness_v1", "records": [{"readiness_id": "ready-july",
        "cycle_id": "cycle-july", "source_id": "redfin", "candidate_artifact_id": targets["redfin"], "consumed": False}]}
    return attested, proof, catalog, readiness, api, validation


def test_fake_transport_cannot_mint_production():
    class Fake:
        def request(self, *args, **kwargs): raise AssertionError("fake transport must not be called")
    with pytest.raises(ValueError, match="injectable"):
        ACSGovernance.from_main(Fake())
    fixture = ACSGovernance.from_fixture(registry()); fixture.namespace = "production"; fixture.provenance = "main:" + "a" * 40
    with pytest.raises(ValueError, match="test_only"):
        fixture.require_production()


def test_remote_snapshot_not_worktree_and_capability_integrity(tmp_path, monkeypatch):
    reg = registry(); reg["namespace"] = "production"; authority = scope(reg, "census_acs1")
    monkeypatch.setattr(Path, "read_text", lambda *a, **k: (_ for _ in ()).throw(AssertionError("local authority read")))
    gov = main_governance(reg); gov.require_production()
    assert gov.production_snapshot()["commit"] == "a" * 40
    gov._registry["current_scope"]["census_acs1"] = "invented"
    with pytest.raises(ValueError, match="test_only"): gov.require_production()


@pytest.mark.parametrize("attack", ["missing", "nonexistent", "wrong_key", "wrong_product", "ambiguous"])
def test_registry_corruption_rejected(tmp_path, attack):
    inputs, _ = make_family(tmp_path)
    reg = deepcopy(inputs["governance"]._registry)
    key = reg["current_state"]["census_acs1"]
    if attack == "missing": del reg["current_state"]["census_acs1"]
    elif attack == "nonexistent": reg["current_state"]["census_acs1"] = "absent"
    elif attack == "wrong_key": reg["states"]["alias"] = reg["states"].pop(key); reg["current_state"]["census_acs1"] = "alias"
    elif attack == "wrong_product": reg["current_state"]["census_acs1"] = reg["current_state"]["census_acs5"]
    else: reg["current_state"]["census_acs1"] = [key, key]
    with pytest.raises(ValueError): ACSGovernance.from_fixture(reg)


def test_attested_promotion_recovery_and_tampering(tmp_path, monkeypatch):
    prepared, proof, catalog, readiness, api, validation = promotion_fixture(tmp_path)
    cas = CatalogCAS(catalog); ready = JSONCAS(readiness); store = JSONCAS()
    assert hosted.persist_prepared(store, prepared, acs_proof=proof)[1]
    hosted.execute_one(prepared, cas, ready, acs_proof=proof)
    # Recovery performs real package download and reconstruction at the pinned snapshot.
    monkeypatch.setattr(ACSGovernance, "load_production", lambda *a, **k: validation["governance"])
    recovered = hosted._recover_acs_proof(prepared, cas.value, api, tmp_path / "recovery")
    outcome = hosted.execute_to_completion(prepared, cas, ready, acs_proof=recovered)
    assert outcome["exact_rerun_noop"] and ready.writes == 1
    again = hosted._recover_acs_proof(prepared, cas.value, api, tmp_path / "recovery")
    assert not hosted.execute_one(prepared, cas, ready, acs_proof=again)["changed"]
    bad = deepcopy(prepared); bad["acs_validation"]["resolution"]["output_content_hash"] = "0" * 64
    with pytest.raises(ValueError): hosted.execute_one(bad, cas, ready, acs_proof=recovered)
    drift = deepcopy(cas.value)
    next(r for r in drift["immutable_records"] if r["object_id"] == prepared["canonical_artifact_id"])["package_sha256"] = "0" * 64
    drift_cas = CatalogCAS(drift)
    with pytest.raises(ValueError, match="catalog bindings"):
        hosted.execute_one(prepared, drift_cas, ready, acs_proof=recovered)
    assert drift_cas.writes == 0


def test_legacy_live_run_rejects_before_executor_or_writes(tmp_path, monkeypatch):
    prepared, proof, catalog, readiness, api, validation = promotion_fixture(tmp_path)
    legacy = hosted._core(prepared)
    cas = CatalogCAS(catalog); ready = JSONCAS(readiness)
    monkeypatch.setattr(hosted, "GitHubCatalogCAS", lambda *a, **k: cas)
    monkeypatch.setattr(hosted, "GitHubJSONCAS", lambda api, path, branch: ready if path == hosted.READINESS_PATH else JSONCAS(legacy))
    monkeypatch.setattr(hosted, "validate_readiness", lambda *a, **k: None)
    monkeypatch.setattr(hosted, "execute_to_completion", lambda *a, **k: (_ for _ in ()).throw(AssertionError("executor reached")))
    with pytest.raises(ValueError, match="attestation"):
        hosted.run(api=api, branch="main", cycle_id="cycle-july", workspace=tmp_path / "live", git_sha="test", mutate=True,
                   supplied_authorization=hosted.authorization_token(legacy))
    assert cas.writes == ready.writes == 0


def test_fabricated_durable_record_rejected_before_store_io(tmp_path):
    from test_phase2_full_cohort import result, resolution
    from jobs.monthly_refresh.cohort import barrier_evidence, logical_cohort_plan, REQUIRED_SOURCES
    evidence = barrier_evidence(cycle={"cycle_id": "cycle", "invocation_mode": "resume"},
        results=[result(s) for s in REQUIRED_SOURCES], pins=None, github={})
    plan = logical_cohort_plan(physical_evidence=evidence, bps_resolution=resolution("bps", {"census_bps", "census_bps_provisional"}),
                               acs_resolution=resolution("acs", {"census_acs1", "census_acs5"}))
    plan["family_resolutions"]["acs"].update(schema_version="acs_family_resolution_record_v3", provider_discovery_performed=False)
    plan["family_resolutions"]["bps"]["provider_discovery_performed"] = False
    plan["plan_id"] = "logical_cohort_plan__" + sha256_json({k: v for k, v in plan.items() if k != "plan_id"})[:24]
    class ForbiddenAPI:
        def request(self, *a, **k): raise AssertionError("store I/O before ACS proof")
    with pytest.raises(ValueError, match="attestation"):
        GitHubCohortPlanStore(ForbiddenAPI(), "main").put(plan)


def test_valid_attested_live_run_and_completed_recovery(tmp_path, monkeypatch):
    prepared, proof, catalog, readiness, api, validation = promotion_fixture(tmp_path)
    cas = CatalogCAS(catalog); ready = JSONCAS(readiness)
    monkeypatch.setattr(hosted, "GitHubCatalogCAS", lambda *a, **k: cas)
    monkeypatch.setattr(hosted, "GitHubJSONCAS", lambda api, path, branch: ready if path == hosted.READINESS_PATH else JSONCAS(prepared))
    monkeypatch.setattr(hosted, "validate_readiness", lambda *a, **k: None)
    monkeypatch.setattr(ACSGovernance, "load_production", lambda *a, **k: validation["governance"])
    arguments = dict(api=api, branch="main", cycle_id="cycle-july", workspace=tmp_path / "live", git_sha="test", mutate=True,
                     supplied_authorization=hosted.authorization_token(prepared))
    assert hosted.run(**arguments)["exact_rerun_noop"]
    writes = cas.writes + ready.writes
    assert hosted.run(**arguments)["exact_rerun_noop"]
    assert cas.writes + ready.writes == writes


def test_rehashed_promotion_evidence_cannot_replace_reconstruction(tmp_path):
    prepared, proof, catalog, readiness, api, validation = promotion_fixture(tmp_path)
    bad = deepcopy(prepared)
    bad["acs_validation"]["resolution"]["output_content_hash"] = "0" * 64
    evidence = bad["acs_validation"]
    evidence["attestation_id"] = "acs_reconstruction_attestation_v1__" + sha256_json({k: v for k, v in evidence.items() if k != "attestation_id"})
    with pytest.raises(ValueError): require(bad, proof)
    with pytest.raises(ValueError, match="disagrees"):
        verify(bad, promotion_api=api, promotion_workspace=tmp_path / "attack", **validation)
    cas = CatalogCAS(catalog); ready = JSONCAS(readiness)
    with pytest.raises(ValueError): hosted.execute_one(bad, cas, ready, acs_proof=object())
    assert cas.writes == ready.writes == 0


def durable_fixture(tmp_path):
    import base64
    from test_phase2_full_cohort import result, resolution
    from jobs.monthly_refresh.cohort import barrier_evidence, logical_cohort_plan, REQUIRED_SOURCES
    prepared, _, catalog, _, _, validation = promotion_fixture(tmp_path)
    acs = prepared["acs_validation"]["resolution"]
    physical = [result(s) for s in REQUIRED_SOURCES]
    for item in physical:
        if item["source_id"] in {"census_acs1", "census_acs5"}:
            parent = acs["context"]["historical_states"][item["source_id"]]["inventory"]["parents"][-1]
            item.update(candidate_artifact_id=parent["artifact_id"], artifact_content_hash=parent["artifact_content_hash"], package_sha256=parent["package_sha256"])
    evidence = barrier_evidence(cycle={"cycle_id": "cycle", "invocation_mode": "resume"}, results=physical, pins=None, github={})
    bps = resolution("bps", {"census_bps", "census_bps_provisional"}); bps["record"]["provider_discovery_performed"] = False
    plan = logical_cohort_plan(physical_evidence=evidence, bps_resolution=bps, acs_resolution=acs, acs_validation=validation)
    return plan, validation


def test_durable_plan_requires_exact_reconstruction_capability(tmp_path):
    plan, validation = durable_fixture(tmp_path)
    api = AuthorityAPI(); store = GitHubCohortPlanStore(api, "main")
    with pytest.raises(ValueError, match="capability"): store.put(plan)
    assert api.calls == []
    proof = verify(plan, **validation)
    assert store.put(plan, acs_proof=proof)[1]
    assert not store.put(plan, acs_proof=proof)[1]
    # Recompute all structural/content hashes; serialized evidence is still no capability.
    forged = deepcopy(plan)
    forged["family_resolutions"]["acs"]["output_content_hash"] = "0" * 64
    next(s for s in forged["sources"] if s["source_id"] == "acs")["artifact_content_hash"] = "0" * 64
    forged["acs_validation"]["resolution"] = deepcopy(forged["family_resolutions"]["acs"])
    from jobs.monthly_refresh.acs_attestation import target_identity
    ev = forged["acs_validation"]; ev["target_sha256"] = target_identity(forged)
    ev["attestation_id"] = "acs_reconstruction_attestation_v1__" + sha256_json({k: v for k, v in ev.items() if k != "attestation_id"})
    forged["plan_id"] = "logical_cohort_plan__" + sha256_json({k: v for k, v in forged.items() if k != "plan_id"})[:24]
    validate_durable_plan(forged)  # Structural validation explicitly does not establish approval.
    before = list(api.calls)
    with pytest.raises(ValueError, match="capability"): store.put(forged, acs_proof=proof)
    assert api.calls == before
    with pytest.raises(ValueError, match="disagrees"): verify(forged, **validation)
    ambiguous = deepcopy(plan); ambiguous["target_source_pointers"] = {}
    ev = ambiguous["acs_validation"]; ev["target_sha256"] = target_identity(ambiguous)
    ev["attestation_id"] = "acs_reconstruction_attestation_v1__" + sha256_json({k: v for k, v in ev.items() if k != "attestation_id"})
    ambiguous["plan_id"] = "logical_cohort_plan__" + sha256_json({k: v for k, v in ambiguous.items() if k != "plan_id"})[:24]
    with pytest.raises(ValueError, match="ambiguous"): verify(ambiguous, **validation)


def test_valid_stale_and_other_product_prior_rejected(tmp_path):
    from test_acs_historical_materialization import parent, build
    from jobs.monthly_refresh.acs_history import inventory
    reg = registry(); paths = {}; records = {}; source = "census_acs1"
    parents = [parent(tmp_path, source, y, paths, records) for y in (2022, 2023)]
    authority = scope(reg, source, end=2022)
    old = build(tmp_path, reg, inventory(authority=authority, parents=parents[:1]), paths, records)["state"]
    accept(reg, old); authority = scope(reg, source, end=2023)
    inv = inventory(authority=authority, parents=parents)
    new = build(tmp_path, reg, inv, paths, records, mode="UPDATE", prior=old, name="new")["state"]
    accept(reg, new)
    for prior in (None, old):
        with pytest.raises(ValueError, match="exact current"):
            build(tmp_path, reg, inv, paths, records, mode="UPDATE", prior=prior, name="attack")
    other = "census_acs5"; authority5 = scope(reg, other, end=2022)
    parent5 = parent(tmp_path, other, 2022, paths, records)
    other_state = build(tmp_path, reg, inventory(authority=authority5, parents=[parent5]), paths, records, name="other")["state"]
    accept(reg, other_state)
    with pytest.raises(ValueError, match="exact current"):
        build(tmp_path, reg, inv, paths, records, mode="UPDATE", prior=other_state, name="attack")


def test_stale_current_pointer_fails_even_when_state_is_valid(tmp_path):
    from test_acs_historical_materialization import parent, build
    from jobs.monthly_refresh.acs_history import inventory
    reg = registry(); paths = {}; records = {}; source = "census_acs1"
    parents = [parent(tmp_path, source, y, paths, records) for y in (2022, 2023)]
    a = scope(reg, source, end=2022)
    old = build(tmp_path, reg, inventory(authority=a, parents=parents[:1]), paths, records)["state"]
    accept(reg, old); a = scope(reg, source, end=2023)
    new = build(tmp_path, reg, inventory(authority=a, parents=parents), paths, records, mode="UPDATE", prior=old, name="new")["state"]
    accept(reg, new); reg["current_state"][source] = old["state_id"]
    with pytest.raises(ValueError, match="stale"): ACSGovernance.from_fixture(reg)


@pytest.mark.parametrize("attack", ["unavailable", "duplicate", "diverged", "content_changed", "valid"])
def test_production_loader_fails_closed(tmp_path, monkeypatch, attack):
    import base64
    reg = registry(); reg["namespace"] = "production"
    original = main_governance(reg).production_snapshot()
    class Response:
        headers = {}
        def __init__(self, value): self.value = value
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def getcode(self): return 200
        def read(self): return json.dumps(self.value).encode()
    class Opener:
        def open(self, request):
            assert request.method == "GET"
            if request.full_url.endswith("/commits/main"): return Response({"sha": "b" * 40})
            if "/compare/" in request.full_url:
                return Response({"merge_base_commit": {"sha": ("c" if attack == "diverged" else "a") * 40}})
            assert request.full_url.endswith("/contents/config/acs_historical_governance.json?ref=" + "a" * 40)
            if attack == "unavailable": return Response(None)
            raw = json.dumps(reg)
            if attack == "duplicate": raw = raw[:-1] + ', "current_state": {}}'
            if attack == "content_changed": raw = json.dumps({**reg, "extra": "changed"})
            return Response({"content": base64.b64encode(raw.encode()).decode()})
    monkeypatch.setattr("urllib.request.build_opener", lambda *a, **k: Opener())
    if attack == "valid":
        gov = ACSGovernance.load_production("fixture/repo", "test", snapshot=original)
        assert gov.production_snapshot() == original
        with pytest.raises(ValueError, match="recovery-only"): gov.require_production()
    else:
        with pytest.raises(ValueError): ACSGovernance.load_production("fixture/repo", "test", snapshot=original)


@pytest.mark.parametrize("field", ["canonical_artifact_hash", "source_set_semantic_sha256", "target_source_pointers"])
def test_rehashed_promotion_cannot_change_immutable_graph(tmp_path, field):
    prepared, _, _, _, api, validation = promotion_fixture(tmp_path)
    bad = deepcopy(prepared)
    if field == "target_source_pointers": bad[field]["acs"] = "invented"
    else: bad[field] = "0" * 64
    from jobs.monthly_refresh.acs_attestation import target_identity
    ev = bad["acs_validation"]; ev["target_sha256"] = target_identity(bad)
    ev["attestation_id"] = "acs_reconstruction_attestation_v1__" + sha256_json({k: v for k, v in ev.items() if k != "attestation_id"})
    with pytest.raises((ValueError, PublicationError), match="ACS promotion|bind promoted"):
        verify(bad, promotion_api=api, promotion_workspace=tmp_path / "attack", **validation)


def test_recovery_proof_cannot_create_new_prepared_record(tmp_path):
    prepared, _, _, _, api, validation = promotion_fixture(tmp_path)
    proof = verify(prepared, promotion_api=api, promotion_workspace=tmp_path / "reverify", **validation)
    store = JSONCAS()
    with pytest.raises(ValueError, match="capability"):
        hosted.persist_prepared(store, prepared, acs_proof=proof)
    assert store.writes == 0 and store.value is None


@pytest.mark.parametrize("schema", [None, "acs_family_resolution_record_v1", "caller-invented"])
def test_schema_downgrade_cannot_authorize_storage(tmp_path, schema):
    plan, _ = durable_fixture(tmp_path)
    plan.pop("acs_validation")
    acs = plan["family_resolutions"]["acs"]
    acs.pop("schema_version")
    if schema is not None: acs["schema_version"] = schema
    plan["plan_id"] = "logical_cohort_plan__" + sha256_json({k: v for k, v in plan.items() if k != "plan_id"})[:24]
    validate_durable_plan(plan)  # Legacy remains readable, never writable.
    api = AuthorityAPI()
    with pytest.raises(ValueError, match="attestation"):
        GitHubCohortPlanStore(api, "main").put(plan)
    assert api.calls == []


def advance_remote(governance):
    # Simulate a real remote advance below the constrained production HTTP loader.
    from jobs.monthly_refresh.acs_scope import _READERS
    remote = _READERS[governance].opener
    remote.registry = {**deepcopy(remote.registry), "approval_revision": "G1"}
    remote.tip = "b" * 40


@pytest.mark.parametrize("boundary", ["plan", "prepared", "family", "publication"])
def test_g0_to_remote_g1_rejects_new_work_before_destination_io(tmp_path, boundary):
    if boundary == "plan":
        target, validation = durable_fixture(tmp_path); proof = verify(target, **validation)
    else:
        target, proof, _, _, _, validation = promotion_fixture(tmp_path)
    advance_remote(validation["governance"])
    api = AuthorityAPI()
    with pytest.raises(ValueError, match="superseded"):
        if boundary == "plan": GitHubCohortPlanStore(api, "main").put(target, acs_proof=proof)
        elif boundary == "prepared":
            store = JSONCAS(); store.api = api
            hosted.persist_prepared(store, target, acs_proof=proof)
        else:
            from jobs.monthly_refresh.acs_history_family import resolve_inputs, publish_family
            from jobs.monthly_refresh.acs_family_resolution import GitHubFamilyResolutionStore
            rec = target["acs_validation"]["resolution"]
            inputs = resolve_inputs(parents=rec["parents"], **validation)
            artifact = validation["resolver"].resolve(f"artifact://source/acs/{rec['output_artifact_id']}")
            if boundary == "family":
                GitHubFamilyResolutionStore(api, "main").put(rec, artifact=artifact,
                    catalog_record=inputs["records"][rec["output_artifact_id"]], validation_inputs=inputs)
            else: publish_family(artifact=artifact, api=api, cas=None, workspace=tmp_path, git_sha="test", **inputs)
    assert api.calls == []


def test_governance_advance_during_write_cannot_cross_main_cas(tmp_path):
    target, validation = durable_fixture(tmp_path); proof = verify(target, **validation)
    api = AuthorityAPI()
    def concurrent(api):
        advance_remote(validation["governance"]); api.tip = "b" * 40
    api.before_ref = concurrent
    with pytest.raises(PublicationError, match="non-fast-forward"):
        GitHubCohortPlanStore(api, "main").put(target, acs_proof=proof)
    assert api.value is None and api.tip == "b" * 40
    assert not any(m == "PUT" for m, _ in api.calls)


@pytest.mark.parametrize("boundary", ["plan", "prepared", "family", "publication", "recovery"])
def test_repository_a_authority_cannot_write_repository_b(tmp_path, boundary):
    if boundary == "plan": target, validation = durable_fixture(tmp_path); proof = verify(target, **validation)
    else: target, proof, catalog, readiness, _, validation = promotion_fixture(tmp_path)
    api = AuthorityAPI(); api.repository = "other/repository"
    with pytest.raises(ValueError, match="repository mismatch"):
        if boundary == "plan": GitHubCohortPlanStore(api, "main").put(target, acs_proof=proof)
        elif boundary == "prepared":
            store = JSONCAS(); store.api = api
            hosted.persist_prepared(store, target, acs_proof=proof)
        elif boundary == "recovery":
            store = CatalogCAS(catalog); store.api = api
            hosted.execute_one(target, store, JSONCAS(readiness), acs_proof=proof)
        else:
            from jobs.monthly_refresh.acs_history_family import resolve_inputs, publish_family
            from jobs.monthly_refresh.acs_family_resolution import GitHubFamilyResolutionStore
            rec = target["acs_validation"]["resolution"]
            inputs = resolve_inputs(parents=rec["parents"], **validation)
            artifact = validation["resolver"].resolve(f"artifact://source/acs/{rec['output_artifact_id']}")
            if boundary == "family": GitHubFamilyResolutionStore(api, "main").put(rec, artifact=artifact,
                catalog_record=inputs["records"][rec["output_artifact_id"]], validation_inputs=inputs)
            else: publish_family(artifact=artifact, api=api, cas=None, workspace=tmp_path, git_sha="test", **inputs)
    assert api.calls == []


def test_pinned_recovery_remains_valid_after_governance_advance(tmp_path):
    target, _, catalog, readiness, api, validation = promotion_fixture(tmp_path)
    advance_remote(validation["governance"])
    recovered = verify(target, promotion_api=api, promotion_workspace=tmp_path / "recover", **validation)
    store = CatalogCAS(catalog); ready = JSONCAS(readiness)
    assert hosted.execute_to_completion(target, store, ready, acs_proof=recovered)["exact_rerun_noop"]
    assert not hosted.execute_one(target, store, ready, acs_proof=recovered)["changed"]
    with pytest.raises(ValueError, match="capability"):
        hosted.persist_prepared(JSONCAS(), target, acs_proof=recovered)


def test_august_nrc_adapter_cannot_write_acs_plan():
    from jobs.monthly_refresh.august_nrc_geography_correction import require_correction_plan_write
    for plan in ({"sources": [{"source_id": "acs"}]}, {"family_resolutions": {"acs": {}}}):
        with pytest.raises(PublicationError, match="governed ACS"):
            require_correction_plan_write(plan)
    require_correction_plan_write({"sources": [{"source_id": "nrc"}]})
