"""ACS evidence is portable; authority comes from reconstruction, never its hash.

The process-local capability is deliberately not serializable. Persisted evidence
must be revalidated against its exact remote main snapshot and immutable parents.
"""
from copy import deepcopy
from weakref import WeakKeyDictionary

from core.source_artifacts.hashing import sha256_json
from jobs.monthly_refresh.acs_history_family import RECORD_VERSION, validate_cohort

VERSION = "acs_reconstruction_attestation_v1"
_CAPABILITIES = WeakKeyDictionary()


class _ValidatedACS:
    pass


def target_identity(target):
    return sha256_json({k: v for k, v in target.items() if k not in {"acs_validation", "plan_id"}})


def issue(target, record, *, governance, promotion_api=None, promotion_workspace=None, **inputs):
    governance.require_production()
    validate_cohort(record, governance=governance, **inputs)
    evidence = {"schema_version": VERSION, "target_sha256": target_identity(target),
                "governance": governance.production_snapshot(), "resolution": deepcopy(record)}
    if "target_source_pointers" in target:
        evidence["object_bindings"] = _catalog_bindings({**target, "acs_validation": evidence}, inputs["catalog"])
    evidence["attestation_id"] = VERSION + "__" + sha256_json(evidence)
    result = {**deepcopy(target), "acs_validation": evidence}
    _bindings(result, inputs["catalog"], promotion_api, promotion_workspace)
    return result, _mint(result, governance=governance)


def check(target, record=None):
    evidence = target.get("acs_validation", {})
    semantic = {k: v for k, v in evidence.items() if k != "attestation_id"}
    if (evidence.get("schema_version") != VERSION
            or evidence.get("target_sha256") != target_identity(target)
            or evidence.get("attestation_id") != VERSION + "__" + sha256_json(semantic)
            or evidence.get("resolution", {}).get("schema_version") != RECORD_VERSION
            or (record is not None and evidence.get("resolution") != record)):
        raise ValueError("ACS reconstruction attestation absent or inconsistent")
    return evidence


def _mint(target, *, governance, recovery=False):
    proof = _ValidatedACS()
    _CAPABILITIES[proof] = (sha256_json(target), recovery, governance)
    return proof


def verify(target, *, governance, promotion_api=None, promotion_workspace=None, **inputs):
    evidence = check(target)
    if evidence["governance"] != governance.production_snapshot():
        raise ValueError("ACS attestation governance snapshot mismatch")
    validate_cohort(evidence["resolution"], governance=governance,
                    _recovery="target_source_pointers" in target, **inputs)
    _bindings(target, inputs["catalog"], promotion_api, promotion_workspace)
    return _mint(target, governance=governance, recovery="target_source_pointers" in target)


def _catalog_bindings(target, catalog):
    identities = (("source", target["acs_validation"]["resolution"]["output_artifact_id"]),
                  ("source_set", target["source_set_id"]),
                  ("canonical_market", target["canonical_artifact_id"]))
    result = []
    for kind, identity in identities:
        matches = [r for r in catalog["immutable_records"] if r["object_type"] == kind and r["object_id"] == identity]
        if len(matches) != 1:
            raise ValueError("ACS promotion catalog binding does not resolve once")
        result.append({k: matches[0][k] for k in ("object_type", "object_id", "artifact_content_hash", "package_sha256")})
    return result


def check_catalog_bindings(target, catalog):
    if check(target).get("object_bindings") != _catalog_bindings(target, catalog):
        raise ValueError("ACS promotion catalog bindings differ from verified packages")


def _bindings(target, catalog, api, workspace):
    if "target_source_pointers" in target and "family_resolutions" in target:
        raise ValueError("ambiguous ACS attestation target kind")
    evidence = check(target)
    if "target_source_pointers" in target:
        if api is None or workspace is None:
            raise ValueError("promotion proof requires verified immutable object bindings")
        check_catalog_bindings(target, catalog)
        from jobs.monthly_refresh.cohort_promotion_hosted import _promotion_objects
        _promotion_objects(target, catalog, api, workspace)
    elif "family_resolutions" in target:
        if target["family_resolutions"].get("acs") != evidence["resolution"]:
            raise ValueError("durable plan ACS resolution differs from reconstructed evidence")
        record = evidence["resolution"]
        acs = [s for s in target["sources"] if s["source_id"] == "acs"]
        if len(acs) != 1 or any(acs[0].get(k) != record[v] for k, v in
                (("artifact_id", "output_artifact_id"), ("artifact_content_hash", "output_content_hash"),
                 ("package_sha256", "output_package_sha256"), ("resolution_id", "resolution_id"))):
            raise ValueError("durable plan ACS source differs from reconstructed resolution")
    else:
        raise ValueError("unsupported ACS attestation target")


def require(target, proof, *, preparing=False):
    if (type(proof) is not _ValidatedACS or proof not in _CAPABILITIES or _CAPABILITIES[proof][0] != sha256_json(target)
            or (preparing and _CAPABILITIES[proof][1])):
        raise ValueError("ACS storage/promotion requires independently reconstructed attestation capability")


def authority(target, proof, repository, *, preparing=False):
    """Recover provenance only from a registered, exact-object capability."""
    require(target, proof, preparing=preparing)
    governance = _CAPABILITIES[proof][2]
    governance.require_destination(repository, recovery=not preparing)
    return governance
