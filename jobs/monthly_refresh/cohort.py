"""Offline control plane for the Phase 3B Redfin/FRED source cohort."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from core.source_artifacts.hashing import write_canonical_json
from core.source_artifacts.hashing import sha256_json
from jobs.monthly_refresh.production import evaluate_barrier, validate_source_result
from jobs.monthly_refresh.readiness import eligible_record

EXECUTION_REGISTRY = Path("config/monthly_source_execution_registry.json")
READINESS = Path("config/monthly_refresh_readiness.json")
CATALOG = Path("config/artifact_catalog.json")
RESULT_REGISTRY = Path("config/monthly_source_cycle_results.json")
POLICY = Path("config/monthly_refresh_policy.json")


def _authority_path(root: Path | None, path: Path) -> Path:
    """Resolve a control-plane input inside one pre-acquired authority snapshot."""
    if root is None:
        return path
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("authority input paths must remain inside the authority snapshot")
    return root / path


def required_sources(policy: dict[str, Any] | None = None) -> tuple[str, ...]:
    """Resolve the implemented hosted barrier inventory from governed policy."""
    registry = policy
    if registry is None or registry.get("schema_version") != "monthly_source_execution_registry_v1":
        registry = json.loads(EXECUTION_REGISTRY.read_text())
    sources = registry.get("members")
    if not isinstance(sources, list):
        raise ValueError("monthly policy source inventory is missing")
    enabled = [str(item.get("source_id") or "") for item in sources
               if item.get("required") is True and item.get("hosted_cohort_enabled") is True]
    if not enabled or any(not source for source in enabled) or len(enabled) != len(set(enabled)):
        raise ValueError("monthly policy hosted source inventory is invalid")
    return tuple(enabled)


# Compatibility export for source-specific smoke/tooling. Runtime paths resolve
# membership from policy rather than this value.
REQUIRED_SOURCES = ("redfin", "fred_macro", "ces", "laus", "census_bps",
                    "census_bps_provisional", "census_acs1", "census_acs5",
                    "bea_gdp_qtr", "bea_gdp_ann",
                    "census_nrc")
LOGICAL_DIRECT_SOURCES = ("fred_macro", "ces", "laus", "redfin", "bps", "acs",
                          "bea_gdp_qtr", "bea_gdp_ann", "census_nrc")
FAMILY_PHYSICAL_SOURCES = frozenset({"census_bps", "census_bps_provisional",
                                     "census_acs1", "census_acs5"})
PIN_FIELDS = ("candidate_artifact_id", "artifact_content_hash", "package_sha256",
              "publication_state", "provider_release_id")
RESULT_REGISTRY_VERSION = "monthly_source_cycle_results_v1"
RESULT_CONTRACT = "monthly_source_execution_result_v1"


def resolve_invocation(*, mode: str, policy_path: Path, readiness: dict[str, Any],
                       catalog: dict[str, Any], supplied_cycle_id: str | None = None) -> dict[str, Any]:
    if mode not in {"normal", "resume", "replay"}:
        raise ValueError("mode must be normal, resume, or replay")
    if mode in {"resume", "replay"} and not supplied_cycle_id:
        raise ValueError(f"{mode} requires explicit cycle identity")
    record = eligible_record(readiness, catalog=catalog, policy_path=policy_path,
                             requested_cycle_id=supplied_cycle_id if mode in {"resume", "replay"} else None)
    if mode == "normal" and record is None:
        return {"status": "no_op", "reason": "no_eligible_redfin_catalyst", "fan_out": False,
                "invocation_mode": mode}
    if record is None: raise ValueError(f"{mode} governed Redfin readiness did not resolve")
    pin = {k:record[k] for k in ("candidate_artifact_id", "artifact_content_hash", "package_sha256",
        "publication_state", "release_id", "asset_id")}
    return {"status":"cycle_ready", "cycle_id":record["cycle_id"], "drop_id":record["drop_id"],
        "drop_content_hash":record["drop_content_hash"], "target_month":record["target_month"],
        "readiness_id":record["readiness_id"], "redfin_candidate_pin":pin,
        "fan_out":True, "invocation_mode":mode}


def durable_redfin_result(*, cycle: dict[str, Any], catalog: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct a resume pin only from the validated readiness/catalog record."""
    pin = cycle["redfin_candidate_pin"]
    matches = [r for r in catalog["immutable_records"] if r["object_type"] == "source"
               and r["object_id"] == pin["candidate_artifact_id"]]
    if len(matches) != 1:
        raise ValueError("pinned Redfin artifact does not resolve exactly once")
    item = matches[0]
    for key in ("artifact_content_hash", "package_sha256", "publication_state", "release_id", "asset_id"):
        if item[key] != pin[key]:
            raise ValueError(f"pinned Redfin durable identity drift: {key}")
    accepted = catalog["accepted"]["source"].get("redfin")
    prior = next((r for r in catalog["immutable_records"] if r["object_id"] == accepted), None)
    result = {"schema_version":"monthly_source_execution_result_v1","source_id":"redfin",
        "cycle_id":cycle["cycle_id"],"status":"succeeded","candidate_artifact_id":item["object_id"],
        "artifact_content_hash":item["artifact_content_hash"],"package_sha256":item["package_sha256"],
        "publication_state":"published_verified","validation_status":"passed",
        "provider_release_id":item["metadata"]["provider_release_id"],
        "observation_max":item["metadata"]["observation_max"],"prior_artifact_id":accepted,
        "source_change_detected":prior is None or item["metadata"]["data_sha256"] != prior["metadata"]["data_sha256"],
        "retryability":"not_applicable","accepted_pointer_changed":False,"evidence_uri":item["logical_artifact_uri"]}
    return validate_source_result(result, expected_cycle_id=cycle["cycle_id"])


def durable_automated_results(*, cycle: dict[str, Any], catalog: dict[str, Any],
                              registry: dict[str, Any], policy: dict[str, Any],
                              execution_registry: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Resolve fail-closed, cycle-scoped successful source pins from durable state."""
    if registry.get("schema_version") != RESULT_REGISTRY_VERSION:
        raise ValueError("unsupported monthly source cycle-result registry")
    if policy.get("source_execution_result_schema") != RESULT_CONTRACT:
        raise ValueError("source result contract is not compatible with durable pins")
    required = set(required_sources(execution_registry or policy))
    # Hosted execution membership is governed by the execution registry.  The
    # stable refresh policy remains part of cycle identity and deliberately is
    # not rewritten as sources join the hosted barrier.
    automated = required - {"redfin"}
    resolved = []
    records = [r for r in registry.get("records", []) if r.get("cycle_id") == cycle["cycle_id"]]
    if len({r.get("source_id") for r in records}) != len(records):
        raise ValueError("duplicate durable source result for cycle")
    for record in records:
        source_id = record.get("source_id")
        if source_id not in automated or source_id not in required:
            continue
        if record.get("result_contract") != RESULT_CONTRACT or record.get("policy_schema_version") != policy.get("schema_version"):
            continue  # explicitly incompatible/expired evidence is selected for rerun
        result = validate_source_result(record["result"], expected_cycle_id=cycle["cycle_id"])
        if result["source_id"] != source_id or result["status"] != "succeeded":
            continue
        if result["validation_status"] != "passed" or result["publication_state"] != "published_verified":
            continue
        if result["accepted_pointer_changed"] is not False:
            raise ValueError(f"durable {source_id} result changed accepted pointer")
        matches = [item for item in catalog["immutable_records"]
                   if item.get("object_type") == "source" and item.get("object_id") == result["candidate_artifact_id"]]
        if len(matches) != 1:
            continue  # missing durable catalog evidence is not reusable
        item = matches[0]
        expected = {"artifact_content_hash": item.get("artifact_content_hash"),
                    "package_sha256": item.get("package_sha256"),
                    "publication_state": "published_verified",
                    "provider_release_id": item.get("metadata", {}).get("provider_release_id")}
        for field, value in expected.items():
            if result[field] != value:
                raise ValueError(f"durable {source_id} identity drift: {field}")
        if item.get("publication_state") != "published_immutable_verified" or item.get("metadata", {}).get("source_id") != source_id:
            raise ValueError(f"durable {source_id} catalog governance mismatch")
        resolved.append(result)
    return sorted(resolved, key=lambda result: result["source_id"])


def resolve_resume_results(*, cycle: dict[str, Any], catalog: dict[str, Any],
                           registry: dict[str, Any], policy: dict[str, Any],
                           execution_registry: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve durable completion before fan-out for normal and resume.

    Invocation mode does not grant permission to replace an already-complete
    cycle/source result. Replay retains its separate governed execution path.
    """
    required = required_sources(execution_registry or policy)
    if cycle["invocation_mode"] == "replay":
        return {"reuse": [], "run": list(required), "results": [], "pins": {}}
    results = [durable_redfin_result(cycle=cycle, catalog=catalog),
               *durable_automated_results(cycle=cycle, catalog=catalog, registry=registry,
                                          policy=policy, execution_registry=execution_registry)]
    plan = resume_plan(required, results, expected_cycle_id=cycle["cycle_id"])
    return {**plan, "results": results}


def resume_plan(required: tuple[str, ...], previous_results: list[dict[str, Any]], *, expected_cycle_id: str) -> dict[str, Any]:
    decision = evaluate_barrier(expected_cycle_id=expected_cycle_id,
                                required_source_ids=required, results=previous_results)
    pins = {r["source_id"]: {k: r[k] for k in PIN_FIELDS} for r in decision.candidates}
    return {"reuse": list(decision.reusable_source_ids), "run": list(decision.retry_source_ids), "pins": pins}


def barrier_evidence(*, cycle: dict[str, Any], results: list[dict[str, Any]],
                     pins: dict[str, Any] | None, github: dict[str, Any],
                     reused_results: list[dict[str, Any]] | None = None,
                     policy: dict[str, Any] | None = None) -> dict[str, Any]:
    reused = [validate_source_result(result, expected_cycle_id=cycle["cycle_id"])
              for result in (reused_results or [])]
    if any(result["status"] != "succeeded" for result in reused):
        raise ValueError("reused source result must be a successful cycle pin")
    if policy is None:
        policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    decision = evaluate_barrier(expected_cycle_id=cycle["cycle_id"], required_source_ids=required_sources(policy),
                                results=[*results, *reused], pinned_candidates=pins)
    return {"schema_version": "monthly_source_cohort_evidence_v1", "cycle_id": cycle["cycle_id"],
            "invocation_mode": cycle["invocation_mode"], "barrier_status": decision.status,
            "candidates": list(decision.candidates),
            "reused_source_ids": sorted(result["source_id"] for result in reused),
            "retry_source_ids": list(decision.retry_source_ids), "github": github,
            "source_set_created": False, "accepted_pointers_advanced": False,
            "redfin_consumption_committed": False}


def logical_cohort_plan(*, physical_evidence: dict[str, Any],
                        bps_resolution: dict[str, Any],
                        acs_resolution: dict[str, Any]) -> dict[str, Any]:
    """Validate post-barrier family outputs and return a non-promoting input plan."""
    if physical_evidence.get("barrier_status") != "ready":
        raise ValueError("logical planning requires a ready physical barrier")
    candidates = physical_evidence.get("candidates", [])
    by_source = {item.get("source_id"): item for item in candidates}
    if len(by_source) != len(candidates) or set(by_source) != set(REQUIRED_SOURCES):
        raise ValueError("exact 11-source physical barrier inventory mismatch")
    if any(not item.get("candidate_artifact_id") for item in candidates):
        raise ValueError("physical candidate identity is absent")

    def family(record: dict[str, Any], logical: str, parents: set[str]) -> dict[str, Any]:
        record = record.get("record", record)
        if record.get("accepted_pointer_changed") is not False or record.get("source_set_created") is not False:
            raise ValueError(f"{logical} resolution contains forbidden mutation")
        actual = {p.get("source_id"): p.get("artifact_id") for p in record.get("parents", [])}
        expected = {source: by_source[source]["candidate_artifact_id"] for source in parents}
        if actual != expected:
            raise ValueError(f"{logical} resolution did not consume exact cohort parents")
        artifact_id = record.get("output_artifact_id")
        if not artifact_id or not artifact_id.startswith(f"src__{logical}__"):
            raise ValueError(f"{logical} immutable output identity is absent")
        return {"source_id": logical, "artifact_id": artifact_id,
                "artifact_content_hash": record.get("output_content_hash"),
                "package_sha256": record.get("output_package_sha256"),
                "resolution_id": record.get("resolution_id")}

    bps_record = bps_resolution.get("record", bps_resolution)
    acs_record = acs_resolution.get("record", acs_resolution)
    bps = family(bps_record, "bps", {"census_bps", "census_bps_provisional"})
    acs = family(acs_record, "acs", {"census_acs1", "census_acs5"})
    direct = [{"source_id": source, "artifact_id": by_source[source]["candidate_artifact_id"],
               "artifact_content_hash": by_source[source]["artifact_content_hash"],
               "package_sha256": by_source[source]["package_sha256"]}
              for source in LOGICAL_DIRECT_SOURCES if source not in {"bps", "acs"}]
    indexed = {item["source_id"]: item for item in [*direct, bps, acs]}
    if set(indexed) != set(LOGICAL_DIRECT_SOURCES):
        raise AssertionError("logical/direct cohort inventory drift")
    payload = {"schema_version": "monthly_logical_cohort_plan_v1", "plan_id": "",
            "cycle_id": physical_evidence["cycle_id"],
            "physical_source_inventory": list(REQUIRED_SOURCES),
            "physical_candidates": [{"source_id": source,
                "artifact_id": by_source[source]["candidate_artifact_id"],
                "artifact_content_hash": by_source[source]["artifact_content_hash"],
                "package_sha256": by_source[source]["package_sha256"]}
                for source in REQUIRED_SOURCES],
            "physical_results": [by_source[source] for source in REQUIRED_SOURCES],
            "logical_source_inventory": list(LOGICAL_DIRECT_SOURCES),
            "sources": [indexed[source] for source in LOGICAL_DIRECT_SOURCES],
            "family_resolutions": {"bps": bps_record, "acs": acs_record},
            "family_resolution_order": ["physical_barrier", "bps", "acs", "logical_plan"],
            "accepted_pointers_advanced": False, "source_set_created": False,
            "canonical_market_created": False, "serving_market_created": False,
            "redfin_consumption_committed": False}
    payload["plan_id"] = "logical_cohort_plan__" + sha256_json(
        {key:value for key,value in payload.items() if key != "plan_id"})[:24]
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(); sub = parser.add_subparsers(dest="command", required=True)
    resolve = sub.add_parser("resolve"); resolve.add_argument("--mode", required=True)
    resolve.add_argument("--authority-root", type=Path,
        help="root of one read-only production authority snapshot")
    resolve.add_argument("--readiness", type=Path, default=READINESS)
    resolve.add_argument("--catalog", type=Path, default=CATALOG)
    resolve.add_argument("--cycle-id"); resolve.add_argument("--policy", type=Path, default=POLICY); resolve.add_argument("--output", type=Path, required=True)
    plan = sub.add_parser("resume-plan"); plan.add_argument("--cycle-json", type=Path, required=True)
    plan.add_argument("--authority-root", type=Path,
        help="root of one read-only production authority snapshot")
    plan.add_argument("--catalog", type=Path, default=CATALOG)
    plan.add_argument("--registry", type=Path, default=RESULT_REGISTRY)
    plan.add_argument("--policy", type=Path, default=POLICY); plan.add_argument("--output", type=Path, required=True)
    barrier = sub.add_parser("barrier"); barrier.add_argument("--cycle-json", type=Path, required=True)
    barrier.add_argument("--result", action="append", type=Path, default=[])
    barrier.add_argument("--reused-result", action="append", type=Path, default=[])
    barrier.add_argument("--pins-json", type=Path)
    barrier.add_argument("--policy", type=Path, default=Path("config/monthly_refresh_policy.json"))
    barrier.add_argument("--output", type=Path, required=True)
    logical = sub.add_parser("logical-plan")
    logical.add_argument("--physical-evidence", type=Path, required=True)
    logical.add_argument("--bps-resolution", type=Path, required=True)
    logical.add_argument("--acs-resolution", type=Path, required=True)
    logical.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "resolve":
        policy = _authority_path(args.authority_root, args.policy)
        readiness = _authority_path(args.authority_root, args.readiness)
        catalog = _authority_path(args.authority_root, args.catalog)
        value = resolve_invocation(mode=args.mode, policy_path=policy,
            readiness=json.loads(readiness.read_text()), catalog=json.loads(catalog.read_text()),
            supplied_cycle_id=args.cycle_id)
    elif args.command == "resume-plan":
        from jobs.monthly_refresh.cycle_results import load_registry
        catalog = _authority_path(args.authority_root, args.catalog)
        registry = _authority_path(args.authority_root, args.registry)
        policy = _authority_path(args.authority_root, args.policy)
        execution_registry = _authority_path(args.authority_root, EXECUTION_REGISTRY)
        value = resolve_resume_results(cycle=json.loads(args.cycle_json.read_text()),
            catalog=json.loads(catalog.read_text()), registry=load_registry(registry),
            policy=json.loads(policy.read_text()),
            execution_registry=json.loads(execution_registry.read_text()))
    elif args.command == "barrier":
        cycle = json.loads(args.cycle_json.read_text()); results = [json.loads(p.read_text()) for p in args.result]
        reused_results = [json.loads(p.read_text()) for p in args.reused_result]
        pins = json.loads(args.pins_json.read_text()) if args.pins_json else None
        value = barrier_evidence(cycle=cycle, results=results, reused_results=reused_results,
                                 pins=pins, github={}, policy=json.loads(args.policy.read_text()))
    else:
        value = logical_cohort_plan(
            physical_evidence=json.loads(args.physical_evidence.read_text()),
            bps_resolution=json.loads(args.bps_resolution.read_text()),
            acs_resolution=json.loads(args.acs_resolution.read_text()))
    write_canonical_json(args.output, value); print(json.dumps(value, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
