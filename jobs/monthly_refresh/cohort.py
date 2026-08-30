"""Offline control plane for the Phase 3B Redfin/FRED source cohort."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.production import evaluate_barrier, validate_source_result
from jobs.monthly_refresh.readiness import eligible_record

REQUIRED_SOURCES = ("redfin", "fred_macro", "ces")
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
                              registry: dict[str, Any], policy: dict[str, Any]) -> list[dict[str, Any]]:
    """Resolve fail-closed, cycle-scoped successful source pins from durable state."""
    if registry.get("schema_version") != RESULT_REGISTRY_VERSION:
        raise ValueError("unsupported monthly source cycle-result registry")
    if policy.get("source_execution_result_schema") != RESULT_CONTRACT:
        raise ValueError("source result contract is not compatible with durable pins")
    automated = {s["source_id"] for s in policy["sources"] if s.get("acquisition_mode") == "automated"}
    resolved = []
    records = [r for r in registry.get("records", []) if r.get("cycle_id") == cycle["cycle_id"]]
    if len({r.get("source_id") for r in records}) != len(records):
        raise ValueError("duplicate durable source result for cycle")
    for record in records:
        source_id = record.get("source_id")
        if source_id not in automated or source_id not in REQUIRED_SOURCES:
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
                           registry: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    """Classify every source generically; replay and normal never use resume pins."""
    if cycle["invocation_mode"] != "resume":
        return {"reuse": [], "run": list(REQUIRED_SOURCES), "results": [], "pins": {}}
    results = [durable_redfin_result(cycle=cycle, catalog=catalog),
               *durable_automated_results(cycle=cycle, catalog=catalog, registry=registry, policy=policy)]
    plan = resume_plan(REQUIRED_SOURCES, results, expected_cycle_id=cycle["cycle_id"])
    return {**plan, "results": results}


def resume_plan(required: tuple[str, ...], previous_results: list[dict[str, Any]], *, expected_cycle_id: str) -> dict[str, Any]:
    decision = evaluate_barrier(expected_cycle_id=expected_cycle_id,
                                required_source_ids=required, results=previous_results)
    pins = {r["source_id"]: {k: r[k] for k in PIN_FIELDS} for r in decision.candidates}
    return {"reuse": list(decision.reusable_source_ids), "run": list(decision.retry_source_ids), "pins": pins}


def barrier_evidence(*, cycle: dict[str, Any], results: list[dict[str, Any]],
                     pins: dict[str, Any] | None, github: dict[str, Any],
                     reused_results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    reused = [validate_source_result(result, expected_cycle_id=cycle["cycle_id"])
              for result in (reused_results or [])]
    if any(result["status"] != "succeeded" for result in reused):
        raise ValueError("reused source result must be a successful cycle pin")
    decision = evaluate_barrier(expected_cycle_id=cycle["cycle_id"], required_source_ids=REQUIRED_SOURCES,
                                results=[*results, *reused], pinned_candidates=pins)
    return {"schema_version": "monthly_source_cohort_evidence_v1", "cycle_id": cycle["cycle_id"],
            "invocation_mode": cycle["invocation_mode"], "barrier_status": decision.status,
            "candidates": list(decision.candidates),
            "reused_source_ids": sorted(result["source_id"] for result in reused),
            "retry_source_ids": list(decision.retry_source_ids), "github": github,
            "source_set_created": False, "accepted_pointers_advanced": False,
            "redfin_consumption_committed": False}


def main() -> int:
    parser = argparse.ArgumentParser(); sub = parser.add_subparsers(dest="command", required=True)
    resolve = sub.add_parser("resolve"); resolve.add_argument("--mode", required=True)
    resolve.add_argument("--readiness", type=Path, default=Path("config/monthly_refresh_readiness.json"))
    resolve.add_argument("--catalog", type=Path, default=Path("config/artifact_catalog.json"))
    resolve.add_argument("--cycle-id"); resolve.add_argument("--policy", type=Path, default=Path("config/monthly_refresh_policy.json")); resolve.add_argument("--output", type=Path, required=True)
    plan = sub.add_parser("resume-plan"); plan.add_argument("--cycle-json", type=Path, required=True)
    plan.add_argument("--catalog", type=Path, default=Path("config/artifact_catalog.json"))
    plan.add_argument("--registry", type=Path, default=Path("config/monthly_source_cycle_results.json"))
    plan.add_argument("--policy", type=Path, default=Path("config/monthly_refresh_policy.json")); plan.add_argument("--output", type=Path, required=True)
    barrier = sub.add_parser("barrier"); barrier.add_argument("--cycle-json", type=Path, required=True)
    barrier.add_argument("--result", action="append", type=Path, default=[])
    barrier.add_argument("--reused-result", action="append", type=Path, default=[])
    barrier.add_argument("--pins-json", type=Path); barrier.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "resolve":
        value = resolve_invocation(mode=args.mode, policy_path=args.policy,
            readiness=json.loads(args.readiness.read_text()), catalog=json.loads(args.catalog.read_text()),
            supplied_cycle_id=args.cycle_id)
    elif args.command == "resume-plan":
        from jobs.monthly_refresh.cycle_results import load_registry
        value = resolve_resume_results(cycle=json.loads(args.cycle_json.read_text()),
            catalog=json.loads(args.catalog.read_text()), registry=load_registry(args.registry),
            policy=json.loads(args.policy.read_text()))
    else:
        cycle = json.loads(args.cycle_json.read_text()); results = [json.loads(p.read_text()) for p in args.result]
        reused_results = [json.loads(p.read_text()) for p in args.reused_result]
        pins = json.loads(args.pins_json.read_text()) if args.pins_json else None
        value = barrier_evidence(cycle=cycle, results=results, reused_results=reused_results,
                                 pins=pins, github={})
    write_canonical_json(args.output, value); print(json.dumps(value, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
