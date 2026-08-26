"""Offline control plane for the Phase 3B Redfin/FRED source cohort."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from core.source_artifacts.hashing import sha256_file, write_canonical_json
from jobs.monthly_refresh.production import cycle_id, evaluate_barrier, redfin_ready

REQUIRED_SOURCES = ("redfin", "fred_macro")
PIN_FIELDS = ("candidate_artifact_id", "artifact_content_hash", "package_sha256",
              "publication_state", "provider_release_id")


def resolve_invocation(*, mode: str, policy_path: Path, drop: dict[str, Any] | None,
                       consumed_drop_ids: set[str], supplied_cycle_id: str | None = None) -> dict[str, Any]:
    if mode not in {"normal", "resume", "replay"}:
        raise ValueError("mode must be normal, resume, or replay")
    if mode == "normal" and not redfin_ready(drop, consumed_drop_ids):
        return {"status": "no_op", "reason": "no_eligible_redfin_catalyst", "fan_out": False,
                "invocation_mode": mode}
    if not drop:
        raise ValueError(f"{mode} requires explicit governed Redfin drop identity")
    required = ("drop_id", "drop_content_hash", "target_month")
    if any(not drop.get(k) for k in required):
        raise ValueError(f"{mode} requires explicit governed Redfin drop identity")
    resolved = cycle_id(redfin_drop_id=drop["drop_id"], redfin_drop_hash=drop["drop_content_hash"],
                        target_month=drop["target_month"], policy_sha256=sha256_file(policy_path))
    if mode in {"resume", "replay"} and not supplied_cycle_id:
        raise ValueError(f"{mode} requires explicit cycle identity")
    if supplied_cycle_id and supplied_cycle_id != resolved:
        raise ValueError("supplied cycle identity does not match governed drop identity")
    return {"status": "cycle_ready", "cycle_id": resolved, "drop_id": drop["drop_id"],
            "drop_content_hash": drop["drop_content_hash"], "target_month": drop["target_month"],
            "fan_out": True, "invocation_mode": mode}


def resume_plan(required: tuple[str, ...], previous_results: list[dict[str, Any]], *, expected_cycle_id: str) -> dict[str, Any]:
    decision = evaluate_barrier(expected_cycle_id=expected_cycle_id,
                                required_source_ids=required, results=previous_results)
    pins = {r["source_id"]: {k: r[k] for k in PIN_FIELDS} for r in decision.candidates}
    return {"reuse": list(decision.reusable_source_ids), "run": list(decision.retry_source_ids), "pins": pins}


def barrier_evidence(*, cycle: dict[str, Any], results: list[dict[str, Any]],
                     pins: dict[str, Any] | None, github: dict[str, Any]) -> dict[str, Any]:
    decision = evaluate_barrier(expected_cycle_id=cycle["cycle_id"], required_source_ids=REQUIRED_SOURCES,
                                results=results, pinned_candidates=pins)
    return {"schema_version": "monthly_source_cohort_evidence_v1", "cycle_id": cycle["cycle_id"],
            "invocation_mode": cycle["invocation_mode"], "barrier_status": decision.status,
            "candidates": list(decision.candidates), "reused_source_ids": list(decision.reusable_source_ids),
            "retry_source_ids": list(decision.retry_source_ids), "github": github,
            "source_set_created": False, "accepted_pointers_advanced": False,
            "redfin_consumption_committed": False}


def main() -> int:
    parser = argparse.ArgumentParser(); sub = parser.add_subparsers(dest="command", required=True)
    resolve = sub.add_parser("resolve"); resolve.add_argument("--mode", required=True); resolve.add_argument("--drop-json", type=Path)
    resolve.add_argument("--cycle-id"); resolve.add_argument("--policy", type=Path, default=Path("config/monthly_refresh_policy.json")); resolve.add_argument("--output", type=Path, required=True)
    barrier = sub.add_parser("barrier"); barrier.add_argument("--cycle-json", type=Path, required=True)
    barrier.add_argument("--result", action="append", type=Path, default=[]); barrier.add_argument("--pins-json", type=Path); barrier.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "resolve":
        drop = json.loads(args.drop_json.read_text()) if args.drop_json else None
        value = resolve_invocation(mode=args.mode, policy_path=args.policy, drop=drop,
                                   consumed_drop_ids=set(), supplied_cycle_id=args.cycle_id)
    else:
        cycle = json.loads(args.cycle_json.read_text()); results = [json.loads(p.read_text()) for p in args.result]
        pins = json.loads(args.pins_json.read_text()) if args.pins_json else None
        value = barrier_evidence(cycle=cycle, results=results, pins=pins, github={})
    write_canonical_json(args.output, value); print(json.dumps(value, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
