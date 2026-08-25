"""Pure, offline primitives for governed production monthly-refresh coordination.

Provider acquisition and remote publication deliberately live outside this module.
The coordinator records identities and decides whether a cohort may cross a barrier.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable


RESULT_VERSION = "monthly_source_execution_result_v1"


class CycleState(str, Enum):
    WAITING_FOR_REDFIN = "waiting_for_redfin"
    READY = "ready"
    SOURCE_REFRESH_RUNNING = "source_refresh_running"
    SOURCE_BARRIER = "source_barrier"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"
    SOURCE_SET_VALIDATED = "source_set_validated"
    CANONICAL_ASSEMBLY_VALIDATED = "canonical_assembly_validated"
    PROMOTION_COMMIT = "promotion_commit"
    COMPLETE = "complete"


def cycle_id(*, redfin_drop_id: str, redfin_drop_hash: str, target_month: str,
             policy_sha256: str) -> str:
    """Return a stable logical identity; Actions run/attempt IDs are only evidence."""
    semantic = json.dumps({"policy_sha256": policy_sha256, "redfin_drop_hash": redfin_drop_hash,
        "redfin_drop_id": redfin_drop_id, "target_month": target_month}, sort_keys=True,
        separators=(",", ":")).encode()
    return f"monthly_cycle__{target_month}__{hashlib.sha256(semantic).hexdigest()[:16]}"


def redfin_ready(drop: dict[str, Any] | None, consumed_drop_ids: set[str]) -> bool:
    """A catalyst is a complete governed drop, never merely inbox contents."""
    if not drop:
        return False
    return (drop.get("status") == "validated" and drop.get("validation_status") == "passed"
            and drop.get("complete_family_count") == drop.get("required_family_count") == 7
            and bool(drop.get("drop_content_hash"))
            and drop.get("drop_id") not in consumed_drop_ids
            and not drop.get("quarantined", False))


@dataclass(frozen=True)
class BarrierDecision:
    state: CycleState
    reusable_source_ids: tuple[str, ...]
    retry_source_ids: tuple[str, ...]
    candidates: tuple[dict[str, Any], ...]


def validate_source_result(result: dict[str, Any], *, expected_cycle_id: str) -> dict[str, Any]:
    required = {"schema_version", "source_id", "cycle_id", "status", "candidate_artifact_id",
        "artifact_content_hash", "package_sha256", "publication_state", "validation_status",
        "provider_release_id", "observation_max", "prior_artifact_id", "source_change_detected",
        "retryability", "evidence_uri"}
    if set(result) != required or result.get("schema_version") != RESULT_VERSION:
        raise ValueError("source execution result schema mismatch")
    if result["cycle_id"] != expected_cycle_id:
        raise ValueError("source result cycle identity mismatch")
    if result["status"] == "succeeded":
        if result["validation_status"] != "passed" or result["publication_state"] != "published_verified":
            raise ValueError("successful candidate is not validated and durably verified")
        for key in ("candidate_artifact_id", "artifact_content_hash", "package_sha256", "evidence_uri"):
            if not result[key]: raise ValueError(f"successful result missing {key}")
    return result


def evaluate_barrier(*, expected_cycle_id: str, required_source_ids: Iterable[str],
                     results: Iterable[dict[str, Any]], pinned_candidates: dict[str, str] | None = None) -> BarrierDecision:
    """Validate the complete cohort, retaining exact successes for deterministic retry."""
    required = tuple(sorted(set(required_source_ids))); by_source: dict[str, dict[str, Any]] = {}
    for raw in results:
        result = validate_source_result(raw, expected_cycle_id=expected_cycle_id)
        source = result["source_id"]
        if source in by_source or source not in required: raise ValueError("unexpected or duplicate source result")
        expected = (pinned_candidates or {}).get(source)
        if expected and result.get("candidate_artifact_id") != expected:
            raise ValueError(f"pinned candidate drift for {source}")
        by_source[source] = result
    retry = tuple(s for s in required if s not in by_source or by_source[s]["status"] != "succeeded")
    reusable = tuple(s for s in required if s in by_source and by_source[s]["status"] == "succeeded")
    if retry:
        terminal = any(s in by_source and by_source[s]["retryability"] == "terminal" for s in retry)
        state = CycleState.FAILED_TERMINAL if terminal else CycleState.FAILED_RETRYABLE
    else:
        state = CycleState.SOURCE_SET_VALIDATED
    return BarrierDecision(state, reusable, retry, tuple(by_source[s] for s in reusable))
