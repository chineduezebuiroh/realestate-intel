from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .campaign import SUPPORTED_CAMPAIGN_PHASES, _json_value, _text


SUPPORTED_GATE_SEVERITIES = frozenset({"blocking", "warning", "informational"})
SUPPORTED_GATE_STATUSES = frozenset({"pass", "warn", "fail", "not_evaluated"})


@dataclass(frozen=True, slots=True)
class PromotionGate:
    gate_id: str
    gate_version: str
    title: str
    description: str
    campaign_phase: str
    severity: str
    required_evidence: tuple[str, ...]
    evaluation_scope: dict[str, Any]
    thresholds: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("gate_id", "gate_version", "title", "description"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        phase = _text(self.campaign_phase, "campaign_phase").lower()
        severity = _text(self.severity, "severity").lower()
        if phase not in SUPPORTED_CAMPAIGN_PHASES:
            raise ValueError(f"Unsupported campaign_phase: {phase!r}")
        if severity not in SUPPORTED_GATE_SEVERITIES:
            raise ValueError(f"Unsupported gate severity: {severity!r}")
        object.__setattr__(self, "campaign_phase", phase)
        object.__setattr__(self, "severity", severity)
        evidence = tuple(sorted(_text(value, "required_evidence") for value in self.required_evidence))
        if not evidence or len(evidence) != len(set(evidence)):
            raise ValueError("required_evidence must contain unique evidence IDs")
        object.__setattr__(self, "required_evidence", evidence)
        scope = _json_value(self.evaluation_scope, "evaluation_scope")
        if not scope:
            raise ValueError("evaluation_scope must be non-empty")
        object.__setattr__(self, "evaluation_scope", scope)
        object.__setattr__(self, "thresholds", _json_value(self.thresholds, "thresholds"))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PromotionGateResult:
    gate_id: str
    gate_version: str
    candidate_policy_id: str
    status: str
    severity: str
    measured_values: dict[str, Any]
    thresholds: dict[str, Any]
    evaluation_scope: dict[str, Any]
    evidence_references: tuple[str, ...]
    rationale: str
    exceptions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("gate_id", "gate_version", "candidate_policy_id", "rationale"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        status = _text(self.status, "status").lower()
        severity = _text(self.severity, "severity").lower()
        if status not in SUPPORTED_GATE_STATUSES:
            raise ValueError(f"Unsupported gate status: {status!r}")
        if severity not in SUPPORTED_GATE_SEVERITIES:
            raise ValueError(f"Unsupported gate severity: {severity!r}")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "severity", severity)
        for name in ("measured_values", "thresholds", "evaluation_scope"):
            normalized = _json_value(getattr(self, name), name)
            if name == "evaluation_scope" and not normalized:
                raise ValueError("evaluation_scope must be non-empty")
            object.__setattr__(self, name, normalized)
        references = tuple(sorted(_text(value, "evidence_references") for value in self.evidence_references))
        if len(references) != len(set(references)):
            raise ValueError("evidence_references must be unique")
        object.__setattr__(self, "evidence_references", references)
        exceptions = tuple(sorted(_text(value, "exceptions") for value in self.exceptions))
        if len(exceptions) != len(set(exceptions)):
            raise ValueError("exceptions must be unique")
        object.__setattr__(self, "exceptions", exceptions)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
