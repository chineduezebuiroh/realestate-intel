from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Mapping

from .artifact_writer import ReviewArtifactWriter


DecisionRecommendation = Literal[
    "promote",
    "reject",
    "needs_review",
]


@dataclass(slots=True)
class DecisionSummary:
    recommendation: DecisionRecommendation
    rationale: str
    reviewer: str | None = None
    approved: bool = False
    notes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        allowed = {
            "promote",
            "reject",
            "needs_review",
        }

        if self.recommendation not in allowed:
            raise ValueError(
                "Unsupported recommendation: "
                f"{self.recommendation!r}"
            )

        if not self.rationale.strip():
            raise ValueError(
                "DecisionSummary.rationale must be non-empty"
            )

        if self.approved and self.recommendation == "needs_review":
            raise ValueError(
                "A needs_review recommendation cannot be approved"
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> DecisionSummary:
        return cls(
            recommendation=payload["recommendation"],
            rationale=str(payload["rationale"]),
            reviewer=(
                None
                if payload.get("reviewer") is None
                else str(payload["reviewer"])
            ),
            approved=bool(payload.get("approved", False)),
            notes=[
                str(note)
                for note in payload.get("notes", [])
            ],
            metadata=dict(payload.get("metadata", {})),
        )

    def write(
        self,
        writer: ReviewArtifactWriter,
    ):
        return writer.write_decision_summary(
            self.to_dict()
        )
