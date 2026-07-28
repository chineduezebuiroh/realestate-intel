from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping

from .artifact_writer import ReviewArtifactWriter


@dataclass(slots=True)
class ReviewManifest:
    schema_version: str
    campaign_id: str
    run_id: str
    created_at: str
    framework_version: str
    source_run_id: str
    challenger_run_id: str | None = None
    outputs: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        required_text_fields = {
            "schema_version": self.schema_version,
            "campaign_id": self.campaign_id,
            "run_id": self.run_id,
            "created_at": self.created_at,
            "framework_version": self.framework_version,
            "source_run_id": self.source_run_id,
        }

        empty_fields = [
            field_name
            for field_name, value in required_text_fields.items()
            if not value.strip()
        ]

        if empty_fields:
            raise ValueError(
                "ReviewManifest requires non-empty fields: "
                f"{sorted(empty_fields)}"
            )

        for index, output in enumerate(self.outputs):
            required = {"path"}

            missing = required.difference(output)

            if missing:
                raise ValueError(
                    f"outputs[{index}] is missing fields: "
                    f"{sorted(missing)}"
                )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> ReviewManifest:
        return cls(
            schema_version=str(payload["schema_version"]),
            campaign_id=str(payload["campaign_id"]),
            run_id=str(payload["run_id"]),
            created_at=str(payload["created_at"]),
            framework_version=str(
                payload["framework_version"]
            ),
            source_run_id=str(payload["source_run_id"]),
            challenger_run_id=(
                None
                if payload.get("challenger_run_id") is None
                else str(payload["challenger_run_id"])
            ),
            outputs=[
                dict(output)
                for output in payload.get("outputs", [])
            ],
            metadata=dict(payload.get("metadata", {})),
        )

    def write(
        self,
        writer: ReviewArtifactWriter,
    ):
        return writer.write_manifest(self.to_dict())
