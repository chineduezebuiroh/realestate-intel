from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping

from regime._06_axis_engine import _build_axis_weights


SUPPORTED_CAMPAIGN_PHASES = frozenset({"phase_a", "phase_b"})


def _text(value: object, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _json_value(value: Any, path: str) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} must not contain NaN or infinity")
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        invalid_keys = [key for key in value if not isinstance(key, str)]
        if invalid_keys:
            raise ValueError(f"{path} mapping keys must be strings")
        return {
            key: _json_value(item, f"{path}.{key}")
            for key, item in sorted(value.items())
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item, f"{path}[]") for item in value]
    raise ValueError(f"{path} contains a non-serializable value: {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class CalibrationCampaign:
    campaign_id: str
    campaign_version: str
    campaign_phase: str
    baseline_run_id: str
    incumbent_run_id: str
    baseline_policy_id: str
    incumbent_policy_id: str
    candidate_policy_ids: tuple[str, ...]
    target_metric: str
    target_dimension: str
    target_axis: str
    primary_decomposition_axes: tuple[str, ...] = ("supply",)
    supporting_coordinate_axes: tuple[str, ...] = ("supply", "demand")
    allowed_geo_levels: tuple[str, ...] = ("county",)
    manual_geo_ids: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "campaign_id", "campaign_version", "baseline_run_id",
            "incumbent_run_id", "baseline_policy_id", "incumbent_policy_id",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        phase = _text(self.campaign_phase, "campaign_phase").lower()
        if phase not in SUPPORTED_CAMPAIGN_PHASES:
            raise ValueError(f"Unsupported campaign_phase: {phase!r}")
        object.__setattr__(self, "campaign_phase", phase)

        candidates = tuple(_text(value, "candidate_policy_ids") for value in self.candidate_policy_ids)
        if not candidates:
            raise ValueError("candidate_policy_ids must be non-empty")
        if len(candidates) != len(set(candidates)):
            raise ValueError("candidate_policy_ids must be unique")
        if self.incumbent_policy_id in candidates:
            raise ValueError("incumbent_policy_id cannot also be a challenger")
        object.__setattr__(self, "candidate_policy_ids", candidates)

        expected = {
            "target_metric": "active_inventory",
            "target_dimension": "supply",
            "target_axis": "supply",
        }
        for name, expected_value in expected.items():
            actual = _text(getattr(self, name), name)
            if actual != expected_value:
                raise ValueError(f"{name} must be {expected_value!r}; received {actual!r}")
            object.__setattr__(self, name, actual)

        registry_axes = set(_build_axis_weights()["axis"].astype(str))
        governed_axes = {}
        for name in ("primary_decomposition_axes", "supporting_coordinate_axes"):
            supplied = tuple(_text(value, name).lower() for value in getattr(self, name))
            if not supplied or len(supplied) != len(set(supplied)):
                raise ValueError(f"{name} must be non-empty and contain no duplicate axes")
            unknown = set(supplied).difference(registry_axes)
            if unknown:
                raise ValueError(f"{name} contains unknown axes: {sorted(unknown)}")
            governed_axes[name] = tuple(sorted(supplied))
            object.__setattr__(self, name, governed_axes[name])
        if not set(governed_axes["primary_decomposition_axes"]).issubset(
            governed_axes["supporting_coordinate_axes"]
        ):
            raise ValueError("primary_decomposition_axes must be a subset of supporting_coordinate_axes")

        levels = tuple(sorted(_text(value, "allowed_geo_levels").lower() for value in self.allowed_geo_levels))
        if not levels or len(levels) != len(set(levels)):
            raise ValueError("allowed_geo_levels must be non-empty and unique")
        object.__setattr__(self, "allowed_geo_levels", levels)
        geos = tuple(sorted(_text(value, "manual_geo_ids") for value in self.manual_geo_ids))
        if len(geos) != len(set(geos)):
            raise ValueError("manual_geo_ids must be unique")
        object.__setattr__(self, "manual_geo_ids", geos)
        object.__setattr__(self, "metadata", _json_value(self.metadata, "metadata"))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
