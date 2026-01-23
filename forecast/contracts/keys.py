from __future__ import annotations
# forecast/contracts/keys.py

from dataclasses import dataclass, asdict
from typing import Literal, Optional

RunKind = Literal["backtest", "live_near", "live_outlook", "bridge"]


@dataclass(frozen=True)
class TargetKey:
    target_metric_id: str
    target_geo_id: str
    target_property_type_id: str
    freq: str  # "M" expected, but keep generic

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SelectorBatchKey:
    """
    Canonical definition: a selector batch must NOT mix targets or freqs.
    """
    batch_id: str
    target: TargetKey

    def to_dict(self) -> dict:
        d = {"batch_id": self.batch_id}
        d.update(self.target.to_dict())
        return d


@dataclass(frozen=True)
class ArtifactKey:
    """
    Uniquely identifies a persisted artifact (selected_features, design_matrix, etc).
    """
    batch_id: str
    anchor_date: str                # YYYY-MM-DD
    data_asof_effective: str        # YYYY-MM-DD
    target: TargetKey
    model_name: str
    model_version: str

    def to_dict(self) -> dict:
        d = {
            "batch_id": self.batch_id,
            "anchor_date": self.anchor_date,
            "data_asof_effective": self.data_asof_effective,
            "model_name": self.model_name,
            "model_version": self.model_version,
        }
        d.update(self.target.to_dict())
        return d


@dataclass(frozen=True)
class RunKey:
    """
    Uniquely identifies a forecast run record (forecast_runs row).
    """
    batch_id: str
    run_kind: RunKind
    anchor_date: str
    data_asof_effective: str
    target: TargetKey
    model_name: str
    model_version: str

    def to_dict(self) -> dict:
        d = {
            "batch_id": self.batch_id,
            "run_kind": self.run_kind,
            "anchor_date": self.anchor_date,
            "data_asof_effective": self.data_asof_effective,
            "model_name": self.model_name,
            "model_version": self.model_version,
        }
        d.update(self.target.to_dict())
        return d


@dataclass(frozen=True)
class EvalKey:
    """
    Apples-to-apples comparison group.

    IMPORTANT:
    Keep this minimal and stable. Anything that differs here is NOT comparable.
    """
    target: TargetKey
    anchor_date: str
    train_start: str
    train_end: str
    horizon_max_months: int
    data_asof_effective: str
    run_kind: RunKind

    def to_dict(self) -> dict:
        d = {
            "anchor_date": self.anchor_date,
            "train_start": self.train_start,
            "train_end": self.train_end,
            "horizon_max_months": self.horizon_max_months,
            "data_asof_effective": self.data_asof_effective,
            "run_kind": self.run_kind,
        }
        d.update(self.target.to_dict())
        return d
