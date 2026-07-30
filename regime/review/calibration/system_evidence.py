"""Adapters for immutable downstream calibration evidence.

This module deliberately contains no regime-engine calculation.  It normalizes
already materialized engine outputs into a small, campaign-neutral review
contract which the bundle renderer can validate, copy, and plot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import pandas as pd


SYSTEM_SECTIONS = (
    "dimension_chronology",
    "axis_chronology",
    "coordinate_trajectories",
    "regime_chronology",
    "transition_windows",
    "cancellation_diagnostics",
)


@dataclass(frozen=True, slots=True)
class CalibrationSystemEvidence:
    """Persisted engine outputs and deterministic review selections."""

    campaign_id: str
    campaign_version: str
    candidate_policy_ids: tuple[str, ...]
    incumbent_policy_id: str
    baseline_policy_id: str
    target_metric: str
    target_dimension: str
    target_axis: str
    tables: Mapping[str, pd.DataFrame]
    representative_geography_rule: str
    transition_window_rule: str

    def copied_tables(self) -> dict[str, pd.DataFrame]:
        return {name: frame.copy(deep=True) for name, frame in self.tables.items()}


def validate_system_evidence(evidence: CalibrationSystemEvidence) -> None:
    """Fail closed when any required immutable system view is inconsistent."""
    missing = sorted(set(SYSTEM_SECTIONS).difference(evidence.tables))
    if missing:
        raise ValueError(f"Missing required system evidence: {missing}")
    identities = {"baseline", *evidence.candidate_policy_ids}
    selected: set[str] | None = None
    for name in SYSTEM_SECTIONS:
        frame = evidence.tables[name]
        if frame.empty:
            raise ValueError(f"Required system evidence is empty: {name}")
        required = {"campaign_id", "campaign_version", "series_id", "geo_id"}
        absent = sorted(required.difference(frame.columns))
        if absent:
            raise ValueError(f"{name} is missing identity columns: {absent}")
        if set(frame["campaign_id"].astype(str)) != {evidence.campaign_id} or set(
            frame["campaign_version"].astype(str)
        ) != {evidence.campaign_version}:
            raise ValueError(f"{name} campaign identity mismatch")
        if set(frame["series_id"].astype(str)) != identities:
            raise ValueError(f"{name} candidate identity mismatch")
        geos = set(frame["geo_id"].astype(str))
        selected = geos if selected is None else selected
        if geos != selected:
            raise ValueError(f"{name} representative geography mismatch")
        keys = [c for c in ("series_id", "geo_id", "date", "window_id") if c in frame]
        if keys and frame.duplicated(keys).any():
            raise ValueError(f"{name} contains duplicate evidence keys")
    if not evidence.representative_geography_rule.strip():
        raise ValueError("Representative geography selection rule is required")
    if not evidence.transition_window_rule.strip():
        raise ValueError("System transition-window selection rule is required")
