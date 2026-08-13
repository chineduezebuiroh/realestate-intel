"""Pure mechanics for the diagnostic-only Demand promotion fidelity audit.

This module intentionally has no production write path.  The artifact builder
uses these small, fail-closed functions against copies of persisted frames.
"""
from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

IDENTITY = {
    "labor_force_membership": "LF-IN",
    "ma_window": "MA9",
    "laus_weight_policy": "LAUS-W-80-10-10",
    "balance_policy": "BAL-S25-C75",
}
TIGHT_TOLERANCE = 1e-12


def resolve_scenario(registry: pd.DataFrame) -> pd.DataFrame:
    """Resolve the governed winner by factors, never by row order."""
    required = {"scenario_id", *IDENTITY}
    missing = required - set(registry.columns)
    if missing:
        raise ValueError(f"scenario registry missing required fields: {sorted(missing)}")
    mask = pd.Series(True, index=registry.index)
    for column, value in IDENTITY.items():
        mask &= registry[column].eq(value)
    selected = registry.loc[mask, ["scenario_id", *IDENTITY]].copy()
    if len(selected) != 1:
        raise ValueError(f"expected exactly one governed scenario; found {len(selected)}")
    return selected.reset_index(drop=True)


def _crossings(values: pd.Series, tolerance: float) -> int:
    signs = np.sign(values.where(values.abs() > tolerance)).replace(0, np.nan).dropna()
    return int(signs.ne(signs.shift()).iloc[1:].sum()) if len(signs) > 1 else 0


def _reversals(values: pd.Series, tolerance: float) -> int:
    direction = np.sign(values.diff().where(values.diff().abs() > tolerance)).dropna()
    return int(direction.ne(direction.shift()).iloc[1:].sum()) if len(direction) > 1 else 0


def comparison(left: pd.DataFrame, right: pd.DataFrame, left_value: str,
               right_value: str, tolerance: float = TIGHT_TOLERANCE) -> dict:
    """Compare two keyed chronologies without treating correlation as parity."""
    keys = ["geo_id", "date"]
    for name, frame, value in (("left", left, left_value), ("right", right, right_value)):
        missing = set(keys + [value]) - set(frame.columns)
        if missing:
            raise ValueError(f"{name} chronology missing fields: {sorted(missing)}")
        if frame.duplicated(keys).any():
            raise ValueError(f"{name} chronology has duplicate geography/date rows")
    q = left[keys + [left_value]].merge(
        right[keys + [right_value]], on=keys, how="inner", validate="one_to_one"
    ).dropna().sort_values(keys)
    a, b = q[left_value].astype(float), q[right_value].astype(float)
    difference = b - a
    first = q.loc[difference.abs().gt(tolerance), "date"]
    return {
        "common_month_count": int(len(q)),
        "correlation": float(a.corr(b)) if len(q) > 1 and a.nunique() > 1 and b.nunique() > 1 else np.nan,
        "mean_absolute_difference": float(difference.abs().mean()),
        "median_absolute_difference": float(difference.abs().median()),
        "max_absolute_difference": float(difference.abs().max()),
        "mean_signed_difference": float(difference.mean()),
        "std_left": float(a.std()), "std_right": float(b.std()),
        "range_left": float(a.max() - a.min()), "range_right": float(b.max() - b.min()),
        "sign_agreement_share": float(np.sign(a).eq(np.sign(b)).mean()),
        "direction_agreement_share": float(np.sign(a.diff()).eq(np.sign(b.diff())).iloc[1:].mean()) if len(q) > 1 else np.nan,
        "zero_crossings_left": _crossings(a, tolerance),
        "zero_crossings_right": _crossings(b, tolerance),
        "reversal_1m_left": _reversals(a, tolerance),
        "reversal_1m_right": _reversals(b, tolerance),
        "first_difference_date": first.iloc[0] if len(first) else pd.NaT,
        "exact_parity": bool(difference.eq(0).all()),
        "numerical_parity": bool(difference.abs().le(tolerance).all()),
    }


def reconstruct_axis(dimensions: pd.DataFrame, registry: pd.DataFrame,
                     axis: str = "demand") -> pd.DataFrame:
    """Availability-normalized reconstruction matching ``score_axes``."""
    required_d = {"geo_id", "date", "dimension", "dimension_score"}
    required_r = {"axis", "dimension", "dimension_weight", "enabled"}
    if required_d - set(dimensions) or required_r - set(registry):
        raise ValueError("dimension chronology or axis registry has an invalid schema")
    enabled = registry.enabled.astype(str).str.lower().isin({"true", "1", "yes", "y"})
    weights = registry.loc[enabled & registry.axis.str.lower().eq(axis.lower()),
                           ["dimension", "dimension_weight"]].copy()
    weights["dimension_weight"] = pd.to_numeric(weights.dimension_weight, errors="raise")
    if weights.dimension.duplicated().any() or weights.empty:
        raise ValueError("active axis dimensions must be nonempty and unique")
    q = dimensions.merge(weights, on="dimension", how="inner").dropna(subset=["dimension_score"])
    q["weighted_dimension_contribution"] = q.dimension_score * q.dimension_weight
    totals = q.groupby(["geo_id", "date"], as_index=False).agg(
        reconstructed_axis_score=("weighted_dimension_contribution", "sum"),
        available_dimension_weight=("dimension_weight", "sum"),
        active_dimension_count=("dimension", "size"),
    )
    totals.reconstructed_axis_score /= totals.available_dimension_weight
    return q.merge(totals, on=["geo_id", "date"], validate="many_to_one")


def contribution_rows(scores: pd.DataFrame, metric_registry: pd.DataFrame) -> pd.DataFrame:
    """Return realized Demand metric and block arithmetic for audit reporting."""
    required = {"geo_id", "date", "canonical_metric_key", "metric_score"}
    if required - set(scores):
        raise ValueError("metric scores have an invalid schema")
    meta = metric_registry.rename(columns={"metric_key": "source_metric_key"}).copy()
    meta = meta.loc[meta.dimension.eq("demand") & meta.enabled.astype(str).str.lower().eq("true")]
    meta = meta[["canonical_metric_key", "metric_weight", "demand_block", "block_weight"]].drop_duplicates()
    if meta.canonical_metric_key.duplicated().any():
        raise ValueError("active canonical Demand metrics must be unique")
    q = scores.merge(meta, on="canonical_metric_key", how="inner").dropna(subset=["metric_score"])
    q["metric_weight"] = pd.to_numeric(q.metric_weight)
    q["block_weight"] = pd.to_numeric(q.block_weight)
    group = ["geo_id", "date", "demand_block"]
    q["available_metric_weight"] = q.groupby(group).metric_weight.transform("sum")
    q["within_block_contribution"] = q.metric_score * q.metric_weight / q.available_metric_weight
    q["final_weighted_metric_contribution"] = q.within_block_contribution * q.block_weight
    return q


def cancellation_detected(contributions: pd.DataFrame, value: str,
                          gross_floor: float = .1, net_share: float = .1) -> bool:
    """Detect materially offsetting signed contributions in at least one month."""
    grouped = contributions.groupby(["geo_id", "date"])[value]
    gross = grouped.apply(lambda x: x.abs().sum())
    net = grouped.sum().abs()
    return bool(((gross >= gross_floor) & (net <= gross * net_share)).any())


def production_files_unchanged(before: Mapping[str, str], after: Mapping[str, str]) -> bool:
    """Compare caller-provided content digests for the immutable run surface."""
    return dict(before) == dict(after)
