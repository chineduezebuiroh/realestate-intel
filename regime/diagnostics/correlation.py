"""Warning-free Pearson correlation for diagnostic evidence surfaces."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

CORRELATION_STD_ABS_TOLERANCE = 1e-12


@dataclass(frozen=True)
class CorrelationResult:
    """A correlation value and the evidence needed to audit its validity."""

    correlation: float
    status: str
    overlap_count: int
    finite_left_count: int
    finite_right_count: int
    left_std: float
    right_std: float


def safe_corr(left, right, min_obs: int = 3) -> CorrelationResult:
    """Compute pairwise-finite Pearson correlation without NumPy warnings.

    Undefined results remain NaN. Variance at or below the absolute diagnostic
    tolerance is classified as constant before Pearson arithmetic is attempted.
    Counts are reported before pairwise intersection, while standard deviations
    describe the pairwise-finite observations used by the comparison.
    """
    if min_obs < 2:
        raise ValueError("min_obs must be at least 2")
    left_values = pd.to_numeric(pd.Series(left).reset_index(drop=True), errors="coerce").to_numpy(dtype=float)
    right_values = pd.to_numeric(pd.Series(right).reset_index(drop=True), errors="coerce").to_numpy(dtype=float)
    if len(left_values) != len(right_values):
        raise ValueError("correlation inputs must have equal length")
    left_finite = np.isfinite(left_values)
    right_finite = np.isfinite(right_values)
    pairwise = left_finite & right_finite
    finite_left_count = int(left_finite.sum())
    finite_right_count = int(right_finite.sum())
    overlap_count = int(pairwise.sum())
    if overlap_count < min_obs:
        left_limited = len(left_values) >= min_obs and finite_left_count < min_obs
        right_limited = len(right_values) >= min_obs and finite_right_count < min_obs
        if left_limited and right_limited:
            status = "both_nonfinite"
        elif left_limited:
            status = "left_nonfinite"
        elif right_limited:
            status = "right_nonfinite"
        else:
            status = "insufficient_overlap"
        return CorrelationResult(np.nan, status, overlap_count, finite_left_count, finite_right_count, np.nan, np.nan)
    paired_left = left_values[pairwise]
    paired_right = right_values[pairwise]
    left_std = float(np.std(paired_left, ddof=0))
    right_std = float(np.std(paired_right, ddof=0))
    left_constant = left_std <= CORRELATION_STD_ABS_TOLERANCE
    right_constant = right_std <= CORRELATION_STD_ABS_TOLERANCE
    if left_constant or right_constant:
        status = "both_constant" if left_constant and right_constant else "left_constant" if left_constant else "right_constant"
        return CorrelationResult(np.nan, status, overlap_count, finite_left_count, finite_right_count, left_std, right_std)
    correlation = float(np.corrcoef(paired_left, paired_right)[0, 1])
    return CorrelationResult(correlation, "ok", overlap_count, finite_left_count, finite_right_count, left_std, right_std)
