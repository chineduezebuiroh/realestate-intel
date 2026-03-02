from __future__ import annotations
# forecast/models/sarimax_exog/policy_b.py

from dataclasses import dataclass
from typing import Any, Dict, Tuple, Optional
import numpy as np


@dataclass(frozen=True)
class PolicyBThresholds:
    min_obs_per_param: float = 5.0
    max_exog_cond: float = 1e12
    require_full_rank: bool = True
    svd_rtol: float = 1e-12  # rank tol relative to s[0]


class PolicyBViolation(RuntimeError):
    def __init__(self, report: Dict[str, Any]):
        super().__init__("Policy B violation")
        self.report = report


def estimate_arima_param_count(
    order: Tuple[int, int, int],
    seasonal_order: Tuple[int, int, int, int],
    trend: Optional[str] = None,
) -> int:
    """
    Conservative param count for obs/param gate.

    We count AR and MA params only (non-seasonal + seasonal) plus trend terms.
    Differencing (d, D) does not add parameters.
    """
    p, d, q = order
    P, D, Q, s = seasonal_order

    n = int(p) + int(q) + int(P) + int(Q)

    if trend:
        # statsmodels trend strings can include: 'c', 't', 'ct'
        if "c" in trend:
            n += 1
        if "t" in trend:
            n += 1
    return n


def compute_exog_diagnostics(X: np.ndarray, *, svd_rtol: float = 1e-12) -> Dict[str, Any]:
    """
    Compute conditioning diagnostics deterministically via SVD.
    Use on the exact training X matrix that will be passed to SARIMAX.fit().
    """
    if X.ndim != 2:
        raise ValueError("X must be 2D")

    n_obs, n_exogs = X.shape
    if n_exogs == 0:
        return {
            "n_obs": int(n_obs),
            "n_exogs": 0,
            "exog_rank": 0,
            "exog_cond": float("inf"),
            "exog_smin": 0.0,
            "exog_smax": 0.0,
        }

    # SVD
    s = np.linalg.svd(X, full_matrices=False, compute_uv=False)
    smax = float(s[0])
    smin = float(s[-1])
    tol = svd_rtol * smax
    rank = int(np.sum(s > tol))

    if smin == 0.0:
        cond = float("inf")
    else:
        cond = float(smax / smin)

    return {
        "n_obs": int(n_obs),
        "n_exogs": int(n_exogs),
        "exog_rank": rank,
        "exog_cond": cond,
        "exog_smin": smin,
        "exog_smax": smax,
        "svd_rtol": float(svd_rtol),
        "svd_tol": float(tol),
    }


def enforce_policy_b(
    *,
    n_obs_train: int,
    n_exogs: int,
    arima_param_count: int,
    exog_rank: int,
    exog_cond: float,
    thresholds: PolicyBThresholds,
    context: Dict[str, Any],
) -> None:
    denom = int(n_exogs) + int(arima_param_count)
    obs_per_param = (float(n_obs_train) / float(denom)) if denom > 0 else float("inf")

    failed = []
    if denom > 0 and obs_per_param < thresholds.min_obs_per_param:
        failed.append("obs_per_param")
    if thresholds.require_full_rank and exog_rank < n_exogs:
        failed.append("rank_deficient")
    if exog_cond > thresholds.max_exog_cond:
        failed.append("ill_conditioned")

    if failed:
        report = {
            "failed_checks": failed,
            "thresholds": {
                "min_obs_per_param": thresholds.min_obs_per_param,
                "max_exog_cond": thresholds.max_exog_cond,
                "require_full_rank": thresholds.require_full_rank,
                "svd_rtol": thresholds.svd_rtol,
            },
            "diagnostics": {
                "n_obs_train": int(n_obs_train),
                "n_exogs": int(n_exogs),
                "arima_param_count": int(arima_param_count),
                "obs_per_param": float(obs_per_param),
                "exog_rank": int(exog_rank),
                "exog_cond": float(exog_cond),
            },
            "context": context,
        }
        raise PolicyBViolation(report)


def write_failure_artifact(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    
