from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
# scripts/smoke_tests/40_49/48_labor_production_readiness_challenge.py


import numpy as np

from regime.experiments.labor_production_readiness_challenge import (
    CATEGORY_WEIGHTS,
    FINALIST_ROLE,
    build_labor_production_readiness_challenge,
)


def _assert_nonempty_file(path: Path) -> None:
    if not path.exists():
        raise AssertionError(f"Expected output missing: {path}")
    if path.stat().st_size <= 0:
        raise AssertionError(f"Expected output empty: {path}")


def main() -> int:
    print(
        "[labor_readiness] building production readiness challenge..."
    )

    result = build_labor_production_readiness_challenge()

    policy_summary = result["policy_summary"]
    seasonality_summary = result["seasonality_summary"]
    responsiveness_summary = result["responsiveness_summary"]
    scorecard = result["scorecard"]
    category_summary = result["category_summary"]
    decision_summary = result["decision_summary"]

    for name, frame in (
        ("policy summary", policy_summary),
        ("seasonality summary", seasonality_summary),
        ("responsiveness summary", responsiveness_summary),
        ("scorecard", scorecard),
        ("category summary", category_summary),
        ("decision summary", decision_summary),
    ):
        if frame.empty:
            raise AssertionError(f"{name} is empty")

    if set(category_summary["category"].unique()) != set(CATEGORY_WEIGHTS):
        raise AssertionError(
            "Production-readiness categories do not match the frozen rubric"
        )

    if not np.isclose(
        sum(CATEGORY_WEIGHTS.values()),
        1.0,
        rtol=0.0,
        atol=1e-12,
    ):
        raise AssertionError("Category weights do not sum to 1")

    finalist = policy_summary[
        policy_summary["run_role"].eq(FINALIST_ROLE)
    ]
    if len(finalist) != 1:
        raise AssertionError("Expected exactly one MA6 finalist row")

    required_columns = {
        "category",
        "criterion",
        "observed_value",
        "threshold",
        "pass",
        "hard_gate",
        "criterion_weight",
        "evidence",
    }
    missing = required_columns - set(scorecard.columns)
    if missing:
        raise AssertionError(
            f"Scorecard is missing columns: {sorted(missing)}"
        )

    hard_failures = scorecard[
        scorecard["hard_gate"] & ~scorecard["pass"]
    ]
    decision = decision_summary.iloc[0]

    if int(decision["hard_gate_failures"]) != len(hard_failures):
        raise AssertionError(
            "Decision hard-gate count does not match the scorecard"
        )

    overall_score = float(decision["overall_score"])
    if (
        not np.isfinite(overall_score)
        or overall_score < 0
        or overall_score > 1
    ):
        raise AssertionError(
            "Overall readiness score fell outside [0, 1]"
        )

    if decision["decision"] != "PROMOTE_MA6":
        raise AssertionError(
            "Readiness decision changed; expected PROMOTE_MA6, "
            f"found {decision['decision']!r}"
        )

    for path in result["csv_outputs"].values():
        _assert_nonempty_file(Path(path))
    _assert_nonempty_file(Path(result["decision_markdown"]))

    print(f"[labor_readiness] output root: {result['output_root']}")

    print("\n[labor_readiness] policy summary:")
    print(policy_summary.to_string(index=False))

    print("\n[labor_readiness] seasonality summary:")
    print(seasonality_summary.to_string(index=False))

    print("\n[labor_readiness] responsiveness summary:")
    print(responsiveness_summary.to_string(index=False))

    print("\n[labor_readiness] category summary:")
    print(category_summary.to_string(index=False))

    print("\n[labor_readiness] failed hard gates:")
    if hard_failures.empty:
        print("[labor_readiness] none")
    else:
        print(
            hard_failures[
                [
                    "category",
                    "criterion",
                    "observed_value",
                    "threshold",
                ]
            ].to_string(index=False)
        )

    print("\n[labor_readiness] decision:")
    print(decision_summary.to_string(index=False))

    print("\n[labor_readiness] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
