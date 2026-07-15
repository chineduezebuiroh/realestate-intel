from __future__ import annotations
# regime/experiments/labor_production_readiness_challenge.py

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.experiments.labor_demand_chronological_review import (
    DECISION_START_DATE,
    FOCUS_GEOS,
    build_labor_demand_chronological_review,
)
from regime.experiments.labor_demand_comparison import (
    build_labor_demand_comparison,
)
from regime.experiments.labor_demand_source_diagnostic import (
    build_labor_demand_source_diagnostic,
)

DEFAULT_OUTPUT_ROOT = Path(
    "artifacts/regime/comparisons/labor_production_readiness"
)

INCUMBENT_ROLE = "baseline"
CONTROL_ROLE = "labor_ma3_momentum_lag3"
FINALIST_ROLE = "labor_ma6_momentum_lag3"

POLICY_LABELS = {
    INCUMBENT_ROLE: "Current production baseline",
    CONTROL_ROLE: "LAUS MA3 momentum lag3",
    FINALIST_ROLE: "LAUS MA6 momentum lag3",
}

CATEGORY_WEIGHTS = {
    "structural_fidelity": 0.30,
    "economic_responsiveness": 0.25,
    "regime_stability": 0.30,
    "interpretability": 0.10,
    "operational_cost": 0.05,
}


@dataclass(frozen=True)
class ReadinessThresholds:
    minimum_short_volatility_reduction: float = 0.25
    minimum_metric_volatility_reduction: float = 0.25
    minimum_dimension_volatility_reduction: float = 0.25
    minimum_axis_volatility_reduction: float = 0.20
    minimum_sign_flip_reduction: float = 0.20
    maximum_near_zero_rate_increase: float = 0.02
    minimum_cancellation_reduction: float = 0.01
    maximum_median_absolute_core_lag_months: float = 2.0
    maximum_core_lag_months: float = 4.0
    maximum_median_absolute_axis_lag_months: float = 2.0
    maximum_axis_lag_months: float = 12.0
    maximum_warmup_months: int = 18
    minimum_post_2019_coverage_rate: float = 0.95
    minimum_overall_score: float = 0.75


def _ensure_output_root(path: str | Path) -> Path:
    output = Path(path)
    output.mkdir(parents=True, exist_ok=True)
    return output


def _safe_divide(numerator: float, denominator: float) -> float:
    if not np.isfinite(denominator) or denominator == 0:
        return np.nan
    return float(numerator / denominator)


def _reduction(baseline: float, challenger: float) -> float:
    ratio = _safe_divide(challenger, baseline)
    return np.nan if not np.isfinite(ratio) else 1.0 - ratio


def _mean_for_role(
    frame: pd.DataFrame,
    *,
    role: str,
    column: str,
    filters: dict[str, object] | None = None,
) -> float:
    work = frame[frame["run_role"].eq(role)].copy()
    for key, value in (filters or {}).items():
        work = work[work[key].eq(value)]
    values = pd.to_numeric(work[column], errors="coerce").dropna()
    return np.nan if values.empty else float(values.mean())


def _build_policy_summary(
    comparison: dict[str, pd.DataFrame],
    chronology: dict[str, object],
) -> pd.DataFrame:
    feature = comparison["feature_stability_summary"]
    metric = comparison["metric_stability_summary"]
    dimension = comparison["dimension_stability_summary"]
    axis = comparison["axis_stability_summary"]
    cancellation = comparison["cancellation_summary"]

    rows: list[dict[str, object]] = []
    for role in (INCUMBENT_ROLE, CONTROL_ROLE, FINALIST_ROLE):
        rows.append(
            {
                "run_role": role,
                "policy_label": POLICY_LABELS[role],
                "short_feature_mean_monthly_movement": _mean_for_role(
                    feature,
                    role=role,
                    column="mean_absolute_change_1m",
                    filters={"feature_component": "short"},
                ),
                "labor_metric_mean_monthly_movement": _mean_for_role(
                    metric,
                    role=role,
                    column="mean_absolute_change_1m",
                ),
                "labor_metric_sign_flip_rate": _mean_for_role(
                    metric,
                    role=role,
                    column="sign_flip_rate",
                ),
                "core_demand_mean_monthly_movement": _mean_for_role(
                    dimension,
                    role=role,
                    column="mean_absolute_change_1m",
                ),
                "core_demand_sign_flip_rate": _mean_for_role(
                    dimension,
                    role=role,
                    column="sign_flip_rate",
                ),
                "core_demand_near_zero_rate": _mean_for_role(
                    dimension,
                    role=role,
                    column="near_zero_rate",
                ),
                "demand_axis_mean_monthly_movement": _mean_for_role(
                    axis,
                    role=role,
                    column="mean_absolute_change_1m",
                ),
                "demand_axis_sign_flip_rate": _mean_for_role(
                    axis,
                    role=role,
                    column="sign_flip_rate",
                ),
                "demand_axis_near_zero_rate": _mean_for_role(
                    axis,
                    role=role,
                    column="near_zero_rate",
                ),
                "core_demand_cancellation_rate": _mean_for_role(
                    cancellation,
                    role=role,
                    column="mean_cancellation_rate",
                ),
                "core_demand_full_cancellation_rate": _mean_for_role(
                    cancellation,
                    role=role,
                    column="full_cancellation_rate",
                ),
            }
        )

    output = pd.DataFrame(rows)
    baseline = output[output["run_role"].eq(INCUMBENT_ROLE)].iloc[0]
    measures = [
        column
        for column in output.columns
        if column not in {"run_role", "policy_label"}
    ]
    for column in measures:
        output[f"{column}_change_vs_baseline"] = (
            output[column] - float(baseline[column])
        )
        output[f"{column}_reduction_vs_baseline"] = output[column].map(
            lambda value, base=float(baseline[column]): _reduction(base, value)
        )

    coverage = chronology["complete_coverage"].copy()
    coverage["date"] = pd.to_datetime(coverage["date"], errors="coerce")
    post_2019 = coverage[coverage["date"].ge(DECISION_START_DATE)]
    counts = post_2019.groupby("geo_id")["date"].nunique()
    coverage_rate = _safe_divide(float(counts.min()), float(counts.max()))
    output["post_2019_complete_coverage_rate"] = coverage_rate
    return output


def _build_seasonality_summary(
    diagnostic: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    current = diagnostic["current_feature_summary"].copy()
    current = current[current["feature_component"].eq("short")]

    ma6 = diagnostic["candidate_summary"].copy()
    ma6 = ma6[ma6["window"].eq(6) & ma6["lag_periods"].eq(3)]

    current = current[
        [
            "geo_id",
            "canonical_metric_key",
            "raw_feature_calendar_month_variance_share",
            "feature_score_calendar_month_variance_share",
        ]
    ].rename(
        columns={
            "raw_feature_calendar_month_variance_share": (
                "current_short_calendar_variance_share"
            ),
            "feature_score_calendar_month_variance_share": (
                "current_short_score_calendar_variance_share"
            ),
        }
    )

    ma6 = ma6[
        [
            "geo_id",
            "canonical_metric_key",
            "calendar_month_variance_share",
            "raw_level_correlation",
        ]
    ].rename(
        columns={
            "calendar_month_variance_share": (
                "ma6_short_calendar_variance_share"
            ),
            "raw_level_correlation": (
                "ma6_raw_level_correlation"
            ),
        }
    )

    output = current.merge(
        ma6,
        on=["geo_id", "canonical_metric_key"],
        how="inner",
        validate="one_to_one",
    )
    output["calendar_variance_reduction_vs_current_raw"] = output.apply(
        lambda row: _reduction(
            row["current_short_calendar_variance_share"],
            row["ma6_short_calendar_variance_share"],
        ),
        axis=1,
    )
    return output


def _criterion(
    category: str,
    criterion: str,
    observed_value: object,
    threshold: object,
    passed: bool,
    hard_gate: bool,
    criterion_weight: float,
    evidence: str,
) -> dict[str, object]:
    return {
        "category": category,
        "criterion": criterion,
        "observed_value": observed_value,
        "threshold": threshold,
        "pass": bool(passed),
        "hard_gate": bool(hard_gate),
        "criterion_weight": float(criterion_weight),
        "evidence": evidence,
    }


def _build_scorecard(
    policy_summary: pd.DataFrame,
    seasonality_summary: pd.DataFrame,
    responsiveness: pd.DataFrame,
    *,
    thresholds: ReadinessThresholds,
) -> pd.DataFrame:
    finalist = policy_summary[policy_summary["run_role"].eq(FINALIST_ROLE)].iloc[0]
    rows: list[dict[str, object]] = []

    seasonal_reduction = float(
        seasonality_summary["calendar_variance_reduction_vs_current_raw"].mean()
    )
    rows.append(
        _criterion(
            "structural_fidelity",
            "Short-feature calendar-month variance decreases",
            seasonal_reduction,
            "> 0",
            np.isfinite(seasonal_reduction) and seasonal_reduction > 0,
            True,
            0.30,
            "Mean reduction across three LAUS metrics and two geographies.",
        )
    )

    structural_checks = [
        (
            "Short-feature monthly movement falls materially",
            "short_feature_mean_monthly_movement_reduction_vs_baseline",
            thresholds.minimum_short_volatility_reduction,
            0.35,
        ),
        (
            "Labor metric monthly movement falls materially",
            "labor_metric_mean_monthly_movement_reduction_vs_baseline",
            thresholds.minimum_metric_volatility_reduction,
            0.35,
        ),
    ]
    for label, column, threshold, weight in structural_checks:
        value = float(finalist[column])
        rows.append(
            _criterion(
                "structural_fidelity",
                label,
                value,
                threshold,
                value >= threshold,
                True,
                weight,
                "Compared with the current production baseline.",
            )
        )

    core = responsiveness[responsiveness["series_name"].eq("core_demand_dimension")]
    axis = responsiveness[responsiveness["series_name"].eq("demand_axis")]
    response_checks = [
        (
            "Core Demand median absolute turning-point lag",
            float(core["median_absolute_lag_months"].max()),
            thresholds.maximum_median_absolute_core_lag_months,
            0.30,
        ),
        (
            "Core Demand maximum absolute turning-point lag",
            float(core["maximum_absolute_lag_months"].max()),
            thresholds.maximum_core_lag_months,
            0.25,
        ),
        (
            "Demand-axis median absolute turning-point lag",
            float(axis["median_absolute_lag_months"].max()),
            thresholds.maximum_median_absolute_axis_lag_months,
            0.25,
        ),
        (
            "Demand-axis maximum absolute turning-point lag",
            float(axis["maximum_absolute_lag_months"].max()),
            thresholds.maximum_axis_lag_months,
            0.20,
        ),
    ]
    for label, value, threshold, weight in response_checks:
        rows.append(
            _criterion(
                "economic_responsiveness",
                label,
                value,
                threshold,
                value <= threshold,
                True,
                weight,
                "One-to-one matched events within the hard ±12-month window.",
            )
        )

    stability_checks = [
        (
            "Core Demand monthly movement falls materially",
            "core_demand_mean_monthly_movement_reduction_vs_baseline",
            thresholds.minimum_dimension_volatility_reduction,
            ">=",
            0.25,
            True,
        ),
        (
            "Demand-axis monthly movement falls materially",
            "demand_axis_mean_monthly_movement_reduction_vs_baseline",
            thresholds.minimum_axis_volatility_reduction,
            ">=",
            0.25,
            True,
        ),
        (
            "Core Demand sign flips fall",
            "core_demand_sign_flip_rate_reduction_vs_baseline",
            thresholds.minimum_sign_flip_reduction,
            ">=",
            0.15,
            True,
        ),
        (
            "Demand-axis sign flips fall",
            "demand_axis_sign_flip_rate_reduction_vs_baseline",
            thresholds.minimum_sign_flip_reduction,
            ">=",
            0.15,
            True,
        ),
        (
            "Demand-axis near-zero frequency does not materially worsen",
            "demand_axis_near_zero_rate_change_vs_baseline",
            thresholds.maximum_near_zero_rate_increase,
            "<=",
            0.10,
            True,
        ),
        (
            "Core Demand cancellation declines",
            "core_demand_cancellation_rate_reduction_vs_baseline",
            thresholds.minimum_cancellation_reduction,
            ">=",
            0.10,
            False,
        ),
    ]
    for label, column, threshold, operator, weight, hard_gate in stability_checks:
        value = float(finalist[column])
        passed = value >= threshold if operator == ">=" else value <= threshold
        rows.append(
            _criterion(
                "regime_stability",
                label,
                value,
                threshold,
                passed,
                hard_gate,
                weight,
                "Downstream comparison against the current production baseline.",
            )
        )

    rows.append(
        _criterion(
            "interpretability",
            "One coherent state definition is used across level, short, and long",
            "level=MA6; short=MA6/lag3; long=MA6/lag12",
            "coherent and documented",
            True,
            True,
            1.0,
            "All components use the same six-month structural state.",
        )
    )

    coverage_rate = float(finalist["post_2019_complete_coverage_rate"])
    rows.extend(
        [
            _criterion(
                "operational_cost",
                "Long-feature warm-up stays within the approved limit",
                17,
                thresholds.maximum_warmup_months,
                17 <= thresholds.maximum_warmup_months,
                True,
                0.50,
                "MA6 plus a 12-month long lag requires 17 observations.",
            ),
            _criterion(
                "operational_cost",
                "Post-2019 complete coverage remains sufficient",
                coverage_rate,
                thresholds.minimum_post_2019_coverage_rate,
                coverage_rate >= thresholds.minimum_post_2019_coverage_rate,
                True,
                0.50,
                "Complete three-metric coverage across baseline, MA3, and MA6.",
            ),
        ]
    )

    scorecard = pd.DataFrame(rows)
    scorecard["weighted_pass_score"] = (
        scorecard["criterion_weight"] * scorecard["pass"].astype(float)
    )
    return scorecard


def _build_category_summary(scorecard: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for category, frame in scorecard.groupby("category"):
        weight_total = float(frame["criterion_weight"].sum())
        weighted_score = float(frame["weighted_pass_score"].sum())
        category_score = _safe_divide(weighted_score, weight_total)
        rows.append(
            {
                "category": category,
                "criteria": len(frame),
                "passed": int(frame["pass"].sum()),
                "hard_gates": int(frame["hard_gate"].sum()),
                "failed_hard_gates": int(
                    (frame["hard_gate"] & ~frame["pass"]).sum()
                ),
                "category_score": category_score,
                "category_weight": CATEGORY_WEIGHTS[category],
                "weighted_category_score": (
                    category_score * CATEGORY_WEIGHTS[category]
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("category").reset_index(drop=True)


def _build_decision_summary(
    scorecard: pd.DataFrame,
    categories: pd.DataFrame,
    *,
    thresholds: ReadinessThresholds,
) -> pd.DataFrame:
    failed_hard = scorecard[scorecard["hard_gate"] & ~scorecard["pass"]]
    overall_score = float(categories["weighted_category_score"].sum())

    if failed_hard.empty and overall_score >= thresholds.minimum_overall_score:
        decision = "PROMOTE_MA6"
        rationale = (
            "MA6 clears every hard production gate and exceeds the minimum "
            "weighted readiness score."
        )
    elif failed_hard.empty:
        decision = "HOLD_FOR_REVIEW"
        rationale = (
            "MA6 clears every hard gate but does not reach the minimum weighted "
            "readiness score."
        )
    else:
        decision = "KEEP_INCUMBENT"
        rationale = "MA6 fails one or more hard production-readiness gates."

    return pd.DataFrame(
        [
            {
                "incumbent": INCUMBENT_ROLE,
                "control": CONTROL_ROLE,
                "finalist": FINALIST_ROLE,
                "decision": decision,
                "overall_score": overall_score,
                "minimum_overall_score": thresholds.minimum_overall_score,
                "hard_gate_failures": len(failed_hard),
                "rationale": rationale,
            }
        ]
    )


def _format(value: object) -> str:
    if isinstance(value, (float, np.floating)):
        return "NaN" if np.isnan(value) else f"{value:.4f}"
    return str(value)


def _write_markdown(
    path: Path,
    decision: pd.DataFrame,
    categories: pd.DataFrame,
    scorecard: pd.DataFrame,
) -> None:
    row = decision.iloc[0]
    lines = [
        "# LAUS Labor Production Readiness Decision",
        "",
        f"**Decision:** `{row['decision']}`",
        "",
        f"**Incumbent:** `{row['incumbent']}`",
        "",
        f"**Control:** `{row['control']}`",
        "",
        f"**Finalist:** `{row['finalist']}`",
        "",
        f"**Overall weighted score:** {row['overall_score']:.3f}",
        "",
        f"**Hard-gate failures:** {int(row['hard_gate_failures'])}",
        "",
        f"**Rationale:** {row['rationale']}",
        "",
        "## Category Results",
        "",
        "| Category | Score | Failed hard gates |",
        "|---|---:|---:|",
    ]
    for _, category in categories.iterrows():
        lines.append(
            f"| {category['category']} | {category['category_score']:.3f} | "
            f"{int(category['failed_hard_gates'])} |"
        )

    lines.extend(
        [
            "",
            "## Criterion Results",
            "",
            "| Category | Criterion | Pass | Observed | Threshold | Hard gate |",
            "|---|---|---:|---|---|---:|",
        ]
    )
    for _, criterion in scorecard.iterrows():
        lines.append(
            f"| {criterion['category']} | {criterion['criterion']} | "
            f"{'YES' if criterion['pass'] else 'NO'} | "
            f"{_format(criterion['observed_value'])} | "
            f"{_format(criterion['threshold'])} | "
            f"{'YES' if criterion['hard_gate'] else 'NO'} |"
        )

    lines.extend(
        [
            "",
            "## Frozen Governance Interpretation",
            "",
            "Similarity to the incumbent is not a promotion criterion. The "
            "finalist is judged against structural fidelity, economic "
            "responsiveness, downstream stability, interpretability, and "
            "operational cost.",
            "",
            "A promotion decision freezes the LAUS feature contract as:",
            "",
            "```text",
            "level = MA6",
            "short = MA6 / lag3(MA6) - 1",
            "long  = MA6 / lag12(MA6) - 1",
            "```",
            "",
        ]
    )
    path.write_text("\n".join(lines))


def build_labor_production_readiness_challenge(
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    thresholds: ReadinessThresholds = ReadinessThresholds(),
) -> dict[str, object]:
    output = _ensure_output_root(output_root)

    comparison = build_labor_demand_comparison(
        artifact_root=artifact_root,
        geo_ids=FOCUS_GEOS,
    )
    chronology = build_labor_demand_chronological_review(
        artifact_root=artifact_root,
        geo_ids=FOCUS_GEOS,
    )
    diagnostic = build_labor_demand_source_diagnostic(
        artifact_root=artifact_root,
        geo_ids=FOCUS_GEOS,
    )

    policy_summary = _build_policy_summary(comparison, chronology)
    seasonality_summary = _build_seasonality_summary(diagnostic)
    responsiveness_summary = chronology["matched_event_summary"].copy()
    responsiveness_summary = responsiveness_summary[
        responsiveness_summary["run_role"].eq(FINALIST_ROLE)
    ].reset_index(drop=True)

    scorecard = _build_scorecard(
        policy_summary,
        seasonality_summary,
        responsiveness_summary,
        thresholds=thresholds,
    )
    category_summary = _build_category_summary(scorecard)
    decision_summary = _build_decision_summary(
        scorecard,
        category_summary,
        thresholds=thresholds,
    )

    csv_outputs = {
        "policy_summary": output / "production_readiness_summary.csv",
        "seasonality_summary": output / "seasonality_summary.csv",
        "responsiveness_summary": output / "responsiveness_summary.csv",
        "scorecard": output / "production_readiness_scorecard.csv",
        "category_summary": output / "production_readiness_categories.csv",
        "decision_summary": output / "production_readiness_decision.csv",
    }

    policy_summary.to_csv(csv_outputs["policy_summary"], index=False)
    seasonality_summary.to_csv(csv_outputs["seasonality_summary"], index=False)
    responsiveness_summary.to_csv(
        csv_outputs["responsiveness_summary"], index=False
    )
    scorecard.to_csv(csv_outputs["scorecard"], index=False)
    category_summary.to_csv(csv_outputs["category_summary"], index=False)
    decision_summary.to_csv(csv_outputs["decision_summary"], index=False)

    decision_markdown = output / "production_readiness_decision.md"
    _write_markdown(
        decision_markdown,
        decision_summary,
        category_summary,
        scorecard,
    )

    return {
        "output_root": output,
        "csv_outputs": csv_outputs,
        "decision_markdown": decision_markdown,
        "policy_summary": policy_summary,
        "seasonality_summary": seasonality_summary,
        "responsiveness_summary": responsiveness_summary,
        "scorecard": scorecard,
        "category_summary": category_summary,
        "decision_summary": decision_summary,
    }
