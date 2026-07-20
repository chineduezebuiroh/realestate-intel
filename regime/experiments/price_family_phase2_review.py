from __future__ import annotations
# regime/experiments/price_family_phase2_review.py

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from regime.experiments.price_family_phase2_diagnostics import (
    CHRONOLOGY_ARTIFACTS,
    CHRONOLOGY_DIR,
    COMPARISON_ROOT,
    STABILITY_ARTIFACTS,
    STABILITY_DIR,
)

# Review thresholds only. These constants are deterministic adjudication defaults
# for this Phase 2 review packet, not immutable production policy.
MIN_MATCHED_TURNS = 5
MATERIAL_LAG_MONTHS = 6
EXTREME_LAG_MONTHS = 12
MATERIAL_LAG_SHARE = 0.20
LOW_CORRELATION_THRESHOLD = 0.80
STRONG_VOLATILITY_REDUCTION = 0.80
STRONG_SEASONALITY_REDUCTION = 0.70
NEAR_ZERO_RATE_SUPPRESSION_CHANGE = 0.25
SIGN_FLIP_RATE_SUPPRESSION_CHANGE = -0.10

TARGET_METRICS = ("median_sale_price", "median_ppsf", "price_to_income", "payment_burden")
TARGET_DIMENSIONS = ("Price", "Affordability")
TARGET_AXES = ("Demand",)
FOCUS_GEOS = ("district_of_columbia_dc__county", "alameda_county_ca__county")
REVIEW_DIR = COMPARISON_ROOT / "phase2_review"
RECOMMENDATIONS = (
    "blocking_diagnostic_issue",
    "insufficient_evidence",
    "retain_ma12_as_current_finalist",
    "ma6_finalist_warranted",
)
REVIEW_ARTIFACTS = (
    "smoothing_scorecard.csv",
    "metric_review.csv",
    "chronology_outliers.csv",
    "seasonality_review.csv",
    "shock_review.csv",
    "focus_case_manifest.csv",
    "ma6_trigger_evidence.csv",
    "review_summary.json",
)
SEASONALITY_REVIEW_COLUMNS = [
    "geo_id",
    "review_series_type",
    "review_series_key",
    "baseline_seasonal_spread",
    "challenger_seasonal_spread",
    "challenger_baseline_seasonal_spread_ratio",
    "percent_seasonal_spread_reduction",
    "baseline_minimum_month",
    "baseline_maximum_month",
    "challenger_minimum_month",
    "challenger_maximum_month",
    "chronology_delay_trigger",
    "attenuation_trigger",
    "seasonality_overkill_trigger",
]


def _clean(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _require(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required Phase 2 artifact: {path}")
    frame = pd.read_csv(path)
    missing = [column for column in (columns or []) if column not in frame.columns]
    if missing:
        raise ValueError(f"{path}: missing required columns {missing}")
    return frame


def _require_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required Phase 2 artifact: {path}")
    return json.loads(path.read_text())


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (numerator / denominator.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    out = frame.copy().replace([np.inf, -np.inf], np.nan)
    for column in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[column]):
            out[column] = out[column].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False)


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default) + "\n")


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    return value


def _series_label(series_type: str, series_key: str) -> tuple[str, str]:
    if series_type == "metric_score":
        return "metric", series_key
    if series_type == "dimension_score":
        return "dimension", series_key
    if series_type == "axis_score":
        return "axis", series_key
    return series_type, series_key


def _target_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        (frame["review_series_type"].eq("metric") & frame["review_series_key"].isin(TARGET_METRICS))
        | (frame["review_series_type"].eq("dimension") & frame["review_series_key"].isin(TARGET_DIMENSIONS))
        | (frame["review_series_type"].eq("axis") & frame["review_series_key"].isin(TARGET_AXES))
    )


def _required_target_series_count(monthly: pd.DataFrame) -> tuple[int, int]:
    supported: set[tuple[str, str]] = set()
    for series_type, series_key in monthly[["series_type", "series_key"]].drop_duplicates().itertuples(index=False):
        review_type, review_key = _series_label(series_type, series_key)
        if (
            (review_type == "metric" and review_key in TARGET_METRICS)
            or (review_type == "dimension" and review_key in TARGET_DIMENSIONS)
            or (review_type == "axis" and review_key in TARGET_AXES)
        ):
            supported.add((review_type, review_key))
    required = len(supported) * len(FOCUS_GEOS)
    return len(supported), required


def build_price_family_phase2_review(output_dir: Path = REVIEW_DIR) -> dict[str, pd.DataFrame]:
    """Build a deterministic Phase 2 review packet from persisted diagnostics only.

    Key grains:
    - smoothing_scorecard/metric_review: geo_id, review_series_type, review_series_key.
    - chronology_outliers: source, geo_id, series_type, series_key, comparison, flag/turn dates, lag, detail.
    - seasonality_review: geo_id, review_series_type, review_series_key.
    - shock_review: geo_id, period.
    - focus_case_manifest: geo_id, review_series_type, review_series_key, review_reason.
    - ma6_trigger_evidence: row_type, geo_id, review_series_type, review_series_key.
    """
    for name in CHRONOLOGY_ARTIFACTS:
        _require(CHRONOLOGY_DIR / name) if name.endswith(".csv") else _require_json(CHRONOLOGY_DIR / name)
    for name in STABILITY_ARTIFACTS:
        _require(STABILITY_DIR / name) if name.endswith(".csv") else _require_json(STABILITY_DIR / name)

    _clean(output_dir)
    monthly = _require(CHRONOLOGY_DIR / "chronology_monthly.csv", ["geo_id", "series_type", "series_key"])
    lag = _require(CHRONOLOGY_DIR / "turning_point_lag_summary.csv", ["geo_id", "series_type", "series_key", "absolute_lag_months", "matched"])
    flags = _require(CHRONOLOGY_DIR / "chronology_flags.csv", ["geo_id", "series_type", "series_key", "flag_type"])
    shock = _require(CHRONOLOGY_DIR / "affordability_shock_summary.csv", ["geo_id", "period"])
    stability_flags = _require(STABILITY_DIR / "stability_flags.csv")
    contract_flag_count = _contract_flag_count(stability_flags)

    stability = pd.concat(
        [
            _prep_stability(_require(STABILITY_DIR / "metric_stability_summary.csv"), "metric", "canonical_metric_key"),
            _prep_stability(_require(STABILITY_DIR / "dimension_stability_summary.csv"), "dimension", "dimension"),
            _prep_stability(_require(STABILITY_DIR / "demand_axis_stability_summary.csv"), "axis", "axis"),
        ],
        ignore_index=True,
        sort=False,
    )
    seasonality = _prep_seasonality(_require(STABILITY_DIR / "seasonality_summary.csv"))
    shock_review = _shock_review(shock, flags)

    score = _chronology_score(lag, flags).merge(
        stability,
        on=["geo_id", "review_series_type", "review_series_key"],
        how="outer",
    ).merge(
        seasonality,
        on=["geo_id", "review_series_type", "review_series_key"],
        how="outer",
    )
    missing_focus = set(FOCUS_GEOS) - set(score["geo_id"].dropna())
    _, required_focus_target_rows = _required_target_series_count(monthly)
    found_focus_target_rows = int((score["geo_id"].isin(FOCUS_GEOS) & _target_mask(score)).sum())
    missing_required_target_series_count = max(required_focus_target_rows - found_focus_target_rows, 0)
    blocking = bool(contract_flag_count or missing_focus or missing_required_target_series_count)

    score = _finalize_scorecard(score, blocking=blocking)
    metric_review = score[_target_mask(score)].copy()
    chronology_outliers = _chronology_outliers(lag, flags)
    seasonality_review = score[SEASONALITY_REVIEW_COLUMNS].copy()
    focus = _focus_manifest(score, shock_review)
    evidence = _evidence(
        score,
        shock_review,
        blocking=blocking,
        missing_focus_geography_count=len(missing_focus),
        missing_required_target_series_count=missing_required_target_series_count,
        contract_flag_count=contract_flag_count,
    )

    outputs = {
        "smoothing_scorecard": score,
        "metric_review": metric_review,
        "chronology_outliers": chronology_outliers,
        "seasonality_review": seasonality_review,
        "shock_review": shock_review,
        "focus_case_manifest": focus,
        "ma6_trigger_evidence": evidence,
    }
    for name, frame in outputs.items():
        _write_csv(frame, output_dir / f"{name}.csv")
    summary = {
        "aggregate_recommendation": evidence.loc[evidence["row_type"].eq("aggregate"), "recommendation"].iloc[0],
        "artifact_key_grains": {
            "smoothing_scorecard.csv": ["geo_id", "review_series_type", "review_series_key"],
            "metric_review.csv": ["geo_id", "review_series_type", "review_series_key"],
            "chronology_outliers.csv": ["source", "geo_id", "series_type", "series_key", "comparison", "flag_type", "direction", "baseline_turn_date", "challenger_turn_date", "signed_lag_months", "detail"],
            "seasonality_review.csv": ["geo_id", "review_series_type", "review_series_key"],
            "shock_review.csv": ["geo_id", "period"],
            "focus_case_manifest.csv": ["geo_id", "review_series_type", "review_series_key", "review_reason"],
            "ma6_trigger_evidence.csv": ["row_type", "geo_id", "review_series_type", "review_series_key"],
        },
        "thresholds_are_review_not_production_policy": True,
        "thresholds": {name: globals()[name] for name in [
            "MIN_MATCHED_TURNS",
            "MATERIAL_LAG_MONTHS",
            "EXTREME_LAG_MONTHS",
            "MATERIAL_LAG_SHARE",
            "LOW_CORRELATION_THRESHOLD",
            "STRONG_VOLATILITY_REDUCTION",
            "STRONG_SEASONALITY_REDUCTION",
            "NEAR_ZERO_RATE_SUPPRESSION_CHANGE",
            "SIGN_FLIP_RATE_SUPPRESSION_CHANGE",
        ]},
        "output_artifacts": list(REVIEW_ARTIFACTS),
    }
    _write_json(summary, output_dir / "review_summary.json")
    return outputs | {"review_summary": pd.DataFrame([summary])}


def _contract_flag_count(flags: pd.DataFrame) -> int:
    if flags.empty or "severity" not in flags.columns:
        return 0
    return int(flags["severity"].astype(str).str.lower().eq("contract").sum())


def _chronology_score(lag: pd.DataFrame, flags: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    lag = lag.copy()
    lag["absolute_lag_months"] = _num(lag["absolute_lag_months"])
    lag["matched_bool"] = lag["matched"].astype(str).str.lower().isin(["true", "1", "yes"])
    for (geo_id, series_type, series_key), group in lag.groupby(["geo_id", "series_type", "series_key"], dropna=False):
        matched = group[group["matched_bool"]]
        abs_lag = matched["absolute_lag_months"].dropna()
        review_type, review_key = _series_label(series_type, series_key)
        series_flags = flags[
            flags["geo_id"].eq(geo_id)
            & flags["series_type"].eq(series_type)
            & flags["series_key"].eq(series_key)
        ]
        rows.append({
            "geo_id": geo_id,
            "review_series_type": review_type,
            "review_series_key": review_key,
            "matched_turning_point_count": int(len(matched)),
            "unmatched_turning_point_count": int(len(group) - len(matched)),
            "median_absolute_lag_months": abs_lag.median(),
            "p75_absolute_lag_months": abs_lag.quantile(0.75),
            "p90_absolute_lag_months": abs_lag.quantile(0.90),
            "maximum_absolute_lag_months": abs_lag.max(),
            "share_matched_turns_lag_ge_3m": float(abs_lag.ge(3).mean()) if len(abs_lag) else np.nan,
            "share_matched_turns_lag_ge_6m": float(abs_lag.ge(MATERIAL_LAG_MONTHS).mean()) if len(abs_lag) else np.nan,
            "share_matched_turns_lag_ge_12m": float(abs_lag.ge(EXTREME_LAG_MONTHS).mean()) if len(abs_lag) else np.nan,
            "chronology_inversion_count": int(series_flags["flag_type"].astype(str).str.contains("inversion", case=False, na=False).sum()),
            "large_lag_flag_count": int(series_flags["flag_type"].astype(str).str.contains("large_lag", case=False, na=False).sum()),
        })
    return pd.DataFrame(rows)


def _prep_stability(frame: pd.DataFrame, review_type: str, key_column: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["geo_id", "review_series_type", "review_series_key"])
    required = ["geo_id", key_column, "run_role", "mean_absolute_change_1m", "sign_flip_rate", "near_zero_rate", "baseline_challenger_correlation", "mean_absolute_baseline_challenger_difference"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"stability {review_type}: missing required columns {missing}")
    baseline = frame[frame["run_role"].eq("baseline")]
    challenger = frame[frame["run_role"].eq("challenger")]
    base = baseline[["geo_id", key_column, "mean_absolute_change_1m", "sign_flip_rate", "near_zero_rate", "baseline_challenger_correlation", "mean_absolute_baseline_challenger_difference"]].rename(columns={key_column: "review_series_key", "mean_absolute_change_1m": "baseline_mean_absolute_1m_change", "sign_flip_rate": "baseline_sign_flip_rate", "near_zero_rate": "baseline_near_zero_rate"})
    chal = challenger[["geo_id", key_column, "mean_absolute_change_1m", "sign_flip_rate", "near_zero_rate"]].rename(columns={key_column: "review_series_key", "mean_absolute_change_1m": "challenger_mean_absolute_1m_change", "sign_flip_rate": "challenger_sign_flip_rate", "near_zero_rate": "challenger_near_zero_rate"})
    out = base.merge(chal, on=["geo_id", "review_series_key"], how="outer")
    out["review_series_type"] = review_type
    out["challenger_baseline_volatility_ratio"] = _safe_div(_num(out["challenger_mean_absolute_1m_change"]), _num(out["baseline_mean_absolute_1m_change"]))
    out["percent_volatility_reduction"] = 1 - out["challenger_baseline_volatility_ratio"]
    out["sign_flip_rate_change"] = _num(out["challenger_sign_flip_rate"]) - _num(out["baseline_sign_flip_rate"])
    out["near_zero_rate_change"] = _num(out["challenger_near_zero_rate"]) - _num(out["baseline_near_zero_rate"])
    return out


def _prep_seasonality(frame: pd.DataFrame) -> pd.DataFrame:
    mappings = {"metric_score": ("metric", "canonical_metric_key"), "dimension_score": ("dimension", "dimension"), "axis_score": ("axis", "axis")}
    rows: list[pd.DataFrame] = []
    for family, (review_type, key_column) in mappings.items():
        sub = frame[frame.get("series_family", pd.Series(dtype=str)).eq(family)]
        if sub.empty or key_column not in sub.columns:
            continue
        baseline = sub[sub["run_role"].eq("baseline")][["geo_id", key_column, "seasonal_spread", "minimum_month", "maximum_month"]].rename(columns={key_column: "review_series_key", "seasonal_spread": "baseline_seasonal_spread", "minimum_month": "baseline_minimum_month", "maximum_month": "baseline_maximum_month"})
        challenger = sub[sub["run_role"].eq("challenger")][["geo_id", key_column, "seasonal_spread", "minimum_month", "maximum_month"]].rename(columns={key_column: "review_series_key", "seasonal_spread": "challenger_seasonal_spread", "minimum_month": "challenger_minimum_month", "maximum_month": "challenger_maximum_month"})
        out = baseline.merge(challenger, on=["geo_id", "review_series_key"], how="outer")
        out["review_series_type"] = review_type
        rows.append(out)
    out = pd.concat(rows, ignore_index=True, sort=False) if rows else pd.DataFrame(columns=["geo_id", "review_series_type", "review_series_key"])
    out["challenger_baseline_seasonal_spread_ratio"] = _safe_div(_num(out.get("challenger_seasonal_spread", pd.Series(dtype=float))), _num(out.get("baseline_seasonal_spread", pd.Series(dtype=float))))
    out["percent_seasonal_spread_reduction"] = 1 - out["challenger_baseline_seasonal_spread_ratio"]
    return out


def _finalize_scorecard(frame: pd.DataFrame, blocking: bool) -> pd.DataFrame:
    out = frame.copy()
    for column in ["matched_turning_point_count", "unmatched_turning_point_count", "chronology_inversion_count", "large_lag_flag_count"]:
        out[column] = _num(out.get(column, pd.Series(dtype=float))).fillna(0).astype(int)
    for column in SEASONALITY_REVIEW_COLUMNS:
        if column not in out.columns and column not in {"geo_id", "review_series_type", "review_series_key", "chronology_delay_trigger", "attenuation_trigger", "seasonality_overkill_trigger"}:
            out[column] = np.nan
    out["eligible_observation"] = out["matched_turning_point_count"].ge(MIN_MATCHED_TURNS) & _target_mask(out)
    out["chronology_delay_trigger"] = out["eligible_observation"] & ((_num(out["median_absolute_lag_months"]).ge(MATERIAL_LAG_MONTHS)) | (_num(out["share_matched_turns_lag_ge_6m"]).ge(MATERIAL_LAG_SHARE)))
    out["attenuation_trigger"] = ((_num(out["percent_volatility_reduction"]).ge(STRONG_VOLATILITY_REDUCTION)) & (_num(out["baseline_challenger_correlation"]).lt(LOW_CORRELATION_THRESHOLD))) | ((_num(out["near_zero_rate_change"]).ge(NEAR_ZERO_RATE_SUPPRESSION_CHANGE)) & (_num(out["sign_flip_rate_change"]).le(SIGN_FLIP_RATE_SUPPRESSION_CHANGE)))
    out["seasonality_overkill_trigger"] = _num(out["percent_seasonal_spread_reduction"]).ge(STRONG_SEASONALITY_REDUCTION) & (out["chronology_delay_trigger"] | out["attenuation_trigger"])
    out["shock_suppression_trigger"] = False
    out["blocking_diagnostic_issue"] = bool(blocking)
    out["independent_trigger_family_count"] = out[["chronology_delay_trigger", "attenuation_trigger", "shock_suppression_trigger"]].sum(axis=1).astype(int)
    out["supporting_seasonality_overkill_count"] = out["seasonality_overkill_trigger"].astype(int)
    out["trigger_family_count"] = out["independent_trigger_family_count"]
    out["recommendation"] = np.select(
        [out["blocking_diagnostic_issue"], ~out["eligible_observation"], out["independent_trigger_family_count"].ge(2)],
        [RECOMMENDATIONS[0], RECOMMENDATIONS[1], RECOMMENDATIONS[3]],
        default=RECOMMENDATIONS[2],
    )
    return out.sort_values(["geo_id", "review_series_type", "review_series_key"], kind="mergesort").reset_index(drop=True)


def _flag_has_period_match(flags: pd.DataFrame, geo_id: str, flag_type: str, period: str) -> bool:
    matches = flags[flags["geo_id"].eq(geo_id) & flags["flag_type"].eq(flag_type)]
    if matches.empty:
        return False
    if "period" in matches.columns:
        period_matches = matches["period"].astype(str).eq(str(period))
        return bool(period_matches.any() or matches["period"].isna().all())
    detail = matches.get("detail", pd.Series(dtype=str)).astype(str)
    return bool(detail.str.contains(str(period), regex=False, na=False).any() or len(matches) > 0)


def _shock_review(shock: pd.DataFrame, flags: pd.DataFrame) -> pd.DataFrame:
    out = shock.copy()
    for column in ["aligned_observation_count", "mortgage_rate_change", "structural_median_sale_price_change", "structural_median_sale_price_pct_change", "payment_burden_change", "price_to_income_change"]:
        if column not in out.columns:
            out[column] = np.nan
        out[column] = _num(out[column])
    if "payment_burden_price_to_income_distinct_aligned" not in out.columns:
        out["payment_burden_price_to_income_distinct_aligned"] = False
    out["payment_burden_price_to_income_distinct_aligned"] = out["payment_burden_price_to_income_distinct_aligned"].astype(str).str.lower().isin(["true", "1", "yes"])
    out["affordability_materiality_review_flag"] = [
        _flag_has_period_match(flags, row.geo_id, "affordability_shock_materiality_review", row.period)
        for row in out[["geo_id", "period"]].itertuples(index=False)
    ]
    out["payment_burden_price_to_income_not_distinct_flag"] = [
        _flag_has_period_match(flags, row.geo_id, "price_to_income_payment_burden_not_distinct", row.period)
        for row in out[["geo_id", "period"]].itertuples(index=False)
    ]
    out["shock_suppression_trigger"] = out["affordability_materiality_review_flag"] | out["payment_burden_price_to_income_not_distinct_flag"]
    return out.sort_values(["geo_id", "period"], kind="mergesort").reset_index(drop=True)


def _chronology_outliers(lag: pd.DataFrame, flags: pd.DataFrame) -> pd.DataFrame:
    lag_out = lag.copy()
    lag_out["absolute_lag_months"] = _num(lag_out["absolute_lag_months"])
    lag_out = lag_out[lag_out["absolute_lag_months"].ge(MATERIAL_LAG_MONTHS)].assign(source="turning_point_lag_summary")
    flags_out = flags.copy().assign(source="chronology_flags")
    out = pd.concat([lag_out, flags_out], ignore_index=True, sort=False)
    for column in ["comparison", "flag_type", "direction", "baseline_turn_date", "challenger_turn_date", "signed_lag_months", "detail"]:
        if column not in out.columns:
            out[column] = ""
    return out.sort_values(["source", "geo_id", "series_type", "series_key", "comparison", "flag_type", "direction", "baseline_turn_date", "challenger_turn_date", "signed_lag_months", "detail"], kind="mergesort").reset_index(drop=True)


def _focus_manifest(score: pd.DataFrame, shock: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    reasons = [
        ("largest_material_lag_share", "share_matched_turns_lag_ge_6m", False),
        ("largest_p90_lag", "p90_absolute_lag_months", False),
        ("lowest_correlation", "baseline_challenger_correlation", True),
        ("strongest_volatility_reduction", "percent_volatility_reduction", False),
        ("strongest_seasonality_reduction", "percent_seasonal_spread_reduction", False),
    ]
    for reason, column, ascending in reasons:
        sub = score[_target_mask(score)].sort_values([column, "geo_id", "review_series_type", "review_series_key"], ascending=[ascending, True, True, True], kind="mergesort").head(10)
        rows.extend(row.to_dict() | {"review_reason": reason} for _, row in sub.iterrows())
    focus = score[score["geo_id"].isin(FOCUS_GEOS) & _target_mask(score)]
    rows.extend(row.to_dict() | {"review_reason": "required_focus_geography_target_metric"} for _, row in focus.iterrows())
    for _, row in shock[shock["shock_suppression_trigger"]].iterrows():
        rows.append({"geo_id": row["geo_id"], "review_series_type": "shock", "review_series_key": row.get("period", ""), "review_reason": "affordability_shock_flags"})
    out = pd.DataFrame(rows).drop_duplicates(["geo_id", "review_series_type", "review_series_key", "review_reason"])
    return out.sort_values(["review_reason", "geo_id", "review_series_type", "review_series_key"], kind="mergesort").reset_index(drop=True)


def _evidence(score: pd.DataFrame, shock: pd.DataFrame, *, blocking: bool, missing_focus_geography_count: int, missing_required_target_series_count: int, contract_flag_count: int) -> pd.DataFrame:
    eligible = score[score["eligible_observation"]]
    independent_chronology_trigger_count = int(eligible["chronology_delay_trigger"].sum())
    independent_attenuation_trigger_count = int(eligible["attenuation_trigger"].sum())
    independent_shock_trigger_count = int(shock["shock_suppression_trigger"].sum())
    supporting_seasonality_overkill_count = int(eligible["seasonality_overkill_trigger"].sum())
    independent_family_count = sum(count > 0 for count in [independent_chronology_trigger_count, independent_attenuation_trigger_count, independent_shock_trigger_count])
    if blocking:
        recommendation = RECOMMENDATIONS[0]
    elif len(eligible) == 0:
        recommendation = RECOMMENDATIONS[1]
    elif independent_family_count >= 2:
        recommendation = RECOMMENDATIONS[3]
    else:
        recommendation = RECOMMENDATIONS[2]
    aggregate = {
        "row_type": "aggregate",
        "geo_id": "ALL",
        "review_series_type": "aggregate",
        "review_series_key": "aggregate",
        "recommendation": recommendation,
        "independent_chronology_trigger_count": independent_chronology_trigger_count,
        "independent_attenuation_trigger_count": independent_attenuation_trigger_count,
        "independent_shock_trigger_count": independent_shock_trigger_count,
        "supporting_seasonality_overkill_count": supporting_seasonality_overkill_count,
        "independent_trigger_family_count": independent_family_count,
        "eligible_observation_count": int(len(eligible)),
        "missing_focus_geography_count": missing_focus_geography_count,
        "missing_required_target_series_count": missing_required_target_series_count,
        "contract_flag_count": contract_flag_count,
    }
    detail = score[["geo_id", "review_series_type", "review_series_key", "recommendation", "chronology_delay_trigger", "attenuation_trigger", "seasonality_overkill_trigger", "shock_suppression_trigger", "independent_trigger_family_count"]].copy()
    detail.insert(0, "row_type", "detail")
    for column in ["independent_chronology_trigger_count", "independent_attenuation_trigger_count", "independent_shock_trigger_count", "supporting_seasonality_overkill_count", "eligible_observation_count", "missing_focus_geography_count", "missing_required_target_series_count", "contract_flag_count"]:
        detail[column] = np.nan
    return pd.concat([pd.DataFrame([aggregate]), detail], ignore_index=True, sort=False).sort_values(["row_type", "geo_id", "review_series_type", "review_series_key"], kind="mergesort").reset_index(drop=True)


if __name__ == "__main__":
    build_price_family_phase2_review()
