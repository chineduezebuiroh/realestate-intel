from __future__ import annotations
# scripts/smoke_tests/50_59/55_price_family_phase2_review.py

import filecmp
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from regime.experiments.price_family_phase2_diagnostics import CHRONOLOGY_ARTIFACTS, CHRONOLOGY_DIR, STABILITY_ARTIFACTS, STABILITY_DIR
from regime.experiments.price_family_phase2_review import (
    FOCUS_GEOS,
    MATERIAL_LAG_SHARE,
    RECOMMENDATIONS,
    REVIEW_ARTIFACTS,
    REVIEW_DIR,
    SEASONALITY_REVIEW_COLUMNS,
    _contract_flag_count,
    _evidence,
    _finalize_scorecard,
    _shock_review,
    build_price_family_phase2_review,
)

NUMERIC_COLUMNS = {
    "smoothing_scorecard.csv": [
        "matched_turning_point_count", "unmatched_turning_point_count", "median_absolute_lag_months",
        "p75_absolute_lag_months", "p90_absolute_lag_months", "maximum_absolute_lag_months",
        "share_matched_turns_lag_ge_3m", "share_matched_turns_lag_ge_6m", "share_matched_turns_lag_ge_12m",
        "chronology_inversion_count", "large_lag_flag_count", "baseline_mean_absolute_1m_change",
        "challenger_mean_absolute_1m_change", "challenger_baseline_volatility_ratio", "percent_volatility_reduction",
        "baseline_challenger_correlation", "mean_absolute_baseline_challenger_difference", "baseline_sign_flip_rate",
        "challenger_sign_flip_rate", "sign_flip_rate_change", "baseline_near_zero_rate", "challenger_near_zero_rate",
        "near_zero_rate_change", "baseline_seasonal_spread", "challenger_seasonal_spread",
        "challenger_baseline_seasonal_spread_ratio", "percent_seasonal_spread_reduction",
    ],
    "seasonality_review.csv": [
        "baseline_seasonal_spread", "challenger_seasonal_spread", "challenger_baseline_seasonal_spread_ratio",
        "percent_seasonal_spread_reduction", "baseline_minimum_month", "baseline_maximum_month",
        "challenger_minimum_month", "challenger_maximum_month",
    ],
    "shock_review.csv": [
        "aligned_observation_count", "mortgage_rate_change", "structural_median_sale_price_change",
        "structural_median_sale_price_pct_change", "payment_burden_change", "price_to_income_change",
    ],
    "ma6_trigger_evidence.csv": [
        "independent_chronology_trigger_count", "independent_attenuation_trigger_count",
        "independent_shock_trigger_count", "supporting_seasonality_overkill_count",
        "independent_trigger_family_count", "eligible_observation_count", "missing_focus_geography_count",
        "missing_required_target_series_count", "contract_flag_count",
    ],
}
KEY_GRAINS = {
    "smoothing_scorecard.csv": ["geo_id", "review_series_type", "review_series_key"],
    "metric_review.csv": ["geo_id", "review_series_type", "review_series_key"],
    "chronology_outliers.csv": ["source", "geo_id", "series_type", "series_key", "comparison", "flag_type", "direction", "baseline_turn_date", "challenger_turn_date", "signed_lag_months", "detail"],
    "seasonality_review.csv": ["geo_id", "review_series_type", "review_series_key"],
    "shock_review.csv": ["geo_id", "period"],
    "focus_case_manifest.csv": ["geo_id", "review_series_type", "review_series_key", "review_reason"],
    "ma6_trigger_evidence.csv": ["row_type", "geo_id", "review_series_type", "review_series_key"],
}


def _assert_numeric(path: Path, frame: pd.DataFrame) -> None:
    for column in NUMERIC_COLUMNS.get(path.name, []):
        if column not in frame.columns:
            raise AssertionError(f"{path}: missing numeric contract column {column}")
        converted = pd.to_numeric(frame[column], errors="coerce")
        bad_text = frame[column].notna() & converted.isna()
        if bad_text.any():
            raise AssertionError(f"{path}: non-null non-numeric text in {column}")
        if np.isinf(converted.dropna()).any():
            raise AssertionError(f"{path}: infinite numeric values in {column}")


def _artifact(path: Path, keys: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise AssertionError(f"missing required artifact: {path}")
    frame = pd.read_csv(path) if path.suffix == ".csv" else pd.DataFrame()
    if keys:
        missing = [column for column in keys if column not in frame.columns]
        if missing:
            raise AssertionError(f"{path}: missing key-grain columns {missing}")
        if frame.duplicated(keys).any():
            dupes = frame.loc[frame.duplicated(keys, keep=False), keys].head(10).to_string(index=False)
            raise AssertionError(f"{path}: duplicate rows for grain {keys}:\n{dupes}")
    _assert_numeric(path, frame)
    return frame


def _required_inputs() -> None:
    for name in CHRONOLOGY_ARTIFACTS:
        _artifact(CHRONOLOGY_DIR / name)
    for name in STABILITY_ARTIFACTS:
        _artifact(STABILITY_DIR / name)


def _validate_outputs(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    for name in REVIEW_ARTIFACTS:
        _artifact(path / name, KEY_GRAINS.get(name))
    score = pd.read_csv(path / "smoothing_scorecard.csv")
    metric = pd.read_csv(path / "metric_review.csv")
    seasonality = pd.read_csv(path / "seasonality_review.csv")
    evidence = pd.read_csv(path / "ma6_trigger_evidence.csv")
    required_score = set(NUMERIC_COLUMNS["smoothing_scorecard.csv"]) | {"chronology_delay_trigger", "attenuation_trigger", "seasonality_overkill_trigger", "recommendation"}
    missing_score = required_score - set(score.columns)
    if missing_score:
        raise AssertionError(f"smoothing_scorecard.csv missing columns {sorted(missing_score)}")
    missing_seasonality = set(SEASONALITY_REVIEW_COLUMNS) - set(seasonality.columns)
    if missing_seasonality:
        raise AssertionError(f"seasonality_review.csv missing explicit columns {sorted(missing_seasonality)}")
    bad_recommendations = set(score["recommendation"].dropna()) - set(RECOMMENDATIONS)
    if bad_recommendations:
        raise AssertionError(f"invalid recommendation values {sorted(bad_recommendations)}")
    if not set(FOCUS_GEOS).issubset(set(score["geo_id"])):
        raise AssertionError("review outputs missing DC County or Alameda County")
    upstream_metrics = set(pd.read_csv(CHRONOLOGY_DIR / "chronology_monthly.csv").query("series_type == 'metric_score'")["series_key"])
    required_metrics = {"median_sale_price", "median_ppsf", "price_to_income", "payment_burden"} & upstream_metrics
    found_metrics = set(metric.query("review_series_type == 'metric'")["review_series_key"])
    if not required_metrics.issubset(found_metrics):
        raise AssertionError(f"missing target metrics supported upstream: {sorted(required_metrics - found_metrics)}")
    aggregate = evidence[evidence["row_type"].eq("aggregate")]
    if len(aggregate) != 1:
        raise AssertionError("ma6_trigger_evidence.csv must contain one aggregate row")
    for column in NUMERIC_COLUMNS["ma6_trigger_evidence.csv"]:
        if column not in aggregate.columns or pd.isna(aggregate[column].iloc[0]):
            raise AssertionError(f"aggregate recommendation lost trigger evidence column {column}")
    return score, evidence


def _synthetic_validation() -> None:
    base = pd.DataFrame([
        {"geo_id":"g1","review_series_type":"metric","review_series_key":"median_sale_price","matched_turning_point_count":5,"maximum_absolute_lag_months":99,"median_absolute_lag_months":0,"share_matched_turns_lag_ge_6m":0,"percent_volatility_reduction":0,"baseline_challenger_correlation":1,"percent_seasonal_spread_reduction":0},
        {"geo_id":"g2","review_series_type":"metric","review_series_key":"median_sale_price","matched_turning_point_count":5,"maximum_absolute_lag_months":8,"median_absolute_lag_months":6,"share_matched_turns_lag_ge_6m":MATERIAL_LAG_SHARE,"percent_volatility_reduction":0.85,"baseline_challenger_correlation":0.5,"percent_seasonal_spread_reduction":0},
        {"geo_id":"g3","review_series_type":"metric","review_series_key":"median_sale_price","matched_turning_point_count":5,"maximum_absolute_lag_months":0,"median_absolute_lag_months":0,"share_matched_turns_lag_ge_6m":0,"percent_volatility_reduction":0,"baseline_challenger_correlation":1,"percent_seasonal_spread_reduction":0.95},
        {"geo_id":"g4","review_series_type":"metric","review_series_key":"median_sale_price","matched_turning_point_count":5,"maximum_absolute_lag_months":10,"median_absolute_lag_months":10,"share_matched_turns_lag_ge_6m":1,"percent_volatility_reduction":0,"baseline_challenger_correlation":1,"percent_seasonal_spread_reduction":0.9},
    ])
    for column in ["unmatched_turning_point_count", "chronology_inversion_count", "large_lag_flag_count"]:
        base[column] = 0
    for column in ["near_zero_rate_change", "sign_flip_rate_change"]:
        base[column] = 0
    out = _finalize_scorecard(base, blocking=False)
    rec = dict(zip(out.geo_id, out.recommendation, strict=True))
    if rec["g1"] == "ma6_finalist_warranted":
        raise AssertionError("isolated extreme maximum alone warranted MA6")
    if rec["g2"] != "ma6_finalist_warranted":
        raise AssertionError("material lag share plus attenuation did not warrant MA6")
    if rec["g3"] == "ma6_finalist_warranted":
        raise AssertionError("strong seasonality alone warranted MA6")
    if rec["g4"] == "ma6_finalist_warranted":
        raise AssertionError("chronology plus derivative seasonality warranted MA6 without independent attenuation/shock")
    blocked = _finalize_scorecard(base.iloc[[1]], blocking=True)
    if blocked["recommendation"].iloc[0] != "blocking_diagnostic_issue":
        raise AssertionError("blocking coverage did not override other recommendation")
    non_contract = pd.DataFrame([{"severity":"review"}])
    contract = pd.DataFrame([{"severity":"contract"}])
    if _contract_flag_count(non_contract) != 0:
        raise AssertionError("non-contract stability flag produced blocking count")
    if _contract_flag_count(contract) != 1:
        raise AssertionError("contract stability flag did not produce blocking count")
    shock = pd.DataFrame([{"geo_id":"g1","period":"p","payment_burden_price_to_income_distinct_aligned":True}])
    flags = pd.DataFrame(columns=["geo_id", "series_type", "series_key", "flag_type", "detail"])
    if bool(_shock_review(shock, flags)["shock_suppression_trigger"].iloc[0]):
        raise AssertionError("distinct-aligned shock evidence implied suppression")
    materiality_flags = pd.DataFrame([{"geo_id":"g1", "series_type":"metric_score", "series_key":"payment_burden", "flag_type":"affordability_shock_materiality_review", "detail":"p"}])
    materiality_review = _shock_review(shock, materiality_flags)
    if not bool(materiality_review["affordability_materiality_review_flag"].iloc[0]):
        raise AssertionError("explicit affordability materiality flag was not preserved")
    if not bool(materiality_review["shock_suppression_trigger"].iloc[0]):
        raise AssertionError("explicit affordability materiality flag did not imply shock suppression")
    not_distinct_flags = pd.DataFrame([{"geo_id":"g1", "series_type":"metric_score", "series_key":"price_to_income", "flag_type":"price_to_income_payment_burden_not_distinct", "detail":"p"}])
    not_distinct_review = _shock_review(shock, not_distinct_flags)
    if not bool(not_distinct_review["payment_burden_price_to_income_not_distinct_flag"].iloc[0]):
        raise AssertionError("explicit payment-burden/price-to-income not-distinct flag was not preserved")
    if not bool(not_distinct_review["shock_suppression_trigger"].iloc[0]):
        raise AssertionError("explicit payment-burden/price-to-income not-distinct flag did not imply shock suppression")
    evidence = _evidence(out[out.geo_id.eq("g4")], _shock_review(shock, flags), blocking=False, missing_focus_geography_count=0, missing_required_target_series_count=0, contract_flag_count=0)
    if evidence[evidence.row_type.eq("aggregate")]["recommendation"].iloc[0] == "ma6_finalist_warranted":
        raise AssertionError("aggregate counted supporting seasonality as an independent trigger")


def main() -> int:
    _required_inputs()
    _synthetic_validation()
    with tempfile.TemporaryDirectory() as tmp:
        first = Path(tmp) / "first"
        second = Path(tmp) / "second"
        build_price_family_phase2_review(first)
        build_price_family_phase2_review(second)
        for name in REVIEW_ARTIFACTS:
            if not filecmp.cmp(first / name, second / name, shallow=False):
                raise AssertionError(f"{name}: deterministic rerun byte equality failed")
        score, evidence = _validate_outputs(second)
        if REVIEW_DIR.exists():
            shutil.rmtree(REVIEW_DIR)
        shutil.copytree(second, REVIEW_DIR)
    aggregate = evidence[evidence.row_type.eq("aggregate")].iloc[0]
    print(f"[price_family_phase2_review] aggregate_recommendation={aggregate.recommendation}")
    print("[price_family_phase2_review] trigger_family_counts=" + ", ".join(f"{column}={int(aggregate[column])}" for column in ["independent_chronology_trigger_count", "independent_attenuation_trigger_count", "independent_shock_trigger_count", "supporting_seasonality_overkill_count"]))
    print("[price_family_phase2_review] top chronology outliers:\n" + score.sort_values(["share_matched_turns_lag_ge_6m", "p90_absolute_lag_months"], ascending=[False, False], kind="mergesort").head(5)[["geo_id", "review_series_type", "review_series_key", "share_matched_turns_lag_ge_6m", "p90_absolute_lag_months"]].to_string(index=False))
    print("[price_family_phase2_review] top attenuation cases:\n" + score.sort_values(["percent_volatility_reduction", "baseline_challenger_correlation"], ascending=[False, True], kind="mergesort").head(5)[["geo_id", "review_series_type", "review_series_key", "percent_volatility_reduction", "baseline_challenger_correlation"]].to_string(index=False))
    print(f"[price_family_phase2_review] focus_case_manifest={REVIEW_DIR / 'focus_case_manifest.csv'}")
    print("[price_family_phase2_review] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
