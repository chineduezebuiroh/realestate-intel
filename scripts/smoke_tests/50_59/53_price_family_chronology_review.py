from __future__ import annotations
# scripts/smoke_tests/50_59/53_price_family_chronology_review.py

import filecmp
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from regime.experiments.linked_price_family_comparison import (
    STRUCTURAL_CHALLENGER_IDS,
    build_linked_price_family_comparison,
)
from regime.experiments.price_family_phase2_diagnostics import (
    CHRONOLOGY_ARTIFACTS,
    CHRONOLOGY_DIR,
    candidate_chronology_dir,
    _chronology_flags,
    _turning_lag,
    build_phase2_chronology,
)

REQUIRED_NUMERIC = {
    "chronology_monthly.csv": [
        "baseline_metric_score",
        "challenger_metric_score",
        "difference_challenger_minus_baseline",
    ],
    "chronology_period_summary.csv": [
        "baseline_row_count",
        "challenger_row_count",
        "overlap_row_count",
        "baseline_mean",
        "challenger_mean",
        "mean_absolute_difference",
    ],
    "turning_point_lag_summary.csv": [
        "signed_lag_months",
        "absolute_lag_months",
        "median_absolute_lag_months_by_series",
        "p90_absolute_lag_months_by_series",
        "maximum_absolute_lag_months_by_series",
        "baseline_meaningful_turn_count",
        "challenger_meaningful_turn_count",
        "matched_turn_count",
        "baseline_unmatched_turn_count",
        "challenger_unmatched_turn_count",
        "matched_turn_share",
        "turn_prominence_minimum",
        "turn_minimum_separation_months",
        "turn_prominence_window_months",
        "turn_prominence_min_side_observations",
        "turn_prominence_min_side_coverage_months",
        "turn_max_match_window_months",
    ],
    "affordability_shock_summary.csv": [
        "aligned_observation_count",
        "mortgage_observation_count",
        "structural_median_sale_price_change",
        "structural_median_sale_price_pct_change",
        "mortgage_rate_change",
        "payment_burden_change",
        "price_to_income_change",
    ],
    "chronology_flags.csv": ["signed_lag_months"],
}


def _assert_numeric_contract(path: Path, frame: pd.DataFrame) -> None:
    for column in REQUIRED_NUMERIC.get(path.name, []):
        if column not in frame.columns:
            raise AssertionError(f"{path}: missing required numeric column {column}")
        converted = pd.to_numeric(frame[column], errors="coerce")
        bad_text = frame[column].notna() & converted.isna()
        if bad_text.any():
            row = frame.loc[bad_text].head(1).to_dict("records")[0]
            raise AssertionError(
                f"{path}: non-numeric non-null {column}; "
                f"geography={row.get('geo_id')} series={row.get('series_key')}"
            )
        if np.isinf(converted.dropna()).any():
            row = frame.loc[np.isinf(converted.fillna(0))].head(1).to_dict("records")[0]
            raise AssertionError(
                f"{path}: infinite {column}; "
                f"geography={row.get('geo_id')} series={row.get('series_key')}"
            )


def _assert_artifact(path: Path, keys: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise AssertionError(f"{path}: missing required chronology artifact")
    frame = pd.read_csv(path)
    if frame.duplicated(keys).any():
        dupes = frame.loc[frame.duplicated(keys, keep=False), keys].head(20).to_string(index=False)
        raise AssertionError(
            f"{path}: duplicate rows; expected key grain {keys}; observed:\n{dupes}"
        )
    _assert_numeric_contract(path, frame)
    return frame


def _assert_candidate_identity(frame: pd.DataFrame, path: Path, candidate_id: str) -> None:
    if "candidate_id" not in frame.columns:
        raise AssertionError(f"{path}: missing candidate_id column")
    if frame.empty:
        return
    if frame["candidate_id"].isna().any():
        raise AssertionError(f"{path}: null candidate_id values")
    if set(frame["candidate_id"]) != {candidate_id}:
        raise AssertionError(f"{path}: candidate_id mismatch")


def _run_once(path: Path, comparison_result: dict[str, pd.DataFrame], candidate_id: str) -> None:
    build_phase2_chronology(
        path,
        comparison_result=comparison_result,
        reproducibility_checked=True,
        candidate_id=candidate_id,
    )
    monthly = _assert_artifact(
        path / "chronology_monthly.csv",
        ["geo_id", "series_type", "series_key", "date"],
    )
    _assert_artifact(
        path / "chronology_period_summary.csv",
        ["geo_id", "series_type", "series_key", "period"],
    )
    _assert_artifact(
        path / "turning_point_lag_summary.csv",
        [
            "geo_id",
            "series_type",
            "series_key",
            "comparison",
            "direction",
            "baseline_turn_date",
            "challenger_turn_date",
        ],
    )
    _assert_artifact(path / "affordability_shock_summary.csv", ["geo_id", "period"])
    _assert_artifact(
        path / "chronology_flags.csv",
        [
            "geo_id",
            "series_type",
            "series_key",
            "flag_type",
            "direction",
            "baseline_turn_date",
            "challenger_turn_date",
            "signed_lag_months",
            "detail",
        ],
    )
    for name in CHRONOLOGY_ARTIFACTS:
        if name.endswith(".csv"):
            _assert_candidate_identity(pd.read_csv(path / name), path / name, candidate_id)
    coverage_metrics = _assert_artifact(
        path / "coverage_metrics.csv",
        ["candidate_id", "geo_id", "series_type", "series_key"],
    )
    for column in ["first_source_date", "first_valid_structural_date", "last_date", "usable_months", "warmup_months", "overlap_months_vs_common_evaluation_window"]:
        if column not in coverage_metrics.columns:
            raise AssertionError(f"coverage_metrics.csv missing {column}")
    expected_geos = {"district_of_columbia_dc__county", "alameda_county_ca__county"}
    found = set(monthly["geo_id"].unique())
    if not expected_geos.issubset(found):
        raise AssertionError(
            f"chronology_monthly.csv: missing focus geographies {sorted(expected_geos - found)}"
        )




def _synthetic_monthly(
    values: list[float],
    challenger: list[float],
    series_key: str = "synthetic",
    dates: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    if dates is None:
        dates = pd.date_range("2020-01-31", periods=len(values), freq="M")
    return pd.DataFrame(
        {
            "geo_id": "geo_synth",
            "series_type": "metric_score",
            "series_key": series_key,
            "date": dates,
            "baseline_metric_score": values,
            "challenger_metric_score": challenger,
        }
    )


def _spike(length: int, index: int, magnitude: float = 2.0) -> list[float]:
    values = [0.0] * length
    values[index - 1] = magnitude / 2
    values[index] = magnitude
    values[index + 1] = magnitude / 2
    return values


def _run_synthetic_turning_point_validation() -> None:
    sparse_dates = pd.DatetimeIndex(
        pd.to_datetime(
            [
                "2020-01-31",
                "2020-04-30",
                "2020-07-31",
                "2020-10-31",
                "2021-01-31",
                "2021-04-30",
                "2021-07-31",
                "2021-10-31",
                "2022-01-31",
                "2022-04-30",
                "2022-07-31",
                "2022-10-31",
                "2023-01-31",
            ]
        )
    )
    sparse_values = [
        0.0,
        0.05,
        0.10,
        0.16,
        0.22,
        0.27,
        0.30,
        0.27,
        0.22,
        0.16,
        0.10,
        0.05,
        0.0,
    ]
    lag = _turning_lag(
        _synthetic_monthly(
            sparse_values,
            sparse_values,
            "sparse_not_six_calendar_months",
            dates=sparse_dates,
        ),
        {},
    )
    if not lag.empty:
        raise AssertionError("sparse observations masqueraded as a six-month prominence window")

    valid_missing_dates = pd.DatetimeIndex(
        pd.to_datetime(
            [
                "2020-01-31",
                "2020-02-29",
                "2020-03-31",
                "2020-05-31",
                "2020-06-30",
                "2020-07-31",
                "2020-08-31",
                "2020-09-30",
                "2020-11-30",
                "2020-12-31",
                "2021-01-31",
            ]
        )
    )
    valid_missing_values = [0.0, 0.05, 0.10, 0.18, 0.24, 0.30, 0.24, 0.18, 0.10, 0.05, 0.0]
    lag = _turning_lag(
        _synthetic_monthly(
            valid_missing_values,
            valid_missing_values,
            "valid_missing_months",
            dates=valid_missing_dates,
        ),
        {},
    )
    if len(lag[lag["direction"].eq("peak")]) != 1:
        raise AssertionError("valid rounded turn with missing intermediate months was not detected")

    rounded_peak = [0.0] * 18
    for offset, value in enumerate([0.0, 0.05, 0.10, 0.16, 0.22, 0.27, 0.30, 0.27, 0.22, 0.16, 0.10, 0.05, 0.0], start=2):
        rounded_peak[offset] = value
    lag = _turning_lag(_synthetic_monthly(rounded_peak, rounded_peak, "rounded_peak"), {})
    peaks = lag[lag["direction"].eq("peak")]
    if len(peaks) != 1 or not bool(peaks.iloc[0]["matched"]):
        raise AssertionError("broad rounded peak was not detected by window prominence")

    rounded_trough = [-value for value in rounded_peak]
    lag = _turning_lag(_synthetic_monthly(rounded_trough, rounded_trough, "rounded_trough"), {})
    troughs = lag[lag["direction"].eq("trough")]
    if len(troughs) != 1 or not bool(troughs.iloc[0]["matched"]):
        raise AssertionError("broad rounded trough was not detected by window prominence")

    edge_peak = _spike(14, 3)
    lag = _turning_lag(_synthetic_monthly(edge_peak, edge_peak, "edge_peak"), {})
    if not lag.empty:
        raise AssertionError("edge candidate without complete prominence windows was detected")

    noisy = [0.0, 0.03] * 12
    lag = _turning_lag(_synthetic_monthly(noisy, noisy, "tiny_oscillation"), {})
    score_lag = lag[lag["comparison"].eq("score_vs_baseline")]
    if not score_lag.empty:
        raise AssertionError("tiny high-frequency oscillations produced meaningful turns")

    lag = _turning_lag(_synthetic_monthly(_spike(24, 8), _spike(24, 15), "within_window"), {})
    matched = lag[lag["matched"]]
    if len(matched) != 1 or int(matched.iloc[0]["signed_lag_months"]) != 7:
        raise AssertionError("true delayed turn within 12 months was not matched correctly")

    lag = _turning_lag(_synthetic_monthly(_spike(60, 8), _spike(60, 44), "outside_window"), {})
    if lag["matched"].any() or lag["absolute_lag_months"].notna().any():
        raise AssertionError("same-direction turn several years later was matched or inflated lag stats")
    row = lag.iloc[0]
    if row["baseline_unmatched_turn_count"] != 1 or row["challenger_unmatched_turn_count"] != 1:
        raise AssertionError("unmatched turns were not reported separately")

    lag = _turning_lag(_synthetic_monthly(_spike(24, 8), [-v for v in _spike(24, 12)], "direction_guard"), {})
    if lag["matched"].any():
        raise AssertionError("peak was matched to trough")

    baseline = [0.0] * 40
    challenger = [0.0] * 40
    for idx in (6, 18, 30):
        baseline[idx - 1] = 1.0
        baseline[idx] = 2.0
        baseline[idx + 1] = 1.0
    for idx in (8, 20, 32):
        challenger[idx - 1] = 1.0
        challenger[idx] = 2.0
        challenger[idx + 1] = 1.0
    lag = _turning_lag(_synthetic_monthly(baseline, challenger, "one_to_one"), {})
    matched = lag[lag["matched"]].sort_values("baseline_turn_date")
    if len(matched) != 3 or matched["challenger_turn_date"].duplicated().any():
        raise AssertionError("one-to-one chronological ordering was not preserved")
    if matched["signed_lag_months"].tolist() != [2, 2, 2]:
        raise AssertionError("chronological matching selected unexpected lags")

    first = _turning_lag(_synthetic_monthly(baseline, challenger, "deterministic"), {})
    second = _turning_lag(_synthetic_monthly(baseline, challenger, "deterministic"), {})
    pd.testing.assert_frame_equal(first, second)

def _run_synthetic_chronology_flag_validation() -> None:
    period = pd.DataFrame(
        columns=["geo_id", "series_type", "series_key", "period", "available"]
    )
    shock = pd.DataFrame(
        columns=[
            "geo_id",
            "aligned_observation_count",
            "payment_burden_change",
            "payment_burden_price_to_income_distinct_aligned",
        ]
    )
    lag_rows: list[dict[str, object]] = []

    for index, lag_months in enumerate([0, 0, 0, 0, 10]):
        baseline = pd.Timestamp("2020-01-31") + pd.DateOffset(
            months=index * 2
        )
        lag_rows.append(
            {
                "geo_id": "geo_alpha",
                "series_type": "metric_score",
                "series_key": "median_sale_price",
                "comparison": "score_vs_baseline",
                "direction": "peak",
                "baseline_turn_date": baseline,
                "challenger_turn_date": (
                    baseline + pd.DateOffset(months=lag_months)
                ),
                "signed_lag_months": lag_months,
                "absolute_lag_months": abs(lag_months),
                "matched": True,
                "matched_turn_count": 5,
                "matched_turn_share": 1.0,
            }
        )

    for index, lag_months in enumerate([10, 10, 10, 10]):
        baseline = pd.Timestamp("2020-01-31") + pd.DateOffset(
            months=index * 2
        )
        lag_rows.append(
            {
                "geo_id": "geo_beta",
                "series_type": "metric_score",
                "series_key": "median_sale_price",
                "comparison": "score_vs_baseline",
                "direction": "peak",
                "baseline_turn_date": baseline,
                "challenger_turn_date": (
                    baseline + pd.DateOffset(months=lag_months)
                ),
                "signed_lag_months": lag_months,
                "absolute_lag_months": abs(lag_months),
                "matched": True,
                "matched_turn_count": 4,
                "matched_turn_share": 1.0,
            }
        )

    for index in range(3):
        baseline = pd.Timestamp("2021-01-31") + pd.DateOffset(
            months=index * 4
        )
        lag_rows.append(
            {
                "geo_id": "geo_gamma",
                "series_type": "metric_score",
                "series_key": "payment_burden",
                "comparison": "score_vs_baseline",
                "direction": "trough",
                "baseline_turn_date": baseline,
                "challenger_turn_date": (
                    baseline - pd.DateOffset(months=2)
                ),
                "signed_lag_months": -2,
                "absolute_lag_months": 2,
                "matched": True,
                "matched_turn_count": 3,
                "matched_turn_share": 1.0,
            }
        )

    lag = pd.DataFrame(lag_rows)
    flags = _chronology_flags(period, lag, shock)

    key = [
        "geo_id",
        "series_type",
        "series_key",
        "flag_type",
        "direction",
        "baseline_turn_date",
        "challenger_turn_date",
        "signed_lag_months",
        "detail",
    ]
    if flags.duplicated(key).any():
        raise AssertionError(
            "synthetic chronology_flags.csv: duplicate rows at revised "
            "turning-point key grain"
        )

    large = flags[
        flags["flag_type"].eq(
            "large_lag_relative_to_observed_distribution"
        )
    ]
    if len(large) != 1:
        raise AssertionError(
            "synthetic chronology_flags.csv: expected exactly one "
            "series-specific p90 flag"
        )

    only = large.iloc[0]
    if only["geo_id"] != "geo_alpha" or only["signed_lag_months"] != 10:
        raise AssertionError(
            "synthetic chronology_flags.csv: p90 flag was not scoped "
            "to the eligible series"
        )

    if large["geo_id"].eq("geo_beta").any():
        raise AssertionError(
            "synthetic chronology_flags.csv: group with fewer than five "
            "turns emitted p90 flag"
        )

    inversions = flags[
        flags["flag_type"].eq("chronology_inversion_review")
    ]
    if len(inversions) != 3:
        raise AssertionError(
            "synthetic chronology_flags.csv: equal-lag inversion turns "
            "were not preserved"
        )

    inversion_detail = inversions[
        [
            "baseline_turn_date",
            "challenger_turn_date",
            "signed_lag_months",
        ]
    ]
    if inversion_detail.isna().any().any():
        raise AssertionError(
            "synthetic chronology_flags.csv: inversion flags lost "
            "turning-point detail"
        )

def main() -> int:
    _run_synthetic_turning_point_validation()
    print("[price_family_phase2_chronology] synthetic turning-point validation OK")
    _run_synthetic_chronology_flag_validation()
    print("[price_family_phase2_chronology] synthetic flag validation OK")
    for candidate_id in STRUCTURAL_CHALLENGER_IDS:
        out_dir = candidate_chronology_dir(candidate_id)
        print(f"[price_family_phase2_chronology] candidate={candidate_id} building upstream comparison once...")
        comparison_result = build_linked_price_family_comparison(challenger_id=candidate_id)
        print(f"[price_family_phase2_chronology] candidate={candidate_id} building deterministic chronology artifacts twice...")
        with tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp) / candidate_id / "first"
            second = Path(tmp) / candidate_id / "second"
            _run_once(first, comparison_result, candidate_id)
            _run_once(second, comparison_result, candidate_id)
            for name in CHRONOLOGY_ARTIFACTS:
                if not filecmp.cmp(first / name, second / name, shallow=False):
                    raise AssertionError(f"{candidate_id} {name}: deterministic rerun row-level equality failed")
            if out_dir.exists():
                shutil.rmtree(out_dir)
            shutil.copytree(second, out_dir)
        print(f"[price_family_phase2_chronology] candidate={candidate_id} artifacts={out_dir}")

    if not set(STRUCTURAL_CHALLENGER_IDS).issubset({p.parent.name for p in CHRONOLOGY_DIR.parent.parent.glob("*/phase2_chronology")}):
        raise AssertionError("not all structural candidates produced isolated chronology artifacts")
    monthly = pd.read_csv(candidate_chronology_dir(STRUCTURAL_CHALLENGER_IDS[-1]) / "chronology_monthly.csv")
    lag = pd.read_csv(candidate_chronology_dir(STRUCTURAL_CHALLENGER_IDS[-1]) / "turning_point_lag_summary.csv")
    flags = pd.read_csv(candidate_chronology_dir(STRUCTURAL_CHALLENGER_IDS[-1]) / "chronology_flags.csv")
    coverage = monthly.groupby("geo_id")["date"].agg(["min", "max", "count"]).reset_index()
    lag_summary = (
        lag.groupby("geo_id")["absolute_lag_months"].agg(["count", "median", "max"]).reset_index()
        if not lag.empty
        else pd.DataFrame()
    )
    print(f"[price_family_phase2_chronology] monthly_rows={len(monthly)} lag_rows={len(lag)} flags={len(flags)}")
    print("[price_family_phase2_chronology] coverage:\n" + coverage.to_string(index=False))
    if not lag_summary.empty:
        print("[price_family_phase2_chronology] lag_summary:\n" + lag_summary.to_string(index=False))
    print(f"[price_family_phase2_chronology] artifacts_root={CHRONOLOGY_DIR.parent.parent}")
    print("[price_family_phase2_chronology] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
