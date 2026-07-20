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
    build_linked_price_family_comparison,
)
from regime.experiments.price_family_phase2_diagnostics import (
    CHRONOLOGY_ARTIFACTS,
    CHRONOLOGY_DIR,
    PRODUCTION_CANDIDATE_ID,
    _chronology_flags,
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
        "maximum_absolute_lag_months_by_series",
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


def _run_once(path: Path, comparison_result: dict[str, pd.DataFrame]) -> None:
    build_phase2_chronology(
        path,
        comparison_result=comparison_result,
        reproducibility_checked=True,
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
    expected_geos = {"district_of_columbia_dc__county", "alameda_county_ca__county"}
    found = set(monthly["geo_id"].unique())
    if not expected_geos.issubset(found):
        raise AssertionError(
            f"chronology_monthly.csv: missing focus geographies {sorted(expected_geos - found)}"
        )



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
            }
        )

    for index in range(2):
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
    if len(inversions) != 2:
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
    _run_synthetic_chronology_flag_validation()
    print("[price_family_phase2_chronology] synthetic flag validation OK")
    print("[price_family_phase2_chronology] building upstream comparison once...")
    comparison_result = build_linked_price_family_comparison(
        challenger_id=PRODUCTION_CANDIDATE_ID
    )
    print("[price_family_phase2_chronology] building deterministic chronology artifacts twice...")
    with tempfile.TemporaryDirectory() as tmp:
        first = Path(tmp) / "first"
        second = Path(tmp) / "second"
        _run_once(first, comparison_result)
        _run_once(second, comparison_result)
        for name in CHRONOLOGY_ARTIFACTS:
            if not filecmp.cmp(first / name, second / name, shallow=False):
                raise AssertionError(f"{name}: deterministic rerun row-level equality failed")
        if CHRONOLOGY_DIR.exists():
            shutil.rmtree(CHRONOLOGY_DIR)
        shutil.copytree(second, CHRONOLOGY_DIR)

    monthly = pd.read_csv(CHRONOLOGY_DIR / "chronology_monthly.csv")
    lag = pd.read_csv(CHRONOLOGY_DIR / "turning_point_lag_summary.csv")
    flags = pd.read_csv(CHRONOLOGY_DIR / "chronology_flags.csv")
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
    print(f"[price_family_phase2_chronology] artifacts={CHRONOLOGY_DIR}")
    print("[price_family_phase2_chronology] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
