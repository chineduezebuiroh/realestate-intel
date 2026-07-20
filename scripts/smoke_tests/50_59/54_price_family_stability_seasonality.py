from __future__ import annotations
# scripts/smoke_tests/50_59/54_price_family_stability_seasonality.py

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
    PRODUCTION_CANDIDATE_ID,
    STABILITY_ARTIFACTS,
    STABILITY_DIR,
    build_phase2_stability_seasonality,
)

REQUIRED_NUMERIC_COLUMNS = [
    "rows",
    "value_std",
    "mean_absolute_change_1m",
    "p90_absolute_change_1m",
    "maximum_absolute_change_1m",
    "sign_flip_rate",
    "near_zero_rate",
    "baseline_challenger_correlation",
    "mean_absolute_baseline_challenger_difference",
    "mean_absolute_axis_score",
    "median_absolute_axis_score",
    "near_origin_rate_005",
    "near_origin_rate_010",
    "strong_conviction_rate_025",
    "calendar_month_observation_count",
    "calendar_month_mean_absolute_change",
    "minimum_calendar_month_mean_absolute_change",
    "maximum_calendar_month_mean_absolute_change",
    "seasonal_spread",
    "baseline_seasonal_spread",
    "seasonal_spread_ratio_challenger_vs_baseline",
]


def _assert_numeric_contract(path: Path, frame: pd.DataFrame) -> None:
    for column in REQUIRED_NUMERIC_COLUMNS:
        if column not in frame.columns:
            continue
        converted = pd.to_numeric(frame[column], errors="coerce")
        bad_text = frame[column].notna() & converted.isna()
        if bad_text.any():
            row = frame.loc[bad_text].head(1).to_dict("records")[0]
            raise AssertionError(
                f"{path}: non-numeric non-null {column}; "
                f"geography={row.get('geo_id')} series={row.get('canonical_metric_key') or row.get('axis') or row.get('dimension')}"
            )
        if np.isinf(converted.dropna()).any():
            row = frame.loc[np.isinf(converted.fillna(0))].head(1).to_dict("records")[0]
            raise AssertionError(
                f"{path}: column {column} emitted infinity; "
                f"geography={row.get('geo_id')}"
            )


def _assert_frame(path: Path, keys: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise AssertionError(f"{path}: missing required stability/seasonality artifact")
    frame = pd.read_csv(path)
    if frame.duplicated(keys).any():
        dupes = frame.loc[frame.duplicated(keys, keep=False), keys].head(20).to_string(index=False)
        raise AssertionError(
            f"{path}: duplicate rows; expected key grain {keys}; observed:\n{dupes}"
        )
    _assert_numeric_contract(path, frame)
    return frame


def _run_once(path: Path, comparison_result: dict[str, pd.DataFrame]) -> None:
    build_phase2_stability_seasonality(
        path,
        comparison_result=comparison_result,
        reproducibility_checked=True,
    )
    _assert_frame(
        path / "feature_stability_summary.csv",
        [
            "geo_id",
            "canonical_metric_key",
            "feature_component",
            "feature_origin",
            "series_variant",
            "run_role",
        ],
    )
    _assert_frame(path / "metric_stability_summary.csv", ["geo_id", "canonical_metric_key", "run_role"])
    _assert_frame(path / "dimension_stability_summary.csv", ["geo_id", "dimension", "run_role"])
    _assert_frame(path / "demand_axis_stability_summary.csv", ["geo_id", "axis", "run_role"])
    _assert_frame(
        path / "seasonality_calendar_month.csv",
        [
            "series_family",
            "comparison_pair",
            "geo_id",
            "run_role",
            "calendar_month",
            "canonical_metric_key",
            "feature_component",
            "feature_origin",
            "series_variant",
            "dimension",
            "axis",
        ],
    )
    _assert_frame(
        path / "seasonality_summary.csv",
        [
            "series_family",
            "comparison_pair",
            "geo_id",
            "run_role",
            "canonical_metric_key",
            "feature_component",
            "feature_origin",
            "series_variant",
            "dimension",
            "axis",
        ],
    )
    flags = _assert_frame(path / "stability_flags.csv", ["geo_id", "series_family", "series_key", "flag_type", "detail"])
    _assert_raw_structural_pairing(path)
    contract_flags = flags[flags.get("severity", pd.Series(dtype=str)).eq("contract")]
    if not contract_flags.empty:
        raise AssertionError(
            "stability_flags.csv: isolation contract failures observed:\n"
            + contract_flags.to_string(index=False)
        )


def _assert_raw_structural_pairing(path: Path) -> None:
    feature = pd.read_csv(path / "feature_stability_summary.csv")
    seasonality = pd.read_csv(path / "seasonality_summary.csv")
    expected_geos = {"district_of_columbia_dc__county", "alameda_county_ca__county"}
    expected_metrics = {"median_sale_price", "median_ppsf"}

    raw_feature = feature[
        feature["feature_origin"].eq("structural_ma12")
        & feature["series_variant"].eq("ma12_level")
        & feature["feature_component"].eq("level")
        & feature["canonical_metric_key"].isin(expected_metrics)
        & feature["geo_id"].isin(expected_geos)
    ].copy()

    expected_pairs = {
        (geo_id, metric)
        for geo_id in expected_geos
        for metric in expected_metrics
    }

    observed_feature_pairs = set(
        raw_feature[["geo_id", "canonical_metric_key"]]
        .itertuples(index=False, name=None)
    )
    missing_feature_pairs = expected_pairs - observed_feature_pairs
    if missing_feature_pairs:
        raise AssertionError(
            "feature_stability_summary.csv: missing expected structural MA12 "
            f"feature pairs: {sorted(missing_feature_pairs)}"
        )

    for row in raw_feature.itertuples(index=False):
        if pd.isna(row.overlap_rows) or row.overlap_rows <= 0:
            raise AssertionError(
                "feature_stability_summary.csv: expected positive raw/structural "
                f"overlap for geography={row.geo_id} "
                f"metric={row.canonical_metric_key}"
            )

        if pd.isna(row.mean_absolute_baseline_challenger_difference):
            raise AssertionError(
                "feature_stability_summary.csv: raw/structural mean absolute "
                f"difference missing for geography={row.geo_id} "
                f"metric={row.canonical_metric_key}"
            )

        if row.overlap_rows >= 2 and pd.isna(
            row.baseline_challenger_correlation
        ):
            raise AssertionError(
                "feature_stability_summary.csv: raw/structural correlation missing "
                f"for geography={row.geo_id} "
                f"metric={row.canonical_metric_key}"
            )

    structural_seasonality = seasonality[
        seasonality["series_family"].eq("raw_structural_feature")
        & seasonality["comparison_pair"].eq("raw_vs_structural_ma12")
        & seasonality["feature_origin"].eq("structural_ma12")
        & seasonality["series_variant"].eq("ma12_level")
        & seasonality["feature_component"].eq("level")
        & seasonality["canonical_metric_key"].isin(expected_metrics)
        & seasonality["geo_id"].isin(expected_geos)
    ].copy()

    observed_seasonality_pairs = set(
        structural_seasonality[["geo_id", "canonical_metric_key"]]
        .itertuples(index=False, name=None)
    )
    missing_seasonality_pairs = expected_pairs - observed_seasonality_pairs
    if missing_seasonality_pairs:
        raise AssertionError(
            "seasonality_summary.csv: missing expected structural MA12 "
            f"seasonality pairs: {sorted(missing_seasonality_pairs)}"
        )
        
    for row in structural_seasonality.itertuples(index=False):
        if pd.notna(row.baseline_seasonal_spread):
            if row.baseline_seasonal_spread != 0 and pd.isna(row.seasonal_spread_ratio_challenger_vs_baseline):
                raise AssertionError(
                    "seasonality_summary.csv: structural MA12 seasonal spread ratio missing "
                    f"for geography={row.geo_id} metric={row.canonical_metric_key}"
                )
        elif pd.notna(row.seasonal_spread):
            raise AssertionError(
                "seasonality_summary.csv: structural MA12 row has no raw baseline spread "
                f"for geography={row.geo_id} metric={row.canonical_metric_key}"
            )

def main() -> int:
    print("[price_family_phase2_stability] building upstream comparison once...")
    comparison_result = build_linked_price_family_comparison(
        challenger_id=PRODUCTION_CANDIDATE_ID
    )
    print("[price_family_phase2_stability] building deterministic stability/seasonality artifacts twice...")
    with tempfile.TemporaryDirectory() as tmp:
        first = Path(tmp) / "first"
        second = Path(tmp) / "second"
        _run_once(first, comparison_result)
        _run_once(second, comparison_result)
        for name in STABILITY_ARTIFACTS:
            if not filecmp.cmp(first / name, second / name, shallow=False):
                raise AssertionError(f"{name}: deterministic rerun row-level equality failed")
        if STABILITY_DIR.exists():
            shutil.rmtree(STABILITY_DIR)
        shutil.copytree(second, STABILITY_DIR)

    metric = pd.read_csv(STABILITY_DIR / "metric_stability_summary.csv")
    axis = pd.read_csv(STABILITY_DIR / "demand_axis_stability_summary.csv")
    seasonality = pd.read_csv(STABILITY_DIR / "seasonality_summary.csv")
    flags = pd.read_csv(STABILITY_DIR / "stability_flags.csv")
    print(
        f"[price_family_phase2_stability] metric_rows={len(metric)} "
        f"demand_axis_rows={len(axis)} seasonality_rows={len(seasonality)} flags={len(flags)}"
    )
    print(
        "[price_family_phase2_stability] compact_metric_stability:\n"
        + metric.groupby("run_role")["mean_absolute_change_1m"].mean().reset_index().to_string(index=False)
    )
    print(f"[price_family_phase2_stability] artifacts={STABILITY_DIR}")
    print("[price_family_phase2_stability] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
