from __future__ import annotations

import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from regime._00_config_loader import RegimeConfig, load_regime_config
from regime._01_feature_engine import _compute_feature, build_feature_matrix
from regime.canonical_metrics import resolve_canonical_metrics
from regime.derived_metrics import build_derived_metrics_with_lineage

LAUS_FEATURE_KEYS = [
    "laus_employment_level",
    "laus_employment_short",
    "laus_employment_long",
    "laus_labor_force_level",
    "laus_labor_force_short",
    "laus_labor_force_long",
    "laus_unemployment_rate_level",
    "laus_unemployment_rate_short",
    "laus_unemployment_rate_long",
]

EXPECTED_LAUS = {
    "laus_employment_level": ("ma_level", "6m"),
    "laus_employment_short": ("ma_pct_change", "6m/lag3m"),
    "laus_employment_long": ("ma_pct_change", "6m/lag12m"),
    "laus_labor_force_level": ("ma_level", "6m"),
    "laus_labor_force_short": ("ma_pct_change", "6m/lag3m"),
    "laus_labor_force_long": ("ma_pct_change", "6m/lag12m"),
    "laus_unemployment_rate_level": ("ma_level", "6m"),
    "laus_unemployment_rate_short": ("ma_pct_change", "6m/lag3m"),
    "laus_unemployment_rate_long": ("ma_pct_change", "6m/lag12m"),
}

EXPECTED_NON_LAUS_REGISTRY = """feature_key,metric_key,feature_type,transform,feature_weight,feature_window,dimension_context
redfin_inventory_level,redfin_inventory,level,ma_level,0.50,12m,supply
redfin_inventory_short,redfin_inventory,short_term_change,ma_pct_change,0.25,12m/lag3m,supply
redfin_inventory_long,redfin_inventory,long_term_change,ma_pct_change,0.25,12m/lag12m,supply
laus_unemployment_level,laus_unemployment,level,level_zscore,0.25,,demand
laus_unemployment_short,laus_unemployment,short_term_change,mom_zscore,0.35,1m,demand
laus_unemployment_long,laus_unemployment,long_term_change,yoy_zscore,0.40,12m,demand
ces_total_nonfarm_level,ces_total_nonfarm,level,level_zscore,0.25,,demand
ces_total_nonfarm_short,ces_total_nonfarm,short_term_change,mom_zscore,0.35,1m,demand
ces_total_nonfarm_long,ces_total_nonfarm,long_term_change,yoy_zscore,0.40,12m,demand
bps_total_units_level,bps_total_units,level,ma_level,0.50,12m,supply
bps_total_units_short,bps_total_units,short_term_change,ma_pct_change,0.25,12m/lag3m,supply
bps_total_units_long,bps_total_units,long_term_change,ma_pct_change,0.25,12m/lag12m,supply
bps_total_buildings_level,bps_total_buildings,level,level_zscore,0.25,,supply
bps_total_buildings_short,bps_total_buildings,short_term_change,mom_zscore,0.35,1m,supply
bps_total_buildings_long,bps_total_buildings,long_term_change,yoy_zscore,0.40,12m,supply
bps_total_value_level,bps_total_value,level,level_zscore,0.25,,supply
bps_total_value_short,bps_total_value,short_term_change,mom_zscore,0.35,1m,supply
bps_total_value_long,bps_total_value,long_term_change,yoy_zscore,0.40,12m,supply
permit_intensity_level,derived_permit_intensity,level,ma_level,0.50,12m,supply
permit_intensity_short,derived_permit_intensity,short_term_change,ma_pct_change,0.25,12m/lag3m,supply
permit_intensity_long,derived_permit_intensity,long_term_change,ma_pct_change,0.25,12m/lag12m,supply
"""

REGISTRY_COMPARE_KEYS = [
    "redfin_inventory_level",
    "redfin_inventory_short",
    "redfin_inventory_long",
    "laus_unemployment_level",
    "laus_unemployment_short",
    "laus_unemployment_long",
    "ces_total_nonfarm_level",
    "ces_total_nonfarm_short",
    "ces_total_nonfarm_long",
    "bps_total_units_level",
    "bps_total_units_short",
    "bps_total_units_long",
    "bps_total_buildings_level",
    "bps_total_buildings_short",
    "bps_total_buildings_long",
    "bps_total_value_level",
    "bps_total_value_short",
    "bps_total_value_long",
    "permit_intensity_level",
    "permit_intensity_short",
    "permit_intensity_long",
]


def _assert_close(actual: float, expected: float, label: str) -> None:
    if not np.isclose(actual, expected, rtol=0.0, atol=1e-12):
        raise AssertionError(f"{label}: expected {expected}, got {actual}")


def _minimal_config(config: RegimeConfig, features: pd.DataFrame) -> RegimeConfig:
    return RegimeConfig(
        source_metrics=config.source_metrics,
        features=features,
        metric_dimensions=config.metric_dimensions,
        axes=config.axes,
    )


def test_registry_contract(config: RegimeConfig) -> None:
    features = config.features.copy()
    by_key = features.set_index("feature_key")

    for feature_key, (transform, window) in EXPECTED_LAUS.items():
        row = by_key.loc[feature_key]
        if row["transform"] != transform or row["feature_window"] != window:
            raise AssertionError(
                f"{feature_key} expected {transform}/{window}, got "
                f"{row['transform']}/{row['feature_window']}"
            )
        if row["transform"] in {"level_zscore", "mom_zscore", "yoy_zscore"}:
            raise AssertionError(f"{feature_key} still uses raw/lag1 transform")

    actual = features[features["feature_key"].isin(REGISTRY_COMPARE_KEYS)][
        [
            "feature_key",
            "metric_key",
            "feature_type",
            "transform",
            "feature_weight",
            "feature_window",
            "dimension_context",
        ]
    ].reset_index(drop=True)
    expected = pd.read_csv(
        io.StringIO(EXPECTED_NON_LAUS_REGISTRY),
        dtype=str,
    ).fillna("")
    try:
        assert_frame_equal(actual, expected, check_dtype=False)
    except AssertionError as exc:
        merged = actual.merge(
            expected,
            on="feature_key",
            how="outer",
            suffixes=("_actual", "_expected"),
            indicator=True,
        )
        raise AssertionError(
            "Unrelated registry rows changed unexpectedly:\n"
            + merged.to_string(index=False)
        ) from exc


def test_laus_unemployment_status(config: RegimeConfig) -> None:
    row = config.metric_dimensions.set_index("metric_key").loc["laus_unemployment"]
    if row["enabled"] != "false" or row["diagnostic_only"] != "true":
        raise AssertionError("Unexpected laus_unemployment status")
    if row["macro_enabled"] != "false" or row["local_enabled"] != "false":
        raise AssertionError("laus_unemployment should remain unused in production scoring")


def test_ma6_math_and_exact_warmup() -> None:
    dates = pd.date_range("2020-01-01", periods=20, freq="MS")
    values = pd.Series(np.arange(1.0, 21.0))
    group = pd.DataFrame({"date": dates, "value": values})

    level = _compute_feature(group, "ma_level", "6m", "test_level")
    short = _compute_feature(group, "ma_pct_change", "6m/lag3m", "test_short")
    long = _compute_feature(group, "ma_pct_change", "6m/lag12m", "test_long")

    if not level.iloc[:5].isna().all() or pd.isna(level.iloc[5]):
        raise AssertionError("MA6 level first valid must be observation 6")
    if not short.iloc[:8].isna().all() or pd.isna(short.iloc[8]):
        raise AssertionError("MA6 lag3 short first valid must be observation 9")
    if not long.iloc[:17].isna().all() or pd.isna(long.iloc[17]):
        raise AssertionError("MA6 lag12 long first valid must be observation 18")

    _assert_close(level.iloc[5], values.iloc[0:6].mean(), "first MA6 level")
    _assert_close(short.iloc[8], level.iloc[8] / level.iloc[5] - 1.0, "MA6 lag3 short")
    _assert_close(long.iloc[17], level.iloc[17] / level.iloc[5] - 1.0, "MA6 lag12 long")


def test_strict_ma_config_errors_include_feature_key() -> None:
    malformed = [
        ("ma_level", ""),
        ("ma_level", "0m"),
        ("ma_level", "-6m"),
        ("ma_level", "6"),
        ("ma_level", "6q"),
        ("ma_level", "6m/lag3m"),
        ("ma_pct_change", "6m"),
        ("ma_pct_change", "6m/3m"),
        ("ma_pct_change", "0m/lag3m"),
        ("ma_pct_change", "6m/lag0m"),
        ("ma_pct_change", "6q/lag3m"),
    ]
    group = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=3, freq="MS"),
            "value": [1.0, 2.0, 3.0],
        }
    )
    for transform, feature_window in malformed:
        try:
            _compute_feature(group, transform, feature_window, "bad_feature")
        except ValueError as exc:
            message = str(exc)
            if "bad_feature" not in message or feature_window not in message:
                raise AssertionError(f"Error is not actionable: {message}") from exc
        else:
            raise AssertionError(f"Malformed MA config did not fail: {transform}/{feature_window}")


def test_origin_switch_and_calendar_gap_reset_ma_windows() -> None:
    dates = pd.date_range("2020-01-01", periods=24, freq="MS")
    origins = ["ces_total_nonfarm"] * 8 + ["laus_employment"] * 8 + ["ces_total_nonfarm"] * 8
    group = pd.DataFrame(
        {
            "date": dates,
            "value": np.arange(100.0, 124.0),
            "metric_origin": origins,
        }
    )
    level = _compute_feature(group, "ma_level", "6m", "laus_employment_level")
    if not level.iloc[8:13].isna().all() or pd.isna(level.iloc[13]):
        raise AssertionError("MA6 level crossed CES -> LAUS source switch")
    if not level.iloc[16:21].isna().all() or pd.isna(level.iloc[21]):
        raise AssertionError("MA6 level crossed LAUS -> CES source switch")

    gap_group = group[group["date"] != pd.Timestamp("2020-06-01")].reset_index(drop=True)
    gap_level = _compute_feature(gap_group, "ma_level", "6m", "gap_feature")
    july_position = gap_group.index[gap_group["date"].eq(pd.Timestamp("2020-07-01"))][0]
    if not gap_level.iloc[july_position:july_position + 5].isna().all():
        raise AssertionError("MA6 level crossed a missing monthly calendar row")


def test_origin_specific_employment_preserves_ces(config: RegimeConfig) -> None:
    dates = pd.date_range("2020-01-01", periods=18, freq="MS")
    observations = []
    for geo_id, origin, base in [
        ("laus_county", "laus_employment", 100.0),
        ("ces_state", "ces_total_nonfarm", 1000.0),
    ]:
        for i, date in enumerate(dates):
            observations.append(
                {
                    "geo_id": geo_id,
                    "date": date,
                    "canonical_metric_key": "employment",
                    "value": base + i,
                    "metric_origin": origin,
                }
            )
    features = build_feature_matrix(
        config=config,
        canonical_observations=pd.DataFrame(observations),
        derived_metric_lineage=pd.DataFrame(),
    )

    laus = features[features["feature_key"].str.startswith("laus_employment")]
    ces = features[features["feature_key"].str.startswith("ces_total_nonfarm")]

    if set(laus["geo_id"].unique()) != {"laus_county"}:
        raise AssertionError("LAUS employment features leaked onto CES-origin observations")
    if set(ces["geo_id"].unique()) != {"ces_state"}:
        raise AssertionError("CES employment features did not remain CES-origin only")

    ces_short = ces[
        (ces["feature_key"] == "ces_total_nonfarm_short")
        & (ces["date"] == dates[1])
    ]["raw_feature_value"].iloc[0]
    _assert_close(ces_short, 1001.0 / 1000.0 - 1.0, "CES lag1 short")


def test_identical_transform_physical_defs_do_not_deduplicate(config: RegimeConfig) -> None:
    features = pd.DataFrame(
        [
            {
                "feature_key": "laus_employment_test_level",
                "metric_key": "laus_employment",
                "feature_type": "level",
                "transform": "ma_level",
                "feature_weight": "1.0",
                "feature_window": "6m",
                "dimension_context": "demand",
            },
            {
                "feature_key": "ces_total_nonfarm_test_level",
                "metric_key": "ces_total_nonfarm",
                "feature_type": "level",
                "transform": "ma_level",
                "feature_weight": "1.0",
                "feature_window": "6m",
                "dimension_context": "demand",
            },
        ]
    )
    mini_config = _minimal_config(config, features)
    dates = pd.date_range("2020-01-01", periods=6, freq="MS")
    canonical = pd.DataFrame(
        [
            {
                "geo_id": "laus_geo",
                "date": date,
                "canonical_metric_key": "employment",
                "value": float(i),
                "metric_origin": "laus_employment",
            }
            for i, date in enumerate(dates, start=1)
        ]
        + [
            {
                "geo_id": "ces_geo",
                "date": date,
                "canonical_metric_key": "employment",
                "value": float(i + 100),
                "metric_origin": "ces_total_nonfarm",
            }
            for i, date in enumerate(dates, start=1)
        ]
    )
    result = build_feature_matrix(
        config=mini_config,
        canonical_observations=canonical,
        derived_metric_lineage=pd.DataFrame(),
    )
    if set(result["feature_key"]) != {
        "laus_employment_test_level",
        "ces_total_nonfarm_test_level",
    }:
        raise AssertionError("Identical transform/window physical definitions were collapsed")


def test_canonical_resolution_audit(config: RegimeConfig) -> None:
    raw = pd.DataFrame(
        [
            {"geo_id": "g", "date": "2020-01-01", "metric_key": "ces_total_nonfarm", "value": 10.0},
            {"geo_id": "g", "date": "2020-01-01", "metric_key": "laus_employment", "value": 20.0},
            {"geo_id": "g", "date": "2020-02-01", "metric_key": "laus_employment", "value": 30.0},
            {"geo_id": "g", "date": "2020-01-01", "metric_key": "redfin_median_sale_price", "value": 100.0},
            {"geo_id": "g", "date": "2020-01-01", "metric_key": "acs1_median_household_income", "value": 50.0},
            {"geo_id": "g", "date": "2020-01-01", "metric_key": "fred_mortgage_30y", "value": 0.05},
        ]
    )
    resolved = resolve_canonical_metrics(raw, config)
    uniqueness = resolved.duplicated(["geo_id", "date", "canonical_metric_key"])
    if uniqueness.any():
        raise AssertionError("Canonical row uniqueness changed")
    employment_jan = resolved[
        resolved["canonical_metric_key"].eq("employment")
        & pd.to_datetime(resolved["date"]).eq(pd.Timestamp("2020-01-01"))
    ].iloc[0]
    if employment_jan["value"] != 10.0 or employment_jan["source_metric_key"] != "ces_total_nonfarm":
        raise AssertionError("Source-priority resolution changed")

    comparable = resolved[["geo_id", "date", "canonical_metric_key", "value"]]
    derived, lineage = build_derived_metrics_with_lineage(comparable)
    if derived.empty or lineage.empty:
        raise AssertionError("Derived metric consumers broke after source_metric_key addition")


def main() -> int:
    config = load_regime_config(validate=True)
    test_registry_contract(config)
    test_laus_unemployment_status(config)
    test_ma6_math_and_exact_warmup()
    test_strict_ma_config_errors_include_feature_key()
    test_origin_switch_and_calendar_gap_reset_ma_windows()
    test_origin_specific_employment_preserves_ces(config)
    test_identical_transform_physical_defs_do_not_deduplicate(config)
    test_canonical_resolution_audit(config)
    print("[laus_ma6_production_contract] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
