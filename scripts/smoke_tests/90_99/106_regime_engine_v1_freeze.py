"""Fail-closed contract smoke for governed production configuration."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from regime._00_config_loader import load_regime_config


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "config/regime_engine_v1_0_release.json"

FEATURE_POLICIES = {
    "redfin_median_sale_price": ("ma_level", "12m", .35, "ma_pct_change", "12m/lag3m", .20, "ma_pct_change", "12m/lag12m", .45),
    "redfin_median_ppsf": ("ma_level", "12m", .35, "ma_pct_change", "12m/lag3m", .20, "ma_pct_change", "12m/lag12m", .45),
    "redfin_inventory": ("ma_level", "12m", .40, "ma_pct_change", "12m/lag3m", .15, "ma_pct_change", "12m/lag12m", .45),
    "bps_total_units": ("ma_level", "12m", .75, "ma_pct_change", "12m/lag6m", .10, "ma_pct_change", "12m/lag12m", .15),
    "derived_permit_intensity": ("ma_level", "12m", .40, "ma_pct_change", "12m/lag3m", .15, "ma_pct_change", "12m/lag12m", .45),
    "derived_price_to_income": ("ma_level", "12m", .35, "ma_pct_change", "12m/lag3m", .20, "ma_pct_change", "12m/lag12m", .45),
    "derived_payment_burden": ("ma_level", "12m", .35, "ma_pct_change", "12m/lag3m", .20, "ma_pct_change", "12m/lag12m", .45),
    "fred_mortgage_30y": ("ma_level", "12m", .60, "ma_pct_change", "12m/lag3m", .20, "ma_pct_change", "12m/lag12m", .20),
    "fred_mortgage_15y": ("ma_level", "12m", .60, "ma_pct_change", "12m/lag3m", .20, "ma_pct_change", "12m/lag12m", .20),
    "fred_10y": ("ma_level", "12m", .60, "ma_pct_change", "12m/lag3m", .20, "ma_pct_change", "12m/lag12m", .20),
    "fred_fedfunds": ("ma_level", "3m", .60, "ma_pct_change", "3m/lag3m", .20, "ma_pct_change", "3m/lag12m", .20),
    "fred_2y10y_spread": ("ma_level", "9m", .60, "ma_difference", "9m/lag3m", .20, "ma_difference", "9m/lag12m", .20),
    "fred_10y_fedfunds_spread": ("ma_level", "9m", .60, "ma_difference", "9m/lag3m", .20, "ma_difference", "9m/lag12m", .20),
    "laus_labor_force": ("ma_level", "9m", .40, "ma_pct_change", "9m/lag3m", .15, "ma_pct_change", "9m/lag12m", .45),
    "laus_employment": ("ma_level", "9m", .40, "ma_pct_change", "9m/lag3m", .15, "ma_pct_change", "9m/lag12m", .45),
    "laus_unemployment_rate": ("ma_level", "9m", .40, "ma_pct_change", "9m/lag3m", .15, "ma_pct_change", "9m/lag12m", .45),
}

METRIC_WEIGHTS = {
    "redfin_median_sale_price": .50, "redfin_median_ppsf": .50,
    "redfin_inventory": .60, "redfin_homes_sold": .50,
    "redfin_pending_sales": .50, "redfin_dom": .3333,
    "redfin_months_supply": .3333, "redfin_sale_to_list": .3334,
    "acs1_population": 1/3, "acs1_median_household_income": 1/3,
    "acs5_population": 1/3, "acs5_median_household_income": 1/3,
    "bea_annual_gdp": 1/3, "laus_labor_force": 1/3,
    "laus_employment": 1/3, "laus_unemployment_rate": 1/3,
    "ces_total_nonfarm": 1/3, "bps_total_units": .20,
    "derived_permit_intensity": .20, "derived_price_to_income": .50,
    "derived_payment_burden": .50, "fred_mortgage_30y": .15,
    "fred_mortgage_15y": .15, "fred_10y": .15, "fred_fedfunds": .10,
    "fred_2y10y_spread": .225, "fred_10y_fedfunds_spread": .225,
}
AXIS_WEIGHTS = {
    ("demand", "demand"): .65, ("demand", "price"): .175,
    ("demand", "affordability"): .075, ("demand", "capital_markets"): .10,
    ("supply", "supply"): .85, ("supply", "capital_markets"): .15,
}


def _policy_tuple(rows, metric_key: str) -> tuple[object, ...]:
    family = rows[rows.metric_key.eq(metric_key)].set_index("feature_type")
    result: list[object] = []
    for feature_type in ("level", "short_term_change", "long_term_change"):
        row = family.loc[feature_type]
        result.extend((row["transform"], row["feature_window"], float(row["feature_weight"])))
    return tuple(result)


def main() -> int:
    release = json.loads(MANIFEST.read_text())
    assert release["release_id"] == "regime_engine_v1_0"
    assert release["scope"] == "county_macro_regime"
    assert release["release_status"] == "candidate_for_main_merge"
    assert release["post_release_next_stage"] == "visualization_mvp"
    for field in ("production_config_files", "decision_documents", "acceptance_tests"):
        assert release[field] and len(release[field]) == len(set(release[field]))
        for path in release[field]:
            assert (ROOT / path).is_file(), f"Missing governed path: {path}"

    config = load_regime_config(validate=True)
    for metric_key, expected in FEATURE_POLICIES.items():
        assert _policy_tuple(config.features, metric_key) == expected, metric_key

    metrics = config.metric_dimensions.copy()
    active = metrics.enabled.str.lower().eq("true") & ~metrics.diagnostic_only.str.lower().eq("true") & metrics.macro_enabled.str.lower().eq("true")
    production = metrics[active]
    assert not production.empty
    assert not production.diagnostic_only.str.lower().eq("true").any()
    excluded = metrics[~active]
    assert not excluded[excluded.diagnostic_only.str.lower().eq("true")].enabled.str.lower().eq("true").any()
    assert set(production.metric_key) == set(METRIC_WEIGHTS)
    for metric_key, expected in METRIC_WEIGHTS.items():
        row = production[production.metric_key.eq(metric_key)]
        assert len(row) == 1 and np.isclose(float(row.iloc[0].metric_weight), expected), metric_key

    axes = config.axes[config.axes.enabled.str.lower().eq("true")]
    assert len(axes) == len(AXIS_WEIGHTS)
    for (axis, dimension), expected in AXIS_WEIGHTS.items():
        row = axes[axes.axis.eq(axis) & axes.dimension.eq(dimension)]
        assert len(row) == 1 and np.isclose(float(row.iloc[0].dimension_weight), expected)
    print("[regime_engine_v1_freeze] release manifest and production contracts: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
