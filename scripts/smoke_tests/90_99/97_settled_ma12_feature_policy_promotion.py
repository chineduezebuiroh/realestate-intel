"""Validate settled MA12 production feature-policy promotion scope."""
from __future__ import annotations

from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END

from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import build_feature_matrix_with_lineage
from regime._02_feature_normalizer import normalize_features
from regime._03_metric_scorer import score_metrics
from regime.artifacts import RegimeArtifactStore
from regime.derived_metrics import build_derived_metrics_with_lineage

PROMOTION_VERSION = "settled_production_feature_policy_2026_08_09"
EVIDENCE_SOURCE = "ma12_structural_feature_weight_experiment_v1"

TARGETS = {
    "active_inventory": "redfin_inventory",
    "permit_activity": "bps_total_units",
    "permit_intensity": "derived_permit_intensity",
    "median_sale_price": "redfin_median_sale_price",
    "median_ppsf": "redfin_median_ppsf",
}
DIRECT_METRICS = {
    "active_inventory",
    "permit_activity",
    "median_sale_price",
    "median_ppsf",
}
ORIGINAL_MA12_DEFINITION = {
    "level": ("ma_level", "12m", 0.50),
    "short_term_change": ("ma_pct_change", "12m/lag3m", 0.25),
    "long_term_change": ("ma_pct_change", "12m/lag12m", 0.25),
}
PRICE_P6_DEFINITION = {
    "level": ("ma_level", "12m", 0.35),
    "short_term_change": ("ma_pct_change", "12m/lag3m", 0.20),
    "long_term_change": ("ma_pct_change", "12m/lag12m", 0.45),
}
# BPS-H-LAG6 was governed after the original 2026-08-05 MA12 promotion.  It is
# deliberately the sole exception to the original five-target contract.
BPS_LAG6_DEFINITION = {
    **ORIGINAL_MA12_DEFINITION,
    "level": ("ma_level", "12m", 0.80),
    "short_term_change": ("ma_pct_change", "12m/lag6m", 0.10),
    "long_term_change": ("ma_pct_change", "12m/lag12m", 0.10),
}
# Phase 4A and AFF-FW-A subsequently settled derive-first MA12 features at
# 50/20/30; these expectations must not retain the pre-Phase-4A transforms.
AFFORDABILITY = {
    feature_key: (metric, feature_type, transform, window, weight)
    for metric, prefix in (("derived_price_to_income", "price_to_income"),
                           ("derived_payment_burden", "payment_burden"))
    for feature_key, feature_type, transform, window, weight in (
        (f"{prefix}_level", "level", "ma_level", "12m", 0.50),
        (f"{prefix}_short", "short_term_change", "ma_pct_change", "12m/lag3m", 0.20),
        (f"{prefix}_long", "long_term_change", "ma_pct_change", "12m/lag12m", 0.30),
    )
}
# MW-TEMPERED-C subsequently settled the mixed structural transform
# architecture and 60/20/20 feature weights.
CAPITAL_MARKETS = {
    "fred_mortgage_15y": (("ma_level", "ma_pct_change", "ma_pct_change"), ("12m", "12m/lag12m", "12m/lag3m"), (0.60, 0.20, 0.20)),
    # Treasury 2y is an inactive zero-weight control, not one of the six
    # MW-TEMPERED-C members, so its legacy feature contract stays frozen.
    "fred_2y": (("level_zscore", "yoy_zscore", "mom_zscore"), ("", "12m", "1m"), (0.40, 0.30, 0.30)),
    "fred_10y_fedfunds_spread": (("ma_level", "ma_difference", "ma_difference"), ("9m", "9m/lag12m", "9m/lag3m"), (0.60, 0.20, 0.20)),
    "fred_mortgage_30y": (("ma_level", "ma_pct_change", "ma_pct_change"), ("12m", "12m/lag12m", "12m/lag3m"), (0.60, 0.20, 0.20)),
    "fred_fedfunds": (("ma_level", "ma_pct_change", "ma_pct_change"), ("3m", "3m/lag12m", "3m/lag3m"), (0.60, 0.20, 0.20)),
    "fred_10y": (("ma_level", "ma_pct_change", "ma_pct_change"), ("12m", "12m/lag12m", "12m/lag3m"), (0.60, 0.20, 0.20)),
    "fred_2y10y_spread": (("ma_level", "ma_difference", "ma_difference"), ("9m", "9m/lag12m", "9m/lag3m"), (0.60, 0.20, 0.20)),
}
METRIC_WEIGHTS = {
    "redfin_inventory": 0.60,
    "bps_total_units": 0.20,
    "derived_permit_intensity": 0.20,
    "redfin_median_sale_price": 0.5,
    "redfin_median_ppsf": 0.5,
    "derived_price_to_income": 0.50,
    "derived_payment_burden": 0.50,
    "fred_mortgage_30y": 0.15,
    "fred_mortgage_15y": 0.15,
    "fred_10y": 0.15,
    "fred_fedfunds": 0.10,
    "fred_2y": 0.0,
    "fred_2y10y_spread": 0.225,
    "fred_10y_fedfunds_spread": 0.225,
}
AXIS_WEIGHTS = {
    ("demand", "demand"): 0.65,
    ("demand", "price"): 0.175,
    ("demand", "affordability"): 0.075,
    ("demand", "capital_markets"): 0.10,
    ("supply", "supply"): 0.85,
    ("supply", "capital_markets"): 0.15,
}
SOURCE_PRIORITY_TWO = {
    "acs5_population",
    "acs5_median_household_income",
    "bea_annual_gdp",
    "laus_employment",
    "fred_unemployment_rate",
}
EXPECTED_METRIC_DIMENSION_ROWS = 43
PRIOR_LINEAGE = {
    "active_inventory": (("level_zscore", "mom_zscore", "yoy_zscore"), (0.25, 0.35, 0.40)),
    "permit_activity": (("ma12_level", "ma3_vs_ma12_pct", "ma12_yoy_pct"), (0.25, 0.35, 0.40)),
    "permit_intensity": (("ma12_level", "ma3_vs_ma12_pct", "ma12_yoy_pct"), (0.25, 0.35, 0.40)),
    "median_sale_price": (("level_zscore", "mom_zscore", "yoy_zscore"), (0.20, 0.40, 0.40)),
    "median_ppsf": (("level_zscore", "mom_zscore", "yoy_zscore"), (0.20, 0.40, 0.40)),
}


def _feature_rows(config: object) -> pd.DataFrame:
    feature = config.features.copy()
    metric = config.metric_dimensions[["metric_key", "canonical_metric_key", "enabled"]].copy()
    rows = feature.merge(metric, on="metric_key", how="left", validate="many_to_one")
    rows["feature_weight"] = pd.to_numeric(rows["feature_weight"], errors="raise")
    return rows


def _assert_registry_scope(config: object) -> None:
    rows = _feature_rows(config)
    promoted = rows[rows.canonical_metric_key.isin(TARGETS)].copy()
    if set(promoted.canonical_metric_key) != set(TARGETS):
        raise AssertionError("Exactly five target metrics must be present")
    if len(promoted) != 15:
        raise AssertionError("Exactly five level/short/long feature families must be promoted")
    for metric, registry_key in TARGETS.items():
        family = promoted[promoted.canonical_metric_key.eq(metric)].copy()
        if set(family.metric_key) != {registry_key}:
            raise AssertionError(f"{metric} changed source/metric ownership")
        expected_definition = (
            BPS_LAG6_DEFINITION if metric == "permit_activity"
            else PRICE_P6_DEFINITION if metric in {"median_sale_price", "median_ppsf"}
            else ORIGINAL_MA12_DEFINITION
        )
        if family.feature_type.duplicated().any() or set(family.feature_type) != set(expected_definition):
            raise AssertionError(f"{metric} does not resolve one level, short, and long feature")
        if not np.isclose(family.feature_weight.sum(), 1.0):
            raise AssertionError(f"{metric} promoted weights do not sum to 1.0")
        for feature_type, (transform, window, weight) in expected_definition.items():
            row = family[family.feature_type.eq(feature_type)].iloc[0]
            if (row["transform"], row["feature_window"], float(row["feature_weight"])) != (transform, window, weight):
                raise AssertionError(f"{metric}/{feature_type} is not its governed MA12 production policy")
    changed_from_prior = {
        metric for metric, (prior_transforms, prior_weights) in PRIOR_LINEAGE.items()
        if prior_transforms != tuple(promoted[promoted.canonical_metric_key.eq(metric)].sort_values("feature_type")["transform"])
        or not np.allclose(prior_weights, tuple(promoted[promoted.canonical_metric_key.eq(metric)].sort_values("feature_type").feature_weight))
    }
    if changed_from_prior != set(TARGETS):
        raise AssertionError("Promotion lineage should identify exactly five changed metrics")

    for feature_key, expected in AFFORDABILITY.items():
        row = rows[rows.feature_key.eq(feature_key)].iloc[0]
        actual = (row.metric_key, row.feature_type, row["transform"], row["feature_window"], float(row["feature_weight"]))
        if actual != expected:
            raise AssertionError(f"Affordability settled derive-first feature changed: {feature_key}")

    capital = rows[rows.dimension_context.eq("capital_markets")].copy()
    if set(capital.metric_key) != set(CAPITAL_MARKETS):
        raise AssertionError("Capital Markets feature ownership changed")
    for metric_key, (transforms, windows, weights) in CAPITAL_MARKETS.items():
        family = capital[capital.metric_key.eq(metric_key)].sort_values("feature_type")
        # Sorted feature-type order is level, long, short.
        if tuple(family["transform"]) != transforms or tuple(family["feature_window"]) != windows:
            raise AssertionError(f"Capital Markets settled transforms/windows changed for {metric_key}")
        if not np.allclose(tuple(family["feature_weight"]), weights):
            raise AssertionError(f"Capital Markets settled feature weights changed for {metric_key}")

    metric_rows = config.metric_dimensions.copy()
    metric_rows["metric_weight"] = pd.to_numeric(metric_rows["metric_weight"], errors="raise")
    for metric_key, weight in METRIC_WEIGHTS.items():
        row = metric_rows[metric_rows.metric_key.eq(metric_key)].iloc[0]
        if not np.isclose(row.metric_weight, weight):
            raise AssertionError(f"Metric weight changed for {metric_key}")

    axis = config.axes.copy()
    axis["dimension_weight"] = pd.to_numeric(axis["dimension_weight"], errors="raise")
    for key, weight in AXIS_WEIGHTS.items():
        row = axis[axis.axis.eq(key[0]) & axis.dimension.eq(key[1])].iloc[0]
        if not np.isclose(row.dimension_weight, weight):
            raise AssertionError(f"Axis/dimension weight changed for {key}")

    source = config.source_metrics.set_index("metric_key")
    if source.loc["redfin_inventory", "geo_levels"] != "nation|state|cbsa_metro|county|zip":
        raise AssertionError("Redfin geography policy changed")
    if source.loc["bps_total_units", "geo_levels"] != "state|county|place":
        raise AssertionError("BPS geography policy changed")
    if source.loc["derived_permit_intensity", "geo_levels"] != "state|county":
        raise AssertionError("Permit-intensity geography policy changed")
    # Freeze the complete settled precedence shape rather than comparing the
    # registry to a second load of itself (which would be tautological).
    priorities = pd.to_numeric(config.metric_dimensions["source_priority"], errors="raise")
    priority_two = set(config.metric_dimensions.loc[priorities.eq(2), "metric_key"])
    if (len(config.metric_dimensions) != EXPECTED_METRIC_DIMENSION_ROWS
            or priority_two != SOURCE_PRIORITY_TWO
            or not priorities.isin({1, 2}).all()):
        raise AssertionError("Settled source precedence changed")


def _synthetic_observations() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.date_range("2020-01-31", periods=72, freq=MONTH_END)
    geo = "promotion_fixture__county"
    rows: list[dict[str, object]] = []
    for i, date in enumerate(dates, start=1):
        rows.extend([
            {"geo_id": geo, "date": date, "canonical_metric_key": "active_inventory", "value": 1000 + i * 7},
            {"geo_id": geo, "date": date, "canonical_metric_key": "permit_activity", "value": 25 + (i % 5) * 3 + i},
            {"geo_id": geo, "date": date, "canonical_metric_key": "median_sale_price", "value": 300000 + i * 2500},
            {"geo_id": geo, "date": date, "canonical_metric_key": "median_ppsf", "value": 220 + i * 1.7},
            {"geo_id": "united_states__nation", "date": date, "canonical_metric_key": "mortgage_30y", "value": 4.0 + i * 0.02},
            {"geo_id": geo, "date": date, "canonical_metric_key": "population", "value": 100000 + i * 200},
            {"geo_id": geo, "date": date, "canonical_metric_key": "median_household_income", "value": 90000 + i * 100},
        ])
    source = pd.DataFrame(rows)
    derived, lineage = build_derived_metrics_with_lineage(source)
    observations = pd.concat([
        source.assign(metric_origin="synthetic"),
        derived.assign(metric_origin="derived"),
    ], ignore_index=True)
    return observations, lineage


def _assert_production_resolution(config: object) -> pd.DataFrame:
    observations, lineage = _synthetic_observations()
    features, feature_lineage = build_feature_matrix_with_lineage(
        config=config,
        canonical_observations=observations,
        derived_metric_lineage=lineage,
    )
    promoted_keys = set(config.features[config.features.metric_key.isin(TARGETS.values())].feature_key)
    produced_keys = set(features.feature_key)
    if not promoted_keys.issubset(produced_keys):
        raise AssertionError("Production feature engine did not resolve all promoted feature keys")
    normalized = normalize_features(features)
    metric_scores = score_metrics(normalized)
    if not set(TARGETS).issubset(set(metric_scores.canonical_metric_key)):
        raise AssertionError("Production scoring did not resolve promoted metrics")

    geo = "promotion_fixture__county"
    date = pd.Timestamp("2024-12-31")
    raw = observations[observations.geo_id.eq(geo)].pivot(index="date", columns="canonical_metric_key", values="value").sort_index()
    raw["permit_intensity"] = raw["permit_activity"] / raw["population"].ffill() * 1000.0
    metric_to_features = {
        "active_inventory": ("redfin_inventory_level", "redfin_inventory_short", "redfin_inventory_long"),
        "permit_activity": ("bps_total_units_level", "bps_total_units_short", "bps_total_units_long"),
        "median_sale_price": ("redfin_median_sale_price_level", "redfin_median_sale_price_short", "redfin_median_sale_price_long"),
        "median_ppsf": ("redfin_median_ppsf_level", "redfin_median_ppsf_short", "redfin_median_ppsf_long"),
        "permit_intensity": ("permit_intensity_level", "permit_intensity_short", "permit_intensity_long"),
    }
    equality_rows = []
    feature_weights = config.features.set_index("feature_key")["feature_weight"].astype(float)
    for metric, (level_key, short_key, long_key) in metric_to_features.items():
        level = raw[metric].rolling(12, min_periods=12).mean()
        # The later BPS-H-LAG6 promotion changed only BPS short-horizon math;
        # every other original MA12 target remains on lag3.
        short_lag = 6 if metric == "permit_activity" else 3
        expected = {
            level_key: level,
            short_key: level / level.shift(short_lag) - 1.0,
            long_key: level / level.shift(12) - 1.0,
        }
        wrong_ma3_raw = raw[metric].rolling(3, min_periods=3).mean() / level - 1.0
        wrong_ma3_level = level.rolling(3, min_periods=3).mean() / level - 1.0
        actual_short = features[(features.geo_id.eq(geo)) & (features.date.eq(date)) & (features.feature_key.eq(short_key))].raw_feature_value.iloc[0]
        if np.isclose(actual_short, wrong_ma3_raw.loc[date]):
            raise AssertionError(f"{metric} short accepted the forbidden MA3(raw)/MA12(raw)-1 formula")
        if np.isclose(actual_short, wrong_ma3_level.loc[date]):
            raise AssertionError(f"{metric} short accepted the forbidden MA3(level)/level-1 formula")
        for feature_key, series in expected.items():
            actual = features[(features.geo_id.eq(geo)) & (features.date.eq(date)) & (features.feature_key.eq(feature_key))].raw_feature_value.iloc[0]
            if not np.isclose(actual, series.loc[date]):
                raise AssertionError(f"{metric}/{feature_key} does not match the governing MA12 lag formula")
            norm = normalized[(normalized.geo_id.eq(geo)) & (normalized.date.eq(date)) & (normalized.feature_key.eq(feature_key))]
            metric_row = metric_scores[(metric_scores.geo_id.eq(geo)) & (metric_scores.date.eq(date)) & (metric_scores.canonical_metric_key.eq(metric))]
            equality_rows.append({
                "geo_id": geo,
                "date": date.date().isoformat(),
                "canonical_metric_key": metric,
                "feature_key": feature_key,
                "raw_transformed_feature_value": float(actual),
                "normalized_feature_score": float(norm.feature_score.iloc[0]) if not norm.empty and pd.notna(norm.feature_score.iloc[0]) else np.nan,
                "configured_weight": float(feature_weights.loc[feature_key]),
                "metric_score": float(metric_row.metric_score.iloc[0]) if not metric_row.empty and pd.notna(metric_row.metric_score.iloc[0]) else np.nan,
                "formula_equality": "pass",
            })
    expected_intensity = raw["permit_intensity"].rolling(12, min_periods=12).mean().loc[date]
    wrong_parent_smoothed = (raw["permit_activity"].rolling(12, min_periods=12).mean() / raw["population"].ffill() * 1000.0).loc[date]
    actual_intensity = features[(features.geo_id.eq(geo)) & (features.date.eq(date)) & (features.feature_key.eq("permit_intensity_level"))].raw_feature_value.iloc[0]
    if not np.isclose(actual_intensity, expected_intensity):
        raise AssertionError("permit_intensity is not derived first and smoothed once afterward")
    if np.isclose(actual_intensity, wrong_parent_smoothed):
        raise AssertionError("permit_intensity appears to be derived from already-smoothed permit_activity")
    if feature_lineage[feature_lineage.derived_metric_key.eq("permit_intensity")].component_metric_key.isin({"permit_activity", "population"}).sum() == 0:
        raise AssertionError("permit_intensity lineage does not preserve raw parent components")
    equality = pd.DataFrame(equality_rows)
    print("[settled_ma12_feature_policy_promotion] equality evidence:")
    print(equality.to_string(index=False))
    warmup = features[features.feature_key.isin({key for keys in metric_to_features.values() for key in keys})].groupby("feature_key").agg(first_valid_date=("date", "min"), rows=("raw_feature_value", "size")).reset_index()
    print("[settled_ma12_feature_policy_promotion] warmup evidence:")
    print(warmup.sort_values("feature_key").to_string(index=False))
    return features


def _assert_deterministic_artifacts(features: pd.DataFrame) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = RegimeArtifactStore(Path(tmp))
        run_id = "settled_ma12_policy_validation"
        store.initialize_run(run_id, experiment_id=PROMOTION_VERSION, metadata={"evidence_source": EVIDENCE_SOURCE})
        store.write_dataframe(run_id, "features", features, validation=False)
        manifest = store.read_manifest(run_id)
        store.update_manifest(run_id, status="complete")
        verification = store.verify_run(run_id)
        if verification.empty or not verification["exists"].all() or not verification["hash_matches"].all():
            raise AssertionError("Deterministic validation artifact verification failed")
        if manifest["experiment_id"] != PROMOTION_VERSION:
            raise AssertionError("Promotion artifact identity was not recorded")


def main() -> None:
    config = load_regime_config(validate=True)
    _assert_registry_scope(config)
    features = _assert_production_resolution(config)
    _assert_deterministic_artifacts(features)
    print(f"[settled_ma12_feature_policy_promotion] version={PROMOTION_VERSION} evidence={EVIDENCE_SOURCE} metrics={','.join(TARGETS)}")
    print("[settled_ma12_feature_policy_promotion] OK")


if __name__ == "__main__":
    main()
