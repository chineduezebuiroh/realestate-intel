import hashlib
from pathlib import Path

import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import build_feature_matrix_with_lineage
from regime._02_feature_normalizer import normalize_features
from regime._03_metric_scorer import score_metrics
from test_regime_serving_input import GEO, database, row

FROZEN_HASHES = {
    "source_metric_registry.csv": "14acfa965907b8884dbb6225e9f97b4143e6b248cefe53e13c3b3307cf435e4f",
    "feature_registry.csv": "4d6bcc5ba77e630a97f4bb423c5f114189dea5e2d6b4d0b03f6f0a81b4832699",
    "metric_dimension_registry.csv": "26943e769720d99955a6ad5e8792f661f38344c0f6ea045430dd94c377d4a89b",
    "axis_registry.csv": "68c7cd8a79feb87563192f880ccdf43d02ddfeed4e3408d4ae6bac1154b5f1fc",
    "normalization_registry.csv": "8362953929cfbcd7db710601faa1cd24c1ce868912363031e92d4f8537fded73",
    "derived_input_freshness_registry.csv": "4fd9a6a9bfeb3564e1428d599961a7b3e07a3312830addd832c4613041e82bf5",
    "metric_smoothing_experiments.csv": "fa410b33c61874fa225b4221421a4e0f732e8f8818a92de0743eaab2e79711e4",
}


def fixture_rows(logical=True, months=144):
    cfg = load_regime_config()
    rows = []
    for i, date in enumerate(pd.date_range("2010-01-31", periods=months, freq="ME")):
        for metric in cfg.source_metrics.itertuples():
            if metric.source_id == "derived":
                continue
            source, physical = metric.source_id, metric.metric_id
            if source == "census_acs5":
                continue  # equivalent selected ACS1 observations
            if logical and source == "census_acs1":
                source, physical = "acs", physical.replace("census_acs1_", "census_acs_")
            if logical and source == "census_bps":
                source = "bps"
            value = 100 + i * .5
            if "income" in physical:
                value *= 1000
            if physical == "median_sale_price_nsa":
                value *= 4000
            if source == "fred_macro":
                value = 4 + i * .01
            if physical == "fred_spread_2y_10y":
                value = -.5 - i * .001
            geo = "united_states__nation" if source in {"fred_macro", "fred_unemp"} else GEO
            rows.append(row(source, physical, value, date=date, geo=geo))
    return rows


def test_all_frozen_registry_hashes_unchanged():
    for name, expected in FROZEN_HASHES.items():
        assert hashlib.sha256(Path("config", name).read_bytes()).hexdigest() == expected


def test_equivalent_logical_and_legacy_features_and_scores(tmp_path):
    outputs = []
    for logical in [False, True]:
        path = database(tmp_path / f"{logical}.duckdb", fixture_rows(logical))
        features, _ = build_feature_matrix_with_lineage(db_path=path)
        assert not features.duplicated(["geo_id", "date", "canonical_metric_key", "feature_key"]).any()
        assert {"population", "median_household_income", "permit_activity", "permit_intensity", "price_to_income", "payment_burden"}.issubset(set(features.canonical_metric_key))
        assert "fred_unemployment_rate" not in set(features.canonical_metric_key)
        outputs.append((features, score_metrics(normalize_features(features))))
    for index, keys in [(0, ["geo_id", "date", "feature_key"]), (1, ["geo_id", "date", "canonical_metric_key"])]:
        pd.testing.assert_frame_equal(outputs[0][index].sort_values(keys).reset_index(drop=True),
                                      outputs[1][index].sort_values(keys).reset_index(drop=True))
    cfg = load_regime_config()
    fred = cfg.metric_dimensions[cfg.metric_dimensions.metric_key.eq("fred_unemployment_rate")]
    assert fred.enabled.tolist() == ["false"] and fred.diagnostic_only.tolist() == ["true"]
    assert "fred_unemp" not in set(cfg.axes.dimension)
