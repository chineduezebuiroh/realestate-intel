"""Validate the closed MA12/P6 Price production promotion and its scope."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from regime._00_config_loader import load_regime_config, validate_regime_config


ROOT = Path(__file__).resolve().parents[3]
TARGETS = {"redfin_median_sale_price", "redfin_median_ppsf"}
EXPECTED = {
    "level": ("ma_level", "12m", 0.35),
    "short_term_change": ("ma_pct_change", "12m/lag3m", 0.20),
    "long_term_change": ("ma_pct_change", "12m/lag12m", 0.45),
}


def main() -> None:
    config = load_regime_config()
    validate_regime_config(config)
    price = config.features[config.features.metric_key.isin(TARGETS)]
    assert set(price.metric_key) == TARGETS
    assert len(price) == 6
    for metric in TARGETS:
        family = price[price.metric_key.eq(metric)]
        assert set(family.feature_type) == set(EXPECTED)
        assert np.isclose(family.feature_weight.astype(float).sum(), 1.0)
        for feature_type, expected in EXPECTED.items():
            row = family[family.feature_type.eq(feature_type)].iloc[0]
            actual = (row["transform"], row.feature_window, float(row.feature_weight))
            assert actual == expected, f"{metric}/{feature_type}: {actual} != {expected}"

    decision = json.loads(
        (ROOT / "config/price_policy_promotion_2026_08_15.json").read_text()
    )
    assert decision["selected_policy"] == "MA12/P6"
    assert decision["promotion_state"] == "promoted"
    assert set(decision["metrics"]) == {"median_sale_price", "median_ppsf"}
    assert decision["feature_weights"] == {"level": 0.35, "short": 0.20, "long": 0.45}
    assert decision["level_window_months"] == 12
    print("[price_ma12_p6_production_promotion] governed registry and decision: OK")


if __name__ == "__main__":
    main()
