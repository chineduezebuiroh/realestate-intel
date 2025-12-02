# scripts/run_sarimax_exog_dc_city_price.py

import sys
from pathlib import Path

# Ensure project root is on sys.path so 'forecast' package can be imported
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from forecast.sarimax_exog import run_sarimax_exog
from forecast.feature_loader import FeatureSpec


def main():
    metric_id = "median_sale_price"
    geo_id = "dc_city"
    pt_id = "-1"  # All Residential

    feature_specs = [
        FeatureSpec(
            name="median_ppsf",
            metric_id="median_ppsf",
            geo_id=geo_id,
            property_type_id=pt_id,
            lags=[1, 2, 3],
        ),
        FeatureSpec(
            name="median_dom",
            metric_id="median_dom",
            geo_id=geo_id,
            property_type_id=pt_id,
            lags=[1, 2, 3, 6],
        ),
    ]

    run_id = run_sarimax_exog(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=pt_id,
        feature_specs=feature_specs,
        horizon_max_months=12,
        notes="SARIMAX exog: price ~ PPSF + DOM (dc_city, All Res)",
    )

    print(f"SARIMAX-exog run_id={run_id}")


if __name__ == "__main__":
    main()
