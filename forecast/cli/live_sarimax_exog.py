from __future__ import annotations

from forecast.db_forecast import new_batch_id
from forecast.models.sarimax_exog.live_runner import run_live_latest_artifact

def main() -> int:
    # hardcode defaults now; later this becomes argparse
    run_live_latest_artifact(
        metric_id="median_sale_price",
        geo_id="dc_city",
        property_type_id="-1",
        freq="M",
        horizon=12,
        batch_id=new_batch_id(),
        data_asof="2025-12-31",  # TEMP: replace with resolved asof policy later
        runs_root="runs",
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
