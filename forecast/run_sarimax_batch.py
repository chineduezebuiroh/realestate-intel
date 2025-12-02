# forecast/run_sarimax_batch.py

import os
from datetime import datetime, timezone
from typing import Optional, List, Dict

import duckdb

from forecast.sarimax_redfin import run_sarimax_forecast


def get_duckdb_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path)


# -----------------------------------------
# Targets and thresholds
# -----------------------------------------

# Adjust metric_ids here to match fact_timeseries. You already confirmed "median_sale_price" works.
# Likely siblings are "median_ppsf" and "median_dom". Tweak if your IDs differ.
TARGETS: List[Dict] = [
    # All Residential (-1)
    {"metric_id": "median_sale_price", "geo_id": "dc_city",  "property_type_id": "-1"},
    {"metric_id": "median_sale_price", "geo_id": "dc_msa",   "property_type_id": "-1"},
    {"metric_id": "median_sale_price", "geo_id": "20019_dc", "property_type_id": "-1"},
    {"metric_id": "median_sale_price", "geo_id": "20016_dc", "property_type_id": "-1"},

    {"metric_id": "median_ppsf",       "geo_id": "dc_city",  "property_type_id": "-1"},
    {"metric_id": "median_ppsf",       "geo_id": "dc_msa",   "property_type_id": "-1"},
    {"metric_id": "median_ppsf",       "geo_id": "20019_dc", "property_type_id": "-1"},
    {"metric_id": "median_ppsf",       "geo_id": "20016_dc", "property_type_id": "-1"},

    {"metric_id": "median_dom",        "geo_id": "dc_city",  "property_type_id": "-1"},
    {"metric_id": "median_dom",        "geo_id": "dc_msa",   "property_type_id": "-1"},
    {"metric_id": "median_dom",        "geo_id": "20019_dc", "property_type_id": "-1"},
    {"metric_id": "median_dom",        "geo_id": "20016_dc", "property_type_id": "-1"},

    # Townhouse (13)
    {"metric_id": "median_sale_price", "geo_id": "dc_city",  "property_type_id": "13"},
    {"metric_id": "median_sale_price", "geo_id": "dc_msa",   "property_type_id": "13"},
    {"metric_id": "median_sale_price", "geo_id": "20019_dc", "property_type_id": "13"},
    {"metric_id": "median_sale_price", "geo_id": "20016_dc", "property_type_id": "13"},

    {"metric_id": "median_ppsf",       "geo_id": "dc_city",  "property_type_id": "13"},
    {"metric_id": "median_ppsf",       "geo_id": "dc_msa",   "property_type_id": "13"},
    {"metric_id": "median_ppsf",       "geo_id": "20019_dc", "property_type_id": "13"},
    {"metric_id": "median_ppsf",       "geo_id": "20016_dc", "property_type_id": "13"},

    {"metric_id": "median_dom",        "geo_id": "dc_city",  "property_type_id": "13"},
    {"metric_id": "median_dom",        "geo_id": "dc_msa",   "property_type_id": "13"},
    {"metric_id": "median_dom",        "geo_id": "20019_dc", "property_type_id": "13"},
    {"metric_id": "median_dom",        "geo_id": "20016_dc", "property_type_id": "13"},

    # Condo/Co-Op (3)
    {"metric_id": "median_sale_price", "geo_id": "dc_city",  "property_type_id": "3"},
    {"metric_id": "median_sale_price", "geo_id": "dc_msa",   "property_type_id": "3"},
    {"metric_id": "median_sale_price", "geo_id": "20019_dc", "property_type_id": "3"},
    {"metric_id": "median_sale_price", "geo_id": "20016_dc", "property_type_id": "3"},

    {"metric_id": "median_ppsf",       "geo_id": "dc_city",  "property_type_id": "3"},
    {"metric_id": "median_ppsf",       "geo_id": "dc_msa",   "property_type_id": "3"},
    {"metric_id": "median_ppsf",       "geo_id": "20019_dc", "property_type_id": "3"},
    {"metric_id": "median_ppsf",       "geo_id": "20016_dc", "property_type_id": "3"},

    {"metric_id": "median_dom",        "geo_id": "dc_city",  "property_type_id": "3"},
    {"metric_id": "median_dom",        "geo_id": "dc_msa",   "property_type_id": "3"},
    {"metric_id": "median_dom",        "geo_id": "20019_dc", "property_type_id": "3"},
    {"metric_id": "median_dom",        "geo_id": "20016_dc", "property_type_id": "3"},

    # Single Family Residential (6)
    {"metric_id": "median_sale_price", "geo_id": "dc_city",  "property_type_id": "6"},
    {"metric_id": "median_sale_price", "geo_id": "dc_msa",   "property_type_id": "6"},
    {"metric_id": "median_sale_price", "geo_id": "20019_dc", "property_type_id": "6"},
    {"metric_id": "median_sale_price", "geo_id": "20016_dc", "property_type_id": "6"},

    {"metric_id": "median_ppsf",       "geo_id": "dc_city",  "property_type_id": "6"},
    {"metric_id": "median_ppsf",       "geo_id": "dc_msa",   "property_type_id": "6"},
    {"metric_id": "median_ppsf",       "geo_id": "20019_dc", "property_type_id": "6"},
    {"metric_id": "median_ppsf",       "geo_id": "20016_dc", "property_type_id": "6"},

    {"metric_id": "median_dom",        "geo_id": "dc_city",  "property_type_id": "6"},
    {"metric_id": "median_dom",        "geo_id": "dc_msa",   "property_type_id": "6"},
    {"metric_id": "median_dom",        "geo_id": "20019_dc", "property_type_id": "6"},
    {"metric_id": "median_dom",        "geo_id": "20016_dc", "property_type_id": "6"},
]

# Thresholds – tune as you learn more
MAX_AGE_DAYS = 190       # re-run if model is older than this
MAX_MAPE_3M = 15.0      # re-run if 3-month MAPE > 15%
FORECAST_HORIZON = 12   # months ahead


# -----------------------------------------
# Helpers to read latest run & decide refresh
# -----------------------------------------

def get_latest_run_info(
    con,
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[str],
):
    """
    Return latest run info for this target, or None if no run exists.
    """
    pt_id = property_type_id  # stored as VARCHAR or NULL

    rows = con.execute(
        """
        SELECT
            r.run_id,
            r.created_at,
            v.mape_3m,
            v.rmse_3m
        FROM forecast_runs r
        LEFT JOIN v_forecast_eval v ON v.run_id = r.run_id
        WHERE r.target_metric_id = ?
          AND r.target_geo_id = ?
          AND (
              (r.target_property_type_id IS NULL AND ? IS NULL)
              OR  r.target_property_type_id = ?
          )
        ORDER BY r.created_at DESC
        LIMIT 1
        """,
        [metric_id, geo_id, pt_id, pt_id],
    ).fetchall()

    if not rows:
        return None

    run_id, created_at, mape_3m, rmse_3m = rows[0]
    return {
        "run_id": run_id,
        "created_at": created_at,
        "mape_3m": mape_3m,
        "rmse_3m": rmse_3m,
    }


def should_refresh(run_info: Optional[Dict]) -> bool:
    """
    Logic for whether to refresh a forecast:

    - If no previous run: refresh.
    - If run older than MAX_AGE_DAYS: refresh.
    - If 3-month MAPE is available and > MAX_MAPE_3M: refresh.
    """
    if run_info is None:
        return True

    now = datetime.now(timezone.utc)
    created_at = run_info["created_at"]
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    age_days = (now - created_at).days
    mape_3m = run_info["mape_3m"]

    if age_days > MAX_AGE_DAYS:
        return True

    if mape_3m is not None and mape_3m > MAX_MAPE_3M:
        return True

    return False


# -----------------------------------------
# Batch runner
# -----------------------------------------

def run_batch():
    con = get_duckdb_connection()

    refreshed = []
    skipped = []

    for t in TARGETS:
        metric_id = t["metric_id"]
        geo_id = t["geo_id"]
        pt_id = t.get("property_type_id")

        print(f"\n=== Target: metric={metric_id}, geo={geo_id}, pt={pt_id} ===")

        run_info = get_latest_run_info(con, metric_id, geo_id, pt_id)

        if run_info is None:
            print("No previous runs found. Will run forecast.")
        else:
            print(
                f"Latest run_id={run_info['run_id']}, "
                f"created_at={run_info['created_at']}, "
                f"mape_3m={run_info['mape_3m']}, "
                f"rmse_3m={run_info['rmse_3m']}"
            )

        if should_refresh(run_info):
            print("-> Refreshing forecast...")
            run_id = run_sarimax_forecast(
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=pt_id,
                horizon_max_months=FORECAST_HORIZON,
                notes="Batch SARIMAX run",
            )
            print(f"   New run_id={run_id}")
            refreshed.append((metric_id, geo_id, pt_id, run_id))
        else:
            print("-> Keeping existing forecast.")
            if run_info is not None:
                skipped.append((metric_id, geo_id, pt_id, run_info["run_id"]))
            else:
                skipped.append((metric_id, geo_id, pt_id, None))

    print("\n=== Batch summary ===")
    print(f"Refreshed {len(refreshed)} targets.")
    print(f"Skipped {len(skipped)} targets.")


if __name__ == "__main__":
    run_batch()
