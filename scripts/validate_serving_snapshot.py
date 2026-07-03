from __future__ import annotations
# scripts/validate_serving_snapshot.py

import os
from pathlib import Path

import duckdb
import pandas as pd


DB_PATH = os.getenv("SERVING_DUCKDB_PATH", "data/market_serving.duckdb")
GEO_MANIFEST_PATH = Path("config/geo_manifest.generated.csv")


EXPECTED_SOURCES = {
    "redfin",
    "ces",
    "laus",
    "fred_macro",
    "fred_unemp",
    "bea_gdp_ann",
    "bea_gdp_qtr",
    "census_acs1",
    "census_acs5",
    "census_bps",
    "census_bps_provisional",
    "census_nrc_fred",
}

EXPECTED_CORE_METRICS = {
    # Redfin
    "median_sale_price_nsa",
    "median_sale_price_per_sqft",
    "median_days_on_market_days",
    "inventory",
    "homes_sold",
    "new_listings",

    # ACS
    "census_acs1_pop_total",
    "census_acs1_median_household_income",
    "census_acs5_pop_total",
    "census_acs5_median_household_income",

    # BEA
    "bea_agdp_real_total_chained2017",
    "bea_qgdp_real_total_chained2017_saar",

    # FRED / Census housing
    "fred_mortgage_30y_avg",
    "fred_fedfunds",
    "fred_gs10",
    "fred_spread_2y_10y",
    "census_housing_starts_total_saar",
    "census_housing_completions_total_saar",
}

LEGACY_GEO_IDS = {
    "us_nation",
    "dc_state",
    "md_state",
    "va_state",
    "dc_msa",
    "baltimore_msa",
}


SOURCE_FLAG_MAP = {
    "redfin": "include_redfin",
    "ces": "include_ces",
    "laus": "include_laus",
    "fred_unemp": "include_fred_unemp",
    "fred_macro": "include_fred",
    "bea_gdp_ann": "include_bea_agdp",
    "bea_gdp_qtr": "include_bea_qgdp",
    "census_acs1": "include_census",
    "census_acs5": "include_census",
    "census_bps": "include_census_bps",
}


def fail(msg: str) -> None:
    raise SystemExit(f"[serving:validate] FAIL {msg}")


def load_manifest() -> pd.DataFrame:
    if not GEO_MANIFEST_PATH.exists():
        fail(f"missing geo manifest: {GEO_MANIFEST_PATH}")

    geo = pd.read_csv(GEO_MANIFEST_PATH, dtype=str).fillna("")
    if "geo_slug" not in geo.columns:
        fail("geo_manifest.generated.csv missing geo_slug")

    return geo


def truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})


def main() -> int:
    con = duckdb.connect(DB_PATH)
    geo = load_manifest()

    # 1. Expected sources exist
    sources = {
        r[0]
        for r in con.execute("""
            SELECT DISTINCT source_id
            FROM fact_timeseries
            WHERE source_id IS NOT NULL
        """).fetchall()
    }

    missing_sources = sorted(EXPECTED_SOURCES - sources)
    if missing_sources:
        fail(f"missing expected sources: {missing_sources}")

    # 2. Expected core metrics exist
    metrics = {
        r[0]
        for r in con.execute("""
            SELECT DISTINCT metric_id
            FROM fact_timeseries
            WHERE metric_id IS NOT NULL
        """).fetchall()
    }

    missing_metrics = sorted(EXPECTED_CORE_METRICS - metrics)
    if missing_metrics:
        fail(f"missing expected core metrics: {missing_metrics}")

    # 3. No legacy geo IDs
    legacy = con.execute("""
        SELECT source_id, geo_id, COUNT(*) AS rows
        FROM fact_timeseries
        WHERE geo_id IN ({})
        GROUP BY 1,2
        ORDER BY rows DESC
    """.format(",".join(["?"] * len(LEGACY_GEO_IDS))), list(LEGACY_GEO_IDS)).fetchdf()

    if not legacy.empty:
        fail(f"legacy geo IDs found:\n{legacy.to_string(index=False)}")

    # 4. No duplicate fact keys
    dupes = con.execute("""
        SELECT geo_id, metric_id, date, property_type_id, source_id, COUNT(*) AS n
        FROM fact_timeseries
        GROUP BY 1,2,3,4,5
        HAVING COUNT(*) > 1
        LIMIT 25
    """).fetchdf()

    if not dupes.empty:
        fail(f"duplicate fact keys found:\n{dupes.to_string(index=False)}")

    # 5. Manifest-driven geo coverage by source
    coverage_failures = []

    for source_id, flag_col in SOURCE_FLAG_MAP.items():
        if flag_col not in geo.columns:
            continue

        expected_geos = set(geo.loc[truthy(geo[flag_col]), "geo_slug"])
        if not expected_geos:
            continue

        actual_geos = {
            r[0]
            for r in con.execute("""
                SELECT DISTINCT geo_id
                FROM fact_timeseries
                WHERE source_id = ?
            """, [source_id]).fetchall()
        }

        present = expected_geos & actual_geos
        if not present:
            coverage_failures.append(
                f"{source_id}: no manifest-requested geos present "
                f"(expected {len(expected_geos)})"
            )

    if coverage_failures:
        fail("manifest-driven coverage failures:\n" + "\n".join(coverage_failures))

    print("[serving:validate] source summary:")
    print(con.execute("""
        SELECT source_id, COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
        FROM fact_timeseries
        GROUP BY 1
        ORDER BY 1
    """).fetchdf().to_string(index=False))

    print("[serving:validate] OK")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
