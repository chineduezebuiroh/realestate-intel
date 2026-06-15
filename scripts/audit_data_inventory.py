from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


DB_PATH = os.getenv("DUCKDB_PATH", "data/market.duckdb")
OUT_DIR = Path(os.getenv("AUDIT_OUT_DIR", "artifacts/audit"))


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    rows = con.execute("PRAGMA show_tables").fetchall()
    return name in {r[0] for r in rows}


def _columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]


def _infer_geo_level_sql(geo_col: str = "geo_id") -> str:
    return f"""
    CASE
      WHEN {geo_col} = 'us_nation' THEN 'nation'
      WHEN {geo_col} LIKE '%_state' THEN 'state'
      WHEN {geo_col} LIKE '%_msa' THEN 'metro'
      WHEN {geo_col} LIKE '%_msd' THEN 'metro_division'
      WHEN {geo_col} LIKE '%_county%' THEN 'county'
      WHEN {geo_col} LIKE 'zip_%' THEN 'zip'
      WHEN {geo_col} LIKE '%_city' THEN 'city'
      WHEN {geo_col} LIKE '%neighborhood%' THEN 'neighborhood'
      ELSE 'unknown'
    END
    """


def _source_inventory(con: duckdb.DuckDBPyConnection, refresh_ts: str) -> pd.DataFrame:
    cols = _columns(con, "fact_timeseries")

    required = {"source_id", "metric_id", "geo_id", "date"}
    missing = required - set(cols)
    if missing:
        raise ValueError(f"fact_timeseries missing required columns: {sorted(missing)}")

    q = f"""
    SELECT
        COALESCE(source_id, 'unknown') AS source_id,
        COUNT(DISTINCT metric_id) AS metric_count,
        COUNT(DISTINCT geo_id) AS geo_count,
        COUNT(*) AS row_count,
        MIN(date) AS first_date,
        MAX(date) AS last_date
    FROM fact_timeseries
    GROUP BY 1
    ORDER BY source_id
    """

    df = con.execute(q).df()
    df["latest_refresh_timestamp"] = refresh_ts

    SOURCE_FREQUENCY = {
        "redfin": "monthly",
        "ces": "monthly",
        "laus": "monthly",
        "fred_macro": "monthly",
        "fred_unemp": "monthly",
        "bea_gdp_qtr": "quarterly",
        "census_acs5": "annual",
        "census_bps": "monthly",
        "census_nrc_fred": "monthly",
    }
    
    def estimate_frequency(row):
        source_id = str(row["source_id"]).lower()
    
        if source_id in SOURCE_FREQUENCY:
            return SOURCE_FREQUENCY[source_id]
    
        # fallback inference for future sources
        n = row["row_count"]
        first = pd.to_datetime(row["first_date"])
        last = pd.to_datetime(row["last_date"])
    
        if pd.isna(first) or pd.isna(last) or n == 0:
            return "unknown"
    
        years = max((last - first).days / 365.25, 0.01)
        obs_per_year = n / max(
            row["metric_count"] * row["geo_count"] * years,
            1
        )
    
        if obs_per_year >= 10:
            return "monthly_or_higher"
        if obs_per_year >= 3:
            return "quarterly"
        if obs_per_year >= 0.5:
            return "annual"
    
        return "sparse"
    
    df["estimated_frequency"] = df.apply(estimate_frequency, axis=1)

    return df[
        [
            "source_id",
            "metric_count",
            "geo_count",
            "row_count",
            "first_date",
            "last_date",
            "estimated_frequency",
            "latest_refresh_timestamp",
        ]
    ]


def _history_inventory(con: duckdb.DuckDBPyConnection, refresh_ts: str) -> pd.DataFrame:
    cols = _columns(con, "fact_timeseries")
    pt_expr = "property_type_id" if "property_type_id" in cols else "'all'"

    if _table_exists(con, "dim_geo"):
        dim_cols = _columns(con, "dim_geo")
        if "geo_id" in dim_cols and "level" in dim_cols:
            geo_level_expr = "COALESCE(g.level, 'unknown')"
            join_sql = "LEFT JOIN dim_geo g ON ft.geo_id = g.geo_id"
        elif "geo_id" in dim_cols and "geo_level" in dim_cols:
            geo_level_expr = "COALESCE(g.geo_level, 'unknown')"
            join_sql = "LEFT JOIN dim_geo g ON ft.geo_id = g.geo_id"
        else:
            geo_level_expr = _infer_geo_level_sql("ft.geo_id")
            join_sql = ""
    else:
        geo_level_expr = _infer_geo_level_sql("ft.geo_id")
        join_sql = ""

    q = f"""
    SELECT
        COALESCE(ft.source_id, 'unknown') AS source_id,
        ft.metric_id,
        {geo_level_expr} AS geo_level,
        CAST(COALESCE(CAST({pt_expr} AS VARCHAR), 'all') AS VARCHAR) AS property_type_id,
        COUNT(DISTINCT ft.geo_id) AS geo_count,
        COUNT(*) AS observation_count,
        MIN(ft.date) AS first_date,
        MAX(ft.date) AS last_date
    FROM fact_timeseries ft
    {join_sql}
    GROUP BY 1,2,3,4
    ORDER BY source_id, metric_id, geo_level, property_type_id
    """

    df = con.execute(q).df()
    df["first_date"] = pd.to_datetime(df["first_date"])
    df["last_date"] = pd.to_datetime(df["last_date"])
    df["approx_years_available"] = (
        (df["last_date"] - df["first_date"]).dt.days / 365.25
    ).round(2)
    df["latest_refresh_timestamp"] = refresh_ts

    return df[
        [
            "source_id",
            "metric_id",
            "geo_level",
            "property_type_id",
            "geo_count",
            "observation_count",
            "first_date",
            "last_date",
            "approx_years_available",
            "latest_refresh_timestamp",
        ]
    ]


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    refresh_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

    con = duckdb.connect(DB_PATH)

    if not _table_exists(con, "fact_timeseries"):
        raise SystemExit(f"Missing fact_timeseries table in {DB_PATH}")

    source_df = _source_inventory(con, refresh_ts)
    history_df = _history_inventory(con, refresh_ts)

    source_path = OUT_DIR / "data_inventory.csv"
    history_path = OUT_DIR / "indicator_history_inventory.csv"

    source_df.to_csv(source_path, index=False)
    history_df.to_csv(history_path, index=False)

    print(f"[audit] wrote {len(source_df):,} rows -> {source_path}")
    print(f"[audit] wrote {len(history_df):,} rows -> {history_path}")

    print("\n[audit] source inventory:")
    print(source_df.to_string(index=False))

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
