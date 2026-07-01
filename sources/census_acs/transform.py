# sources/census/transform.py
import os
from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = os.getenv("DUCKDB_PATH")
if not DB_PATH:
    raise SystemExit("[census:transform] DUCKDB_PATH not set (refusing to run against default db)")

RAW_PATH = Path("data/census/census_acs5_raw.csv")

SOURCE_IDS = ["census_acs5", "census_acs1"]

VAR_TO_METRIC = {
    ("census_acs5", "B01003_001E"): "census_acs5_pop_total",
    ("census_acs5", "B19013_001E"): "census_acs5_median_household_income",
    ("census_acs1", "B01003_001E"): "census_acs1_pop_total",
    ("census_acs1", "B19013_001E"): "census_acs1_median_household_income",
}


def main():
    if not RAW_PATH.exists():
        raise SystemExit(f"[census:transform] missing {RAW_PATH}; run census ingest first")

    try:
        df = pd.read_csv(RAW_PATH)
    except pd.errors.EmptyDataError:
        print("[census:transform] raw file has no columns/rows; nothing to load")
        return
        
    if df.empty:
        print("[census:transform] raw file empty; nothing to load")
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date

    if "source_id" not in df.columns:
        df["source_id"] = "census_acs5"
    df["source_id"] = df["source_id"].fillna("census_acs5").astype(str).str.strip()
    
    df["metric_id"] = [
        VAR_TO_METRIC.get((sid, var))
        for sid, var in zip(df["source_id"], df["variable_code"])
    ]

    unknown = sorted(set(df.loc[df["metric_id"].isna(), "variable_code"].dropna().astype(str)))
    if unknown:
        raise SystemExit(f"[census:transform] unknown variable_code(s): {unknown}")

    df["property_type_id"] = "all"
    df["property_type"] = None

    # coerce value
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    con = duckdb.connect(DB_PATH)

    # mirror Redfin schema (property_type column included)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_timeseries(
      geo_id TEXT NOT NULL,
      metric_id TEXT NOT NULL,
      date DATE NOT NULL,
      property_type_id TEXT NOT NULL DEFAULT 'all',
      value DOUBLE,
      source_id TEXT,
      property_type TEXT,
      PRIMARY KEY (geo_id, metric_id, date, property_type_id)
    );
    """)

    # ensure dim_source + dim_metric exist (same style as CES)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_source(
      source_id TEXT PRIMARY KEY, name TEXT, url TEXT, cadence TEXT, license TEXT
    );
    """)

    for source_id, name in [
        ("census_acs5", "Census ACS 5-year"),
        ("census_acs1", "Census ACS 1-year"),
    ]:
        con.execute("""
        INSERT INTO dim_source(source_id, name, url, cadence, license)
        SELECT ?, ?, 'https://www.census.gov/programs-surveys/acs', 'annual', 'public'
        WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id=?);
        """, [source_id, name, source_id])

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_metric(
      metric_id TEXT PRIMARY KEY, name TEXT, frequency TEXT, unit TEXT, category TEXT
    );
    """)

    meta = {
        "census_acs5_pop_total": ("Total population (ACS 5-year)", "annual", "persons", "census"),
        "census_acs5_median_household_income": ("Median household income (ACS 5-year)", "annual", "usd", "census"),
        "census_acs1_pop_total": ("Total population (ACS 1-year)", "annual", "persons", "census"),
        "census_acs1_median_household_income": ("Median household income (ACS 1-year)", "annual", "usd", "census"),
    }
    for mid in sorted(df["metric_id"].unique()):
        name, freq, unit, cat = meta.get(mid, (mid, "annual", "value", "census"))
        con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT ?,?,?,?,?
        WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?)
        """, [mid, name, freq, unit, cat, mid])


    # dedupe and insert
    df = (df.sort_values(["geo_id","metric_id","date","property_type_id"])
            .drop_duplicates(subset=["geo_id","metric_id","date","property_type_id"], keep="last"))

    dup_mask = df.duplicated(subset=["geo_id", "metric_id", "date", "property_type_id"], keep=False)
    if dup_mask.any():
        print("[census:transform] WARNING: duplicate PK rows detected; sample:")
        print(
            df.loc[dup_mask, ["geo_id","metric_id","date","property_type_id","value","variable_code","census_code","geo_level"]]
              .head(25)
              .to_string(index=False)
        )

    # Deduplicate exactly on the fact_timeseries primary key
    before = len(df)
    df = (
        df.sort_values(["geo_id", "metric_id", "date", "property_type_id"])
          .drop_duplicates(subset=["geo_id", "metric_id", "date", "property_type_id"], keep="last")
    )
    dropped = before - len(df)
    if dropped:
        print(f"[census:transform] dropped {dropped} duplicate raw rows on PK")

    # ---- Normalize key fields (avoid whitespace variants) ----
    df["geo_id"] = df["geo_id"].astype(str).str.strip()
    df["metric_id"] = df["metric_id"].astype(str).str.strip()
    df["property_type_id"] = df["property_type_id"].astype(str).str.strip()
    
    # ---- HARD duplicate diagnosis on the exact PK ----
    pk_cols = ["geo_id", "metric_id", "date", "property_type_id"]
    
    dup_counts = (
        df.groupby(pk_cols, dropna=False)
          .size()
          .reset_index(name="n")
    )
    
    dups = dup_counts[dup_counts["n"] > 1].sort_values("n", ascending=False)
    
    if not dups.empty:
        print("[census:transform] FAIL: duplicate PK rows detected in staging (showing up to 50):")
        print(dups.head(50).to_string(index=False))
    
        # show the raw rows for the first duplicate key
        k = dups.iloc[0][pk_cols].to_dict()
        sample = df
        for c in pk_cols:
            sample = sample[sample[c] == k[c]]
        print("[census:transform] sample duplicate raw rows:")
        cols_show = pk_cols + ["value", "variable_code", "census_code", "geo_level", "year", "census_name"]
        cols_show = [c for c in cols_show if c in df.columns]
        print(sample[cols_show].head(25).to_string(index=False))
    
        raise SystemExit("[census:transform] aborting due to duplicate PK rows; fix upstream or dedupe explicitly")
    
    # ---- If we ever allow duplicates upstream, dedupe deterministically here ----
    before = len(df)
    df = (
        df.sort_values(pk_cols)
          .drop_duplicates(subset=pk_cols, keep="last")
    )
    dropped = before - len(df)
    if dropped:
        print(f"[census:transform] dropped {dropped} duplicate raw rows on PK")

    con.register("c_stage", df[[
        "geo_id","metric_id","date","property_type_id","value","source_id","property_type"
    ]])

    # Key-based wipe of any existing rows that would collide with these staged facts
    # (PK does not include source_id)
    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1
      FROM c_stage s
      WHERE s.geo_id = f.geo_id
        AND s.metric_id = f.metric_id
        AND s.date = f.date
        AND s.property_type_id = f.property_type_id
    )
    """)
    print("[census:transform] cleared existing rows for staged keys (any source_id)")


    con.execute("""
    INSERT INTO fact_timeseries(geo_id,metric_id,date,property_type_id,value,source_id,property_type)
    SELECT geo_id,metric_id,date,property_type_id,CAST(value AS DOUBLE),source_id,property_type
    FROM c_stage
    """)

    print(con.execute("""
      SELECT geo_id, metric_id, MIN(date) AS first, MAX(date) AS last, COUNT(*) AS n
      FROM fact_timeseries
      WHERE source_id IN ('census_acs5','census_acs1')
      GROUP BY 1,2
      ORDER BY 1,2
    """).fetchdf())

    con.close()


if __name__ == "__main__":
    main()
