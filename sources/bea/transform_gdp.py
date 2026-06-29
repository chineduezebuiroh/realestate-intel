from __future__ import annotations
# sources/bea/transform_gdp.py

from pathlib import Path
import pandas as pd

from core.db import connect  # you said this DEFINITELY exists

"""
sources/bea_qgdp/transform.py

Read BEA QGDP raw artifact from:
  data/bea/bea_qgdp_raw_long.csv

Then:
- ensure dim_source, dim_metric, dim_market
- upsert into fact_timeseries (PK: geo_id, metric_id, date, property_type_id)
"""

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_QGDP_PATH = REPO_ROOT / "data" / "bea" / "bea_qgdp_raw_long.csv"
RAW_AGDP_PATH = REPO_ROOT / "data" / "bea" / "bea_agdp_raw_long.csv"


def ensure_dims(con, df: pd.DataFrame) -> None:
    sources = [
        (
            "bea_gdp_qtr",
            "BEA GDP (Quarterly)",
            "quarterly",
        ),
        (
            "bea_gdp_ann",
            "BEA GDP (Annual)",
            "annual",
        ),
    ]
    
    for source_id, name, cadence in sources:
        con.execute("""
        INSERT INTO dim_source(source_id,name,url,cadence,license)
        SELECT ?, ?, 'https://www.bea.gov/data', ?, 'public'
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_source WHERE source_id=?
        );
        """, [source_id, name, cadence, source_id])
    
    
    metrics = [
        (
            "bea_qgdp_real_total_chained2017_saar",
            "Real GDP (Quarterly, chained 2017 dollars SAAR)",
            "quarterly",
        ),
        (
            "bea_agdp_real_total_chained2017",
            "Real GDP (Annual, chained 2017 dollars)",
            "annual",
        ),
    ]
    
    unit = None
    if "unit" in df.columns and df["unit"].notna().any():
        unit = str(df.loc[df["unit"].notna(), "unit"].iloc[0])
    
    for metric_id, name, freq in metrics:
        con.execute("""
        INSERT INTO dim_metric(metric_id,name,frequency,unit,category)
        SELECT ?,?,?,?,?
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_metric
            WHERE metric_id=?
        );
        """,
        [
            metric_id,
            name,
            freq,
            unit,
            "gdp",
            metric_id,
        ])

    # dim_market
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_market(
      geo_id TEXT PRIMARY KEY,
      name TEXT,
      type TEXT,
      fips TEXT
    );
    """)
    mkts = (
        df[["geo_id", "geo_name", "bea_geo_fips"]]
        .drop_duplicates()
        .rename(columns={"geo_name": "name", "bea_geo_fips": "fips"})
        .assign(type="bea_geo")
    )
    con.register("bea_mkts", mkts)
    con.execute("""
    INSERT INTO dim_market(geo_id, name, type, fips)
    SELECT geo_id, name, type, fips FROM bea_mkts
    WHERE geo_id NOT IN (SELECT geo_id FROM dim_market);
    """)


def upsert_fact_timeseries(con, df: pd.DataFrame) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_timeseries(
      geo_id TEXT NOT NULL,
      metric_id TEXT NOT NULL,
      date DATE NOT NULL,
      property_type_id TEXT NOT NULL DEFAULT 'all',
      value DOUBLE,
      source_id TEXT,
      PRIMARY KEY (geo_id, metric_id, date, property_type_id)
    );
    """)

    stage = df[["geo_id", "metric_id", "date", "property_type_id", "value", "source_id"]].copy()
    stage["date"] = pd.to_datetime(stage["date"]).dt.date

    # dedupe
    stage = (
        stage.sort_values(["geo_id", "metric_id", "date", "property_type_id"])
             .drop_duplicates(["geo_id", "metric_id", "date", "property_type_id"], keep="last")
    )

    con.register("bea_stage", stage)

    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1 FROM bea_stage s
      WHERE s.geo_id = f.geo_id
        AND s.metric_id = f.metric_id
        AND s.date = f.date
        AND s.property_type_id = f.property_type_id
    );
    """)

    con.execute("""
    INSERT INTO fact_timeseries(geo_id, metric_id, date, property_type_id, value, source_id)
    SELECT geo_id, metric_id, date, property_type_id, CAST(value AS DOUBLE), source_id
    FROM bea_stage;
    """)


def main():

    frames = []

    if RAW_QGDP_PATH.exists():
        q = pd.read_csv(RAW_QGDP_PATH)
        if not q.empty:
            frames.append(q)

    if RAW_AGDP_PATH.exists():
        a = pd.read_csv(RAW_AGDP_PATH)
        if not a.empty:
            frames.append(a)

    if not frames:
        raise SystemExit(
            "[bea:transform] No quarterly or annual raw BEA files found."
        )

    df = pd.concat(frames, ignore_index=True)

    con = connect()

    ensure_dims(con, df)
    upsert_fact_timeseries(con, df)

    summary = con.execute("""
        SELECT
            source_id,
            metric_id,
            COUNT(*) as rows,
            MIN(date) as first_date,
            MAX(date) as last_date
        FROM fact_timeseries
        WHERE source_id IN (
            'bea_gdp_qtr',
            'bea_gdp_ann'
        )
        GROUP BY 1,2
        ORDER BY 1,2
    """).fetchdf()

    print(summary)

    con.close()


if __name__ == "__main__":
    main()
