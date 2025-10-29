# utils/db.py
import os, duckdb, pathlib, argparse
from dotenv import load_dotenv

load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

DDL = '''
-- Recreate dims with PRIMARY KEYs so we can do idempotent inserts cleanly
CREATE OR REPLACE TABLE dim_market(
  geo_id VARCHAR PRIMARY KEY,
  name   VARCHAR,
  type   VARCHAR,
  fips   VARCHAR
);

CREATE OR REPLACE TABLE dim_metric(
  metric_id  VARCHAR PRIMARY KEY,
  name       VARCHAR,
  frequency  VARCHAR,
  unit       VARCHAR,
  category   VARCHAR
);

CREATE OR REPLACE TABLE dim_source(
  source_id VARCHAR PRIMARY KEY,
  name      VARCHAR,
  url       VARCHAR,
  cadence   VARCHAR,
  license   VARCHAR
);

-- Facts don’t need PKs now; we append time series
CREATE OR REPLACE TABLE fact_timeseries(
  geo_id    VARCHAR,
  metric_id VARCHAR,
  date      DATE,
  value     DOUBLE,
  vintage_ts TIMESTAMP DEFAULT now(),
  source_id VARCHAR
);

CREATE OR REPLACE TABLE fact_forecast(
  geo_id     VARCHAR,
  metric_id  VARCHAR,
  date       DATE,
  horizon_m  INTEGER,
  forecast   DOUBLE,
  pi_low     DOUBLE,
  pi_high    DOUBLE,
  model_id   VARCHAR,
  backtest_fold INTEGER,
  trained_at TIMESTAMP DEFAULT now()
);

CREATE OR REPLACE TABLE fact_backtest(
  geo_id     VARCHAR,
  metric_id  VARCHAR,
  horizon_m  INTEGER,
  fold       INTEGER,
  mae        DOUBLE,
  mape       DOUBLE,
  rmse       DOUBLE,
  model_id   VARCHAR,
  trained_at TIMESTAMP
);

CREATE OR REPLACE TABLE fact_live_error(
  geo_id     VARCHAR,
  metric_id  VARCHAR,
  date       DATE,
  y_true     DOUBLE,
  y_pred     DOUBLE,
  error      DOUBLE,
  model_id   VARCHAR,
  horizon_m  INTEGER,
  scored_at  TIMESTAMP DEFAULT now()
);

CREATE OR REPLACE TABLE fact_drift(
  geo_id    VARCHAR,
  metric_id VARCHAR,
  feature   VARCHAR,
  psi       DOUBLE,
  test_stat DOUBLE,
  p_value   DOUBLE,
  tested_at TIMESTAMP DEFAULT now()
);

CREATE OR REPLACE TABLE fact_quality(
  geo_id          VARCHAR,
  metric_id       VARCHAR,
  date            DATE,
  completeness    DOUBLE,
  freshness_days  INTEGER,
  anomalies_flag  BOOLEAN,
  checked_at      TIMESTAMP DEFAULT now()
);
'''

def build():
    pathlib.Path(os.path.dirname(DUCKDB_PATH) or ".").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(DDL)
    con.close()
    print(f"[db] Initialized schema at {DUCKDB_PATH}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    args = ap.parse_args()
    if args.build:
        build()
