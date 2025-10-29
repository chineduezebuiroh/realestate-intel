import os, duckdb, pathlib, argparse
from dotenv import load_dotenv

load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

DDL = '''
CREATE TABLE IF NOT EXISTS dim_market(
  geo_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  type VARCHAR,
  fips VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_metric(
  metric_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  frequency VARCHAR,
  unit VARCHAR,
  category VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_source(
  source_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  url VARCHAR,
  cadence VARCHAR,
  license VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_timeseries(
  geo_id VARCHAR,
  metric_id VARCHAR,
  date DATE,
  value DOUBLE,
  vintage_ts TIMESTAMP DEFAULT now(),
  source_id VARCHAR
);
'''

def build():
    pathlib.Path(os.path.dirname(DUCKDB_PATH)).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(DDL)
    con.close()
    print(f"[db] Initialized schema at {DUCKDB_PATH}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()
    if args.build: build()
