from __future__ import annotations
# scripts/build_dims.py

import os
from pathlib import Path

import duckdb

DB = os.getenv("DUCKDB_PATH", "data/market.duckdb")


def ensure_v_dim_geo(con: duckdb.DuckDBPyConnection) -> None:
    manifest_path = Path("config/geo_manifest.generated.csv").as_posix()

    con.execute("DROP VIEW IF EXISTS v_dim_geo")
    con.execute(
        f"""
        CREATE VIEW v_dim_geo AS
        SELECT *
        FROM read_csv_auto('{manifest_path}', header=true)
        """
    )
    print(f"[dims] recreated v_dim_geo from {manifest_path}")


def main() -> int:
    con = duckdb.connect(DB)

    # Self-bootstrap the manifest view on fresh environments
    ensure_v_dim_geo(con)

    # Materialize dims from views (fast, deterministic snapshot)
    con.execute("CREATE OR REPLACE TABLE dim_geo AS SELECT * FROM v_dim_geo;")

    # sanity
    cols = [r[1] for r in con.execute("PRAGMA table_info('dim_geo')").fetchall()]
    if "geo_id" not in cols:
        raise SystemExit(f"dim_geo missing geo_id. cols={cols}")

    n = con.execute("SELECT COUNT(*) FROM dim_geo").fetchone()[0]
    n_redfin = con.execute("SELECT COUNT(*) FROM dim_geo WHERE include_redfin=1").fetchone()[0]

    print(f"[dims] dim_geo rows={n} include_redfin={n_redfin}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
