from __future__ import annotations

import os
import duckdb

DB = os.getenv("DUCKDB_PATH", "data/market.duckdb")

def main() -> int:
    con = duckdb.connect(DB)

    # Require v_dim_geo to exist
    views = {r[0] for r in con.execute("PRAGMA show_tables").fetchall()}
    if "v_dim_geo" not in views:
        raise SystemExit("Missing view v_dim_geo. Create it first (view over config/geo_manifest.csv).")

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
