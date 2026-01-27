import os
import duckdb

def connect():
    db = (os.getenv("DUCKDB_PATH") or "").strip()
    if not db:
        raise SystemExit("[fatal] DUCKDB_PATH not set — refusing to run")
    return duckdb.connect(db)
