from __future__ import annotations

from pathlib import Path
import pandas as pd

from core.db import connect
from .governance import BASELINE_ID, METRICS, RAW_ROOT, GovernanceError
from .storage import atomic_json, read_json


def apply_candidate(drop_id: str, candidate: Path, connection=None, root: Path = RAW_ROOT) -> int:
    meta_path = root / "drops" / drop_id / "metadata.json" if drop_id != BASELINE_ID else root / "baseline" / BASELINE_ID / "candidate_metadata.json"; meta = read_json(meta_path)
    if meta.get("status") == "serving_refreshed" and Path(meta.get("candidate_path", "")) == candidate:
        return int(meta["serving_rows"])
    if meta.get("status") != "candidate_validated" or Path(meta.get("candidate_path", "")) != candidate:
        raise GovernanceError("candidate must be explicitly candidate_validated before serving mutation")
    frame = pd.read_parquet(candidate); keys = ["geo_id", "metric_id", "date", "property_type_id"]
    if frame.empty or frame.duplicated(keys).any() or frame.value.isna().any(): raise GovernanceError("candidate integrity validation failed")
    owned = connection is None; con = connection or connect()
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("""CREATE TABLE IF NOT EXISTS fact_timeseries(geo_id TEXT NOT NULL, metric_id TEXT NOT NULL, date DATE NOT NULL, property_type_id TEXT NOT NULL DEFAULT 'all', value DOUBLE, source_id TEXT, property_type TEXT, PRIMARY KEY(geo_id,metric_id,date,property_type_id))""")
        con.register("redfin_candidate", frame); con.execute("DELETE FROM fact_timeseries WHERE source_id='redfin'")
        con.execute("""INSERT INTO fact_timeseries SELECT geo_id,metric_id,date,property_type_id,CAST(value AS DOUBLE),'redfin',property_type FROM redfin_candidate""")
        inserted = con.execute("SELECT count(*) FROM fact_timeseries WHERE source_id='redfin'").fetchone()[0]
        if inserted != len(frame): raise GovernanceError("post-insert row count mismatch")
        metrics = {row[0] for row in con.execute("SELECT DISTINCT metric_id FROM fact_timeseries WHERE source_id='redfin'").fetchall()}
        latest = str(con.execute("SELECT max(date) FROM fact_timeseries WHERE source_id='redfin'").fetchone()[0])[:7]
        duplicates = con.execute("SELECT count(*) FROM (SELECT 1 FROM fact_timeseries WHERE source_id='redfin' GROUP BY geo_id,metric_id,date,property_type_id HAVING count(*)>1)").fetchone()[0]
        nulls = con.execute("SELECT count(*) FROM fact_timeseries WHERE source_id='redfin' AND (geo_id IS NULL OR metric_id IS NULL OR date IS NULL OR property_type_id IS NULL OR value IS NULL)").fetchone()[0]
        if metrics != METRICS or latest != drop_id or duplicates or nulls: raise GovernanceError("post-insert serving integrity validation failed")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK"); raise
    finally:
        if owned: con.close()
    meta.update(status="serving_refreshed", serving_rows=inserted); atomic_json(meta_path, meta); return inserted


def main() -> int:
    raise SystemExit("Use scripts/apply_redfin_candidate.py with an explicit validated candidate")

if __name__ == "__main__": main()
