"""Smoke 163: monthly state, locking, gates, atomic serving, and fixture E2E."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from jobs.monthly_refresh.orchestrator import Orchestrator, exclusive_lock
from sources.redfin.storage import atomic_json


def database(path: Path, value: float = 1.0) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE dim_source(source_id VARCHAR)")
    con.execute("CREATE TABLE dim_metric(metric_id VARCHAR)")
    con.execute("""CREATE TABLE fact_timeseries(
        geo_id VARCHAR, metric_id VARCHAR, date DATE, value DOUBLE,
        property_type_id VARCHAR, source_id VARCHAR)""")
    con.execute("INSERT INTO fact_timeseries VALUES ('fixture','median_sale_price_nsa','2026-08-31',?,'all','redfin')", [value])
    con.close()


def main() -> int:
    with TemporaryDirectory(prefix="monthly-refresh-") as temp:
        root = Path(temp); raw = root / "raw"; db = root / "full.duckdb"; serving = root / "serving.duckdb"
        database(db); database(serving, 99)
        orchestration = root / "artifacts"

        missing = Orchestrator("2026-08", orchestration, db, serving, raw_root=raw)
        assert missing.execute()["status"] == "waiting_for_manual_redfin"
        assert serving.exists() and duckdb.connect(str(serving), read_only=True).execute("SELECT value FROM fact_timeseries").fetchone()[0] == 99

        meta = raw / "drops/2026-08/metadata.json"
        atomic_json(meta, {"drop_id": "2026-08", "status": "registered"})
        registered = Orchestrator("2026-08", orchestration, db, serving, raw_root=raw)
        assert registered.execute()["status"] == "waiting_for_manual_redfin"
        assert "validate_redfin_drop.py" in registered.manifest["required_action"]

        atomic_json(meta, {"drop_id": "2026-08", "status": "promoted"})
        events: list[str] = []
        def runner(command: list[str]) -> None:
            events.append(" ".join(command))
            if "scripts.run_regime_pipeline" in command:
                run_id = command[command.index("--run-id") + 1]
                out = Path(command[command.index("--artifact-root") + 1]) / run_id
                out.mkdir(parents=True, exist_ok=True)
                (out / "payload.txt").write_text("fixture")
                (out / "manifest.json").write_text(json.dumps({"status":"complete", "artifacts":{"payload":{}}}))
            elif "scripts.build_macro_regime_site" in command:
                out = Path(command[command.index("--output") + 1]); out.mkdir(parents=True); (out / "index.html").write_text("fixture")
        run = Orchestrator("2026-08", orchestration, db, serving, raw_root=raw,
                           runner=runner, serving_validator=lambda candidate: events.append("serving validated"),
                           regime_artifact_root=root / "regime",
                           redfin_finalizer=lambda serving_db, target: events.append("redfin promoted"))
        result = run.execute()
        assert result["status"] == "analytics_complete"
        assert run.manifest["completed_stages"] == list(("sources_validated", "serving_candidate_built", "serving_validated",
            "serving_promoted", "redfin_promoted", "regime_built", "regime_validated", "site_built", "publish_bundle_created"))
        assert len(events) == 4 and events[:2] == ["serving validated", "redfin promoted"]
        # Mark publication complete and prove the month is a no-op thereafter.
        run.save("complete", completed_at="fixture")
        assert Orchestrator("2026-08", orchestration, db, serving, raw_root=raw).execute()["status"] == "already_complete"

        lock = orchestration / "lock"
        with exclusive_lock(lock, "2026-08"):
            try:
                with exclusive_lock(lock, "2026-08"):
                    raise AssertionError("second lock acquired")
            except RuntimeError:
                pass

        # A forced validation failure preserves the old live DB byte-for-byte.
        failure_root = root / "failure"; old = serving.read_bytes()
        failed = Orchestrator("2026-08", failure_root, db, serving, raw_root=raw,
                              serving_validator=lambda candidate: (_ for _ in ()).throw(RuntimeError("forced")))
        try: failed.execute()
        except RuntimeError: pass
        else: raise AssertionError("validation failure was swallowed")
        assert serving.read_bytes() == old and failed.manifest["failure_stage"] == "serving_validated"
        assert not any("regime" in event for event in events[4:])
    print("[monthly_refresh] smoke 163: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
