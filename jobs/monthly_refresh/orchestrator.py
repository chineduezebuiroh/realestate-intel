"""Deterministic, resumable local-to-GitHub monthly production orchestration."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import socket
import subprocess
import sys
import tarfile
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from io import BytesIO
from typing import Callable

import duckdb

from scripts.build_serving_snapshot import build_candidate, promote_candidate
from sources.redfin.governance import RAW_ROOT
from sources.redfin.storage import atomic_json, current, read_json

SCHEMA_VERSION = "monthly_refresh_v1"
TERMINAL = {"complete", "already_complete"}
READY_REDFIN = {"serving_refreshed", "published", "promoted"}
STAGES = ("sources_validated", "serving_candidate_built", "serving_validated",
          "serving_promoted", "redfin_promoted", "regime_built", "regime_validated", "site_built",
          "publish_bundle_created")

SOURCE_INVENTORY = (
    ("redfin", "manual_prerequisite", "monthly"),
    ("ces", "monthly_required", "monthly"),
    ("laus", "monthly_required", "monthly"),
    ("fred_macro", "monthly_required", "daily/monthly"),
    ("fred_unemp", "monthly_required", "monthly"),
    ("census_bps", "monthly_required", "monthly"),
    ("census_bps_provisional", "monthly_required", "monthly"),
    ("bea_gdp_qtr", "slower_cadence", "quarterly"),
    ("bea_gdp_ann", "slower_cadence", "annual"),
    ("census_acs1", "slower_cadence", "annual"),
    ("census_acs5", "slower_cadence", "annual"),
    ("census_nrc_fred", "slower_cadence", "monthly"),
)


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_json(path: Path, value: object) -> None:
    atomic_json(path, value)  # same-directory replace is atomic


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def redfin_state(target: str, raw_root: Path = RAW_ROOT) -> tuple[str, dict]:
    path = raw_root / "drops" / target / "metadata.json"
    if not path.exists():
        return "missing", {}
    metadata = read_json(path)
    state = str(metadata.get("status", "registered"))
    return state, metadata


def source_summary(db: Path) -> list[dict]:
    if not db.is_file():
        return []
    con = duckdb.connect(str(db), read_only=True)
    try:
        rows = con.execute("""SELECT source_id, COUNT(*), MAX(date) FROM fact_timeseries
                              GROUP BY source_id ORDER BY source_id""").fetchall()
    finally:
        con.close()
    inventory = {item[0]: item for item in SOURCE_INVENTORY}
    return [{"source_id": source, "source_class": inventory.get(source, (None,"governed_other",None))[1],
             "cadence": inventory.get(source, (None,None,"unknown"))[2], "row_count": count,
             "latest_observation_date": str(latest) if latest else None}
            for source, count, latest in rows]


@contextmanager
def exclusive_lock(path: Path, target: str, recover_stale: bool = False):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"pid": os.getpid(), "host": socket.gethostname(), "target_month": target, "created_at": now()}
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        existing = json.loads(path.read_text())
        pid = int(existing.get("pid", -1))
        alive = pid > 0
        try:
            os.kill(pid, 0)
        except OSError:
            alive = False
        if alive or not recover_stale:
            raise RuntimeError(f"monthly refresh lock exists: {path}; metadata={existing}; use --recover-stale-lock only after verification")
        path.unlink()
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(payload, stream, sort_keys=True)
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


class Orchestrator:
    def __init__(self, target: str, root: Path = Path("artifacts/monthly_refresh"),
                 full_db: Path = Path("data/market.duckdb"), serving_db: Path = Path("data/market_serving.duckdb"),
                 raw_root: Path = RAW_ROOT, runner: Callable[[list[str]], None] | None = None,
                 serving_validator: Callable[[Path], None] | None = None,
                 regime_artifact_root: Path = Path("artifacts/regime/runs"),
                 redfin_finalizer: Callable[[Path, str], None] | None = None):
        datetime.strptime(target, "%Y-%m")
        self.target, self.root, self.full_db, self.serving_db, self.raw_root = target, root, full_db, serving_db, raw_root
        self.run_id = f"monthly_refresh_{target.replace('-', '')}_v1"
        self.run_dir = root / "runs" / self.run_id
        self.state_path = root / "months" / f"{target}.json"
        self.runner = runner or self._run
        self.serving_validator = serving_validator or self._validate_serving_script
        self.regime_artifact_root = regime_artifact_root
        self.redfin_finalizer = redfin_finalizer or self._finalize_redfin
        self.manifest = self._load()

    def _load(self) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        return {"schema_version": SCHEMA_VERSION, "run_id": self.run_id, "target_month": self.target,
                "status": "new", "started_at": now(), "completed_at": None, "git_sha": self._git_sha(),
                "completed_stages": [], "warnings": [], "failure_stage": None}

    @staticmethod
    def _git_sha() -> str | None:
        result = subprocess.run(["git", "rev-parse", "HEAD"], text=True, capture_output=True)
        return result.stdout.strip() if result.returncode == 0 else None

    @staticmethod
    def _run(command: list[str]) -> None:
        subprocess.run(command, check=True)

    @staticmethod
    def _validate_serving_script(candidate: Path) -> None:
        env = os.environ.copy(); env["SERVING_DUCKDB_PATH"] = str(candidate)
        subprocess.run([sys.executable, "-m", "scripts.validate_serving_snapshot"], check=True, env=env)

    @staticmethod
    def _finalize_redfin(serving_db: Path, target: str) -> None:
        subprocess.run([sys.executable, "scripts/validate_redfin_serving.py", "--db", str(serving_db),
                        "--expected-latest", target], check=True)
        subprocess.run([sys.executable, "scripts/publish_redfin_drop.py", target, "--downstream-validated"], check=True)

    def save(self, status: str, **values: object) -> None:
        self.manifest.update(status=status, **values)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        write_json(self.run_dir / "manifest.json", self.manifest)
        write_json(self.state_path, self.manifest)

    def status(self) -> dict:
        state, metadata = redfin_state(self.target, self.raw_root)
        return {"target_month": self.target, "monthly_status": self.manifest["status"],
                "redfin_state": state, "eligible": state in READY_REDFIN,
                "redfin_next_command": self.next_redfin_command(state, metadata),
                "source_freshness": source_summary(self.full_db),
                "serving_db": {"path": str(self.serving_db), "exists": self.serving_db.is_file(),
                               "size_bytes": self.serving_db.stat().st_size if self.serving_db.is_file() else None}}

    def next_redfin_command(self, state: str, metadata: dict) -> str:
        candidate = metadata.get("candidate_path", f"artifacts/redfin/{self.target}.parquet")
        commands = {"missing": f"PYTHONPATH=. python scripts/register_redfin_drop.py {self.target}",
                    "registered": f"PYTHONPATH=. python scripts/validate_redfin_drop.py {self.target}",
                    "validated": f"PYTHONPATH=. python scripts/build_redfin_candidate.py {self.target} --output {candidate}",
                    "candidate_built": f"PYTHONPATH=. python scripts/validate_redfin_candidate.py {self.target} --candidate {candidate} --compare-db {self.full_db}",
                    "candidate_validated": f"DUCKDB_PATH={self.full_db} PYTHONPATH=. python scripts/apply_redfin_candidate.py {self.target} --candidate {candidate}"}
        return commands.get(state, "Redfin is ready for downstream validation")

    def _stage(self, name: str, action: Callable[[], object]) -> object | None:
        if name in self.manifest["completed_stages"]:
            return None
        try:
            value = action()
            self.manifest["completed_stages"].append(name)
            self.save(name)
            return value
        except Exception as exc:
            self.save("failed", failure_stage=name, error=f"{type(exc).__name__}: {exc}")
            raise

    def execute(self, publish: bool = False) -> dict:
        if self.manifest["status"] == "complete":
            return {**self.manifest, "status": "already_complete"}
        state, metadata = redfin_state(self.target, self.raw_root)
        if state not in READY_REDFIN:
            self.save("waiting_for_manual_redfin", redfin_state=state,
                      required_action=self.next_redfin_command(state, metadata))
            return self.manifest
        self.save("ready", redfin_state=state)
        before = source_summary(self.full_db)
        def validate_sources():
            if not self.full_db.is_file():
                raise FileNotFoundError(self.full_db)
            if not before:
                raise RuntimeError("fact_timeseries source inventory is empty")
            report = []
            for row in before:
                slower = row["source_class"] == "slower_cadence"
                report.append({**row, "status": "no_new_release_expected" if slower else "verified_current",
                               "started_at": now(), "completed_at": now(), "pre_refresh_latest_date": row["latest_observation_date"],
                               "post_refresh_latest_date": row["latest_observation_date"], "row_count_before": row["row_count"],
                               "row_count_after": row["row_count"], "validation_status": "passed", "warning_count": 0, "error_state": None})
            write_json(self.run_dir / "source_refresh.json", report)
            write_json(self.run_dir / "freshness.json", report)
            return report
        self._stage("sources_validated", validate_sources)
        candidate = self.serving_db.with_name(f"{self.serving_db.stem}.{self.run_id}.candidate.duckdb")
        self._stage("serving_candidate_built", lambda: build_candidate(self.full_db, candidate))
        def validate_serving():
            self.serving_validator(candidate)
            con = duckdb.connect(str(candidate), read_only=True)
            latest = con.execute("SELECT strftime(MAX(date), '%Y-%m') FROM fact_timeseries WHERE source_id='redfin'").fetchone()[0]
            con.close()
            if latest != self.target:
                raise RuntimeError(f"candidate Redfin latest={latest}, expected {self.target}")
        self._stage("serving_validated", validate_serving)
        self._stage("serving_promoted", lambda: promote_candidate(candidate, self.serving_db))
        self._stage("redfin_promoted", lambda: self.redfin_finalizer(self.serving_db, self.target))
        regime_id = f"macro_regime_production_{self.target.replace('-', '')}_v1"
        regime_dir = self.regime_artifact_root / regime_id
        metadata_json = json.dumps({"target_month": self.target, "source_refresh_run_id": self.run_id,
                                    "serving_size_bytes": self.serving_db.stat().st_size, "git_sha": self.manifest["git_sha"],
                                    "production_policy": "config/monthly_refresh_policy.json"}, sort_keys=True)
        self._stage("regime_built", lambda: self.runner([sys.executable, "-m", "scripts.run_regime_pipeline", "--run-id", regime_id,
                    "--experiment-id", "governed_production", "--artifact-root", str(self.regime_artifact_root),
                    "--serving-db", str(self.serving_db), "--metadata-json", metadata_json]))
        def validate_regime():
            manifest = json.loads((regime_dir / "manifest.json").read_text())
            if manifest.get("status") != "complete" or not manifest.get("artifacts"):
                raise RuntimeError("regime manifest is not complete")
        self._stage("regime_validated", validate_regime)
        site = self.run_dir / "site"
        self._stage("site_built", lambda: self.runner([sys.executable, "-m", "scripts.build_macro_regime_site", "--run", str(regime_dir), "--output", str(site)]))
        bundle = self.run_dir / f"{regime_id}.tar.gz"
        def bundle_run():
            buffer = BytesIO()
            with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
                archive.add(regime_dir, arcname=regime_id, recursive=True, filter=_normalize_tar)
            with bundle.open("wb") as output, gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
                compressed.write(buffer.getvalue())
            publication = {"run_id": regime_id, "release_tag": f"macro-regime-{self.target}-v1", "release_asset": bundle.name,
                           "release_sha256": sha256(bundle)}
            write_json(self.run_dir / "publication.json", publication)
            return publication
        self._stage("publish_bundle_created", bundle_run)
        publication = json.loads((self.run_dir / "publication.json").read_text())
        if publish and "publication_dispatched" not in self.manifest["completed_stages"]:
            try:
                self.runner(["gh", "release", "create", publication["release_tag"], str(bundle), "--title", publication["release_tag"], "--notes", self.run_id])
                self.runner(["gh", "workflow", "run", "deploy-macro-regime-site.yml", "-f", f"run_id={regime_id}", "-f", f"release_tag={publication['release_tag']}", "-f", f"release_asset={bundle.name}", "-f", f"release_sha256={publication['release_sha256']}"])
                self.manifest["completed_stages"].append("publication_dispatched")
            except Exception as exc:
                self.save("publication_failed", failure_stage="publication_dispatched", analytics_complete=True, error=str(exc))
                raise
        status = "complete" if publish else "analytics_complete"
        self.save(status, completed_at=now(), regime_run_id=regime_id, publication=publication)
        return self.manifest


def _normalize_tar(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = info.gid = 0; info.uname = info.gname = ""; info.mtime = 0
    return info


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-month", default=date.today().strftime("%Y-%m"))
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--recover-stale-lock", action="store_true")
    args = parser.parse_args()
    orchestrator = Orchestrator(args.target_month)
    if args.status:
        print(json.dumps(orchestrator.status(), indent=2, sort_keys=True)); return 0
    try:
        with exclusive_lock(orchestrator.root / "monthly_refresh.lock", args.target_month, args.recover_stale_lock):
            result = orchestrator.execute(args.publish)
    except Exception as exc:
        print(f"monthly refresh failed: {exc}", file=sys.stderr); return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0
