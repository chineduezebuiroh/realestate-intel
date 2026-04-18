from __future__ import annotations
# jobs/common.py

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", str(REPO_ROOT))
    env.setdefault("DUCKDB_PATH", "data/market.duckdb")
    env.setdefault("ARTIFACT_ROOT", "artifacts/phasea")
    return env


def run_module(module: str) -> None:
    cmd = [sys.executable, "-m", module]
    print(f"[job] running module: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT, env=_env())


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def print_context(job_name: str) -> None:
    env = _env()
    print(f"[job] name={job_name}")
    print(f"[job] repo_root={REPO_ROOT}")
    print(f"[job] python={sys.executable}")
    print(f"[job] DUCKDB_PATH={env['DUCKDB_PATH']}")
    print(f"[job] ARTIFACT_ROOT={env['ARTIFACT_ROOT']}")
