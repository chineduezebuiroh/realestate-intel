from __future__ import annotations
# jobs/common.py

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _env(overrides: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)

    if "DUCKDB_PATH" not in env:
        env["DUCKDB_PATH"] = "data/market.duckdb"

    if "ARTIFACT_ROOT" not in env:
        env["ARTIFACT_ROOT"] = "artifacts/phasea"

    if overrides:
        env.update(overrides)

    return env


def run_module(module: str, env_overrides: dict[str, str] | None = None) -> None:
    cmd = [sys.executable, "-m", module]
    print(f"[job] running module: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT, env=_env(env_overrides))


def run_optional_module(module: str, env_overrides: dict[str, str] | None = None) -> None:
    cmd = [sys.executable, "-m", module]
    print(f"[job] running OPTIONAL module: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True, cwd=REPO_ROOT, env=_env(env_overrides))
        return True
    except subprocess.CalledProcessError as e:
        print(f"[job][warn] optional module failed: {module}")
        print(f"[job][warn] continuing anyway; error={e}")
        return False


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def print_context(name: str, env_overrides: dict[str, str] | None = None) -> None:
    env = _env(env_overrides)

    print(f"[job] name={name}")
    print(f"[job] repo_root={REPO_ROOT}")
    print(f"[job] python={sys.executable}")
    print(f"[job] DUCKDB_PATH={env.get('DUCKDB_PATH')}")
    print(f"[job] ARTIFACT_ROOT={env.get('ARTIFACT_ROOT')}")
    
