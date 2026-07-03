from __future__ import annotations
# jobs/publish_serving_snapshot.py

import subprocess
import sys
from pathlib import Path

from jobs.common import print_context, run_module


SERVING_DB = Path("data/market_serving.duckdb")


def run(cmd: list[str]) -> None:
    print("[publish]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    print_context("publish_serving_snapshot")

    run_module("scripts.build_serving_snapshot")
    run_module("scripts.validate_serving_snapshot")

    if not SERVING_DB.exists():
        raise SystemExit(f"[publish][fatal] missing {SERVING_DB}")

    run(["git", "add", str(SERVING_DB)])

    status = subprocess.run(
        ["git", "diff", "--cached", "--quiet", "--", str(SERVING_DB)]
    )

    if status.returncode == 0:
        print("[publish] no serving snapshot changes to commit")
        return 0

    run(["git", "commit", "-m", "Update serving snapshot database"])
    run(["git", "push"])

    print("[publish] serving snapshot published")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
